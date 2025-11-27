package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;

/**
 * A streaming chunk provider that fetches chunk links proactively and downloads chunks in parallel.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>No dependency on total chunk count - streams until end of data
 *   <li>Proactive link prefetching with configurable window
 *   <li>Memory-bounded parallel downloads
 *   <li>Automatic link refresh on expiration
 * </ul>
 *
 * <p>This provider uses two key windows:
 *
 * <ul>
 *   <li>Link prefetch window: How many links to fetch ahead of consumption
 *   <li>Download window: How many chunks to keep in memory (downloading or ready)
 * </ul>
 */
public class StreamingChunkProvider implements ChunkProvider {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(StreamingChunkProvider.class);
  private static final String DOWNLOAD_THREAD_PREFIX = "databricks-jdbc-streaming-downloader-";
  private static final String PREFETCH_THREAD_NAME = "databricks-jdbc-link-prefetcher";

  // Configuration
  private final int linkPrefetchWindow;
  private final int maxChunksInMemory;
  private final int chunkReadyTimeoutSeconds;

  // Dependencies
  private final ChunkLinkFetcher linkFetcher;
  private final IDatabricksHttpClient httpClient;
  private final CompressionCodec compressionCodec;
  private final StatementId statementId;
  private final double cloudFetchSpeedThreshold;

  // Chunk storage
  private final ConcurrentMap<Long, ArrowResultChunk> chunks = new ConcurrentHashMap<>();

  // Position tracking
  private volatile long currentChunkIndex = -1;
  private volatile long highestKnownChunkIndex = -1;
  private volatile long nextLinkFetchIndex = 0;
  private volatile long nextRowOffsetToFetch = 0;
  private volatile long nextDownloadIndex = 0;

  // State flags
  private volatile boolean endOfStreamReached = false;
  private volatile boolean closed = false;

  // Row tracking
  private final AtomicLong totalRowCount = new AtomicLong(0);

  // Synchronization for prefetch thread
  private final ReentrantLock prefetchLock = new ReentrantLock();
  private final Condition consumerAdvanced = prefetchLock.newCondition();
  private final Condition chunkCreated = prefetchLock.newCondition();

  // Synchronization for download coordination
  private final ReentrantLock downloadLock = new ReentrantLock();

  // Executors
  private final ExecutorService downloadExecutor;
  private final Thread linkPrefetchThread;

  // Track chunks currently in memory (for sliding window)
  private final AtomicInteger chunksInMemory = new AtomicInteger(0);

  /**
   * Creates a new StreamingChunkProvider.
   *
   * @param linkFetcher Fetcher for chunk links
   * @param httpClient HTTP client for downloads
   * @param compressionCodec Codec for decompressing chunk data
   * @param statementId Statement ID for logging and chunk creation
   * @param maxChunksInMemory Maximum chunks to keep in memory (download window)
   * @param linkPrefetchWindow How many links to fetch ahead
   * @param chunkReadyTimeoutSeconds Timeout waiting for chunk to be ready
   * @param cloudFetchSpeedThreshold Speed threshold for logging warnings
   * @param initialLinks Initial links provided with result data (avoids extra fetch), may be null
   */
  public StreamingChunkProvider(
      ChunkLinkFetcher linkFetcher,
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      StatementId statementId,
      int maxChunksInMemory,
      int linkPrefetchWindow,
      int chunkReadyTimeoutSeconds,
      double cloudFetchSpeedThreshold,
      ChunkLinkFetchResult initialLinks)
      throws DatabricksParsingException {

    this.linkFetcher = linkFetcher;
    this.httpClient = httpClient;
    this.compressionCodec = compressionCodec;
    this.statementId = statementId;
    this.maxChunksInMemory = maxChunksInMemory;
    this.linkPrefetchWindow = linkPrefetchWindow;
    this.chunkReadyTimeoutSeconds = chunkReadyTimeoutSeconds;
    this.cloudFetchSpeedThreshold = cloudFetchSpeedThreshold;

    LOGGER.info(
        "Creating StreamingChunkProvider for statement {}: maxChunksInMemory={}, linkPrefetchWindow={}",
        statementId,
        maxChunksInMemory,
        linkPrefetchWindow);

    // Process initial links if provided
    processInitialLinks(initialLinks);

    // Create download executor
    this.downloadExecutor = createDownloadExecutor(maxChunksInMemory);

    // Start link prefetch thread
    this.linkPrefetchThread = new Thread(this::linkPrefetchLoop, PREFETCH_THREAD_NAME);
    this.linkPrefetchThread.setDaemon(true);
    this.linkPrefetchThread.start();

    // Trigger initial downloads and prefetch
    triggerDownloads();
    notifyConsumerAdvanced();
  }

  /** Convenience constructor with default prefetch window. */
  public StreamingChunkProvider(
      ChunkLinkFetcher linkFetcher,
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      StatementId statementId,
      int maxChunksInMemory,
      int chunkReadyTimeoutSeconds,
      double cloudFetchSpeedThreshold,
      ChunkLinkFetchResult initialLinks)
      throws DatabricksParsingException {
    this(
        linkFetcher,
        httpClient,
        compressionCodec,
        statementId,
        maxChunksInMemory,
        64, // Default prefetch window
        chunkReadyTimeoutSeconds,
        cloudFetchSpeedThreshold,
        initialLinks);
  }

  // ==================== ChunkProvider Interface ====================

  @Override
  public boolean hasNextChunk() {
    if (closed) {
      return false;
    }

    // If we haven't reached end of stream, there might be more
    if (!endOfStreamReached) {
      return true;
    }

    // We've reached end of stream - check if there are unconsumed chunks
    return currentChunkIndex < highestKnownChunkIndex;
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (closed) {
      return false;
    }

    // Release previous chunk if any
    if (currentChunkIndex >= 0) {
      releaseChunk(currentChunkIndex);
    }

    if (!hasNextChunk()) {
      return false;
    }

    currentChunkIndex++;

    // Notify prefetch thread that consumer advanced
    notifyConsumerAdvanced();

    return true;
  }

  @Override
  public AbstractArrowResultChunk getChunk() throws DatabricksSQLException {
    if (currentChunkIndex < 0) {
      return null;
    }

    ArrowResultChunk chunk = chunks.get(currentChunkIndex);

    if (chunk == null) {
      // Chunk not yet created - wait for it
      LOGGER.debug("Chunk {} not yet available, waiting for prefetch", currentChunkIndex);
      waitForChunkCreation(currentChunkIndex);
      chunk = chunks.get(currentChunkIndex);
    }

    if (chunk == null) {
      throw new DatabricksSQLException(
          "Chunk " + currentChunkIndex + " not found after waiting",
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    // Wait for chunk to be ready (downloaded and processed)
    try {
      chunk.waitForChunkReady();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabricksSQLException(
          "Interrupted waiting for chunk " + currentChunkIndex,
          e,
          DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
    } catch (ExecutionException e) {
      throw new DatabricksSQLException(
          "Failed to prepare chunk " + currentChunkIndex,
          e.getCause(),
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    } catch (TimeoutException e) {
      throw new DatabricksSQLException(
          "Timeout waiting for chunk "
              + currentChunkIndex
              + " (timeout: "
              + chunkReadyTimeoutSeconds
              + "s)",
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    return chunk;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    LOGGER.info("Closing StreamingChunkProvider for statement {}", statementId);
    closed = true;

    // Wake up any waiting threads so they can exit
    notifyConsumerAdvanced();
    notifyChunkCreated();

    // Interrupt prefetch thread
    if (linkPrefetchThread != null) {
      linkPrefetchThread.interrupt();
    }

    // Shutdown download executor
    if (downloadExecutor != null) {
      downloadExecutor.shutdownNow();
    }

    // Release all chunks
    for (ArrowResultChunk chunk : chunks.values()) {
      try {
        chunk.releaseChunk();
      } catch (Exception e) {
        LOGGER.warn("Error releasing chunk: {}", e.getMessage());
      }
    }
    chunks.clear();

    // Close link fetcher
    if (linkFetcher != null) {
      linkFetcher.close();
    }
  }

  @Override
  public long getRowCount() {
    return totalRowCount.get();
  }

  @Override
  public long getChunkCount() {
    // In streaming mode, we don't know total chunks until end of stream
    if (endOfStreamReached) {
      return highestKnownChunkIndex + 1;
    }
    return -1; // Unknown
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  // ==================== Link Prefetch Logic ====================

  private void linkPrefetchLoop() {
    LOGGER.debug("Link prefetch thread started for statement {}", statementId);

    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        prefetchLock.lock();
        try {
          // Calculate target prefetch index
          long targetIndex = currentChunkIndex + linkPrefetchWindow;

          // Wait if we're caught up
          while (!closed && !endOfStreamReached && nextLinkFetchIndex > targetIndex) {
            LOGGER.debug(
                "Prefetch caught up, waiting for consumer. next={}, target={}",
                nextLinkFetchIndex,
                targetIndex);
            consumerAdvanced.await();
            targetIndex = currentChunkIndex + linkPrefetchWindow;
          }
        } finally {
          prefetchLock.unlock();
        }

        if (closed || endOfStreamReached) {
          break;
        }

        // Fetch next batch of links
        fetchNextLinkBatch();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.debug("Link prefetch thread interrupted");
        break;
      } catch (DatabricksSQLException e) {
        LOGGER.error("Error fetching links: {}", e.getMessage());
        // Continue trying - next iteration may succeed
      }
    }

    LOGGER.debug("Link prefetch thread exiting for statement {}", statementId);
  }

  private void fetchNextLinkBatch() throws DatabricksSQLException {
    if (endOfStreamReached || closed) {
      return;
    }

    LOGGER.debug(
        "Fetching links starting from index {}, row offset {} for statement {}",
        nextLinkFetchIndex,
        nextRowOffsetToFetch,
        statementId);

    ChunkLinkFetchResult result = linkFetcher.fetchLinks(nextLinkFetchIndex, nextRowOffsetToFetch);

    if (result.isEndOfStream()) {
      LOGGER.info("End of stream reached for statement {}", statementId);
      endOfStreamReached = true;
      return;
    }

    // Process received links - create chunks
    for (ChunkLinkFetchResult.ChunkLinkInfo linkInfo : result.getChunkLinks()) {
      createChunkFromLink(linkInfo);
    }

    // Update next fetch positions
    if (result.hasMore()) {
      nextLinkFetchIndex = result.getNextFetchIndex();
      nextRowOffsetToFetch = result.getNextRowOffset();
    } else {
      endOfStreamReached = true;
      LOGGER.info("End of stream reached for statement {} (hasMore=false)", statementId);
    }

    // Trigger downloads for new chunks
    triggerDownloads();
  }

  /**
   * Processes initial links provided with the result data. This avoids an extra fetch call for
   * links the server already provided.
   *
   * @param initialLinks The initial links from ResultData, may be null
   */
  private void processInitialLinks(ChunkLinkFetchResult initialLinks)
      throws DatabricksParsingException {
    if (initialLinks == null || initialLinks.isEndOfStream()) {
      LOGGER.debug("No initial links provided for statement {}", statementId);
      return;
    }

    LOGGER.info(
        "Processing {} initial links for statement {}",
        initialLinks.getChunkLinks().size(),
        statementId);

    for (ChunkLinkFetchResult.ChunkLinkInfo linkInfo : initialLinks.getChunkLinks()) {
      createChunkFromLink(linkInfo);
    }

    // Set next fetch positions using unified API
    if (initialLinks.hasMore()) {
      nextLinkFetchIndex = initialLinks.getNextFetchIndex();
      nextRowOffsetToFetch = initialLinks.getNextRowOffset();
      LOGGER.debug(
          "Next fetch position set to chunk index {}, row offset {} from initial links",
          nextLinkFetchIndex,
          nextRowOffsetToFetch);
    } else {
      endOfStreamReached = true;
      LOGGER.info("End of stream reached from initial links for statement {}", statementId);
    }
  }

  /**
   * Creates a chunk from link info and registers it for download.
   *
   * @param linkInfo The chunk link info containing index, row count, offset, and link
   */
  private void createChunkFromLink(ChunkLinkFetchResult.ChunkLinkInfo linkInfo)
      throws DatabricksParsingException {
    long chunkIndex = linkInfo.getChunkIndex();
    if (chunks.containsKey(chunkIndex)) {
      LOGGER.debug("Chunk {} already exists, skipping creation", chunkIndex);
      return;
    }

    ArrowResultChunk chunk =
        ArrowResultChunk.builder()
            .withStatementId(statementId)
            .withChunkMetadata(chunkIndex, linkInfo.getRowCount(), linkInfo.getRowOffset())
            .withChunkReadyTimeoutSeconds(chunkReadyTimeoutSeconds)
            .build();

    chunk.setChunkLink(linkInfo.getLink());
    chunks.put(chunkIndex, chunk);
    highestKnownChunkIndex = Math.max(highestKnownChunkIndex, chunkIndex);
    totalRowCount.addAndGet(linkInfo.getRowCount());

    // Notify any waiting consumers that a chunk is available
    notifyChunkCreated();

    LOGGER.debug(
        "Created chunk {} with {} rows for statement {}",
        chunkIndex,
        linkInfo.getRowCount(),
        statementId);
  }

  // ==================== Download Coordination ====================

  private void triggerDownloads() {
    downloadLock.lock();
    try {
      while (!closed
          && chunksInMemory.get() < maxChunksInMemory
          && nextDownloadIndex <= highestKnownChunkIndex) {
        ArrowResultChunk chunk = chunks.get(nextDownloadIndex);

        if (chunk == null) {
          // Chunk not yet created, wait for prefetch
          break;
        }

        // Only submit if not already downloading/downloaded
        ChunkStatus status = chunk.getStatus();
        if (status == ChunkStatus.PENDING || status == ChunkStatus.URL_FETCHED) {
          submitDownloadTask(chunk);
          chunksInMemory.incrementAndGet();
        }

        nextDownloadIndex++;
      }
    } finally {
      downloadLock.unlock();
    }
  }

  private void submitDownloadTask(ArrowResultChunk chunk) {
    LOGGER.debug("Submitting download task for chunk {}", chunk.getChunkIndex());

    StreamingChunkDownloadTask task =
        new StreamingChunkDownloadTask(
            chunk, httpClient, compressionCodec, linkFetcher, cloudFetchSpeedThreshold);

    downloadExecutor.submit(task);
  }

  // ==================== Resource Management ====================

  private void releaseChunk(long chunkIndex) {
    ArrowResultChunk chunk = chunks.get(chunkIndex);
    if (chunk != null && chunk.releaseChunk()) {
      chunks.remove(chunkIndex);
      chunksInMemory.decrementAndGet();

      LOGGER.debug("Released chunk {}, chunksInMemory={}", chunkIndex, chunksInMemory.get());

      // Trigger more downloads to fill the freed slot
      triggerDownloads();
    }
  }

  private void waitForChunkCreation(long chunkIndex) throws DatabricksSQLException {
    long remainingNanos = 5 * 1_000_000_000L;

    prefetchLock.lock();
    try {
      while (!closed && !chunks.containsKey(chunkIndex)) {
        if (endOfStreamReached && chunkIndex > highestKnownChunkIndex) {
          throw new DatabricksSQLException(
              "Chunk "
                  + chunkIndex
                  + " does not exist (highest known: "
                  + highestKnownChunkIndex
                  + ")",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        if (remainingNanos <= 0) {
          throw new DatabricksSQLException(
              "Timeout waiting for chunk " + chunkIndex + " to be created",
              DatabricksDriverErrorCode.CHUNK_READY_ERROR);
        }

        try {
          remainingNanos = chunkCreated.awaitNanos(remainingNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabricksSQLException(
              "Interrupted waiting for chunk creation",
              e,
              DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
        }
      }
    } finally {
      prefetchLock.unlock();
    }
  }

  // ==================== Synchronization Helpers ====================

  private void notifyConsumerAdvanced() {
    prefetchLock.lock();
    try {
      consumerAdvanced.signalAll();
    } finally {
      prefetchLock.unlock();
    }
  }

  private void notifyChunkCreated() {
    prefetchLock.lock();
    try {
      chunkCreated.signalAll();
    } finally {
      prefetchLock.unlock();
    }
  }

  // ==================== Executor Creation ====================

  private ExecutorService createDownloadExecutor(int poolSize) {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private final AtomicInteger threadCount = new AtomicInteger(1);

          @Override
          public Thread newThread(@Nonnull Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(DOWNLOAD_THREAD_PREFIX + threadCount.getAndIncrement());
            thread.setDaemon(true);
            return thread;
          }
        };

    return Executors.newFixedThreadPool(poolSize, threadFactory);
  }
}
