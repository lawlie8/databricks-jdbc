package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.api.impl.arrow.ArrowResultChunk.SECONDS_BUFFER_FOR_EXPIRY;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A service that manages the downloading and refreshing of external links for chunked data
 * retrieval.
 *
 * <p>This service maintains a CompletableFuture for each chunk's external link.
 *
 * <h3>Key Features:</h3>
 *
 * <h4>1. Download Pipeline:</h4>
 *
 * <ul>
 *   <li>Automatically initiates a download chain when using SQL Execution API
 *   <li>Fetches links in batches, starting from a specified chunk index
 *   <li>Processes batches serially, with each new request starting from (last fetched index + 1)
 *   <li>Completes the corresponding futures as soon as links are received
 * </ul>
 *
 * <h4>2. Link Expiration Handling:</h4>
 *
 * <ul>
 *   <li>Monitors link expiration when chunks request their download links
 *   <li>When an expired link is detected (and its chunk hasn't been downloaded):
 *       <ul>
 *         <li>Finds the earliest chunk index with an expired link
 *         <li>Restarts the download chain from this index
 *       </ul>
 * </ul>
 *
 * <h4>3. Correctness Guarantee:</h4>
 *
 * <p>The service maintains correctness through two mechanisms:
 *
 * <ul>
 *   <li>Monotonically increasing request indexes
 *   <li>Server's guarantee of returning continuous series of chunk links
 * </ul>
 *
 * <p>This design ensures that no chunks are missed and links remain valid during the download
 * process.
 *
 * @param <T> The specific type of {@link AbstractArrowResultChunk} this service manages
 */
public class ChunkLinkDownloadService<T extends AbstractArrowResultChunk> {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ChunkLinkDownloadService.class);

  private final IDatabricksSession session;
  private final StatementId statementId;
  private final AtomicLong nextBatchStartIndex;
  private final AtomicBoolean isDownloadInProgress;
  private final int maxLinksThreshold;
  private volatile boolean isShutdown;
  private volatile CompletableFuture<Void> currentDownloadTask;

  /** An ordered queue of chunk links. */
  private final BlockingQueue<ExternalLink> chunkLinks = new LinkedBlockingQueue<>();

  /** The total number of chunks. */
  private final AtomicLong totalChunks = new AtomicLong(-1);

  /** The next chunk link index to be consumed */
  private final AtomicLong totalChunksInMemory = new AtomicLong(-1);

  /** Download error if any */
  private final AtomicReference<DatabricksSQLException> downloadException = new AtomicReference<>();

  public ChunkLinkDownloadService(
      IDatabricksSession session,
      StatementId statementId,
      long totalChunks,
      ConcurrentMap<Long, T> chunkIndexToChunksMap) {
    LOGGER.info(
        "Initializing ChunkLinkDownloadService for statement {} with total chunks: {}, starting at index: {}",
        statementId,
        totalChunks);

    this.session = session;
    this.statementId = statementId;
    this.isDownloadInProgress = new AtomicBoolean(false);
    this.isShutdown = false;
    this.totalChunks.set(totalChunks);

    // TODO is this correct, can there be another value?
    this.maxLinksThreshold = session.getConnectionContext().getCloudFetchThreadPoolSize() * 2;

    chunkIndexToChunksMap.entrySet().stream()
        .filter(e -> e.getValue().chunkLink != null)
        .sorted(Map.Entry.comparingByKey())
        .forEach(e -> chunkLinks.add(e.getValue().chunkLink));

    // TODO check correctness.
    long nextBatchStartIndex =
        chunkIndexToChunksMap.entrySet().stream().max(Map.Entry.comparingByKey()).get().getKey()
            + 1;
    this.nextBatchStartIndex = new AtomicLong(nextBatchStartIndex);

    // TODO can there be gaps in the links.

    triggerNextBatchDownload();
  }

  /** Shuts down the service and cancels all pending operations. */
  public void shutdown() {
    LOGGER.info("Shutting down ChunkLinkDownloadService for statement {}", statementId);
    isShutdown = true;

    CompletableFuture<Void> currentDownloadTask = this.currentDownloadTask;
    if (currentDownloadTask != null) {
      currentDownloadTask.cancel(true);
    }
  }

  /**
   * Initiates the download of the next batch of chunk links.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>Checks if a download is already in progress
   *   <li>Validates if there are more chunks to download
   *   <li>Makes an async request to fetch the next batch of links
   *   <li>Updates futures with received links
   *   <li>Triggers the next batch download if more chunks remain
   * </ul>
   *
   * <p>If an error occurs during download, all pending futures are completed exceptionally.
   */
  private void triggerNextBatchDownload() {
    if (isShutdown || !isDownloadInProgress.compareAndSet(false, true)) {
      LOGGER.debug(
          "Skipping batch download - Service shutdown: {}, Download in progress: {}",
          isShutdown,
          isDownloadInProgress.get());
      return;
    }

    final long batchStartIndex = nextBatchStartIndex.get();
    LOGGER.info("Starting batch download from index {}", batchStartIndex);

    currentDownloadTask =
        CompletableFuture.runAsync(
            () -> {
              try {
                fetchNextChunkLinks(batchStartIndex);

                // Mark current download as complete and trigger next batch
                isDownloadInProgress.set(false);

                if (chunkLinks.size() > maxLinksThreshold) {
                  return;
                }

                // FIXME.
                if (totalChunks.get() == -1 || nextBatchStartIndex.get() < totalChunks.get()) {
                  LOGGER.debug("Triggering next batch download");
                  triggerNextBatchDownload();
                }
              } catch (DatabricksSQLException e) {
                // If the download fails, complete exceptionally all pending futures
                handleBatchDownloadError(batchStartIndex, e);
              }
            });
  }

  private void fetchNextChunkLinks(long chunkIndex) throws DatabricksSQLException {
    // TODO does a lot of things, reduce the number of responsibilities of this
    //  function.

    Collection<ExternalLink> links =
        session.getDatabricksClient().getResultChunks(statementId, chunkIndex);
    LOGGER.info(
        "Retrieved {} links for batch starting at {} for statement id {}",
        links.size(),
        chunkIndex,
        statementId);

    chunkLinks.addAll(links);

    // TODO better way to do this?
    if (!links.isEmpty()) {
      ExternalLink lastChunk =
          links.stream()
              .max(Comparator.comparingLong(ExternalLink::getChunkIndex))
              .get(); // TODO should fix this?

      // TODO should not be -1.
      if (lastChunk.getNextChunkIndex() == -1) {
        totalChunks.set(lastChunk.getChunkIndex() + 1);
      }

      nextBatchStartIndex.set(lastChunk.getChunkIndex() + 1);
      LOGGER.debug("Updated next batch start index to {}", lastChunk.getChunkIndex() + 1);
    }
  }

  /**
   * Handles errors that occur during batch download.
   *
   * <p>Completes all pending futures exceptionally with the encountered error and resets the
   * download progress flag.
   */
  private void handleBatchDownloadError(long batchStartIndex, DatabricksSQLException e) {
    LOGGER.error(
        e,
        "Failed to download links for batch starting at {} : {}",
        batchStartIndex,
        e.getMessage());

    // Complete exceptionally all pending futures
    downloadException.set(e);

    isDownloadInProgress.set(false);
  }

  /**
   * Creates a CompletableFuture that is already completed exceptionally with the given exception.
   */
  private CompletableFuture<ExternalLink> createExceptionalFuture(Exception e) {
    CompletableFuture<ExternalLink> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  private boolean isChunkLinkExpired(ExternalLink link) {
    if (link == null || link.getExpiration() == null) {
      LOGGER.warn("Link or expiration is null, assuming link is expired");
      return true;
    }
    Instant expirationWithBuffer =
        Instant.parse(link.getExpiration()).minusSeconds(SECONDS_BUFFER_FOR_EXPIRY);

    return expirationWithBuffer.isBefore(Instant.now());
  }

  public ExternalLink nextChunkLink() throws DatabricksSQLException {
    if (isShutdown) {
      LOGGER.warn("Attempt to get next link while chunk download service is shutdown");
      throw new DatabricksValidationException("Chunk Link Download Service is shutdown");
    }

    if (downloadException.get() != null) {
      throw downloadException.get();
    }

    try {
      // TODO blocking indefinitely here?
      ExternalLink link = chunkLinks.take();
      if (isChunkLinkExpired(link)) {
        // TODO trigger download of chunks.

        // Clear the queue of links. All links will have expired.
        chunkLinks.clear();

        // Download next set of links.
        // FIXME can this run concurrently with triggerNextBatchDownload?
        fetchNextChunkLinks(link.getChunkIndex());
      }

      // Trigger the next set of downloadNextLinks.
      // TODO is this correct?
      triggerNextBatchDownload();

      // TODO blocking indefinitely.
      return chunkLinks.take();
    } catch (InterruptedException e) {
      // TODO handle thread cancellation.
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (DatabricksSQLException e) {
      // TODO handle the exception.
      throw new RuntimeException(e);
    }
  }

  public boolean hasNextLink() {
    // TODO check logic.
    return totalChunks.get() == -1
        || !chunkLinks.isEmpty()
        || nextBatchStartIndex.get() < totalChunks.get();
  }
}
