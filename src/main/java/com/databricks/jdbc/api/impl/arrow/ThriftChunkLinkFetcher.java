package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.GetChunksResult;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ChunkLinkFetcher implementation for the Thrift client.
 *
 * <p>Thrift provides chunk links via the getResultChunks API, which returns links with
 * nextChunkIndex to indicate continuation. When nextChunkIndex is null, it indicates no more
 * chunks.
 */
public class ThriftChunkLinkFetcher implements ChunkLinkFetcher {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ThriftChunkLinkFetcher.class);

  private final IDatabricksSession session;
  private final StatementId statementId;

  public ThriftChunkLinkFetcher(IDatabricksSession session, StatementId statementId) {
    this.session = session;
    this.statementId = statementId;
    LOGGER.debug("Created ThriftChunkLinkFetcher for statement {}", statementId);
  }

  @Override
  public ChunkLinkFetchResult fetchLinks(long startChunkIndex, long startRowOffset)
      throws DatabricksSQLException {
    // Thrift uses startRowOffset with FETCH_ABSOLUTE; startChunkIndex is used for metadata
    LOGGER.debug(
        "Fetching links starting from chunk index {}, row offset {} for statement {}",
        startChunkIndex,
        startRowOffset,
        statementId);

    GetChunksResult result =
        session.getDatabricksClient().getResultChunks(statementId, startChunkIndex, startRowOffset);

    Collection<ExternalLink> links = result.getExternalLinks();
    boolean hasMore = result.hasMoreData();
    long nextRowOffset = result.getNextRowOffset();

    if (links == null || links.isEmpty()) {
      // For Thrift, hasMoreData() is the source of truth. Even with no links,
      // if hasMore is true, we should indicate continuation with the same offset.
      if (hasMore) {
        LOGGER.debug(
            "No links returned but hasMoreData=true for statement {}. "
                + "Returning empty result with hasMore=true for retry with offset {}",
            statementId,
            nextRowOffset);
        return ChunkLinkFetchResult.of(new ArrayList<>(), true, startChunkIndex, nextRowOffset);
      }
      LOGGER.debug("No links returned, end of stream reached for statement {}", statementId);
      return ChunkLinkFetchResult.endOfStream();
    }

    List<ChunkLinkFetchResult.ChunkLinkInfo> chunkLinks = new ArrayList<>();
    Long nextIndex = null;

    for (ExternalLink link : links) {
      chunkLinks.add(
          new ChunkLinkFetchResult.ChunkLinkInfo(
              link.getChunkIndex(), link, link.getRowCount(), link.getRowOffset()));

      // Track the last link's nextChunkIndex for logging/metadata
      nextIndex = link.getNextChunkIndex();
    }

    LOGGER.debug(
        "Fetched {} links for statement {}, hasMore={}, nextIndex={}, nextRowOffset={}",
        chunkLinks.size(),
        statementId,
        hasMore,
        nextIndex,
        nextRowOffset);

    // For Thrift, hasMore comes from GetChunksResult.hasMoreData() (the server's hasMoreRows flag)
    return ChunkLinkFetchResult.of(chunkLinks, hasMore, hasMore ? nextIndex : -1, nextRowOffset);
  }

  @Override
  public ExternalLink refetchLink(long chunkIndex, long rowOffset) throws DatabricksSQLException {
    // Thrift uses rowOffset with FETCH_ABSOLUTE
    LOGGER.info(
        "Refetching expired link for chunk {}, row offset {} of statement {}",
        chunkIndex,
        rowOffset,
        statementId);

    // For Thrift, we may need to retry if hasMoreData=true but no links returned yet
    int maxRetries = 100; // Reasonable limit to prevent infinite loops
    int retryCount = 0;

    while (retryCount < maxRetries) {
      GetChunksResult result =
          session.getDatabricksClient().getResultChunks(statementId, chunkIndex, rowOffset);
      Collection<ExternalLink> links = result.getExternalLinks();

      if (links != null && !links.isEmpty()) {
        // Find the link for the requested chunk index
        for (ExternalLink link : links) {
          if (link.getChunkIndex() != null && link.getChunkIndex() == chunkIndex) {
            LOGGER.debug(
                "Successfully refetched link for chunk {} of statement {}",
                chunkIndex,
                statementId);
            return link;
          }
        }

        // Exact match not found - this indicates a server bug
        throw new DatabricksSQLException(
            String.format(
                "Failed to refetch link for chunk %d: server returned links but none matched requested index",
                chunkIndex),
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      }

      // No links returned - check if we should retry
      if (!result.hasMoreData()) {
        // No more data and no links - this is unexpected for a refetch
        throw new DatabricksSQLException(
            String.format(
                "Failed to refetch link for chunk %d: no links returned and hasMoreData=false",
                chunkIndex),
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      }

      // hasMoreData=true but no links yet - retry
      retryCount++;
      LOGGER.debug(
          "No links returned for chunk {} but hasMoreData=true, retrying ({}/{})",
          chunkIndex,
          retryCount,
          maxRetries);
    }

    throw new DatabricksSQLException(
        String.format(
            "Failed to refetch link for chunk %d: max retries (%d) exceeded",
            chunkIndex, maxRetries),
        DatabricksDriverErrorCode.CHUNK_READY_ERROR);
  }

  @Override
  public void close() {
    LOGGER.debug("Closing ThriftChunkLinkFetcher for statement {}", statementId);
    // No resources to clean up for Thrift fetcher
  }
}
