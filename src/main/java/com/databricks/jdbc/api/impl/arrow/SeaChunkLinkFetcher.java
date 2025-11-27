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
 * ChunkLinkFetcher implementation for the SQL Execution API (SEA) client.
 *
 * <p>SEA provides chunk links via the getResultChunks API, which returns links with nextChunkIndex
 * to indicate continuation. When nextChunkIndex is null, it indicates no more chunks.
 */
public class SeaChunkLinkFetcher implements ChunkLinkFetcher {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(SeaChunkLinkFetcher.class);

  private final IDatabricksSession session;
  private final StatementId statementId;

  public SeaChunkLinkFetcher(IDatabricksSession session, StatementId statementId) {
    this.session = session;
    this.statementId = statementId;
    LOGGER.debug("Created SeaChunkLinkFetcher for statement {}", statementId);
  }

  @Override
  public ChunkLinkFetchResult fetchLinks(long startChunkIndex, long startRowOffset)
      throws DatabricksSQLException {
    // SEA uses startChunkIndex; startRowOffset is ignored
    LOGGER.debug(
        "Fetching links starting from chunk index {} for statement {}",
        startChunkIndex,
        statementId);

    GetChunksResult result =
        session.getDatabricksClient().getResultChunks(statementId, startChunkIndex, startRowOffset);
    Collection<ExternalLink> links = result.getExternalLinks();

    if (links == null || links.isEmpty()) {
      LOGGER.debug("No links returned, end of stream reached for statement {}", statementId);
      return ChunkLinkFetchResult.endOfStream();
    }

    List<ChunkLinkFetchResult.ChunkLinkInfo> chunkLinks = new ArrayList<>();
    Long nextIndex = null;

    for (ExternalLink link : links) {
      chunkLinks.add(
          new ChunkLinkFetchResult.ChunkLinkInfo(
              link.getChunkIndex(), link, link.getRowCount(), link.getRowOffset()));

      // Track last link's nextChunkIndex for logging
      nextIndex = link.getNextChunkIndex();
    }

    // Use hasMoreData and nextRowOffset from GetChunksResult (unified with Thrift)
    boolean hasMore = result.hasMoreData();
    long nextRowOffset = result.getNextRowOffset();

    LOGGER.debug(
        "Fetched {} links for statement {}, hasMore={}, nextIndex={}, nextRowOffset={}",
        chunkLinks.size(),
        statementId,
        hasMore,
        nextIndex,
        nextRowOffset);

    return ChunkLinkFetchResult.of(chunkLinks, hasMore, hasMore ? nextIndex : -1, nextRowOffset);
  }

  @Override
  public ExternalLink refetchLink(long chunkIndex, long rowOffset) throws DatabricksSQLException {
    // SEA uses chunkIndex; rowOffset is ignored
    LOGGER.info("Refetching expired link for chunk {} of statement {}", chunkIndex, statementId);

    GetChunksResult result =
        session.getDatabricksClient().getResultChunks(statementId, chunkIndex, rowOffset);
    Collection<ExternalLink> links = result.getExternalLinks();

    if (links == null || links.isEmpty()) {
      throw new DatabricksSQLException(
          String.format("Failed to refetch link for chunk %d: no links returned", chunkIndex),
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    // Find the link for the requested chunk index
    for (ExternalLink link : links) {
      if (link.getChunkIndex() != null && link.getChunkIndex() == chunkIndex) {
        LOGGER.debug(
            "Successfully refetched link for chunk {} of statement {}", chunkIndex, statementId);
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

  @Override
  public void close() {
    LOGGER.debug("Closing SeaChunkLinkFetcher for statement {}", statementId);
    // No resources to clean up for SEA fetcher
  }
}
