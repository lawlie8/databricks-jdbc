package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
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
  public ChunkLinkFetchResult fetchLinks(long startChunkIndex) throws DatabricksSQLException {
    LOGGER.debug("Fetching links starting from chunk index {} for statement {}",
        startChunkIndex, statementId);

    Collection<ExternalLink> links = session.getDatabricksClient()
        .getResultChunks(statementId, startChunkIndex);

    if (links == null || links.isEmpty()) {
      LOGGER.debug("No links returned, end of stream reached for statement {}", statementId);
      return ChunkLinkFetchResult.endOfStream();
    }

    List<ChunkLinkFetchResult.ChunkLinkInfo> chunkLinks = new ArrayList<>();
    Long nextIndex = null;

    for (ExternalLink link : links) {
      chunkLinks.add(new ChunkLinkFetchResult.ChunkLinkInfo(
          link.getChunkIndex(),
          link,
          link.getRowCount() != null ? link.getRowCount() : 0,
          link.getRowOffset() != null ? link.getRowOffset() : 0
      ));

      // SEA uses nextChunkIndex to indicate continuation.
      // The LAST link's nextChunkIndex determines if there are more chunks.
      // null means no more chunks after this batch.
      nextIndex = link.getNextChunkIndex();
    }

    boolean hasMore = (nextIndex != null);

    LOGGER.debug("Fetched {} links for statement {}, hasMore={}, nextIndex={}",
        chunkLinks.size(), statementId, hasMore, nextIndex);

    return ChunkLinkFetchResult.of(chunkLinks, hasMore, hasMore ? nextIndex : -1);
  }

  @Override
  public ExternalLink refetchLink(long chunkIndex) throws DatabricksSQLException {
    LOGGER.info("Refetching expired link for chunk {} of statement {}", chunkIndex, statementId);

    Collection<ExternalLink> links = session.getDatabricksClient()
        .getResultChunks(statementId, chunkIndex);

    if (links == null || links.isEmpty()) {
      throw new DatabricksSQLException(
          String.format("Failed to refetch link for chunk %d: no links returned", chunkIndex), DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    // Find the link for the requested chunk index
    for (ExternalLink link : links) {
      if (link.getChunkIndex() != null && link.getChunkIndex() == chunkIndex) {
        LOGGER.debug("Successfully refetched link for chunk {} of statement {}",
            chunkIndex, statementId);
        return link;
      }
    }

    // If exact match not found, return the first link (server should return the requested chunk)
    ExternalLink firstLink = links.iterator().next();
    LOGGER.warn("Exact chunk index {} not found in response, using first link with index {} for statement {}",
        chunkIndex, firstLink.getChunkIndex(), statementId);
    return firstLink;
  }

  @Override
  public void close() {
    LOGGER.debug("Closing SeaChunkLinkFetcher for statement {}", statementId);
    // No resources to clean up for SEA fetcher
  }
}
