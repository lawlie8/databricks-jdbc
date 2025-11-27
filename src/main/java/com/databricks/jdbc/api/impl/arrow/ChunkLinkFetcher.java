package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ExternalLink;

/**
 * Abstraction for fetching chunk links from either SEA or Thrift backend. Implementations handle
 * the protocol-specific details of how links are retrieved.
 *
 * <p>This interface enables a unified streaming approach for chunk downloads regardless of the
 * underlying client type (SEA or Thrift).
 */
public interface ChunkLinkFetcher {

  /**
   * Fetches the next batch of chunk links starting from the given index.
   *
   * <p>The implementation may return one or more links in a single call. The returned {@link
   * ChunkLinkFetchResult} indicates whether more chunks are available.
   *
   * @param startChunkIndex The chunk index to start fetching from
   * @return ChunkLinkFetchResult containing the fetched links and continuation information
   * @throws DatabricksSQLException if the fetch operation fails
   */
  ChunkLinkFetchResult fetchLinks(long startChunkIndex) throws DatabricksSQLException;

  /**
   * Refetches a specific chunk link that may have expired.
   *
   * <p>This is used when a previously fetched link has expired before the chunk could be
   * downloaded. Both SEA and Thrift clients support this via the getResultChunks API.
   *
   * @param chunkIndex The specific chunk index to refetch
   * @return The refreshed ExternalLink with a new expiration time
   * @throws DatabricksSQLException if the refetch operation fails
   */
  ExternalLink refetchLink(long chunkIndex) throws DatabricksSQLException;

  /** Closes any resources held by the fetcher. */
  void close();
}
