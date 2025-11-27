package com.databricks.jdbc.model.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Result of fetching chunk links from the server.
 *
 * <p>This class encapsulates both the external links and continuation information needed to support
 * both SEA and Thrift protocols:
 *
 * <ul>
 *   <li>SEA: Uses chunkIndex for continuation, hasMoreData derived from nextChunkIndex on last link
 *   <li>Thrift: Uses rowOffset for continuation, hasMoreData from server's hasMoreRows flag
 * </ul>
 */
public class GetChunksResult {

  private final Collection<ExternalLink> externalLinks;
  private final boolean hasMoreData;
  private final long nextRowOffset;

  private GetChunksResult(
      Collection<ExternalLink> externalLinks, boolean hasMoreData, long nextRowOffset) {
    this.externalLinks = externalLinks != null ? externalLinks : Collections.emptyList();
    this.hasMoreData = hasMoreData;
    this.nextRowOffset = nextRowOffset;
  }

  /**
   * Creates a result for SEA client responses.
   *
   * <p>For SEA, hasMoreData is derived from the last link's nextChunkIndex (null means no more
   * data). nextRowOffset is calculated from the last link's rowOffset + rowCount for unified
   * handling with Thrift.
   *
   * @param links The external links from the response
   * @return GetChunksResult with hasMoreData and nextRowOffset derived from links
   */
  public static GetChunksResult forSea(Collection<ExternalLink> links) {
    if (links == null || links.isEmpty()) {
      return new GetChunksResult(Collections.emptyList(), false, 0);
    }

    // Get last link efficiently
    List<ExternalLink> linkList =
        links instanceof List ? (List<ExternalLink>) links : new ArrayList<>(links);
    ExternalLink lastLink = linkList.get(linkList.size() - 1);

    boolean hasMore = lastLink.getNextChunkIndex() != null;
    long nextRowOffset = lastLink.getRowOffset() + lastLink.getRowCount();

    return new GetChunksResult(links, hasMore, nextRowOffset);
  }

  /**
   * Creates a result for Thrift client responses.
   *
   * <p>For Thrift, hasMoreData comes directly from the server's hasMoreRows flag.
   *
   * @param links The external links from the response (may be empty)
   * @param hasMoreRows The hasMoreRows flag from the Thrift response
   * @param nextRowOffset The row offset to use for the next fetch
   * @return GetChunksResult with Thrift continuation info
   */
  public static GetChunksResult forThrift(
      Collection<ExternalLink> links, boolean hasMoreRows, long nextRowOffset) {
    return new GetChunksResult(links, hasMoreRows, nextRowOffset);
  }

  /**
   * Creates an end-of-stream result (no more data available).
   *
   * @return GetChunksResult indicating end of stream
   */
  public static GetChunksResult endOfStream() {
    return new GetChunksResult(Collections.emptyList(), false, 0);
  }

  /**
   * @return The external links from the response, never null
   */
  public Collection<ExternalLink> getExternalLinks() {
    return externalLinks;
  }

  /**
   * @return true if there is more data available to fetch
   */
  public boolean hasMoreData() {
    return hasMoreData;
  }

  /**
   * @return The row offset to use for the next fetch. Populated for both SEA and Thrift.
   */
  public long getNextRowOffset() {
    return nextRowOffset;
  }

  /**
   * @return true if the result contains no links
   */
  public boolean isEmpty() {
    return externalLinks.isEmpty();
  }
}
