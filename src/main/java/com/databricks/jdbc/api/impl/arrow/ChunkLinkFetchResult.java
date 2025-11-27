package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.model.core.ExternalLink;
import java.util.Collections;
import java.util.List;

/**
 * Result of a chunk link fetch operation from {@link ChunkLinkFetcher}.
 *
 * <p>Contains the fetched chunk links and information about whether more chunks are available.
 */
public class ChunkLinkFetchResult {

  private final List<ChunkLinkInfo> chunkLinks;
  private final boolean hasMore;
  private final long nextFetchIndex;

  private ChunkLinkFetchResult(List<ChunkLinkInfo> chunkLinks, boolean hasMore, long nextFetchIndex) {
    this.chunkLinks = chunkLinks;
    this.hasMore = hasMore;
    this.nextFetchIndex = nextFetchIndex;
  }

  /**
   * Creates a result with the given links and continuation info.
   *
   * @param links The fetched chunk links
   * @param hasMore Whether more chunks are available
   * @param nextFetchIndex The next chunk index to fetch from, or -1 if no more
   * @return A new ChunkLinkFetchResult
   */
  public static ChunkLinkFetchResult of(List<ChunkLinkInfo> links, boolean hasMore, long nextFetchIndex) {
    return new ChunkLinkFetchResult(links, hasMore, nextFetchIndex);
  }

  /**
   * Creates a result indicating the end of the stream (no more chunks).
   *
   * @return A ChunkLinkFetchResult representing end of stream
   */
  public static ChunkLinkFetchResult endOfStream() {
    return new ChunkLinkFetchResult(Collections.emptyList(), false, -1);
  }

  /**
   * Returns the list of chunk links fetched in this batch.
   *
   * @return List of ChunkLinkInfo, may be empty
   */
  public List<ChunkLinkInfo> getChunkLinks() {
    return chunkLinks;
  }

  /**
   * Returns whether more chunks are available after this batch.
   *
   * @return true if more chunks can be fetched, false otherwise
   */
  public boolean hasMore() {
    return hasMore;
  }

  /**
   * Returns the next chunk index to fetch from.
   *
   * @return The next fetch index, or -1 if no more chunks
   */
  public long getNextFetchIndex() {
    return nextFetchIndex;
  }

  /**
   * Checks if this result represents the end of the chunk stream.
   *
   * @return true if no more chunks are available
   */
  public boolean isEndOfStream() {
    return !hasMore && chunkLinks.isEmpty();
  }

  /** Information about a single chunk link. */
  public static class ChunkLinkInfo {
    private final long chunkIndex;
    private final ExternalLink link;
    private final long rowCount;
    private final long rowOffset;

    public ChunkLinkInfo(long chunkIndex, ExternalLink link, long rowCount, long rowOffset) {
      this.chunkIndex = chunkIndex;
      this.link = link;
      this.rowCount = rowCount;
      this.rowOffset = rowOffset;
    }

    public long getChunkIndex() {
      return chunkIndex;
    }

    public ExternalLink getLink() {
      return link;
    }

    public long getRowCount() {
      return rowCount;
    }

    public long getRowOffset() {
      return rowOffset;
    }
  }
}
