package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getColumnInfoFromTColumnDesc;

import com.databricks.jdbc.api.impl.ComplexDataTypeParser;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TColumnDesc;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Result container for Arrow-based query results. */
public class ArrowStreamResult implements IExecutionResult {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowStreamResult.class);
  private final ChunkProvider chunkProvider;
  private long currentRowIndex = -1;
  private boolean isClosed;
  private ArrowResultChunkIterator chunkIterator;
  private List<ColumnInfo> columnInfos;
  private final IDatabricksSession session;

  public ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this(
        resultManifest,
        resultData,
        statementId,
        session,
        DatabricksHttpClientFactory.getInstance().getClient(session.getConnectionContext()));
  }

  @VisibleForTesting
  ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {
    this.session = session;
    // Check if the result data contains the arrow data inline
    boolean isInlineArrow = resultData.getAttachment() != null;
    if (isInlineArrow) {
      LOGGER.debug(
          "Creating ArrowStreamResult with inline attachment for statementId: {}",
          statementId.toSQLExecStatementId());
      this.chunkProvider = new InlineChunkProvider(resultData, resultManifest);
    } else {
      LOGGER.debug(
          "Creating ArrowStreamResult with remote links for statementId: {}",
          statementId.toSQLExecStatementId());
      this.chunkProvider =
          createRemoteChunkProvider(statementId, resultManifest, resultData, session, httpClient);
    }
    this.columnInfos =
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns());
  }

  /**
   * Creates the appropriate remote chunk provider based on configuration.
   *
   * @param statementId The statement ID
   * @param resultManifest The result manifest containing chunk metadata
   * @param resultData The result data containing initial external links
   * @param session The session for fetching additional chunks
   * @param httpClient The HTTP client for downloading chunk data
   * @return A ChunkProvider instance
   */
  private static ChunkProvider createRemoteChunkProvider(
      StatementId statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {

    IDatabricksConnectionContext connectionContext = session.getConnectionContext();

    if (connectionContext.isStreamingChunkProviderEnabled()) {
      LOGGER.info(
          "Using StreamingChunkProvider for statementId: {}", statementId.toSQLExecStatementId());

      ChunkLinkFetcher linkFetcher = new SeaChunkLinkFetcher(session, statementId);
      CompressionCodec compressionCodec = resultManifest.getResultCompression();
      int maxChunksInMemory = connectionContext.getCloudFetchThreadPoolSize();
      int chunkReadyTimeoutSeconds = connectionContext.getChunkReadyTimeoutSeconds();
      double cloudFetchSpeedThreshold = connectionContext.getCloudFetchSpeedThreshold();

      // Convert ExternalLinks to ChunkLinkInfo for the provider
      Collection<ChunkLinkFetchResult.ChunkLinkInfo> initialLinks =
          convertToChunkLinkInfos(resultData.getExternalLinks());

      return new StreamingChunkProvider(
          linkFetcher,
          httpClient,
          compressionCodec,
          statementId,
          maxChunksInMemory,
          chunkReadyTimeoutSeconds,
          cloudFetchSpeedThreshold,
          initialLinks);
    } else {
      // Use the original RemoteChunkProvider
      return new RemoteChunkProvider(
          statementId,
          resultManifest,
          resultData,
          session,
          httpClient,
          connectionContext.getCloudFetchThreadPoolSize());
    }
  }

  public ArrowStreamResult(
      TFetchResultsResp resultsResp,
      boolean isInlineArrow,
      IDatabricksStatementInternal parentStatementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this(
        resultsResp,
        isInlineArrow,
        parentStatementId,
        session,
        DatabricksHttpClientFactory.getInstance().getClient(session.getConnectionContext()));
  }

  @VisibleForTesting
  ArrowStreamResult(
      TFetchResultsResp resultsResp,
      boolean isInlineArrow,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {
    this.session = session;
    setColumnInfo(resultsResp.getResultSetMetadata());
    if (isInlineArrow) {
      this.chunkProvider = new InlineChunkProvider(resultsResp, parentStatement, session);
    } else {
      CompressionCodec compressionCodec =
          CompressionCodec.getCompressionMapping(resultsResp.getResultSetMetadata());
      this.chunkProvider =
          new RemoteChunkProvider(
              parentStatement,
              resultsResp,
              session,
              httpClient,
              session.getConnectionContext().getCloudFetchThreadPoolSize(),
              compressionCodec);
    }
  }

  public List<String> getArrowMetadata() throws DatabricksSQLException {
    if (chunkProvider == null || chunkProvider.getChunk() == null) {
      return null;
    }
    return chunkProvider.getChunk().getArrowMetadata();
  }

  /** {@inheritDoc} */
  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    ColumnInfoTypeName requiredType = columnInfos.get(columnIndex).getTypeName();
    String arrowMetadata = chunkIterator.getType(columnIndex);
    if (arrowMetadata == null) {
      arrowMetadata = columnInfos.get(columnIndex).getTypeText();
    }

    // Handle complex type conversion when complex datatype support is disabled
    boolean isComplexDatatypeSupportEnabled =
        this.session.getConnectionContext().isComplexDatatypeSupportEnabled();
    boolean isGeoSpatialSupportEnabled =
        this.session.getConnectionContext().isGeoSpatialSupportEnabled();

    // Check if we need to convert geospatial types to string when geospatial support is disabled
    // This check must come before the general complex type check
    if (!isGeoSpatialSupportEnabled && isGeospatialType(requiredType)) {
      LOGGER.debug("Geospatial support is disabled, converting {} to STRING", requiredType);

      Object result =
          chunkIterator.getColumnObjectAtCurrentRow(
              columnIndex, ColumnInfoTypeName.STRING, "STRING", columnInfos.get(columnIndex));
      if (result == null) {
        return null;
      }
      // Return raw string for geospatial types when support is disabled
      return result;
    }

    if (!isComplexDatatypeSupportEnabled && isComplexType(requiredType)) {
      LOGGER.debug("Complex datatype support is disabled, converting complex type to STRING");

      Object result =
          chunkIterator.getColumnObjectAtCurrentRow(
              columnIndex, ColumnInfoTypeName.STRING, "STRING", columnInfos.get(columnIndex));
      if (result == null) {
        return null;
      }
      ComplexDataTypeParser parser = new ComplexDataTypeParser();
      return parser.formatComplexTypeString(result.toString(), requiredType.name(), arrowMetadata);
    }

    return chunkIterator.getColumnObjectAtCurrentRow(
        columnIndex, requiredType, arrowMetadata, columnInfos.get(columnIndex));
  }

  /**
   * Checks if the given type is a complex type (ARRAY, MAP, STRUCT, GEOMETRY, or GEOGRAPHY).
   *
   * @param type The type to check
   * @return true if the type is a complex type, false otherwise
   */
  @VisibleForTesting
  public static boolean isComplexType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.ARRAY
        || type == ColumnInfoTypeName.MAP
        || type == ColumnInfoTypeName.STRUCT
        || type == ColumnInfoTypeName.GEOMETRY
        || type == ColumnInfoTypeName.GEOGRAPHY;
  }

  /**
   * Checks if the given type is a geospatial type (GEOMETRY or GEOGRAPHY).
   *
   * @param type The type to check
   * @return true if the type is a geospatial type, false otherwise
   */
  @VisibleForTesting
  public static boolean isGeospatialType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.GEOMETRY || type == ColumnInfoTypeName.GEOGRAPHY;
  }

  /** {@inheritDoc} */
  @Override
  public long getCurrentRow() {
    return currentRowIndex;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next() throws DatabricksSQLException {
    if (!hasNext()) {
      return false;
    }

    currentRowIndex++;
    if (chunkIterator == null || !chunkIterator.hasNextRow()) {
      chunkProvider.next();
      chunkIterator = chunkProvider.getChunk().getChunkIterator();
    }

    return chunkIterator.nextRow();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    if (isClosed) {
      return false;
    }

    // Check if there are any more rows available in the current chunk
    if (chunkIterator != null && chunkIterator.hasNextRow()) {
      return true;
    }

    // For inline arrow, check if the chunk extractor has more chunks
    // Otherwise, check the chunk downloader
    return chunkProvider.hasNextChunk();
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    isClosed = true;
    chunkProvider.close();
  }

  @Override
  public long getRowCount() {
    return chunkProvider.getRowCount();
  }

  @Override
  public long getChunkCount() {
    return chunkProvider.getChunkCount();
  }

  private void setColumnInfo(TGetResultSetMetadataResp resultManifest) {
    columnInfos = new ArrayList<>();
    if (resultManifest.getSchema() == null) {
      return;
    }
    for (TColumnDesc tColumnDesc : resultManifest.getSchema().getColumns()) {
      columnInfos.add(getColumnInfoFromTColumnDesc(tColumnDesc));
    }
  }

  /**
   * Converts a collection of ExternalLinks to ChunkLinkInfo objects.
   *
   * @param externalLinks The external links to convert, may be null
   * @return A collection of ChunkLinkInfo objects, or null if input is null
   */
  private static Collection<ChunkLinkFetchResult.ChunkLinkInfo> convertToChunkLinkInfos(
      Collection<ExternalLink> externalLinks) {
    if (externalLinks == null) {
      return null;
    }
    return externalLinks.stream()
        .map(
            link ->
                new ChunkLinkFetchResult.ChunkLinkInfo(
                    link.getChunkIndex() != null ? link.getChunkIndex() : 0,
                    link,
                    link.getRowCount() != null ? link.getRowCount() : 0,
                    link.getRowOffset() != null ? link.getRowOffset() : 0))
        .collect(Collectors.toList());
  }
}
