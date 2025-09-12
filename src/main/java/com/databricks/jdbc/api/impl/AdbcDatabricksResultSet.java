package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.adbc.IArrowIpcStreamIterator;
import com.databricks.jdbc.api.impl.adbc.ArrowIpcStreamIterator;
import com.databricks.jdbc.api.impl.arrow.ArrowStreamResult;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.arrow.flight.ArrowFlightReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import com.databricks.jdbc.api.impl.arrow.AbstractArrowResultChunk;
import com.databricks.jdbc.api.impl.arrow.ChunkProvider;

/**
 * ADBC (Arrow Database Connectivity) compliant implementation of {@link IDatabricksResultSet}. This
 * implementation extends {@link DatabricksResultSet} to provide native Arrow streaming capabilities
 * while maintaining full backward compatibility with JDBC operations.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Native Arrow streaming access via {@link #getArrowStream()} and {@link #getArrowReader()}
 *   <li>Zero-copy columnar data access when possible
 *   <li>Batch-oriented processing for analytical workloads
 *   <li>Full JDBC compatibility for existing applications
 * </ul>
 */
public class AdbcDatabricksResultSet extends DatabricksResultSet {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(AdbcDatabricksResultSet.class);

  private final boolean adbcModeEnabled;
  private final IDatabricksSession session;

  // Constructor for SEA result set with ADBC support
  public AdbcDatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      ResultData resultData,
      ResultManifest resultManifest,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement,
      boolean adbcModeEnabled)
      throws DatabricksSQLException {
    super(
        statementStatus,
        statementId,
        resultData,
        resultManifest,
        statementType,
        session,
        parentStatement);
    this.adbcModeEnabled = adbcModeEnabled;
    this.session = session;
    LOGGER.debug("Created AdbcDatabricksResultSet with ADBC mode: {}", adbcModeEnabled);
  }

  // Constructor for thrift result set with ADBC support
  public AdbcDatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      TFetchResultsResp resultsResp,
      StatementType statementType,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      boolean adbcModeEnabled)
      throws SQLException {
    super(statementStatus, statementId, resultsResp, statementType, parentStatement, session);
    this.adbcModeEnabled = adbcModeEnabled;
    this.session = session;
    LOGGER.debug("Created AdbcDatabricksResultSet from thrift with ADBC mode: {}", adbcModeEnabled);
  }

  @Override
  public boolean supportsArrowStreaming() throws SQLException {
    checkIfClosed();
    // Arrow streaming is supported when:
    // 1. ADBC mode is enabled
    // 2. The underlying execution result is Arrow-based
    // 3. Result set type supports Arrow streaming
    return adbcModeEnabled
        && (getExecutionResult() instanceof ArrowStreamResult)
        && (getResultSetType() == ResultSetType.SEA_ARROW_ENABLED
            || getResultSetType() == ResultSetType.THRIFT_ARROW_ENABLED);
  }

  @Override
  public Stream<VectorSchemaRoot> getArrowStream() throws SQLException {
    checkIfClosed();

    if (!supportsArrowStreaming()) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Arrow streaming is not supported. Enable ADBC mode and ensure result set uses Arrow format.",
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION,
          false);
    }

    ArrowStreamResult arrowResult = (ArrowStreamResult) getExecutionResult();

    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(
            new ArrowVectorSchemaRootIterator(arrowResult, session),
            Spliterator.ORDERED | Spliterator.NONNULL),
        false);
  }

  @Override
  public ArrowFlightReader getArrowReader() throws SQLException {
    checkIfClosed();

    if (!supportsArrowStreaming()) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Arrow reader access is not supported. Enable ADBC mode and ensure result set uses Arrow format.",
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION,
          false);
    }

    // Create a Flight-like reader that wraps our streaming implementation
    ArrowStreamResult arrowResult = (ArrowStreamResult) getExecutionResult();
    return new AdbcArrowFlightReader(arrowResult, session);
  }

  @Override
  public boolean isAdbcMode() throws SQLException {
    checkIfClosed();
    return adbcModeEnabled;
  }

  @Override
  public IArrowIpcStreamIterator getArrowIpcIterator() throws SQLException {
    checkIfClosed();

    if (!supportsArrowIpcStreaming()) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "ArrowIPC streaming is not supported. Enable ADBC mode and ensure result set uses Arrow format.",
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION,
          false);
    }

    try {
      ArrowStreamResult arrowResult = (ArrowStreamResult) getExecutionResult();
      
      // Get allocator from session context or create one
      BufferAllocator allocator = getArrowAllocator();
      
      // Create ArrowIPC iterator from the arrow stream result
      return ArrowIpcStreamIterator.fromArrowStreamResult(arrowResult, allocator);
      
    } catch (Exception e) {
      LOGGER.error("Failed to create ArrowIPC stream iterator", e);
      throw new DatabricksSQLException("Error creating ArrowIPC stream iterator", e);
    }
  }

  @Override
  public ByteBuffer getArrowSchemaIpc() throws SQLException {
    checkIfClosed();

    if (!supportsArrowIpcStreaming()) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "ArrowIPC streaming is not supported. Enable ADBC mode and ensure result set uses Arrow format.",
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION,
          false);
    }

    try {
      // Create a temporary IPC iterator to get the schema
      try (IArrowIpcStreamIterator iterator = getArrowIpcIterator()) {
        return iterator.getSchemaIpc();
      }
      
    } catch (Exception e) {
      LOGGER.error("Failed to get Arrow schema in IPC format", e);
      throw new DatabricksSQLException("Error serializing Arrow schema to IPC format", e);
    }
  }

  @Override
  public boolean supportsArrowIpcStreaming() throws SQLException {
    checkIfClosed();
    
    // ArrowIPC streaming is supported when:
    // 1. Basic Arrow streaming is supported
    // 2. ADBC mode is enabled (IPC is an ADBC-specific feature)
    // 3. We have access to the underlying execution result
    return supportsArrowStreaming() && adbcModeEnabled && (getExecutionResult() != null);
  }

  /**
   * Iterator that converts ArrowStreamResult chunks to VectorSchemaRoot objects. This enables
   * streaming access to Arrow data in a standard Java Iterator pattern.
   */
  private static class ArrowVectorSchemaRootIterator implements Iterator<VectorSchemaRoot> {
    private final ArrowStreamResult arrowResult;
    private final ArrowBatchExtractor batchExtractor;
    private boolean hasNextChecked = false;
    private boolean hasNextValue = false;

    ArrowVectorSchemaRootIterator(ArrowStreamResult arrowResult) {
      this.arrowResult = arrowResult;
      this.batchExtractor = new ArrowBatchExtractor(arrowResult);
    }

    ArrowVectorSchemaRootIterator(ArrowStreamResult arrowResult, IDatabricksSession session) {
      this.arrowResult = arrowResult;
      this.batchExtractor = new ArrowBatchExtractor(arrowResult, session);
    }

    @Override
    public boolean hasNext() {
      if (!hasNextChecked) {
        try {
          hasNextValue = batchExtractor.hasNextBatch();
          hasNextChecked = true;
        } catch (Exception e) {
          LOGGER.error("Error checking for next Arrow batch", e);
          hasNextValue = false;
        }
      }
      return hasNextValue;
    }

    @Override
    public VectorSchemaRoot next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException("No more Arrow batches available");
      }

      try {
        // Reset the hasNext check
        hasNextChecked = false;

        return batchExtractor.getNextBatch();

      } catch (Exception e) {
        throw new RuntimeException("Error retrieving next Arrow batch", e);
      }
    }
  }

  /**
   * Helper class to extract VectorSchemaRoot batches from ArrowStreamResult. This provides the
   * bridge between the existing chunk-based architecture and the ADBC streaming interface.
   */
  private static class ArrowBatchExtractor {
    private final ArrowStreamResult arrowResult;
    private final AdbcArrowStreamOptimizer optimizer;
    private AbstractArrowResultChunk currentChunk;
    private int currentBatchIndex = 0;
    private boolean initialized = false;

    ArrowBatchExtractor(ArrowStreamResult arrowResult) {
      this.arrowResult = arrowResult;
      // Initialize optimizer - we'll need to get the session from somewhere
      this.optimizer = null; // Placeholder - would need session access
    }

    ArrowBatchExtractor(ArrowStreamResult arrowResult, IDatabricksSession session) {
      this.arrowResult = arrowResult;
      this.optimizer = new AdbcArrowStreamOptimizer(session);
    }

    boolean hasNextBatch() throws DatabricksSQLException {
      ensureInitialized();

      // Check if there are more batches in the current chunk
      if (currentChunk != null && currentBatchIndex < currentChunk.getRecordBatchCountInChunk()) {
        return true;
      }

      // Check if there are more chunks available
      return arrowResult.hasNext();
    }

    VectorSchemaRoot getNextBatch() throws DatabricksSQLException {
      if (!hasNextBatch()) {
        throw new java.util.NoSuchElementException("No more Arrow batches available");
      }

      ensureInitialized();

      // If we've exhausted current chunk, move to next
      if (currentChunk == null
          || currentBatchIndex >= currentChunk.getRecordBatchCountInChunk()) {
        if (arrowResult.hasNext()) {
          arrowResult.next(); // Move to next chunk
          currentChunk = getCurrentChunk();
          currentBatchIndex = 0;
        } else {
          throw new java.util.NoSuchElementException("No more chunks available");
        }
      }

      // Extract VectorSchemaRoot from current batch
      return createVectorSchemaRootFromBatch(currentChunk, currentBatchIndex++);
    }

    private void ensureInitialized() throws DatabricksSQLException {
      if (!initialized) {
        if (arrowResult.hasNext()) {
          arrowResult.next(); // Initialize first chunk
          currentChunk = getCurrentChunk();
          currentBatchIndex = 0;
        }
        initialized = true;
      }
    }

    private AbstractArrowResultChunk getCurrentChunk() {
      // Access the current chunk from ArrowStreamResult
      // This requires accessing package-private members or adding public accessors
      try {
        // Use reflection as a temporary solution until we can modify ArrowStreamResult
        java.lang.reflect.Field chunkProviderField =
            ArrowStreamResult.class.getDeclaredField("chunkProvider");
        chunkProviderField.setAccessible(true);
        ChunkProvider chunkProvider = (ChunkProvider) chunkProviderField.get(arrowResult);
        return chunkProvider.getChunk();
      } catch (Exception e) {
        LOGGER.error("Error accessing current chunk from ArrowStreamResult", e);
        throw new RuntimeException("Failed to access Arrow chunk data", e);
      }
    }

    private VectorSchemaRoot createVectorSchemaRootFromBatch(
        AbstractArrowResultChunk chunk, int batchIndex) throws DatabricksSQLException {

      try {
        // Get the record batch (list of value vectors)
        List<List<org.apache.arrow.vector.ValueVector>> recordBatches = chunk.getRecordBatches();
        if (batchIndex >= recordBatches.size()) {
          throw new IndexOutOfBoundsException("Batch index " + batchIndex + " out of bounds");
        }

        List<org.apache.arrow.vector.ValueVector> vectors = recordBatches.get(batchIndex);

        // Create VectorSchemaRoot from value vectors using optimizer if available
        if (optimizer != null) {
          return optimizer.createOptimizedVectorSchemaRoot(vectors, vectors.get(0).getAllocator());
        } else {
          // Fallback to standard creation
          return createVectorSchemaRootFromVectors(vectors);
        }

      } catch (Exception e) {
        throw new DatabricksSQLException("Error creating VectorSchemaRoot from batch", e);
      }
    }

    private VectorSchemaRoot createVectorSchemaRootFromVectors(
        List<org.apache.arrow.vector.ValueVector> vectors) {

      if (vectors.isEmpty()) {
        // Return empty VectorSchemaRoot
        return VectorSchemaRoot.of();
      }

      // Create field vectors and schema
      List<org.apache.arrow.vector.FieldVector> fieldVectors = new ArrayList<>();
      List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();

      for (int i = 0; i < vectors.size(); i++) {
        org.apache.arrow.vector.ValueVector vector = vectors.get(i);
        if (vector instanceof org.apache.arrow.vector.FieldVector) {
          org.apache.arrow.vector.FieldVector fieldVector =
              (org.apache.arrow.vector.FieldVector) vector;
          fieldVectors.add(fieldVector);
          fields.add(fieldVector.getField());
        } else {
          LOGGER.warn("Vector at index {} is not a FieldVector, skipping", i);
        }
      }

      // Create schema and VectorSchemaRoot
      org.apache.arrow.vector.types.pojo.Schema schema =
          new org.apache.arrow.vector.types.pojo.Schema(fields);

      // Create VectorSchemaRoot with the field vectors
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, vectors.get(0).getAllocator());

      // Transfer data from original vectors to new root vectors
      for (int i = 0; i < fieldVectors.size(); i++) {
        org.apache.arrow.vector.FieldVector sourceVector = fieldVectors.get(i);
        org.apache.arrow.vector.FieldVector targetVector = root.getVector(i);

        // Transfer the data
        org.apache.arrow.vector.util.TransferPair transferPair =
            sourceVector.getTransferPair(targetVector);
        transferPair.transfer();
      }

      // Set the row count
      if (!fieldVectors.isEmpty()) {
        root.setRowCount(fieldVectors.get(0).getValueCount());
      }

      return root;
    }
  }

  /**
   * ADBC-compatible ArrowFlightReader implementation that wraps ArrowStreamResult.
   * Provides direct streaming access to Arrow data with minimal overhead.
   */
  private static class AdbcArrowFlightReader extends ArrowFlightReader {
    private final ArrowBatchExtractor batchExtractor;
    private final IDatabricksSession session;
    private VectorSchemaRoot currentRoot;
    private boolean closed = false;

    AdbcArrowFlightReader(ArrowStreamResult arrowResult, IDatabricksSession session) {
      this.batchExtractor = new ArrowBatchExtractor(arrowResult);
      this.session = session;
    }

    @Override
    public VectorSchemaRoot getRoot() {
      return currentRoot;
    }

    @Override
    public boolean next() {
      if (closed) {
        return false;
      }
      
      try {
        if (batchExtractor.hasNextBatch()) {
          if (currentRoot != null) {
            currentRoot.close(); // Clean up previous root
          }
          currentRoot = batchExtractor.getNextBatch();
          return true;
        } else {
          return false;
        }
      } catch (Exception e) {
        LOGGER.error("Error reading next Arrow batch", e);
        return false;
      }
    }

    @Override
    public long bytesRead() {
      // Return approximate bytes read - could be enhanced with actual tracking
      if (currentRoot != null) {
        return currentRoot.getRowCount() * currentRoot.getFieldVectors().size() * 8; // Rough estimate
      }
      return 0;
    }

    @Override
    public void close() throws Exception {
      if (!closed) {
        if (currentRoot != null) {
          currentRoot.close();
          currentRoot = null;
        }
        closed = true;
      }
    }

    @Override
    public java.util.Iterator<VectorSchemaRoot> iterator() {
      return new java.util.Iterator<VectorSchemaRoot>() {
        @Override
        public boolean hasNext() {
          return !closed && AdbcArrowFlightReader.this.next();
        }

        @Override
        public VectorSchemaRoot next() {
          if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more batches available");
          }
          return currentRoot;
        }
      };
    }
  }

  // Protected helper methods to access parent class internals

  /**
   * Get access to the underlying execution result for ADBC operations. This method provides access
   * to the parent class's execution result.
   */
  protected IExecutionResult getExecutionResult() {
    // We need to access the parent's executionResult field
    // This would require making it protected in the parent class or adding a getter
    return super.executionResult;
  }

  /** Get the result set type for determining Arrow streaming support. */
  protected ResultSetType getResultSetType() {
    // We need to access the parent's resultSetType field
    // This would require making it protected in the parent class or adding a getter
    return super.resultSetType;
  }

  /**
   * Gets or creates an Arrow BufferAllocator for IPC operations.
   * This method provides access to memory allocation for Arrow operations.
   */
  private BufferAllocator getArrowAllocator() {
    // In a full implementation, this would get the allocator from the session
    // or connection context. For now, we'll need to access it through the
    // existing Arrow infrastructure or create a child allocator.
    // This is a placeholder that would need to be implemented based on
    // the actual memory management strategy in the driver.
    
    // TODO: Implement proper allocator access from session context
    throw new UnsupportedOperationException(
        "Arrow allocator access needs to be implemented based on driver's memory management");
  }

  @Override
  public String toString() {
    return String.format(
        "AdbcDatabricksResultSet{adbcMode=%s, statementId=%s}", adbcModeEnabled, getStatementId());
  }
}
