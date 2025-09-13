package com.databricks.jdbc.api.adbc;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Iterator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Iterator interface for streaming Arrow IPC (Inter-Process Communication) messages.
 *
 * <p>This interface provides direct access to Arrow IPC format data, enabling zero-copy streaming
 * of columnar data without intermediate conversions. Each iteration returns a ByteBuffer containing
 * a complete Arrow IPC message (either schema or record batch).
 *
 * <p>The iterator follows Arrow IPC streaming format specification:
 *
 * <ul>
 *   <li>First message contains the schema information
 *   <li>Subsequent messages contain record batch data
 *   <li>Messages are prefixed with 32-bit length headers
 *   <li>All data is in little-endian format
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * try (IArrowIpcStreamIterator iterator = resultSet.getArrowIpcIterator()) {
 *     Schema schema = iterator.getSchema();
 *     while (iterator.hasNext()) {
 *         ByteBuffer ipcMessage = iterator.next();
 *         // Process raw IPC message
 *     }
 * }
 * }</pre>
 *
 * <p>This interface is part of the ADBC (Arrow Database Connectivity) extension to provide
 * high-performance columnar data access for analytical workloads.
 */
public interface IArrowIpcStreamIterator extends Iterator<ByteBuffer>, AutoCloseable {

  /**
   * Returns the Arrow schema for this stream.
   *
   * <p>The schema describes the structure of all record batches in the stream, including column
   * names, types, and metadata. This method can be called multiple times and will always return the
   * same schema.
   *
   * @return the Arrow schema for this stream
   * @throws SQLException if the schema cannot be retrieved or if the iterator is closed
   */
  Schema getSchema() throws SQLException;

  /**
   * Returns the Arrow schema serialized in IPC format as a ByteBuffer.
   *
   * <p>This provides direct access to the schema message in Arrow IPC format, which can be used for
   * interoperability with other Arrow-compatible systems. The returned ByteBuffer contains a
   * complete Arrow IPC schema message.
   *
   * @return ByteBuffer containing the schema in Arrow IPC format
   * @throws SQLException if the schema cannot be serialized or if the iterator is closed
   */
  ByteBuffer getSchemaIpc() throws SQLException;

  /**
   * Returns the total number of bytes read from the stream so far.
   *
   * <p>This includes both schema and record batch data. The count is updated after each call to
   * {@link #next()}. This method can be used for progress tracking and performance monitoring.
   *
   * @return total bytes read from the stream
   */
  long getBytesRead();

  /**
   * Returns the number of record batches processed so far.
   *
   * <p>This count excludes the schema message and only counts actual data batches. The count is
   * incremented after each successful call to {@link #next()} that returns a record batch message.
   *
   * @return number of record batches processed
   */
  long getBatchCount();

  /**
   * Checks if the schema has been read and is available.
   *
   * <p>The schema is typically available immediately when the iterator is created, but this method
   * allows checking the state explicitly. If this returns false, calling {@link #getSchema()} may
   * throw an exception.
   *
   * @return true if the schema is available, false otherwise
   */
  boolean hasSchema();

  /**
   * Resets the iterator to the beginning of the stream.
   *
   * <p>After reset, the iterator will start from the first record batch again. The schema remains
   * available and unchanged. This method may not be supported by all implementations, particularly
   * those backed by forward-only streams.
   *
   * @throws SQLException if reset is not supported or if an error occurs during reset
   * @throws UnsupportedOperationException if the underlying stream does not support reset
   */
  void reset() throws SQLException;

  /**
   * Returns whether this iterator supports reset operations.
   *
   * <p>If this returns false, calling {@link #reset()} will throw an UnsupportedOperationException.
   * Stream-based iterators typically do not support reset, while memory-based iterators usually do.
   *
   * @return true if reset is supported, false otherwise
   */
  boolean supportsReset();

  /**
   * Returns the estimated total size of the stream in bytes.
   *
   * <p>This is a best-effort estimate and may not be available for all implementations. Returns -1
   * if the size cannot be estimated. The estimate includes both schema and record batch data.
   *
   * @return estimated total stream size in bytes, or -1 if unknown
   */
  long getEstimatedSize();

  /**
   * Closes this iterator and releases any associated resources.
   *
   * <p>After calling close(), all other methods will throw SQLException. It is safe to call close()
   * multiple times. Resources may include memory buffers, network connections, or file handles.
   *
   * @throws SQLException if an error occurs while closing resources
   */
  @Override
  void close() throws SQLException;

  /**
   * Returns whether this iterator has been closed.
   *
   * @return true if the iterator is closed, false otherwise
   */
  boolean isClosed();
}
