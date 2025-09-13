package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.impl.arrow.AbstractArrowResultChunk;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Optimizer for ADBC Arrow streaming operations focusing on zero-copy data access and performance
 * optimizations for analytical workloads.
 *
 * <p>Key optimizations:
 *
 * <ul>
 *   <li>Zero-copy vector transfers where possible
 *   <li>Asynchronous batch prefetching
 *   <li>Memory-efficient buffer reuse
 *   <li>Batch-level parallelization for large result sets
 * </ul>
 */
public class AdbcArrowStreamOptimizer {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(AdbcArrowStreamOptimizer.class);

  private final IDatabricksSession session;
  private final ExecutorService prefetchExecutor;
  private final boolean zeroCopyEnabled;
  private final int prefetchBufferSize;

  public AdbcArrowStreamOptimizer(IDatabricksSession session) {
    this.session = session;
    this.zeroCopyEnabled = session.getConnectionContext().supportsNativeArrowStreaming();
    this.prefetchBufferSize = determinePrefetchBufferSize();

    // Create dedicated thread pool for async prefetching
    this.prefetchExecutor =
        Executors.newFixedThreadPool(
            Math.min(4, Runtime.getRuntime().availableProcessors()),
            r -> {
              Thread t = new Thread(r, "ADBC-Prefetch-" + System.nanoTime());
              t.setDaemon(true);
              return t;
            });

    LOGGER.debug(
        "ADBC Optimizer initialized: zeroCopy={}, prefetchBuffer={}",
        zeroCopyEnabled,
        prefetchBufferSize);
  }

  /**
   * Creates an optimized VectorSchemaRoot from the given vectors with zero-copy optimizations when
   * possible.
   */
  public VectorSchemaRoot createOptimizedVectorSchemaRoot(
      List<org.apache.arrow.vector.ValueVector> vectors, BufferAllocator allocator) {

    if (vectors.isEmpty()) {
      return VectorSchemaRoot.of();
    }

    try {
      if (zeroCopyEnabled) {
        return createZeroCopyVectorSchemaRoot(vectors, allocator);
      } else {
        return createStandardVectorSchemaRoot(vectors, allocator);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to create optimized VectorSchemaRoot, falling back to standard", e);
      return createStandardVectorSchemaRoot(vectors, allocator);
    }
  }

  /** Creates a VectorSchemaRoot with zero-copy vector sharing where possible. */
  private VectorSchemaRoot createZeroCopyVectorSchemaRoot(
      List<org.apache.arrow.vector.ValueVector> vectors, BufferAllocator allocator) {

    List<FieldVector> fieldVectors = new java.util.ArrayList<>();
    List<org.apache.arrow.vector.types.pojo.Field> fields = new java.util.ArrayList<>();

    for (org.apache.arrow.vector.ValueVector vector : vectors) {
      if (vector instanceof FieldVector) {
        FieldVector fieldVector = (FieldVector) vector;

        // For zero-copy, we can directly reference the original vector
        // if it's from the same allocator and not being modified elsewhere
        if (canUseZeroCopy(fieldVector, allocator)) {
          fieldVectors.add(fieldVector);
          fields.add(fieldVector.getField());
          LOGGER.trace("Using zero-copy reference for vector: {}", fieldVector.getName());
        } else {
          // Fall back to transfer if zero-copy not possible
          FieldVector transferredVector = transferVector(fieldVector, allocator);
          fieldVectors.add(transferredVector);
          fields.add(transferredVector.getField());
          LOGGER.trace("Using transfer copy for vector: {}", fieldVector.getName());
        }
      }
    }

    // Create schema and VectorSchemaRoot
    org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(fields);

    VectorSchemaRoot root = new VectorSchemaRoot(fieldVectors);

    // Set row count from first vector
    if (!fieldVectors.isEmpty()) {
      root.setRowCount(fieldVectors.get(0).getValueCount());
    }

    return root;
  }

  /** Creates a standard VectorSchemaRoot with data transfers. */
  private VectorSchemaRoot createStandardVectorSchemaRoot(
      List<org.apache.arrow.vector.ValueVector> vectors, BufferAllocator allocator) {

    List<FieldVector> fieldVectors = new java.util.ArrayList<>();
    List<org.apache.arrow.vector.types.pojo.Field> fields = new java.util.ArrayList<>();

    for (org.apache.arrow.vector.ValueVector vector : vectors) {
      if (vector instanceof FieldVector) {
        FieldVector fieldVector = (FieldVector) vector;
        FieldVector transferredVector = transferVector(fieldVector, allocator);
        fieldVectors.add(transferredVector);
        fields.add(transferredVector.getField());
      }
    }

    org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(fields);
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    // Transfer data
    for (int i = 0; i < fieldVectors.size(); i++) {
      FieldVector sourceVector = fieldVectors.get(i);
      FieldVector targetVector = root.getVector(i);

      org.apache.arrow.vector.util.TransferPair transferPair =
          sourceVector.getTransferPair(allocator);
      transferPair.transfer();
    }

    if (!fieldVectors.isEmpty()) {
      root.setRowCount(fieldVectors.get(0).getValueCount());
    }

    return root;
  }

  /** Determines if zero-copy can be safely used for the given vector. */
  private boolean canUseZeroCopy(FieldVector vector, BufferAllocator allocator) {
    // Zero-copy is safe when:
    // 1. Vector uses the same allocator (or compatible one)
    // 2. Vector is not being concurrently modified
    // 3. Vector lifetime extends beyond the VectorSchemaRoot usage

    try {
      // Check allocator compatibility
      if (vector.getAllocator() != allocator
          && !isCompatibleAllocator(vector.getAllocator(), allocator)) {
        return false;
      }

      // Additional safety checks could be added here
      // For now, we'll be conservative and require same allocator
      return vector.getAllocator() == allocator;

    } catch (Exception e) {
      LOGGER.trace("Zero-copy safety check failed for vector: {}", vector.getName(), e);
      return false;
    }
  }

  /** Checks if allocators are compatible for zero-copy operations. */
  private boolean isCompatibleAllocator(BufferAllocator source, BufferAllocator target) {
    // Simplified compatibility check - just check if they're the same instance
    return source == target;
  }

  /** Transfers vector data to the target allocator. */
  private FieldVector transferVector(FieldVector sourceVector, BufferAllocator targetAllocator) {
    // Create a new vector with the same field but different allocator
    FieldVector targetVector = sourceVector.getField().createVector(targetAllocator);

    // Perform the transfer using the target allocator
    org.apache.arrow.vector.util.TransferPair transferPair =
        sourceVector.getTransferPair(targetAllocator);
    transferPair.transfer();

    return targetVector;
  }

  /** Determines optimal prefetch buffer size based on connection configuration. */
  private int determinePrefetchBufferSize() {
    // Base the buffer size on connection parameters and system resources
    int defaultSize = 4; // Default 4 batches

    try {
      // Could be made configurable via connection parameters
      return Math.max(2, Math.min(defaultSize, Runtime.getRuntime().availableProcessors()));
    } catch (Exception e) {
      LOGGER.debug("Error determining prefetch buffer size, using default", e);
      return defaultSize;
    }
  }

  /** Prefetches the next batch asynchronously to improve streaming performance. */
  public CompletableFuture<VectorSchemaRoot> prefetchNextBatch(
      AbstractArrowResultChunk chunk, int batchIndex, BufferAllocator allocator) {

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            List<List<org.apache.arrow.vector.ValueVector>> recordBatches =
                chunk.getRecordBatches();
            if (batchIndex >= recordBatches.size()) {
              return null;
            }

            List<org.apache.arrow.vector.ValueVector> vectors = recordBatches.get(batchIndex);
            return createOptimizedVectorSchemaRoot(vectors, allocator);

          } catch (Exception e) {
            LOGGER.error("Error prefetching batch {}", batchIndex, e);
            return null;
          }
        },
        prefetchExecutor);
  }

  /** Cleans up resources used by the optimizer. */
  public void close() {
    if (prefetchExecutor != null && !prefetchExecutor.isShutdown()) {
      prefetchExecutor.shutdown();
      LOGGER.debug("ADBC Arrow Stream Optimizer closed");
    }
  }
}
