package com.databricks.jdbc.api.impl.adbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArrowIpcStreamIteratorTest {

  private BufferAllocator allocator;
  private Schema testSchema;
  private VectorSchemaRoot testRoot;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);

    // Create a simple test schema with two fields
    Field intField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field strField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    testSchema = new Schema(Arrays.asList(intField, strField));

    // Create test data
    testRoot = VectorSchemaRoot.create(testSchema, allocator);
    IntVector intVector = (IntVector) testRoot.getVector("id");
    VarCharVector strVector = (VarCharVector) testRoot.getVector("name");

    intVector.allocateNew(3);
    strVector.allocateNew();

    intVector.set(0, 1);
    intVector.set(1, 2);
    intVector.set(2, 3);

    strVector.set(0, "Alice".getBytes());
    strVector.set(1, "Bob".getBytes());
    strVector.set(2, "Charlie".getBytes());

    intVector.setValueCount(3);
    strVector.setValueCount(3);
    testRoot.setRowCount(3);
  }

  @AfterEach
  void tearDown() {
    if (testRoot != null) {
      testRoot.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  void testIteratorCreation() throws SQLException {
    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot).iterator();

    ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(vectorIterator, allocator, testSchema, false, -1);

    assertNotNull(iterator);
    assertEquals(testSchema, iterator.getSchema());
    assertTrue(iterator.hasSchema());
    assertFalse(iterator.supportsReset());
    assertEquals(-1, iterator.getEstimatedSize());
    assertEquals(0, iterator.getBytesRead());
    assertEquals(0, iterator.getBatchCount());
    assertFalse(iterator.isClosed());
  }

  @Test
  void testSchemaIpcSerialization() throws SQLException {
    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot).iterator();

    try (ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(vectorIterator, allocator, testSchema, false, -1)) {

      ByteBuffer schemaIpc = iterator.getSchemaIpc();
      assertNotNull(schemaIpc);
      assertTrue(schemaIpc.remaining() > 0);

      // Should be able to call multiple times
      ByteBuffer schemaIpc2 = iterator.getSchemaIpc();
      assertEquals(schemaIpc.remaining(), schemaIpc2.remaining());
    }
  }

  @Test
  void testIteratorHasNext() throws SQLException {
    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot).iterator();

    try (ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(vectorIterator, allocator, testSchema, false, -1)) {

      assertTrue(iterator.hasNext());

      // Consume the single batch
      ByteBuffer batch = iterator.next();
      assertNotNull(batch);
      assertTrue(batch.remaining() > 0);

      assertFalse(iterator.hasNext());
      assertEquals(1, iterator.getBatchCount());
      assertTrue(iterator.getBytesRead() > 0);
    }
  }

  @Test
  void testIteratorNoSuchElement() throws SQLException {
    Iterator<VectorSchemaRoot> emptyIterator = Arrays.<VectorSchemaRoot>asList().iterator();

    try (ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(emptyIterator, allocator, testSchema, false, -1)) {

      assertFalse(iterator.hasNext());

      assertThrows(
          NoSuchElementException.class,
          () -> {
            iterator.next();
          });
    }
  }

  @Test
  void testIteratorClose() throws SQLException {
    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot).iterator();

    ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(vectorIterator, allocator, testSchema, false, -1);

    assertFalse(iterator.isClosed());

    iterator.close();
    assertTrue(iterator.isClosed());

    // Multiple close calls should be safe
    iterator.close();
    assertTrue(iterator.isClosed());
  }

  @Test
  void testIteratorClosedOperations() throws SQLException {
    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot).iterator();

    ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(vectorIterator, allocator, testSchema, false, -1);

    iterator.close();

    // Operations on closed iterator should throw
    assertThrows(
        IllegalStateException.class,
        () -> {
          iterator.hasNext();
        });

    assertThrows(
        IllegalStateException.class,
        () -> {
          iterator.next();
        });

    assertThrows(
        IllegalStateException.class,
        () -> {
          iterator.getSchema();
        });

    assertThrows(
        IllegalStateException.class,
        () -> {
          iterator.getSchemaIpc();
        });
  }

  @Test
  void testResetNotSupported() throws SQLException {
    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot).iterator();

    try (ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(
            vectorIterator,
            allocator,
            testSchema,
            false, // reset not supported
            -1)) {

      assertFalse(iterator.supportsReset());

      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            iterator.reset();
          });
    }
  }

  @Test
  void testMultipleBatches() throws SQLException {
    // Create a second batch
    VectorSchemaRoot secondRoot = VectorSchemaRoot.create(testSchema, allocator);
    IntVector intVector2 = (IntVector) secondRoot.getVector("id");
    VarCharVector strVector2 = (VarCharVector) secondRoot.getVector("name");

    intVector2.allocateNew(2);
    strVector2.allocateNew();

    intVector2.set(0, 4);
    intVector2.set(1, 5);

    strVector2.set(0, "David".getBytes());
    strVector2.set(1, "Eve".getBytes());

    intVector2.setValueCount(2);
    strVector2.setValueCount(2);
    secondRoot.setRowCount(2);

    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot, secondRoot).iterator();

    try (ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(vectorIterator, allocator, testSchema, false, -1)) {

      // First batch
      assertTrue(iterator.hasNext());
      ByteBuffer batch1 = iterator.next();
      assertNotNull(batch1);
      assertEquals(1, iterator.getBatchCount());

      // Second batch
      assertTrue(iterator.hasNext());
      ByteBuffer batch2 = iterator.next();
      assertNotNull(batch2);
      assertEquals(2, iterator.getBatchCount());

      // No more batches
      assertFalse(iterator.hasNext());

      assertTrue(iterator.getBytesRead() > 0);

    } finally {
      secondRoot.close();
    }
  }

  @Test
  void testMetricsTracking() throws SQLException {
    Iterator<VectorSchemaRoot> vectorIterator = Arrays.asList(testRoot).iterator();

    try (ArrowIpcStreamIterator iterator =
        new ArrowIpcStreamIterator(
            vectorIterator, allocator, testSchema, false, 1024)) { // estimated size

      assertEquals(0, iterator.getBytesRead());
      assertEquals(0, iterator.getBatchCount());
      assertEquals(1024, iterator.getEstimatedSize());

      // Process batch
      ByteBuffer batch = iterator.next();

      assertTrue(iterator.getBytesRead() > 0);
      assertEquals(1, iterator.getBatchCount());
      assertEquals(1024, iterator.getEstimatedSize()); // Should remain unchanged
    }
  }
}
