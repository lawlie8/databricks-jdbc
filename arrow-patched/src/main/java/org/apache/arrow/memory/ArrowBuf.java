package org.apache.arrow.memory;

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;

/**
 * A ByteBuffer-backed implementation of ArrowBuf that does not use unsafe memory operations. This
 * implementation uses standard java.nio.ByteBuffer for all memory operations instead of
 * MemoryUtil/Unsafe-based direct memory access.
 */
public class ArrowBuf {

  private static final int SHORT_SIZE = Short.BYTES;
  private static final int INT_SIZE = Integer.BYTES;
  private static final int FLOAT_SIZE = Float.BYTES;
  private static final int DOUBLE_SIZE = Double.BYTES;
  private static final int LONG_SIZE = Long.BYTES;
  private static final int LOG_BYTES_PER_ROW = 10;

  private final ByteBuffer byteBuffer;
  private final ReferenceManager referenceManager;
  private final BufferManager bufferManager;
  private final int offset; // offset within the underlying ByteBuffer for sliced buffers
  private volatile long capacity;
  private long readerIndex;
  private long writerIndex;

  /**
   * Constructs a new ArrowBuf backed by a heap ByteBuffer.
   *
   * @param referenceManager The memory manager to track memory usage and reference count
   * @param bufferManager The buffer manager for reallocation support
   * @param capacity The capacity in bytes of this buffer
   * @param memoryAddress Ignored - kept for API compatibility with parent class
   */
  public ArrowBuf(
      ReferenceManager referenceManager,
      BufferManager bufferManager,
      long capacity,
      long memoryAddress) {
    this.referenceManager = referenceManager;
    this.bufferManager = bufferManager;
    this.capacity = capacity;
    this.offset = 0;
    this.readerIndex = 0;
    this.writerIndex = 0;

    if (capacity > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("ArrowBuf does not support capacity > Integer.MAX_VALUE");
    }

    this.byteBuffer = ByteBuffer.allocate((int) capacity);
    this.byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Constructor for creating sliced views or derived buffers that share an underlying ByteBuffer.
   *
   * @param referenceManager The memory manager
   * @param bufferManager The buffer manager
   * @param byteBuffer The underlying ByteBuffer (shared with parent)
   * @param offset The offset within the ByteBuffer
   * @param capacity The capacity of this slice
   */
  ArrowBuf(
      ReferenceManager referenceManager,
      BufferManager bufferManager,
      ByteBuffer byteBuffer,
      int offset,
      long capacity) {
    this.referenceManager = referenceManager;
    this.bufferManager = bufferManager;
    this.byteBuffer = byteBuffer;
    this.offset = offset;
    this.capacity = capacity;
    this.readerIndex = 0;
    this.writerIndex = 0;
  }

  public int refCnt() {
    return referenceManager.getRefCount();
  }

  public void checkBytes(long start, long end) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(start, end - start);
    }
  }

  private void ensureAccessible() {
    if (this.refCnt() == 0) {
      throw new IllegalStateException("Ref count should be >= 1 for accessing the ArrowBuf");
    }
  }

  public ReferenceManager getReferenceManager() {
    return referenceManager;
  }

  public long capacity() {
    return capacity;
  }

  public synchronized ArrowBuf capacity(long newCapacity) {
    if (newCapacity == capacity) {
      return this;
    }

    Preconditions.checkArgument(newCapacity >= 0);

    if (newCapacity >= capacity) {
      throw new UnsupportedOperationException(
          "Buffers don't support resizing that increases the size.");
    }

    this.capacity = newCapacity;
    return this;
  }

  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  public long readableBytes() {
    Preconditions.checkState(
        writerIndex >= readerIndex, "Writer index cannot be less than reader index");
    return writerIndex - readerIndex;
  }

  public long writableBytes() {
    return capacity() - writerIndex;
  }

  public ArrowBuf slice() {
    return slice(readerIndex, readableBytes());
  }

  public ArrowBuf slice(long index, long length) {
    Preconditions.checkPositionIndex(index, this.capacity);
    Preconditions.checkPositionIndex(index + length, this.capacity);

    // Delegate to reference manager's deriveBuffer to ensure consistent behavior
    // with reference counting semantics (derived buffers share ref count with parent)
    final ArrowBuf newBuf = referenceManager.deriveBuffer(this, index, length);
    newBuf.writerIndex(length);
    return newBuf;
  }

  public ByteBuffer nioBuffer() {
    return nioBuffer(readerIndex, checkedCastToInt(readableBytes()));
  }

  public ByteBuffer nioBuffer(long index, int length) {
    chk(index, length);
    // Create a duplicate to avoid affecting the original buffer's position/limit
    ByteBuffer duplicate = byteBuffer.duplicate();
    duplicate.order(ByteOrder.LITTLE_ENDIAN);
    duplicate.position(offset + (int) index);
    duplicate.limit(offset + (int) index + length);
    return duplicate.slice().order(ByteOrder.LITTLE_ENDIAN);
  }

  public long memoryAddress() {
    // ByteBuffer-backed implementation doesn't have a direct memory address
    // Return 0 to indicate this
    return 0;
  }

  @Override
  public String toString() {
    return String.format("ArrowBuf, capacity:%d, offset:%d", capacity, offset);
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  private int bufferIndex(long index) {
    return offset + (int) index;
  }

  private void chk(long index, long length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index, length);
    }
  }

  private void checkIndexD(long index, long fieldLength) {
    ensureAccessible();
    Preconditions.checkArgument(fieldLength >= 0, "expecting non-negative data length");
    if (index < 0 || index > capacity() - fieldLength) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
    }
  }

  // --- Primitive get/set operations using ByteBuffer ---

  public long getLong(long index) {
    chk(index, LONG_SIZE);
    return byteBuffer.getLong(bufferIndex(index));
  }

  public void setLong(long index, long value) {
    chk(index, LONG_SIZE);
    byteBuffer.putLong(bufferIndex(index), value);
  }

  public float getFloat(long index) {
    chk(index, FLOAT_SIZE);
    return byteBuffer.getFloat(bufferIndex(index));
  }

  public void setFloat(long index, float value) {
    chk(index, FLOAT_SIZE);
    byteBuffer.putFloat(bufferIndex(index), value);
  }

  public double getDouble(long index) {
    chk(index, DOUBLE_SIZE);
    return byteBuffer.getDouble(bufferIndex(index));
  }

  public void setDouble(long index, double value) {
    chk(index, DOUBLE_SIZE);
    byteBuffer.putDouble(bufferIndex(index), value);
  }

  public char getChar(long index) {
    chk(index, SHORT_SIZE);
    return byteBuffer.getChar(bufferIndex(index));
  }

  public void setChar(long index, int value) {
    chk(index, SHORT_SIZE);
    byteBuffer.putChar(bufferIndex(index), (char) value);
  }

  public int getInt(long index) {
    chk(index, INT_SIZE);
    return byteBuffer.getInt(bufferIndex(index));
  }

  public void setInt(long index, int value) {
    chk(index, INT_SIZE);
    byteBuffer.putInt(bufferIndex(index), value);
  }

  public short getShort(long index) {
    chk(index, SHORT_SIZE);
    return byteBuffer.getShort(bufferIndex(index));
  }

  public void setShort(long index, int value) {
    setShort(index, (short) value);
  }

  public void setShort(long index, short value) {
    chk(index, SHORT_SIZE);
    byteBuffer.putShort(bufferIndex(index), value);
  }

  public void setByte(long index, int value) {
    chk(index, 1);
    byteBuffer.put(bufferIndex(index), (byte) value);
  }

  public void setByte(long index, byte value) {
    chk(index, 1);
    byteBuffer.put(bufferIndex(index), value);
  }

  public byte getByte(long index) {
    chk(index, 1);
    return byteBuffer.get(bufferIndex(index));
  }

  // --- Writer index based operations ---

  private void ensureWritable(final int length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      Preconditions.checkArgument(length >= 0, "expecting non-negative length");
      this.ensureAccessible();
      if (length > writableBytes()) {
        throw new IndexOutOfBoundsException(
            String.format(
                "writerIndex(%d) + length(%d) exceeds capacity(%d)",
                writerIndex, length, capacity()));
      }
    }
  }

  private void ensureReadable(final int length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      Preconditions.checkArgument(length >= 0, "expecting non-negative length");
      this.ensureAccessible();
      if (length > readableBytes()) {
        throw new IndexOutOfBoundsException(
            String.format(
                "readerIndex(%d) + length(%d) exceeds writerIndex(%d)",
                readerIndex, length, writerIndex));
      }
    }
  }

  public byte readByte() {
    ensureReadable(1);
    final byte b = getByte(readerIndex);
    ++readerIndex;
    return b;
  }

  public void readBytes(byte[] dst) {
    Preconditions.checkArgument(dst != null, "expecting valid dst bytearray");
    ensureReadable(dst.length);
    getBytes(readerIndex, dst, 0, dst.length);
    readerIndex += dst.length;
  }

  public void writeByte(byte value) {
    ensureWritable(1);
    byteBuffer.put(bufferIndex(writerIndex), value);
    ++writerIndex;
  }

  public void writeByte(int value) {
    ensureWritable(1);
    byteBuffer.put(bufferIndex(writerIndex), (byte) value);
    ++writerIndex;
  }

  public void writeBytes(byte[] src) {
    Preconditions.checkArgument(src != null, "expecting valid src array");
    writeBytes(src, 0, src.length);
  }

  public void writeBytes(byte[] src, int srcIndex, int length) {
    ensureWritable(length);
    setBytes(writerIndex, src, srcIndex, length);
    writerIndex += length;
  }

  public void writeShort(int value) {
    ensureWritable(SHORT_SIZE);
    byteBuffer.putShort(bufferIndex(writerIndex), (short) value);
    writerIndex += SHORT_SIZE;
  }

  public void writeInt(int value) {
    ensureWritable(INT_SIZE);
    byteBuffer.putInt(bufferIndex(writerIndex), value);
    writerIndex += INT_SIZE;
  }

  public void writeLong(long value) {
    ensureWritable(LONG_SIZE);
    byteBuffer.putLong(bufferIndex(writerIndex), value);
    writerIndex += LONG_SIZE;
  }

  public void writeFloat(float value) {
    ensureWritable(FLOAT_SIZE);
    byteBuffer.putFloat(bufferIndex(writerIndex), value);
    writerIndex += FLOAT_SIZE;
  }

  public void writeDouble(double value) {
    ensureWritable(DOUBLE_SIZE);
    byteBuffer.putDouble(bufferIndex(writerIndex), value);
    writerIndex += DOUBLE_SIZE;
  }

  // --- Bulk byte array operations ---

  private static boolean isOutOfBounds(long index, long length, long capacity) {
    return (index | length | (index + length) | (capacity - (index + length))) < 0;
  }

  private void checkIndex(long index, long fieldLength) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      this.ensureAccessible();
      if (isOutOfBounds(index, fieldLength, this.capacity())) {
        throw new IndexOutOfBoundsException(
            String.format(
                "index: %d, length: %d (expected: range(0, %d))",
                index, fieldLength, this.capacity()));
      }
    }
  }

  public void getBytes(long index, byte[] dst) {
    getBytes(index, dst, 0, dst.length);
  }

  public void getBytes(long index, byte[] dst, int dstIndex, int length) {
    checkIndex(index, length);
    Preconditions.checkArgument(dst != null, "expecting a valid dst byte array");
    if (isOutOfBounds(dstIndex, length, dst.length)) {
      throw new IndexOutOfBoundsException(
          "Not enough space to copy data into destination" + dstIndex);
    }
    if (length != 0) {
      // Use absolute positioning to avoid affecting buffer state
      ByteBuffer duplicate = byteBuffer.duplicate();
      duplicate.position(bufferIndex(index));
      duplicate.get(dst, dstIndex, length);
    }
  }

  public void setBytes(long index, byte[] src) {
    setBytes(index, src, 0, src.length);
  }

  public void setBytes(long index, byte[] src, int srcIndex, long length) {
    checkIndex(index, length);
    Preconditions.checkArgument(src != null, "expecting a valid src byte array");
    if (isOutOfBounds(srcIndex, length, src.length)) {
      throw new IndexOutOfBoundsException(
          "Not enough space to copy data from byte array" + srcIndex);
    }
    if (length > 0) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      duplicate.position(bufferIndex(index));
      duplicate.put(src, srcIndex, (int) length);
    }
  }

  public void getBytes(long index, ByteBuffer dst) {
    checkIndex(index, dst.remaining());
    if (dst.remaining() != 0) {
      int length = dst.remaining();
      ByteBuffer duplicate = byteBuffer.duplicate();
      duplicate.position(bufferIndex(index));
      duplicate.limit(bufferIndex(index) + length);
      dst.put(duplicate);
    }
  }

  public void setBytes(long index, ByteBuffer src) {
    checkIndex(index, src.remaining());
    int length = src.remaining();
    if (length != 0) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      duplicate.position(bufferIndex(index));
      duplicate.put(src);
    }
  }

  public void setBytes(long index, ByteBuffer src, int srcIndex, int length) {
    checkIndex(index, length);
    if (length != 0) {
      ByteBuffer srcDuplicate = src.duplicate();
      srcDuplicate.position(srcIndex);
      srcDuplicate.limit(srcIndex + length);

      ByteBuffer duplicate = byteBuffer.duplicate();
      duplicate.position(bufferIndex(index));
      duplicate.put(srcDuplicate);
    }
  }

  public void getBytes(long index, ArrowBuf dst, long dstIndex, int length) {
    checkIndex(index, length);
    Preconditions.checkArgument(dst != null, "expecting a valid ArrowBuf");
    if (isOutOfBounds(dstIndex, length, dst.capacity())) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index: %d, length: %d (expected: range(0, %d))", dstIndex, length, dst.capacity()));
    }
    if (length != 0) {
      byte[] tmp = new byte[length];
      getBytes(index, tmp, 0, length);
      dst.setBytes(dstIndex, tmp, 0, length);
    }
  }

  public void setBytes(long index, ArrowBuf src, long srcIndex, long length) {
    checkIndex(index, length);
    Preconditions.checkArgument(src != null, "expecting a valid ArrowBuf");
    if (isOutOfBounds(srcIndex, length, src.capacity())) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index: %d, length: %d (expected: range(0, %d))", srcIndex, length, src.capacity()));
    }
    if (length != 0) {
      byte[] tmp = new byte[(int) length];
      src.getBytes(srcIndex, tmp, 0, (int) length);
      setBytes(index, tmp, 0, length);
    }
  }

  public void setBytes(long index, ArrowBuf src) {
    Preconditions.checkArgument(src != null, "expecting valid ArrowBuf");
    final long length = src.readableBytes();
    checkIndex(index, length);
    byte[] tmp = new byte[(int) length];
    src.getBytes(src.readerIndex(), tmp, 0, (int) length);
    setBytes(index, tmp, 0, length);
    src.readerIndex(src.readerIndex() + length);
  }

  public int setBytes(long index, InputStream in, int length) throws IOException {
    Preconditions.checkArgument(in != null, "expecting valid input stream");
    checkIndex(index, length);
    int readBytes = 0;
    if (length > 0) {
      byte[] tmp = new byte[length];
      readBytes = in.read(tmp);
      if (readBytes > 0) {
        setBytes(index, tmp, 0, readBytes);
      }
    }
    return readBytes;
  }

  public void getBytes(long index, OutputStream out, int length) throws IOException {
    Preconditions.checkArgument(out != null, "expecting valid output stream");
    checkIndex(index, length);
    if (length > 0) {
      byte[] tmp = new byte[length];
      getBytes(index, tmp, 0, length);
      out.write(tmp);
    }
  }

  public void close() {
    referenceManager.release();
  }

  public long getPossibleMemoryConsumed() {
    return referenceManager.getSize();
  }

  public long getActualMemoryConsumed() {
    return referenceManager.getAccountedSize();
  }

  public String toHexString(final long start, final int length) {
    final long roundedStart = (start / LOG_BYTES_PER_ROW) * LOG_BYTES_PER_ROW;

    final StringBuilder sb = new StringBuilder("buffer byte dump\n");
    long index = roundedStart;
    for (long nLogged = 0; nLogged < length; nLogged += LOG_BYTES_PER_ROW) {
      sb.append(String.format(" [%05d-%05d]", index, index + LOG_BYTES_PER_ROW - 1));
      for (int i = 0; i < LOG_BYTES_PER_ROW; ++i) {
        try {
          final byte b = getByte(index++);
          sb.append(String.format(" 0x%02x", b));
        } catch (IndexOutOfBoundsException ioob) {
          sb.append(" <ioob>");
        }
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  @VisibleForTesting
  public void print(StringBuilder sb, int indent, BaseAllocator.Verbosity verbosity) {
    CommonUtil.indent(sb, indent).append(toString());
  }

  public void print(StringBuilder sb, int indent) {
    print(sb, indent, BaseAllocator.Verbosity.LOG_WITH_STACKTRACE);
  }

  public long readerIndex() {
    return readerIndex;
  }

  public long writerIndex() {
    return writerIndex;
  }

  public ArrowBuf readerIndex(long readerIndex) {
    this.readerIndex = readerIndex;
    return this;
  }

  public ArrowBuf writerIndex(long writerIndex) {
    this.writerIndex = writerIndex;
    return this;
  }

  public ArrowBuf setZero(long index, long length) {
    if (length != 0) {
      this.checkIndex(index, length);
      // Fill with zeros using Arrays.fill on the backing array
      int startIdx = bufferIndex(index);
      int endIdx = startIdx + (int) length;
      Arrays.fill(byteBuffer.array(), startIdx, endIdx, (byte) 0);
    }
    return this;
  }

  @Deprecated
  public ArrowBuf setOne(int index, int length) {
    return setOne((long) index, (long) length);
  }

  public ArrowBuf setOne(long index, long length) {
    if (length != 0) {
      this.checkIndex(index, length);
      int startIdx = bufferIndex(index);
      int endIdx = startIdx + (int) length;
      Arrays.fill(byteBuffer.array(), startIdx, endIdx, (byte) 0xff);
    }
    return this;
  }

  public ArrowBuf reallocIfNeeded(final long size) {
    Preconditions.checkArgument(size >= 0, "reallocation size must be non-negative");
    if (this.capacity() >= size) {
      return this;
    }
    if (bufferManager != null) {
      return bufferManager.replace(this, size);
    } else {
      throw new UnsupportedOperationException(
          "Realloc is only available in the context of operator's UDFs");
    }
  }

  public ArrowBuf clear() {
    this.readerIndex = this.writerIndex = 0;
    return this;
  }

  /**
   * Returns the underlying ByteBuffer. This is useful for direct access to the buffer when needed
   * for interoperability with other ByteBuffer-based APIs.
   *
   * @return the underlying ByteBuffer
   */
  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  /**
   * Returns the offset within the underlying ByteBuffer where this buffer's data starts. This is
   * used for sliced buffers that share the same underlying ByteBuffer.
   *
   * @return the offset in bytes
   */
  public int getOffset() {
    return offset;
  }
}
