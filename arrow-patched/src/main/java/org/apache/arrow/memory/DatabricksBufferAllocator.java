package org.apache.arrow.memory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.util.Preconditions;

/**
 * A BufferAllocator implementation that uses DatabricksArrowBuf for memory allocation. This
 * allocator uses heap-based ByteBuffer storage instead of direct/off-heap memory, avoiding the need
 * for sun.misc.Unsafe operations.
 *
 * <p>This implementation is suitable for environments where direct memory access is restricted or
 * where heap-based memory management is preferred.
 */
public class DatabricksBufferAllocator implements BufferAllocator {

  private final String name;
  private final AtomicLong allocatedMemory = new AtomicLong(0);
  private final AtomicLong peakMemory = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AllocationListener listener;
  private final DatabricksBufferAllocator parent;
  private final Set<DatabricksBufferAllocator> children = ConcurrentHashMap.newKeySet();
  private final RoundingPolicy roundingPolicy;
  private final long initReservation;
  private volatile long limit;

  // Empty buffer singleton
  private final ArrowBuf emptyBuffer;

  /** Creates a root allocator with default settings. */
  public DatabricksBufferAllocator() {
    this("ROOT", Long.MAX_VALUE);
  }

  /**
   * Creates a root allocator with specified limit.
   *
   * @param name the allocator name
   * @param maxAllocation the maximum allocation limit
   */
  public DatabricksBufferAllocator(String name, long maxAllocation) {
    this(
        name,
        AllocationListener.NOOP,
        0,
        maxAllocation,
        null,
        DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY);
  }

  /**
   * Creates an allocator with full configuration.
   *
   * @param name the allocator name
   * @param listener the allocation listener
   * @param initReservation initial reservation (not used in this implementation but stored)
   * @param maxAllocation the maximum allocation limit
   * @param parent the parent allocator (null for root)
   * @param roundingPolicy the rounding policy for buffer sizes
   */
  public DatabricksBufferAllocator(
      String name,
      AllocationListener listener,
      long initReservation,
      long maxAllocation,
      DatabricksBufferAllocator parent,
      RoundingPolicy roundingPolicy) {
    this.name = name;
    this.listener = listener;
    this.initReservation = initReservation;
    this.limit = maxAllocation;
    this.parent = parent;
    this.roundingPolicy = roundingPolicy;

    // Create an empty buffer with a no-op reference manager
    this.emptyBuffer = new ArrowBuf(ReferenceManager.NO_OP, null, 0, 0);
  }

  @Override
  public ArrowBuf buffer(long size) {
    return buffer(size, null);
  }

  @Override
  public ArrowBuf buffer(long size, BufferManager manager) {
    assertOpen();
    Preconditions.checkArgument(size >= 0, "Buffer size must be non-negative");

    if (size == 0) {
      return getEmpty();
    }

    // Apply rounding policy
    long actualSize = roundingPolicy.getRoundedSize(size);

    // Check if allocation is within limits
    listener.onPreAllocation(actualSize);

    if (!tryReserve(actualSize)) {
      AllocationOutcome outcome = new AllocationOutcome(AllocationOutcome.Status.FAILED_LOCAL);

      if (!listener.onFailedAllocation(actualSize, outcome)) {
        throw new OutOfMemoryException(
            String.format(
                "Unable to allocate buffer of size %d (allocated: %d, limit: %d) in allocator %s",
                actualSize, getAllocatedMemory(), getLimit(), getName()));
      }

      // Listener indicated retry is possible, try again
      if (!tryReserve(actualSize)) {
        throw new OutOfMemoryException(
            String.format(
                "Unable to allocate buffer of size %d after retry in allocator %s",
                actualSize, getName()));
      }
    }

    // Create the reference manager and buffer
    DatabricksReferenceManager refManager =
        new DatabricksReferenceManager(this, actualSize, manager);
    ArrowBuf buf = new ArrowBuf(refManager, manager, actualSize, 0);

    // Set capacity to requested size (may be smaller than actual allocation due to rounding)
    buf.capacity(size);

    listener.onAllocation(actualSize);

    return buf;
  }

  /**
   * Attempts to reserve memory for allocation.
   *
   * @param size the size to reserve
   * @return true if reservation succeeded
   */
  private boolean tryReserve(long size) {
    // Check parent first if we have one
    if (parent != null && !parent.tryReserve(size)) {
      return false;
    }

    while (true) {
      long current = allocatedMemory.get();
      long newValue = current + size;

      if (newValue > limit) {
        // Release parent reservation if we can't allocate locally
        if (parent != null) {
          parent.releaseBytes(size);
        }
        return false;
      }

      if (allocatedMemory.compareAndSet(current, newValue)) {
        // Update peak memory
        updatePeakMemory(newValue);
        return true;
      }
    }
  }

  private void updatePeakMemory(long current) {
    while (true) {
      long peak = peakMemory.get();
      if (current <= peak || peakMemory.compareAndSet(peak, current)) {
        break;
      }
    }
  }

  @Override
  public BufferAllocator getRoot() {
    if (parent == null) {
      return this;
    }
    return parent.getRoot();
  }

  @Override
  public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
    return newChildAllocator(name, AllocationListener.NOOP, initReservation, maxAllocation);
  }

  @Override
  public BufferAllocator newChildAllocator(
      String name, AllocationListener listener, long initReservation, long maxAllocation) {
    assertOpen();

    DatabricksBufferAllocator child =
        new DatabricksBufferAllocator(
            name, listener, initReservation, maxAllocation, this, roundingPolicy);

    children.add(child);
    this.listener.onChildAdded(this, child);

    return child;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    // Close all children first
    for (DatabricksBufferAllocator child : children) {
      child.close();
    }
    children.clear();

    // Check for memory leaks
    long allocated = allocatedMemory.get();
    if (allocated != 0) {
      throw new IllegalStateException(
          String.format("Allocator %s closed with %d bytes still allocated", name, allocated));
    }

    // Remove from parent's children list
    if (parent != null) {
      parent.children.remove(this);
      parent.listener.onChildRemoved(parent, this);
    }
  }

  @Override
  public long getAllocatedMemory() {
    return allocatedMemory.get();
  }

  @Override
  public long getLimit() {
    return limit;
  }

  @Override
  public long getInitReservation() {
    return initReservation;
  }

  @Override
  public void setLimit(long newLimit) {
    this.limit = newLimit;
  }

  @Override
  public long getPeakMemoryAllocation() {
    return peakMemory.get();
  }

  @Override
  public long getHeadroom() {
    long localHeadroom = limit - allocatedMemory.get();
    if (parent != null) {
      return Math.min(localHeadroom, parent.getHeadroom());
    }
    return localHeadroom;
  }

  @Override
  public boolean forceAllocate(long size) {
    if (parent != null) {
      parent.forceAllocate(size);
    }
    long newValue = allocatedMemory.addAndGet(size);
    updatePeakMemory(newValue);
    return newValue <= limit;
  }

  @Override
  public void releaseBytes(long size) {
    allocatedMemory.addAndGet(-size);
    if (parent != null) {
      parent.releaseBytes(size);
    }
    listener.onRelease(size);
  }

  @Override
  public AllocationListener getListener() {
    return listener;
  }

  @Override
  public BufferAllocator getParentAllocator() {
    return parent;
  }

  @Override
  public Collection<BufferAllocator> getChildAllocators() {
    return Collections.unmodifiableSet(children);
  }

  @Override
  public AllocationReservation newReservation() {
    assertOpen();
    return new DatabricksAllocationReservation();
  }

  @Override
  public ArrowBuf getEmpty() {
    return emptyBuffer;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isOverLimit() {
    if (allocatedMemory.get() > limit) {
      return true;
    }
    if (parent != null) {
      return parent.isOverLimit();
    }
    return false;
  }

  @Override
  public String toVerboseString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Allocator(").append(name).append(") ");
    sb.append(allocatedMemory.get()).append("/").append(limit);
    sb.append(" bytes (peak: ").append(peakMemory.get()).append(")");
    if (!children.isEmpty()) {
      sb.append("\n  Children:\n");
      for (DatabricksBufferAllocator child : children) {
        sb.append("    ").append(child.toVerboseString().replace("\n", "\n    ")).append("\n");
      }
    }
    return sb.toString();
  }

  @Override
  public void assertOpen() {
    if (closed.get()) {
      throw new IllegalStateException("Allocator " + name + " is closed");
    }
  }

  @Override
  public RoundingPolicy getRoundingPolicy() {
    return roundingPolicy;
  }

  @Override
  public ArrowBuf wrapForeignAllocation(ForeignAllocation allocation) {
    throw new UnsupportedOperationException(
        "DatabricksBufferAllocator does not support foreign allocations");
  }

  /**
   * A ReferenceManager implementation for ArrowBuf that tracks reference counts and releases memory
   * back to the allocator when the count reaches zero.
   */
  private class DatabricksReferenceManager implements ReferenceManager {

    private final DatabricksBufferAllocator allocator;
    private final long size;
    private final BufferManager bufferManager;
    private final AtomicInteger refCount = new AtomicInteger(1);

    DatabricksReferenceManager(
        DatabricksBufferAllocator allocator, long size, BufferManager bufferManager) {
      this.allocator = allocator;
      this.size = size;
      this.bufferManager = bufferManager;
    }

    @Override
    public int getRefCount() {
      return refCount.get();
    }

    @Override
    public boolean release() {
      return release(1);
    }

    @Override
    public boolean release(int decrement) {
      Preconditions.checkArgument(decrement > 0, "decrement must be positive");
      int newCount = refCount.addAndGet(-decrement);
      if (newCount < 0) {
        throw new IllegalStateException("Ref count went negative: " + newCount);
      }
      if (newCount == 0) {
        allocator.releaseBytes(size);
        return true;
      }
      return false;
    }

    @Override
    public void retain() {
      retain(1);
    }

    @Override
    public void retain(int increment) {
      Preconditions.checkArgument(increment > 0, "increment must be positive");
      while (true) {
        int current = refCount.get();
        if (current <= 0) {
          throw new IllegalStateException("Cannot retain a released buffer");
        }
        if (refCount.compareAndSet(current, current + increment)) {
          break;
        }
      }
    }

    @Override
    public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
      if (targetAllocator == allocator) {
        retain();
        return srcBuffer;
      }
      // For cross-allocator retain, create a new buffer in the target allocator
      ArrowBuf newBuf = targetAllocator.buffer(srcBuffer.capacity());
      newBuf.setBytes(0, srcBuffer, 0, srcBuffer.capacity());
      newBuf.writerIndex(srcBuffer.writerIndex());
      newBuf.readerIndex(srcBuffer.readerIndex());
      return newBuf;
    }

    @Override
    public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, long index, long length) {
      // Derived buffers share the same reference count with the source buffer.
      // No retain() is called here - the ref count is shared.
      // Users who need independent lifetime must call retain() themselves.
      if (sourceBuffer instanceof ArrowBuf) {
        ArrowBuf srcBuf = (ArrowBuf) sourceBuffer;
        // Create a new buffer sharing the same ByteBuffer and reference manager
        return new ArrowBuf(
            this, bufferManager, srcBuf.getByteBuffer(), srcBuf.getOffset() + (int) index, length);
      }
      // Fallback for non-ArrowBuf: create a new buffer and copy data
      ArrowBuf newBuf = new ArrowBuf(this, bufferManager, length, 0);
      newBuf.setBytes(0, sourceBuffer, index, length);
      newBuf.writerIndex(length);
      return newBuf;
    }

    @Override
    public OwnershipTransferResult transferOwnership(
        ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
      if (targetAllocator == allocator) {
        return new OwnershipTransferResult() {
          @Override
          public boolean getAllocationFit() {
            return true;
          }

          @Override
          public ArrowBuf getTransferredBuffer() {
            retain();
            return sourceBuffer;
          }
        };
      }

      // Transfer to different allocator: allocate in target and copy
      final ArrowBuf newBuf = targetAllocator.buffer(sourceBuffer.capacity());
      newBuf.setBytes(0, sourceBuffer, 0, sourceBuffer.capacity());
      newBuf.writerIndex(sourceBuffer.writerIndex());
      newBuf.readerIndex(sourceBuffer.readerIndex());

      return new OwnershipTransferResult() {
        @Override
        public boolean getAllocationFit() {
          return true;
        }

        @Override
        public ArrowBuf getTransferredBuffer() {
          return newBuf;
        }
      };
    }

    @Override
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    public long getSize() {
      return size;
    }

    @Override
    public long getAccountedSize() {
      return size;
    }
  }

  /** An AllocationReservation implementation for cumulative allocation requests. */
  private class DatabricksAllocationReservation implements AllocationReservation {

    private final AtomicLong reservedSize = new AtomicLong(0);
    private final AtomicBoolean used = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @SuppressWarnings("removal")
    @Override
    @Deprecated
    public boolean add(int nBytes) {
      return add((long) nBytes);
    }

    @Override
    public boolean add(long nBytes) {
      assertNotUsed();
      if (nBytes < 0) {
        return false;
      }
      reservedSize.addAndGet(nBytes);
      return true;
    }

    @SuppressWarnings("removal")
    @Override
    @Deprecated
    public boolean reserve(int nBytes) {
      return reserve((long) nBytes);
    }

    @Override
    public boolean reserve(long nBytes) {
      assertNotUsed();
      if (nBytes < 0) {
        return false;
      }
      // Check if reservation would exceed limits
      long currentReservation = reservedSize.get();
      long newReservation = currentReservation + nBytes;
      if (newReservation > getHeadroom() + currentReservation) {
        return false;
      }
      reservedSize.addAndGet(nBytes);
      return true;
    }

    @Override
    public ArrowBuf allocateBuffer() {
      assertNotUsed();
      if (!used.compareAndSet(false, true)) {
        throw new IllegalStateException("Reservation already used");
      }
      long size = reservedSize.get();
      if (size == 0) {
        return getEmpty();
      }
      return buffer(size);
    }

    @Override
    public int getSize() {
      return (int) Math.min(reservedSize.get(), Integer.MAX_VALUE);
    }

    @Override
    public long getSizeLong() {
      return reservedSize.get();
    }

    @Override
    public boolean isUsed() {
      return used.get();
    }

    @Override
    public boolean isClosed() {
      return closed.get();
    }

    @Override
    public void close() {
      closed.set(true);
    }

    private void assertNotUsed() {
      if (used.get()) {
        throw new IllegalStateException("Reservation already used");
      }
      if (closed.get()) {
        throw new IllegalStateException("Reservation is closed");
      }
    }
  }
}
