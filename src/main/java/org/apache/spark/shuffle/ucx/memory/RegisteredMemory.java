/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

package org.apache.spark.shuffle.ucx.memory;

import org.apache.spark.unsafe.memory.MemoryBlock;
import org.openucx.jucx.ucp.UcpMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Structure to use 1 memory region for multiple ByteBuffers.
 * Keeps track on reference count to memory region.
 */
public class RegisteredMemory extends MemoryBlock {
  private static final Logger logger = LoggerFactory.getLogger(RegisteredMemory.class);

  private final AtomicInteger refcount;
  private final UcpMemory memory;
  private final ByteBuffer buffer;

  RegisteredMemory(AtomicInteger refcount, UcpMemory memory, ByteBuffer buffer) {
    super(null, memory.getAddress(), memory.getLength());
    this.refcount = refcount;
    this.memory = memory;
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  AtomicInteger getRefCount() {
    return refcount;
  }

  public long getAddress() {
    return memory.getAddress();
  }

  public long getLength() {
    return memory.getLength();
  }

  void deregisterNativeMemory() {
    if (refcount.get() != 0) {
      logger.warn("De-registering memory of size {} that has active references.", buffer.capacity());
    }
    if (memory != null && memory.getNativeId() != null) {
      memory.deregister();
    }
  }
}
