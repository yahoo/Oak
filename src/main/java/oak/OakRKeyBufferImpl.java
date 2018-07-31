/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class OakRKeyBufferImpl implements OakRBuffer {

  Chunk chunk;
  ByteBuffer byteBuffer;

  OakRKeyBufferImpl(Chunk chunk, ByteBuffer byteBuffer) {
    this.chunk = chunk;
    this.byteBuffer = byteBuffer;
  }

  @Override
  public int capacity() {
    int capacity = 0;
    if (canStartReading()) {
      capacity = byteBuffer.capacity();
      finishReading();
    }
    return capacity;
  }

  @Override
  public int position() {
    int position = 0;
    if (canStartReading()) {
      position = byteBuffer.position();
      finishReading();
    }
    return position;
  }

  @Override
  public int limit() {
    int limit = 0;
    if (canStartReading()) {
      limit = byteBuffer.limit();
      finishReading();
    }
    return limit;
  }

  @Override
  public int remaining() {
    int remaining = 0;
    if (canStartReading()) {
      remaining = byteBuffer.remaining();
      finishReading();
    }
    return remaining;
  }

  @Override
  public boolean hasRemaining() {
    boolean hasRemaining = false;
    if (canStartReading()) {
      hasRemaining = byteBuffer.hasRemaining();
      finishReading();
    }
    return hasRemaining;
  }

  @Override
  public byte get(int index) {
    byte b = (byte)0xe0;
    if (canStartReading()) {
      b = byteBuffer.get(index);
      finishReading();
    }
    return b;
  }

  @Override
  public ByteOrder order() {
    ByteOrder byteOrder = null;
    if (canStartReading()) {
      byteOrder = byteBuffer.order();
      finishReading();
    }
    return byteOrder;
  }

  @Override
  public char getChar(int index) {
    char c = '0';
    if (canStartReading()) {
      c = byteBuffer.getChar(index);
      finishReading();
    }
    return c;
  }

  @Override
  public short getShort(int index) {
    short s = 0;
    if (canStartReading()) {
      s = byteBuffer.getShort(index);
      finishReading();
    }
    return s;
  }

  @Override
  public int getInt(int index) {
    int i = 0;
    if (canStartReading()) {
      i = byteBuffer.getInt(index);
      finishReading();
    }
    return i;
  }

  @Override
  public long getLong(int index) {
    long l = 0;
    if (canStartReading()) {
      l = byteBuffer.getLong(index);
      finishReading();
    }
    return l;
  }

  @Override
  public float getFloat(int index) {
    float f = 0;
    if (canStartReading()) {
      f = byteBuffer.getFloat(index);
      finishReading();
    }
    return f;
  }

  @Override
  public double getDouble(int index) {
    double d = 0;
    if (canStartReading()) {
      d = byteBuffer.getChar(index);
      finishReading();
    }
    return d;
  }

  /**
   *
   * Returns null when the chunk is a released one and the key cannot be read
   * @throws NullPointerException if the transformer is null;
   */
  @Override
  public <T> T transform(Function<ByteBuffer, T> transformer) {
    if (transformer == null) {
      throw new NullPointerException();
    }

    T transformation = null;
    if (canStartReading()) {
      transformation = transformer.apply(byteBuffer);
      finishReading();
    }
    return transformation;
  }

  /**
   *
   * Returns false when the chunk is a released one and the key cannot be read
   * synchronization and memory management related to read start
   */
  private boolean canStartReading() {
    if (chunk.state() != Chunk.State.RELEASED) {
      chunk.readersCounter.incrementAndGet();
      if (chunk.state() == Chunk.State.RELEASED) {
        chunk.readersCounter.decrementAndGet();
        chunk.releaseKeyManager();
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * synchronization and memory management related to read end
   */
  private void finishReading() {
    chunk.readersCounter.decrementAndGet();
    chunk.releaseKeyManager();
  }
}
