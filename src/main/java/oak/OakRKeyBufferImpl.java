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

  private ByteBuffer byteBuffer;

  OakRKeyBufferImpl(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public int capacity() {
    return byteBuffer.capacity();
  }

  @Override
  public int position() {
    return byteBuffer.position();
  }

  @Override
  public int limit() {
    return byteBuffer.limit();
  }

  @Override
  public int remaining() {
    return byteBuffer.remaining();
  }

  @Override
  public boolean hasRemaining() {
    return byteBuffer.hasRemaining();
  }

  @Override
  public byte get(int index) {
    return byteBuffer.get(index);
  }

  @Override
  public ByteOrder order() {
    return byteBuffer.order();
  }

  @Override
  public char getChar(int index) {
    return byteBuffer.getChar(index);
  }

  @Override
  public short getShort(int index) {
    return byteBuffer.getShort(index);
  }

  @Override
  public int getInt(int index) {
    return byteBuffer.getInt(index);
  }

  @Override
  public long getLong(int index) {
    return byteBuffer.getLong(index);
  }

  @Override
  public float getFloat(int index) {
    return byteBuffer.getFloat(index);
  }

  @Override
  public double getDouble(int index) {
    return byteBuffer.getChar(index);
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

    return transformer.apply(byteBuffer);
  }

  private void startThread() {

  }

}
