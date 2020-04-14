package com.oath.oak.common.intbuffer;

import com.oath.oak.OakReadBuffer;
import com.oath.oak.OakSerializer;
import com.oath.oak.OakUnsafeDirectBuffer;
import com.oath.oak.OakWriteBuffer;

import java.nio.ByteBuffer;

public class OakIntBufferSerializer implements OakSerializer<ByteBuffer> {

    private final int size;

    public OakIntBufferSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(ByteBuffer obj, OakWriteBuffer targetBuffer) {
        OakUnsafeDirectBuffer unsafeTarget = (OakUnsafeDirectBuffer) targetBuffer;
        copyBuffer(obj, 0, size, unsafeTarget.getByteBuffer(), unsafeTarget.getOffset());
    }

    @Override
    public ByteBuffer deserialize(OakReadBuffer byteBuffer) {
        OakUnsafeDirectBuffer unsafeBuffer = (OakUnsafeDirectBuffer) byteBuffer;

        ByteBuffer ret = ByteBuffer.allocate(getSizeBytes());
        copyBuffer(unsafeBuffer.getByteBuffer(), unsafeBuffer.getOffset(), size, ret, 0);
        ret.position(0);
        return ret;
    }

    @Override
    public int calculateSize(ByteBuffer buff) {
        return getSizeBytes();
    }

    public int getSizeBytes() {
        return size * Integer.BYTES;
    }

    public static void copyBuffer(ByteBuffer src, int srcPos, int srcSize, ByteBuffer dst, int dstPos) {
        for (int i = 0; i < srcSize; i++) {
            int data = src.getInt(srcPos + Integer.BYTES * i);
            dst.putInt(dstPos + Integer.BYTES * i, data);
        }
    }
}
