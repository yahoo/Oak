package com.oath.oak.synchrobench.maps;

import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;

class MyBufferOak {

    static OakSerializer<MyBuffer> serializer = new OakSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer key, ByteBuffer targetBuffer) {
            int cap = key.buffer.capacity();
            int pos = targetBuffer.position();
            // write the capacity in the beginning of the buffer
            targetBuffer.putInt(pos, cap);
            for (int i = 0; i < cap; i += Integer.BYTES) {
                targetBuffer.putInt(pos + Integer.BYTES + i, key.buffer.getInt(i));
            }
        }

        @Override
        public MyBuffer deserialize(ByteBuffer serializedKey) {
            int pos = serializedKey.position();
            int cap = serializedKey.getInt(pos);
            MyBuffer ret = new MyBuffer(cap);
            for (int i = 0; i < cap; i += Integer.BYTES) {
                ret.buffer.putInt(i, serializedKey.getInt(pos + Integer.BYTES + i));
            }
            return ret;
        }

        @Override
        public int calculateSize(MyBuffer object) {
            return object.buffer.capacity() + Integer.BYTES;
        }
    };

    static OakComparator<MyBuffer> keysComparator = new OakComparator<MyBuffer>() {

        @Override
        public int compareKeys(MyBuffer key1, MyBuffer key2) {
            return key1.buffer.compareTo(key2.buffer);
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            return serializedKey1.compareTo(serializedKey2);
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, MyBuffer key) {
            int pos = serializedKey.position();
            serializedKey.position(pos + Integer.BYTES);
            int cmp = compareSerializedKeys(serializedKey, key.buffer);
            serializedKey.position(pos);
            return cmp;
        }
    };

}
