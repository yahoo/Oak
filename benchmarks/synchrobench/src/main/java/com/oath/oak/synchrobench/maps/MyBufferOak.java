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
        private int compare(ByteBuffer buffer1, int pos1, int cap1, ByteBuffer buffer2, int pos2, int cap2) {
            for (int i = 0; i < cap1; i += Integer.BYTES) {
                if (i >= cap2) return 1;
                int cmp = Integer.compare(buffer1.getInt(pos1 + i), buffer2.getInt(pos2 + i));
                if (cmp != 0) return cmp;
            }
            return 0;
        }

        @Override
        public int compareKeys(MyBuffer key1, MyBuffer key2) {
            int cap1 = key1.buffer.capacity();
            int cap2 = key2.buffer.capacity();
            return compare(key1.buffer, key1.buffer.position(), cap1, key2.buffer, key2.buffer.position(), cap2);
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            int pos1 = serializedKey1.position();
            int cap1 = serializedKey1.getInt(pos1);
            int pos2 = serializedKey2.position();
            int cap2 = serializedKey2.getInt(pos2);
            return compare(serializedKey1, pos1, cap1, serializedKey2, pos2, cap2);
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, MyBuffer key) {
            int pos1 = serializedKey.position();
            int cap1 = serializedKey.getInt(pos1);
            int pos2 = key.buffer.position();
            int cap2 = key.buffer.capacity();
            return compare(serializedKey, pos1 + Integer.BYTES, cap1, key.buffer, pos2, cap2);

        }
    };

}
