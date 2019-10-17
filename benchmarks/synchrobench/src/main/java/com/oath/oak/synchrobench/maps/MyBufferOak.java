package com.oath.oak.synchrobench.maps;

import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;

class MyBufferOak {

    static OakSerializer<MyBuffer> serializer = new OakSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer key, ByteBuffer targetBuffer) {
            int cap = key.buffer.capacity();
            if(cap != 100 && cap != 4 && cap != 1000){
                throw new AssertionError();
            }
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
        private int compare(ByteBuffer buffer1, int base1, int len1, ByteBuffer buffer2, int base2, int len2) {
            int n = Math.min(len1, len2);
            for (int i = 0; i < n; i += Integer.BYTES) {
                int cmp = Integer.compare(buffer1.getInt(base1 + i), buffer2.getInt(base2 + i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return (len1 - len2);
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
        public int compareKeyAndSerializedKey(MyBuffer key, ByteBuffer serializedKey) {
            int keyPosition = key.buffer.position();
            int keyLength = key.buffer.capacity();
            int serializedKeyPosition = serializedKey.position();
            int serializedKeyLength = serializedKey.getInt(serializedKeyPosition);
            // The order of the arguments is crucial and should match the signature of this function
            // (compareKeyAndSerializedKey).
            // Thus key.buffer with its parameters should be passed, and only then serializedKey with its parameters.
            return compare(key.buffer, keyPosition, keyLength, serializedKey, serializedKeyPosition + Integer.BYTES,
                    serializedKeyLength);

        }
    };

}
