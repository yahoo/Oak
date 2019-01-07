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
            int cap1 = key1.buffer.capacity();
            int cap2 = key2.buffer.capacity();
            for (int i = 0; i < cap1; i += Integer.BYTES) {
                if (i < cap2) {
                    int i1 = key1.buffer.getInt(i);
                    int i2 = key2.buffer.getInt(i);
                    if (i1 > i2) {
                        return 1;
                    } else if (i1 < i2) {
                        return -1;
                    }
                    continue;
                }
                return 1;
            }
            return 0;
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            int pos1 = serializedKey1.position();
            int cap1 = serializedKey1.getInt(pos1);
            int pos2 = serializedKey2.position();
            int cap2 = serializedKey2.getInt(pos2);
            for (int i = 0; i < cap1; i += Integer.BYTES) {
                if (i < cap2) {
                    int i1 = serializedKey1.getInt(pos1 + Integer.BYTES + i);
                    int i2 = serializedKey2.getInt(pos2 + Integer.BYTES + i);
                    if (i1 > i2) {
                        return 1;
                    } else if (i1 < i2) {
                        return -1;
                    }
                    continue;
                }
                return 1;
            }
            return 0;
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, MyBuffer key) {
            int pos1 = serializedKey.position();
            int cap1 = serializedKey.getInt(pos1);
            int cap2 = key.buffer.capacity();
            for (int i = 0; i < cap1; i += Integer.BYTES) {
                if (i < cap2) {
                    int i1 = serializedKey.getInt(pos1 + Integer.BYTES + i);
                    int i2 = key.buffer.getInt(i);
                    if (i1 > i2) {
                        return 1;
                    } else if (i1 < i2) {
                        return -1;
                    }
                    continue;
                }
                return 1;
            }
            return 0;
        }
    };

}
