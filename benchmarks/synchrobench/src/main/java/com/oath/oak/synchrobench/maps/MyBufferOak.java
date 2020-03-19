package com.oath.oak.synchrobench.maps;

import com.oath.oak.*;
import com.sun.beans.editors.ByteEditor;

import java.nio.ByteBuffer;

class MyBufferOak {

    static OakSerializer<MyBuffer> serializer = new OakSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer key, OakWBuffer targetBuffer) {
            OakUnsafeRef ref = (OakUnsafeRef) targetBuffer;
            MyBuffer.serialize(key, ref.getByteBuffer(), ref.getOffset());
        }

        @Override
        public MyBuffer deserialize(OakReadBuffer serializedKey) {
            OakUnsafeRef ref = (OakUnsafeRef) serializedKey;
            return MyBuffer.deserialize(ref.getByteBuffer(), ref.getOffset());
        }

        @Override
        public int calculateSize(MyBuffer object) {
            return object.buffer.capacity() + Integer.BYTES;
        }
    };

    static OakComparator<MyBuffer> keysComparator = new OakComparator<MyBuffer>() {
        @Override
        public int compareKeys(MyBuffer key1, MyBuffer key2) {
            return MyBuffer.compareBuffers(key1, key2);
        }

        @Override
        public int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2) {
            OakUnsafeRef keyRef1 = (OakUnsafeRef) serializedKey1;
            OakUnsafeRef keyRef2 = (OakUnsafeRef) serializedKey2;
            ByteBuffer keyBuff1 = keyRef1.getByteBuffer();
            ByteBuffer keyBuff2 = keyRef2.getByteBuffer();
            int pos1 = keyRef1.getOffset();
            int pos2 = keyRef2.getOffset();
            int cap1 = keyBuff1.getInt(pos1);
            int cap2 = keyBuff2.getInt(pos2);
            return MyBuffer.compareBuffers(keyBuff1, pos1, cap1, keyBuff2, pos2, cap2);
        }

        @Override
        public int compareKeyAndSerializedKey(MyBuffer key, OakReadBuffer serializedKey) {
            OakUnsafeRef serKeyRef = (OakUnsafeRef) serializedKey;
            ByteBuffer serKeyBuff = serKeyRef.getByteBuffer();
            int serializedKeyPosition = serKeyRef.getOffset();
            int serializedKeyLength = serKeyBuff.getInt(serializedKeyPosition);
            serializedKeyPosition += Integer.BYTES;

            int keyPosition = key.buffer.position();
            int keyLength = key.buffer.capacity();
            // The order of the arguments is crucial and should match the signature of this function
            // (compareKeyAndSerializedKey).
            // Thus key.buffer with its parameters should be passed, and only then serializedKey with its parameters.
            return MyBuffer.compareBuffers(key.buffer, keyPosition, keyLength,
                serKeyBuff, serializedKeyPosition, serializedKeyLength);
        }
    };

}
