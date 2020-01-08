package com.oath.oak;

import java.nio.ByteBuffer;

public class IntegerOakMap {

    static OakComparator<Integer> comparator = new OakComparator<Integer>() {

        @Override
        public int compareKeys(Integer key1, Integer key2) {
            return intsCompare(key1, key2);
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            int int1 = serializedKey1.getInt(serializedKey1.position());
            int int2 = serializedKey2.getInt(serializedKey2.position());
            return intsCompare(int1, int2);
        }

        @Override
        public int compareKeyAndSerializedKey(Integer key, ByteBuffer serializedKey) {
            int int1 = serializedKey.getInt(serializedKey.position());
            return intsCompare(key, int1);
        }
    };

    static OakSerializer<Integer> serializer = new OakSerializer<Integer>() {

        @Override
        public void serialize(Integer obj, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), obj);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedObj) {
            return serializedObj.getInt(serializedObj.position());
        }

        @Override
        public int calculateSize(Integer key) {
            return Integer.BYTES;
        }

    };

    public static OakMapBuilder<Integer, Integer> getDefaultBuilder() {
        return new OakMapBuilder<Integer, Integer>(
            comparator, serializer, serializer, Integer.MIN_VALUE);
    }

    private static int intsCompare(int int1, int int2) {
        return Integer.compare(int1, int2);
    }
}
