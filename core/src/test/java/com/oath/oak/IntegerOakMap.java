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

        // hash function from serialized version of the object to an integer
        public int serializedHash(ByteBuffer serializedObj) {
            return serializedObj.getInt(serializedObj.position());
        }

        // hash function from a key to an integer
        public int hash(Integer obj) {
            return obj;
        }

    };

    public static OakMapBuilder<Integer, Integer> getDefaultBuilder() {
        return new OakMapBuilder<Integer, Integer>()
                .setKeySerializer(serializer)
                .setValueSerializer(serializer)
                .setMinKey(Integer.MIN_VALUE)
                .setComparator(comparator);
    }

    private static int intsCompare(int int1, int int2) {
        return Integer.compare(int1, int2);
    }
}
