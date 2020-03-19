package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ToolsFactory {

    // #####################################################################################
    // Integers factories
    // #####################################################################################

    public static Comparator<ByteBuffer> getByteBufferIntComparator() {
        return new Comparator<ByteBuffer>() {
            @Override
            public int compare(ByteBuffer bb1, ByteBuffer bb2) {
                int i1 = bb1.getInt(bb1.position());
                int i2 = bb2.getInt(bb2.position());
                if (i1 > i2) {
                    return 1;
                } else if (i1 < i2) {
                    return -1;
                } else {
                    return 0;
                }
            }

        };
    }

    public static OakSerializer<Integer> getOakIntSerializable() {
        return getOakIntSerializable(Integer.BYTES);
    }

    public static OakSerializer<Integer> getOakIntSerializable(final int size) {
        return new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer value, OakWBuffer targetBuffer) {
                targetBuffer.putInt(0, value);
            }

            @Override
            public Integer deserialize(OakReadBuffer serializedValue) {
                return serializedValue.getInt(0);
            }

            @Override
            public int calculateSize(Integer value) {
                return size;
            }
        };
    }

    static public OakComparator<Integer> getOakIntComparator() {
        return new OakComparator<Integer> () {
            @Override
            public int compareKeys (Integer key1, Integer key2){
                return Integer.compare(key1, key2);
            }

            @Override
            public int compareSerializedKeys (OakReadBuffer serializedKey1, OakReadBuffer serializedKey2){
                int int1 = serializedKey1.getInt(0);
                int int2 = serializedKey2.getInt(0);
                return Integer.compare(int1, int2);
            }

            @Override
            public int compareKeyAndSerializedKey (Integer key, OakReadBuffer serializedKey){
                int int1 = serializedKey.getInt(0);
                return Integer.compare(key, int1);
            }
        };
    }

    public static OakComparator<Integer> defaultIntComparator = ToolsFactory.getOakIntComparator();
    public static OakSerializer<Integer> defaultIntSerializer = ToolsFactory.getOakIntSerializable();

    public static OakMapBuilder<Integer, Integer> getDefaultIntBuilder() {
        return new OakMapBuilder<Integer, Integer>(
            defaultIntComparator, defaultIntSerializer, defaultIntSerializer, Integer.MIN_VALUE);
    }


    // #####################################################################################
    // String factories
    // #####################################################################################

    public static class StringComparator implements OakComparator<String>{

        @Override
        public int compareKeys(String key1, String key2) {
            return key1.compareTo(key2);
        }

        @Override
        public int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2) {

            int size1 = serializedKey1.getInt(0);
            int size2 = serializedKey2.getInt(0);

            int it=0;
            while (it < size1 && it < size2) {
                char c1 = serializedKey1.getChar(Integer.BYTES + it*Character.BYTES);
                char c2 = serializedKey2.getChar(Integer.BYTES + it*Character.BYTES);
                int compare = Character.compare(c1, c2);
                if (compare != 0) {
                    return compare;
                }
                it++;
            }

            if (it == size1 && it == size2) {
                return 0;
            } else if (it == size1) {
                return -1;
            } else
                return 1;
        }

        @Override
        public int compareKeyAndSerializedKey(String key, OakReadBuffer serializedKey) {
            int size1 = key.length();
            int size2 = serializedKey.getInt(0);

            int it=0;
            while (it < size1 && it < size2) {
                char c1 = key.charAt(it);
                char c2 = serializedKey.getChar(Integer.BYTES + it * Character.BYTES);
                int compare = Character.compare(c1, c2);
                if (compare != 0) {
                    return compare;
                }
                it++;
            }

            if (it == size1 && it == size2) {
                return 0;
            } else if (it == size1) {
                return -1;
            } else
                return 1;
        }

    }

    public static class StringSerializer implements OakSerializer<String> {

        @Override
        public void serialize(String object, OakWBuffer targetBuffer) {
            targetBuffer.putInt(0, object.length());
            for (int i = 0; i < object.length(); i++) {
                char c = object.charAt(i);
                targetBuffer.putChar(Integer.BYTES + i * Character.BYTES, c);
            }
        }

        @Override
        public String deserialize(OakReadBuffer byteBuffer) {
            int size = byteBuffer.getInt(0);
            StringBuilder object = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = byteBuffer.getChar(Integer.BYTES + i * Character.BYTES);
                object.append(c);
            }
            return object.toString();
        }

        @Override
        public int calculateSize(String object) {
            return Integer.BYTES + object.length() * Character.BYTES;
        }
    }

    public static OakTransformer<String> getOakStringTransformer() {
        return serialized -> {
            int size = serialized.getInt(0);
            StringBuilder object = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = serialized.getChar(Integer.BYTES + i * Character.BYTES);
                object.append(c);
            }
            return object.toString();
        };
    }
}
