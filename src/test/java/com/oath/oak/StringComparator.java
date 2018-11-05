package com.oath.oak;


import java.nio.ByteBuffer;

class StringComparator implements OakComparator<String>{

    @Override
    public int compareKeys(String key1, String key2) {
        int result = key1.compareTo(key2);
        if (result > 0)
            return 1;
        else if (result < 0)
            return -1;
        else
            return 0;
    }

    @Override
    public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {

        int size1 = serializedKey1.getInt(serializedKey1.position());
        StringBuilder key1 = new StringBuilder(size1);
        for (int i = 0; i < size1; i++) {
            char c = serializedKey1.getChar(Integer.BYTES + serializedKey1.position() + i*Character.BYTES);
            key1.append(c);
        }

        int size2 = serializedKey2.getInt(serializedKey2.position());
        StringBuilder key2 = new StringBuilder(size2);
        for (int i = 0; i < size2; i++) {
            char c = serializedKey2.getChar(Integer.BYTES + serializedKey2.position() + i*Character.BYTES);
            key2.append(c);
        }

        return compareKeys(key1.toString(), key2.toString());
    }

    @Override
    public int compareSerializedKeyAndKey(ByteBuffer serializedKey, String key) {
        int size = serializedKey.getInt(serializedKey.position());
        StringBuilder key1 = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char c = serializedKey.getChar(Integer.BYTES + serializedKey.position() + i*Character.BYTES);
            key1.append(c);
        }
        return compareKeys(key1.toString(), key);
    }

}
