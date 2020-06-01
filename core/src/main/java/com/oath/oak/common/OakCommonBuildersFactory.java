package com.oath.oak.common;

import com.oath.oak.OakComparator;
import com.oath.oak.OakMapBuilder;
import com.oath.oak.OakSerializer;
import com.oath.oak.common.intbuffer.OakIntBufferComparator;
import com.oath.oak.common.intbuffer.OakIntBufferSerializer;
import com.oath.oak.common.integer.OakIntComparator;
import com.oath.oak.common.integer.OakIntSerializer;
import com.oath.oak.common.string.OakStringComparator;
import com.oath.oak.common.string.OakStringSerializer;

import java.nio.ByteBuffer;

public class OakCommonBuildersFactory {

    // #####################################################################################
    // Integers factories
    // #####################################################################################

    public static final OakComparator<Integer> DEFAULT_INT_COMPARATOR = new OakIntComparator();
    public static final OakSerializer<Integer> DEFAULT_INT_SERIALIZER = new OakIntSerializer();

    public static OakMapBuilder<Integer, Integer> getDefaultIntBuilder() {
        return new OakMapBuilder<>(
                DEFAULT_INT_COMPARATOR, DEFAULT_INT_SERIALIZER, DEFAULT_INT_SERIALIZER, Integer.MIN_VALUE);
    }


    // #####################################################################################
    // String factories
    // #####################################################################################

    public static final OakComparator<String> DEFAULT_STRING_COMPARATOR = new OakStringComparator();
    public static final OakSerializer<String> DEFAULT_STRING_SERIALIZER = new OakStringSerializer();

    public static OakMapBuilder<String, String> getDefaultStringBuilder() {
        return new OakMapBuilder<>(
                DEFAULT_STRING_COMPARATOR, DEFAULT_STRING_SERIALIZER, DEFAULT_STRING_SERIALIZER, "");
    }


    // #####################################################################################
    // Int buffer factories
    // #####################################################################################

    public static OakMapBuilder<ByteBuffer, ByteBuffer> getDefaultIntBufferBuilder(int keySize, int valueSize) {
        ByteBuffer minKey = ByteBuffer.allocate(keySize * Integer.BYTES);
        for (int i = 0; i < keySize; i++) {
            minKey.putInt(Integer.BYTES * i, Integer.MIN_VALUE);
        }
        minKey.position(0);

        return new OakMapBuilder<>(new OakIntBufferComparator(keySize),
                new OakIntBufferSerializer(keySize), new OakIntBufferSerializer(valueSize), minKey);
    }
}
