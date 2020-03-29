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
import java.util.Comparator;

public class OakCommonFactory {

    // #####################################################################################
    // Integers factories
    // #####################################################################################

    public static final OakComparator<Integer> defaultIntComparator = new OakIntComparator();
    public static final OakSerializer<Integer> defaultIntSerializer = new OakIntSerializer();

    public static OakMapBuilder<Integer, Integer> getDefaultIntBuilder() {
        return new OakMapBuilder<>(
            defaultIntComparator, defaultIntSerializer, defaultIntSerializer, Integer.MIN_VALUE);
    }


    // #####################################################################################
    // String factories
    // #####################################################################################

    public static final OakComparator<String> defaultStringComparator = new OakStringComparator();
    public static final OakSerializer<String> defaultStringSerializer = new OakStringSerializer();

    public static OakMapBuilder<String, String> getDefaultStringBuilder() {
        return new OakMapBuilder<>(
            defaultStringComparator, defaultStringSerializer, defaultStringSerializer, "");
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
