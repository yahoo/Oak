/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakMapBuilder;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.common.buffer.OakBufferComparator;
import com.yahoo.oak.common.buffer.OakBufferSerializer;
import com.yahoo.oak.common.bytearray.OakByteArrayComparator;
import com.yahoo.oak.common.bytearray.OakByteArraySerializer;
import com.yahoo.oak.common.intbuffer.OakIntBufferComparator;
import com.yahoo.oak.common.intbuffer.OakIntBufferSerializer;
import com.yahoo.oak.common.integer.OakIntComparator;
import com.yahoo.oak.common.integer.OakIntSerializer;
import com.yahoo.oak.common.string.OakStringComparator;
import com.yahoo.oak.common.string.OakStringSerializer;

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
    // Byte array factories
    // #####################################################################################

    public static final OakComparator<byte[]> DEFAULT_BYTE_ARRAY_COMPARATOR = new OakByteArrayComparator();
    public static final OakSerializer<byte[]> DEFAULT_BYTE_ARRAY_SERIALIZER = new OakByteArraySerializer();

    public static OakMapBuilder<byte[], byte[]> getDefaultByteArrayBuilder() {
        return new OakMapBuilder<>(
                DEFAULT_BYTE_ARRAY_COMPARATOR, DEFAULT_BYTE_ARRAY_SERIALIZER, DEFAULT_BYTE_ARRAY_SERIALIZER,
                new byte[] {});
    }


    // #####################################################################################
    // Int buffer factories
    // #####################################################################################

    public static OakMapBuilder<ByteBuffer, ByteBuffer> getDefaultIntBufferBuilder(int keySize, int valueSize) {
        ByteBuffer minKey = ByteBuffer.allocate(keySize * Integer.BYTES);
        int index = 0;
        for (int i = 0; i < keySize; i++) {
            minKey.putInt(index, Integer.MIN_VALUE);
            index += Integer.BYTES;
        }
        minKey.position(0);

        return new OakMapBuilder<>(new OakIntBufferComparator(keySize),
                new OakIntBufferSerializer(keySize), new OakIntBufferSerializer(valueSize), minKey);
    }


    // #####################################################################################
    // Buffer factories
    // #####################################################################################

    public static OakMapBuilder<ByteBuffer, ByteBuffer> getDefaultBufferBuilder(int keySize, int valueSize) {
        ByteBuffer minKey = ByteBuffer.allocate(keySize);
        for (int i = 0; i < keySize; i++) {
            minKey.put(i, Byte.MIN_VALUE);
        }
        minKey.position(0);

        return new OakMapBuilder<>(new OakBufferComparator(keySize),
            new OakBufferSerializer(keySize), new OakBufferSerializer(valueSize), minKey);
    }
}
