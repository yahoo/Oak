/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.intbuffer;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;

import java.nio.ByteBuffer;

public class OakIntBufferComparator implements OakComparator<ByteBuffer> {

    private final int size;

    public OakIntBufferComparator(int size) {
        this.size = size;
    }

    @Override
    public int compareKeys(ByteBuffer buff1, ByteBuffer buff2) {
        return compare(buff1, 0, size, buff2, 0, size);
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
        return compare(serializedKey1, 0, size, serializedKey2, 0, size);
    }

    @Override
    public int compareKeyAndSerializedKey(ByteBuffer key, OakScopedReadBuffer serializedKey) {
        return compare(key, 0, size, serializedKey, 0, size);
    }

    public static int compare(OakScopedReadBuffer buff1, int pos1, int size1, 
        OakScopedReadBuffer buff2, int pos2, int size2) {
        
        int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            int i1 = buff1.getInt(pos1 + Integer.BYTES * i);
            int i2 = buff2.getInt(pos2 + Integer.BYTES * i);
            int compare = Integer.compare(i1, i2);
            if (compare != 0) {
                return compare;
            }
        }

        return Integer.compare(size1, size2);
    }
    
    public static int compare(ByteBuffer buff1, int pos1, int size1, OakScopedReadBuffer buff2, int pos2, int size2) {
        final int minSize = Math.min(size1, size2);

        int offset = 0;
        for (int i = 0; i < minSize; i++) {
            int i1 = buff1.getInt(pos1 + offset);
            int i2 = buff2.getInt(pos2 + offset);
            int compare = Integer.compare(i1, i2);
            if (compare != 0) {
                return compare;
            }
            offset += Integer.BYTES;
        }

        return Integer.compare(size1, size2);
    }
    
    public static int compare(ByteBuffer buff1, int pos1, int size1, ByteBuffer buff2, int pos2, int size2) {
        final int minSize = Math.min(size1, size2);

        int offset = 0;
        for (int i = 0; i < minSize; i++) {
            int i1 = buff1.getInt(pos1 + offset);
            int i2 = buff2.getInt(pos2 + offset);
            int compare = Integer.compare(i1, i2);
            if (compare != 0) {
                return compare;
            }
            offset += Integer.BYTES;
        }

        return Integer.compare(size1, size2);
    }
}
