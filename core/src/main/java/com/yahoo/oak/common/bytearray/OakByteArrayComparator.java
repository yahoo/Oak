/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.common.bytearray;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;

public class OakByteArrayComparator implements OakComparator<byte[]> {

    @Override
    public int compareKeys(byte[] key1, byte[] key2) {
        return compare(
                key1, 0, key1.length,
                key2, 0, key2.length
        );
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2) {
        return compare(
                serializedKey1, 0, OakByteArraySerializer.getSerializedSize(serializedKey1),
                serializedKey2, 0, OakByteArraySerializer.getSerializedSize(serializedKey2)
        );
    }

    @Override
    public int compareKeyAndSerializedKey(byte[] key, OakScopedReadBuffer serializedKey) {
        return compare(
                key, 0, key.length,
                serializedKey, 0, OakByteArraySerializer.getSerializedSize(serializedKey)
        );
    }

    public static int compare(OakScopedReadBuffer buff1, int pos1, int size1,
                              OakScopedReadBuffer buff2, int pos2, int size2) {

        int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            final int compare = Byte.compare(
                    OakByteArraySerializer.getSerializedByte(buff1, pos1 + i),
                    OakByteArraySerializer.getSerializedByte(buff2, pos2 + i)
            );
            if (compare != 0) {
                return compare;
            }
        }

        return Integer.compare(size1, size2);
    }

    public static int compare(byte[] key1, int pos1, int size1, OakScopedReadBuffer buff2, int pos2, int size2) {
        final int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            final int compare = Byte.compare(
                    key1[pos1 + i],
                    OakByteArraySerializer.getSerializedByte(buff2, pos2 + i)
            );
            if (compare != 0) {
                return compare;
            }
        }

        return Integer.compare(size1, size2);
    }

    public static int compare(byte[] key1, int pos1, int size1, byte[] key2, int pos2, int size2) {
        final int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            final int compare = Byte.compare(
                    key1[pos1 + i],
                    key2[pos2 + i]
            );
            if (compare != 0) {
                return compare;
            }
        }

        return Integer.compare(size1, size2);
    }
}
