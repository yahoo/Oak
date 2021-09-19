/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * The MurmurHash3 algorithm was created by Austin Appleby and placed in the public domain.
 * This java port was authored by Yonik Seeley and also placed into the public domain.
 * It has been modified by Konstantin Sobolev and, you guessed it, also placed in the public domain.
 * The author hereby disclaims copyright to this source code.
 * <p>
 * This produces exactly the same hash values as the final C++
 * version of MurmurHash3 and is thus suitable for producing the same hash values across
 * platforms.
 * <p>
 * The 32 bit x86 version of this hash should be the fastest variant for relatively short keys like ids.
 * murmurhash3_x64_128 is a good choice for longer strings or if you need more than 32 bits of hash.
 * <p>
 * Note - The x86 and x64 versions do _not_ produce the same results, as the
 * algorithms are optimized for their respective platforms.
 */
public final class MurmurHash3 {
    static final long MURMUR_128_C1 = 0x87c37b91114253d5L;
    static final long MURMUR_128_C2 = 0x4cf5ad432745937fL;

    static final int MURMUR_32_C1 = 0xcc9e2d51;
    static final int MURMUR_32_C2 = 0x1b873593;

    /**
     * 128 bits of state
     */
    public static final class HashCode128 {
        private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

        /**
         * First part of the hash, use it if you only need 64-bit hash
         */
        private long val1;
        /**
         * Second part of the hash
         */
        private long val2;

        public HashCode128(long v1, long v2) {
            val1 = v1;
            val2 = v2;
        }

        public HashCode128() {
            this(0, 0);
        }

        public byte[] getBytes() {
            return new byte[]{
                    (byte) val1,
                    (byte) (val1 >>> 8),
                    (byte) (val1 >>> 16),
                    (byte) (val1 >>> 24),
                    (byte) (val1 >>> 32),
                    (byte) (val1 >>> 40),
                    (byte) (val1 >>> 48),
                    (byte) (val1 >>> 56),
                    (byte) val2,
                    (byte) (val2 >>> 8),
                    (byte) (val2 >>> 16),
                    (byte) (val2 >>> 24),
                    (byte) (val2 >>> 32),
                    (byte) (val2 >>> 40),
                    (byte) (val2 >>> 48),
                    (byte) (val2 >>> 56),
            };
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final HashCode128 pair = (HashCode128) o;
            return val1 == pair.val1 && val2 == pair.val2;
        }

        @Override
        public int hashCode() {
            return (int) (val1 * 31 + val2);
        }

        public int toInt() {
            return (int) val1;
        }

        public long toLong() {
            return val1;
        }

        @Override
        public String toString() {
            byte[] bytes = getBytes();
            StringBuilder sb = new StringBuilder(2 * bytes.length);
            for (byte b : bytes) {
                sb.append(HEX_DIGITS[(b >>> 4) & 0xf]).append(HEX_DIGITS[b & 0xf]);
            }
            return sb.toString();
        }

        public static MurmurHash3.HashCode128 fromBytes(byte[] bytes) {
            return new HashCode128(
                    getLongLittleEndian(bytes, 0),
                    getLongLittleEndian(bytes, 8)
            );
        }
    }

    public static long fmix64(final long inputK) {
        long k = inputK;
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    /**
     * Gets a long from a byte buffer in little endian byte order.
     */
    public static long getLongLittleEndian(byte[] buf, int offset) {
        return ((long) buf[offset + 7] << 56)   // no mask needed
                | ((buf[offset + 6] & 0xffL) << 48)
                | ((buf[offset + 5] & 0xffL) << 40)
                | ((buf[offset + 4] & 0xffL) << 32)
                | ((buf[offset + 3] & 0xffL) << 24)
                | ((buf[offset + 2] & 0xffL) << 16)
                | ((buf[offset + 1] & 0xffL) << 8)
                | ((buf[offset] & 0xffL));        // no shift needed
    }

    /**
     * Gets a long from a byte buffer in little endian byte order, modified for OakBuffer
     */
    public static long getLongLittleEndian(OakBuffer buf, int offset) {
        return ((long) buf.get(offset + 7) << 56)   // no mask needed
                | ((buf.get(offset + 6) & 0xffL) << 48)
                | ((buf.get(offset + 5) & 0xffL) << 40)
                | ((buf.get(offset + 4) & 0xffL) << 32)
                | ((buf.get(offset + 3) & 0xffL) << 24)
                | ((buf.get(offset + 2) & 0xffL) << 16)
                | ((buf.get(offset + 1) & 0xffL) << 8)
                | ((buf.get(offset) & 0xffL));        // no shift needed
    }


    /**
     * Returns the MurmurHash3_x86_32 hash.
     */
    public static int murmurhash32(byte[] data, int offset, int len, int seed) {
        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data[i] & 0xff) |
                    ((data[i + 1] & 0xff) << 8) |
                    ((data[i + 2] & 0xff) << 16) |
                    (data[i + 3] << 24);
            k1 *= MURMUR_32_C1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= MURMUR_32_C2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= MURMUR_32_C1;
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= MURMUR_32_C2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    /**
     * Returns the MurmurHash3_x86_32 hash, modified for OakBuffer.
     */
    public static int murmurhash32(OakBuffer data, int offset, int len, int seed) {
        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data.get(i) & 0xff) |
                    ((data.get(i + 1) & 0xff) << 8) |
                    ((data.get(i + 2) & 0xff) << 16) |
                    (data.get(i + 3) << 24);
            k1 *= MURMUR_32_C1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= MURMUR_32_C2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (data.get(roundedEnd + 2) & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data.get(roundedEnd + 1) & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= (data.get(roundedEnd) & 0xff);
                k1 *= MURMUR_32_C1;
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= MURMUR_32_C2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    /**
     * Returns the MurmurHash3_x64_128 hash, placing the result in "out".
     */
    public static void murmurhash128(byte[] key, int offset, int len, int seed, HashCode128 out) {
        // The original algorithm does have a 32 bit unsigned seed.
        // We have to mask to match the behavior of the unsigned types and prevent sign extension.
        long h1 = seed & 0x00000000FFFFFFFFL;
        long h2 = seed & 0x00000000FFFFFFFFL;

        int roundedEnd = offset + (len & 0xFFFFFFF0);  // round down to 16 byte block
        for (int i = offset; i < roundedEnd; i += 16) {
            long k1 = getLongLittleEndian(key, i);
            long k2 = getLongLittleEndian(key, i + 8);
            k1 *= MURMUR_128_C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= MURMUR_128_C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= MURMUR_128_C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= MURMUR_128_C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        long k1 = 0;
        long k2 = 0;

        switch (len & 15) {
            case 15:
                k2 = (key[roundedEnd + 14] & 0xffL) << 48;
            case 14:
                k2 |= (key[roundedEnd + 13] & 0xffL) << 40;
            case 13:
                k2 |= (key[roundedEnd + 12] & 0xffL) << 32;
            case 12:
                k2 |= (key[roundedEnd + 11] & 0xffL) << 24;
            case 11:
                k2 |= (key[roundedEnd + 10] & 0xffL) << 16;
            case 10:
                k2 |= (key[roundedEnd + 9] & 0xffL) << 8;
            case 9:
                k2 |= (key[roundedEnd + 8] & 0xffL);
                k2 *= MURMUR_128_C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= MURMUR_128_C1;
                h2 ^= k2;
            case 8:
                k1 = ((long) key[roundedEnd + 7]) << 56;
            case 7:
                k1 |= (key[roundedEnd + 6] & 0xffL) << 48;
            case 6:
                k1 |= (key[roundedEnd + 5] & 0xffL) << 40;
            case 5:
                k1 |= (key[roundedEnd + 4] & 0xffL) << 32;
            case 4:
                k1 |= (key[roundedEnd + 3] & 0xffL) << 24;
            case 3:
                k1 |= (key[roundedEnd + 2] & 0xffL) << 16;
            case 2:
                k1 |= (key[roundedEnd + 1] & 0xffL) << 8;
            case 1:
                k1 |= (key[roundedEnd] & 0xffL);
                k1 *= MURMUR_128_C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= MURMUR_128_C2;
                h1 ^= k1;
        }

        //----------
        // finalization

        h1 ^= len;
        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        out.val1 = h1;
        out.val2 = h2;
    }

    /**
     * Returns the MurmurHash3_x64_128 hash, placing the result in "out".
     */
    public static void murmurhash128(OakBuffer key, int offset, int len, int seed, HashCode128 out) {
        // The original algorithm does have a 32 bit unsigned seed.
        // We have to mask to match the behavior of the unsigned types and prevent sign extension.
        long h1 = seed & 0x00000000FFFFFFFFL;
        long h2 = seed & 0x00000000FFFFFFFFL;

        int roundedEnd = offset + (len & 0xFFFFFFF0);  // round down to 16 byte block
        for (int i = offset; i < roundedEnd; i += 16) {
            long k1 = getLongLittleEndian(key, i);
            long k2 = getLongLittleEndian(key, i + 8);
            k1 *= MURMUR_128_C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= MURMUR_128_C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= MURMUR_128_C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= MURMUR_128_C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        long k1 = 0;
        long k2 = 0;

        switch (len & 15) {
            case 15:
                k2 = (key.get(roundedEnd + 14) & 0xffL) << 48;
            case 14:
                k2 |= (key.get(roundedEnd + 13) & 0xffL) << 40;
            case 13:
                k2 |= (key.get(roundedEnd + 12) & 0xffL) << 32;
            case 12:
                k2 |= (key.get(roundedEnd + 11) & 0xffL) << 24;
            case 11:
                k2 |= (key.get(roundedEnd + 10) & 0xffL) << 16;
            case 10:
                k2 |= (key.get(roundedEnd + 9) & 0xffL) << 8;
            case 9:
                k2 |= (key.get(roundedEnd + 8) & 0xffL);
                k2 *= MURMUR_128_C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= MURMUR_128_C1;
                h2 ^= k2;
            case 8:
                k1 = ((long) key.get(roundedEnd + 7)) << 56;
            case 7:
                k1 |= (key.get(roundedEnd + 6) & 0xffL) << 48;
            case 6:
                k1 |= (key.get(roundedEnd + 5) & 0xffL) << 40;
            case 5:
                k1 |= (key.get(roundedEnd + 4) & 0xffL) << 32;
            case 4:
                k1 |= (key.get(roundedEnd + 3) & 0xffL) << 24;
            case 3:
                k1 |= (key.get(roundedEnd + 2) & 0xffL) << 16;
            case 2:
                k1 |= (key.get(roundedEnd + 1) & 0xffL) << 8;
            case 1:
                k1 |= (key.get(roundedEnd) & 0xffL);
                k1 *= MURMUR_128_C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= MURMUR_128_C2;
                h1 ^= k1;
        }

        //----------
        // finalization

        h1 ^= len;
        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        out.val1 = h1;
        out.val2 = h2;
    }
}
