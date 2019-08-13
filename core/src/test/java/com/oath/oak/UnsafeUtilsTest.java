package com.oath.oak;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

public class UnsafeUtilsTest {


    @Test
    public void testUnsafeCopy(){

        IntHolder minKey = new IntHolder(0, new int[0]);

        OakMapBuilder<IntHolder, IntHolder> builder = new OakMapBuilder<IntHolder, IntHolder>()
                .setKeySerializer(new UnsafeTestSerializer())
                .setValueSerializer(new UnsafeTestSerializer())
                .setMinKey(minKey)
                .setComparator(new UnsafeTestComparator());

        OakMap<IntHolder, IntHolder> oak = builder.build();

        IntHolder key1 = new IntHolder(5, new int[]{1, 2, 3, 4, 5});
        IntHolder value1 = new IntHolder(5, new int[]{1, 2, 3, 4, 5});

        IntHolder key2 = new IntHolder(6, new int[]{10, 20, 30, 40, 50, 60});
        IntHolder value2 = new IntHolder(6, new int[]{10, 20, 30, 40, 50, 60});

        oak.put(key1, value1);
        oak.put(key2, value2);


        IntHolder resValue1 = oak.get(key1);
        assertEquals(value1.size, resValue1.size);

        for (int i = 0; i< value1.size; ++i) {
            assertEquals(value1.array[i], resValue1.array[i]);
        }
        IntHolder resValue2 = oak.get(key2);
        assertEquals(value2.size, resValue2.size);
        for (int i = 0; i< value2.size; ++i) {
            assertEquals(value2.array[i], resValue2.array[i]);
        }

        Iterator<Map.Entry<OakRBuffer, OakRBuffer>> iter = oak.zc().entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<OakRBuffer, OakRBuffer> entry = iter.next();
            int size = entry.getKey().getInt(0);
            int[] dstArrayValue = new int[size];
            int[] dstArrayKey = new int[size];
            entry.getKey().unsafeCopyBufferToIntArray(Integer.BYTES, dstArrayKey, size);
            entry.getValue().unsafeCopyBufferToIntArray(Integer.BYTES, dstArrayValue, size);
            if (size == 5) {
                //value1
                Arrays.equals(value1.array, dstArrayKey);
                Arrays.equals(value1.array, dstArrayValue);
            } else if (size == 6) {
                //value2
                Arrays.equals(value2.array, dstArrayKey);
                Arrays.equals(value2.array, dstArrayValue);
            } else {
                fail();
            }
        }

    }


    private static class IntHolder{

        private final int size;
        private final int[] array;

        private IntHolder(int size, int[] array) {
            this.size = size;
            this.array = array;
        }
        public int getSize() {
            return size;
        }

        public int[] getArray() {
            return array;
        }
    }

    public static class UnsafeTestSerializer implements OakSerializer<IntHolder> {

        @Override
        public void serialize(IntHolder object, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), object.size);
            UnsafeUtils.unsafeCopyIntArrayToBuffer(object.array, targetBuffer, targetBuffer.position() + Integer.BYTES, object.size);
        }

        @Override
        public IntHolder deserialize(ByteBuffer byteBuffer) {
            int size = byteBuffer.getInt(byteBuffer.position());
            int[] array = new int[size];
            UnsafeUtils.unsafeCopyBufferToIntArray(byteBuffer, byteBuffer.position() + Integer.BYTES, array, size);
            return new IntHolder(size, array);
        }

        @Override
        public int calculateSize(IntHolder object) {
            return object.size*Integer.BYTES + Integer.BYTES;
        }
    }
    public static class UnsafeTestComparator implements OakComparator<IntHolder> {


        @Override
        public int compareKeys(IntHolder key1, IntHolder key2) {
            int pos = 0;
            while (pos < key1.size && pos < key2.size) {
                if (key1.array[pos] != key2.array[pos]) {
                    return key1.array[pos] - key2.array[pos];
                }
                pos++;
            }
            return key1.size - key2.size;
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            return compareKeys(new UnsafeTestSerializer().deserialize(serializedKey1),
                    new UnsafeTestSerializer().deserialize(serializedKey1));
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, IntHolder key) {
            return compareKeys(new UnsafeTestSerializer().deserialize(serializedKey),key);
        }
    }

    @Test
    public void testIntsToLong(){
        int i1 = 1, i2 = 2;
        long combine = UnsafeUtils.intsToLong(i1, i2);
        int[] res = UnsafeUtils.longToInts(combine);
        assertEquals(i2, res[0]);
        assertEquals(i1, res[1]);
        i2 = -2;
        combine = UnsafeUtils.intsToLong(i1, i2);
        res = UnsafeUtils.longToInts(combine);
        assertEquals(i2, res[0]);
        assertEquals(i1, res[1]);
        i1 = -1;
        combine = UnsafeUtils.intsToLong(i1, i2);
        res = UnsafeUtils.longToInts(combine);
        assertEquals(i2, res[0]);
        assertEquals(i1, res[1]);
        i2 = 2;
        combine = UnsafeUtils.intsToLong(i1, i2);
        res = UnsafeUtils.longToInts(combine);
        assertEquals(i2, res[0]);
        assertEquals(i1, res[1]);
    }

}
