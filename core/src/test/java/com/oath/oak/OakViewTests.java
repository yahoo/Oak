package com.oath.oak;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.function.Function;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class OakViewTests {

    private OakMap<String, String> oak;
    private static final int ELEMENTS = 1000;

    @Before
    public void init() {
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(100)
                .setChunkBytesPerItem(128)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");

        oak =  builder.build();

        for (int i = 0; i < ELEMENTS; i++) {
            String key = String.valueOf(i);
            String val = String.valueOf(i);
            oak.put(key, val);
        }
    }

    @After
    public void tearDown() {
        oak.close();
    }

    @Test
    public void uniTestOakRBuffer(){


        Function<ByteBuffer, String> deserialize = (byteBuffer) -> {
            int size = byteBuffer.getInt(0);
            StringBuilder object = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = byteBuffer.getChar(Integer.BYTES + byteBuffer.position() + i * Character.BYTES);
                object.append(c);
            }
            return object.toString();
        };

        try(OakBufferView<String> oakBV = oak.createBufferView()) {
            String testVal = String.valueOf(123);
            ByteBuffer testValBB = ByteBuffer.allocate(Integer.BYTES + testVal.length()*Character.BYTES);
            testValBB.putInt(0, testVal.length());
            for (int i = 0; i< testVal.length(); ++i){
                testValBB.putChar(Integer.BYTES + i*Character.BYTES, testVal.charAt(i));
            }


            OakRBuffer valBuffer = oakBV.get(testVal);
            String transformed = valBuffer.transform(deserialize);
            assertEquals(testVal, transformed);

            assertEquals(testValBB.capacity(), valBuffer.capacity());

            assertEquals(testVal.length(), valBuffer.getInt(0));
            assertEquals(testValBB.getInt(1), valBuffer.getInt(1));

            for(int i =0 ; i < testVal.length(); ++i) {
                assertEquals(testVal.charAt(i), valBuffer.getChar(i*2 + Integer.BYTES));
            }

            byte[] testValBytes = testVal.getBytes();
            for(int i =0 ; i < testValBytes.length; ++i) {
                assertEquals(testValBytes[i], valBuffer.get(i*2+1 + Integer.BYTES));
            }

            assertEquals(testValBB.getDouble(1), valBuffer.getDouble(1));

            assertEquals(testValBB.getFloat(1), valBuffer.getFloat(1));

            for(int i =0 ; i < testValBytes.length/Short.BYTES; ++i) {
                assertEquals(testValBB.getShort(i), valBuffer.getShort(i));
            }

            for(int i =0 ; i < testValBytes.length/Long.BYTES; ++i) {
                assertEquals(testValBB.getLong(i), valBuffer.getLong(i));
            }
        }
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testOakRBufferConcurrency(){
        String testVal = "987";
        try(OakBufferView<String> oakBV = oak.createBufferView()) {
            OakRBuffer valBuffer = oakBV.get(testVal);
            oak.remove(testVal);
            valBuffer.get(0);
        }
    }

    @Test
    public void testBufferViewAPIs(){
        Function<ByteBuffer, String> deserialize = (byteBuffer) -> {
            int size = byteBuffer.getInt(0);
            StringBuilder object = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = byteBuffer.getChar(Integer.BYTES + byteBuffer.position() + i * Character.BYTES);
                object.append(c);
            }
            return object.toString();
        };


        try(OakBufferView<String> oakBV = oak.createBufferView()) {

            String[] values = new String[ELEMENTS];
            for (int i = 0; i < ELEMENTS; i++) {
                values[i] = String.valueOf(i);
            }
            Arrays.sort(values);

            OakIterator<ByteBuffer> keyIterator = oakBV.keysIterator();
            for (int i = 0; i < ELEMENTS; i++) {
                ByteBuffer keyBB = keyIterator.next();
                String deserializedKey = deserialize.apply(keyBB);
                assertEquals(values[i],deserializedKey);
            }

            OakIterator<OakRBuffer> valueIterator = oakBV.valuesIterator();
            for (int i = 0; i < ELEMENTS; i++) {
                OakRBuffer valueBB = valueIterator.next();
                String deserializedValue = valueBB.transform(deserialize);
                assertEquals(values[i],deserializedValue);
            }


            OakIterator<Map.Entry<ByteBuffer, OakRBuffer>> entryIterator = oakBV.entriesIterator();
            for (int i = 0; i < ELEMENTS; i++) {
                Map.Entry<ByteBuffer, OakRBuffer> entryBB = entryIterator.next();
                String deserializedValue = entryBB.getValue().transform(deserialize);
                assertEquals(values[i],deserializedValue);
            }
        }
    }

    @Test
    public void testTransformViewAPIs(){
        Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer> transform = (entry) -> {
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());
            int size = entry.getValue().getInt(0);
            StringBuilder object = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                char c = entry.getValue().getChar(Integer.BYTES + i * Character.BYTES);
                object.append(c);
            }
            return Integer.valueOf(object.toString());
        };


        try(OakTransformView<String, Integer> oakTV = oak.createTransformView(transform)) {

            String[] values = new String[ELEMENTS];
            for (int i = 0; i < ELEMENTS; i++) {
                values[i] = String.valueOf(i);
            }
            Arrays.sort(values);

            OakIterator<Integer> entryIterator = oakTV.entriesIterator();
            for (int i = 0; i < ELEMENTS; i++) {
                Integer entryT = entryIterator.next();
                assertEquals(Integer.valueOf(values[i]),entryT);
            }
        }
    }
}
