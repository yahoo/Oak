package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Test;

import com.yahoo.oak.common.floatnum.OakFloatComparator;
import com.yahoo.oak.common.floatnum.OakFloatSerializer;

import com.yahoo.oak.common.integer.OakIntSerializer;
import com.yahoo.oak.common.string.OakStringSerializer;
import com.yahoo.oak.common.integer.OakIntComparator;

public class BuildAllTypes {

    public static int MEBIBYTE = 1024 * 1024;

    @Test
    public void testBuildIntInt() {
        OakIntSerializer int_serializer = new OakIntSerializer();
        OakIntSerializer int_serializer2 = new OakIntSerializer();
        OakIntComparator int_comparator = new OakIntComparator();

        OakMapBuilder<Integer, Integer> builder = new OakMapBuilder<Integer, Integer>(int_comparator, int_serializer,
                int_serializer2, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Integer, Integer> oak = builder.build();

        int my_key = 0;
        int my_val = 1;
        oak.put(my_key, my_val);

        Assert.assertTrue(oak.get(my_key) == my_val);

    }

    @Test
    public void testBuildIntFloat() {
        OakIntSerializer int_serializer = new OakIntSerializer();
        OakFloatSerializer float_serializer = new OakFloatSerializer();
        OakIntComparator int_comparator = new OakIntComparator();

        OakMapBuilder<Integer, Float> builder = new OakMapBuilder<Integer, Float>(int_comparator, int_serializer,
                float_serializer, Integer.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Integer, Float> oak = builder.build();

        int my_key = 0;
        float my_val = (float) 3.14;
        oak.put(my_key, my_val);

        Assert.assertTrue(oak.get(my_key) == my_val);

    }

    @Test
    public void testBuildFloatString() {
        OakFloatSerializer float_serializer = new OakFloatSerializer();
        OakStringSerializer string_serializer = new OakStringSerializer();
        OakFloatComparator float_comparator = new OakFloatComparator();

        OakMapBuilder<Float, String> builder = new OakMapBuilder<Float, String>(float_comparator, float_serializer,
                string_serializer, Float.MIN_VALUE).setMemoryCapacity(MEBIBYTE); // 1MB in bytes

        OakMap<Float, String> oak = builder.build();

        float my_key = (float) 2.72;
        String my_val = "val";
        oak.put(my_key, my_val);

        Assert.assertTrue(oak.get(my_key).equals(my_val));

    }

    // TODO add all type combinations

}
