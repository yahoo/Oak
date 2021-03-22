package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Test;

import com.yahoo.oak.common.floatnum.OakFloatSerializer;

import com.yahoo.oak.common.integer.OakIntSerializer;
import com.yahoo.oak.common.integer.OakIntComparator;


public class BuildAllTypes {

    public static int MEBIBYTE = 1024 * 1024;

    
    @Test
    public void testBuildIntFloat(){
        OakIntSerializer int_serializer = new OakIntSerializer();
        OakFloatSerializer float_serializer = new OakFloatSerializer();
        OakIntComparator int_comparator = new OakIntComparator();

        OakMapBuilder<Integer, Float> builder = new OakMapBuilder<Integer, Float>(int_comparator, int_serializer, float_serializer, Integer.MIN_VALUE)
            .setMemoryCapacity(MEBIBYTE); // 1MB in bytes

    OakMap<Integer,Float> oak = builder.build();
    
    int my_key = 0;
    float my_val = (float) 3.14;
    oak.put(my_key, my_val);

    Assert.assertTrue(oak.get(my_key)==my_val);


    }

    // TODO add all type combinations

}
