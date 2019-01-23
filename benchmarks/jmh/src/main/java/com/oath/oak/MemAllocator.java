package com.oath.oak;

import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class MemAllocator implements OakMemoryAllocator {


    @Override
    public ByteBuffer allocate(int size) {
        return ByteBuffer.allocateDirect(size);
    }

    @Override
    public void free(ByteBuffer bb) {
        Field cleanerField = null;
        try {
            cleanerField = bb.getClass().getDeclaredField("cleaner");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        assert cleanerField != null;
        cleanerField.setAccessible(true);
        Cleaner cleaner = null;
        try {
            cleaner = (Cleaner) cleanerField.get(bb);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        assert cleaner != null;
        cleaner.clean();

    }

    @Override
    public void close() {

    }

    @Override
    public long allocated() {
        return 0;
    }
}
