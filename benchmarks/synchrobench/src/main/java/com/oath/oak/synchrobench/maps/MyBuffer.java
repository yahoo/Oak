package com.oath.oak.synchrobench.maps;


import java.nio.ByteBuffer;

public class MyBuffer implements Comparable<MyBuffer>{

    public ByteBuffer buffer;

    public MyBuffer(int capacity) {
        buffer = ByteBuffer.allocate(capacity);
    }

    @Override // for ConcurrentSkipListMap
    public int compareTo(MyBuffer o) {
        return MyBufferOak.keysComparator.compareKeys(this, o);
    }
}
