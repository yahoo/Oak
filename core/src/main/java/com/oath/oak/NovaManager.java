package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class NovaManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    private ThreadIndexCalculator threadIndexCalculator;
    private List<List<Slice>> releaseLists;
    private AtomicInteger globalNovaNumber;
    private OakBlockMemoryAllocator allocator;

    NovaManager(OakBlockMemoryAllocator allocator) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalNovaNumber = new AtomicInteger(1);
        this.allocator = allocator;
    }

    @Override
    public void close() {
        allocator.close();
    }

    @Override
    public boolean isClosed() {
        return allocator.isClosed();
    }

    @Override
    public int getCurrentVersion() {
        return globalNovaNumber.get();
    }

    @Override
    public long allocated() {
        return allocator.allocated();
    }

    @Override
    public Slice allocateSlice(int size, Allocate allocate) {
        Slice s = allocator.allocateSlice(size, allocate);
        s.setVersion(globalNovaNumber.get());
        assert s.getByteBuffer().remaining() >= size;
        return s;
    }

    @Override
    public void releaseSlice(Slice s) {
        int idx = threadIndexCalculator.getIndex();
        List<Slice> myReleaseList = this.releaseLists.get(idx);
        myReleaseList.add(s.duplicate());
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            globalNovaNumber.incrementAndGet();
            for (Slice releasedSlice : myReleaseList) {
                allocator.freeSlice(releasedSlice);
            }
            myReleaseList.clear();
        }
    }

    @Override
    public ByteBuffer getByteBufferFromBlockID(int blockID, int bufferPosition, int bufferLength) {
        return allocator.readByteBufferFromBlockID(blockID, bufferPosition, bufferLength);
    }

}
