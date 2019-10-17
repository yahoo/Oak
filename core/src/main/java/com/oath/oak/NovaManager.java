package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class NovaManager implements Closeable {
    static final int RELEASE_LIST_LIMIT = 1024;
    static final int NOVA_HEADER_SIZE = 4;
    static final int INVALID_VERSION = 0;
    private ThreadIndexCalculator threadIndexCalculator;
    private List<List<Slice>> releaseLists;
    private AtomicInteger globalNovaNumber;
    private OakNativeMemoryAllocator allocator;

    NovaManager(OakNativeMemoryAllocator allocator) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalNovaNumber = new AtomicInteger(1);
        this.allocator = allocator;
    }

    public OakNativeMemoryAllocator getAllocator(){
        return allocator;
    }

    @Override
    public void close() {
        allocator.close();
    }

    boolean isClosed() {
        return allocator.isClosed();
    }

    int getCurrentVersion() {
        return globalNovaNumber.get();
    }

    public long allocated() {
        return allocator.allocated();
    }

    Slice allocateSlice(int size, boolean isKey) {
        Slice s = allocator.allocateSlice(size, isKey);
        assert s.getByteBuffer().remaining() >= size;
        s.getByteBuffer().putInt(s.getByteBuffer().position(), getCurrentVersion());
        return s;
    }

    void releaseSlice(Slice s) {
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

    Slice getSliceFromBlockID(Integer BlockID, int bufferPosition, int bufferLength) {
        return new Slice(BlockID, getByteBufferFromBlockID(BlockID, bufferPosition, bufferLength));
    }

    ByteBuffer getByteBufferFromBlockID(Integer BlockID, int bufferPosition, int bufferLength) {
        return allocator.readByteBufferFromBlockID(BlockID, bufferPosition, bufferLength);
    }

}
