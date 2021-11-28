/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.MyBuffer;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class OffHeapList<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private ConcurrentSkipListMap<Object, Cell> skipListMap;
    private BlockMemoryAllocator allocator;
    private MemoryManager mm;
    private Comparator<Object> comparator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;

    public OffHeapList() {

        comparator = (o1, o2) -> {
            //TODO YONIGO - what if key gets dfeleted?
            if (o1 instanceof MyBuffer) {

                //o2 is a node and the key is either mybuffer or bytebuffer:
                Cell cell2 = (Cell) o2;
                Object key2 = cell2.key.get();
                if (key2 instanceof MyBuffer) {
                    return MyBuffer.compareBuffers((MyBuffer) o1, (MyBuffer) key2);
                } else {
                    return MyBuffer.compareBuffers((MyBuffer) o1, (ScopedReadBuffer) key2);
                }

            } else if (o2 instanceof MyBuffer) {
                Cell cell1 = (Cell) o1;
                Object key1 = cell1.key.get();
                if (key1 instanceof MyBuffer) {
                    return MyBuffer.compareBuffers((MyBuffer) key1, (MyBuffer) o2);
                } else {
                    return -1 * MyBuffer.compareBuffers((MyBuffer) o2, (ScopedReadBuffer) key1);
                }
            } else if (o1 instanceof OffHeapList.Cell && o2 instanceof OffHeapList.Cell) {
                Cell cell1 = (Cell) o1;
                Object key1 = cell1.key.get();
                Cell cell2 = (Cell) o2;
                Object key2 = cell2.key.get();

                if (key1 instanceof MyBuffer && key2 instanceof MyBuffer) {
                    return MyBuffer.compareBuffers((MyBuffer) key1, (MyBuffer) key2);
                } else if (key1 instanceof ScopedReadBuffer && key2 instanceof ScopedReadBuffer) {
                    return MyBuffer.compareBuffers((ScopedReadBuffer) key1, (ScopedReadBuffer) key2);
                } else if (key1 instanceof MyBuffer && key2 instanceof ScopedReadBuffer) {
                    return MyBuffer.compareBuffers((MyBuffer) key1, (ScopedReadBuffer) key2);
                } else {
                    return -1 * MyBuffer.compareBuffers((MyBuffer) key2, (ScopedReadBuffer) key1);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        };

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mm = new SeqExpandMemoryManager(allocator);
    }

    @Override
    public boolean getOak(K key) {
        Cell value = skipListMap.get(key);
        if (Parameters.confZeroCopy) {
            return value != null && value.value != null;
        } else {
            if (value != null && value.value != null) {
                MyBuffer des = MyBuffer.deserialize(value.value.get());
                return (des != null);
            } else {
                return false;
            }
        }
    }

    @Override
    public void putOak(K key, V value) {

        Cell newCell = new Cell();
        newCell.key.set(key);
        Cell prevValue = skipListMap.putIfAbsent(newCell, newCell);

        if (prevValue == null) {
            ScopedReadBuffer keybb = new ScopedReadBuffer(mm.getEmptySlice());
            ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
            allocator.allocate(keybb.getSlice(), key.calculateSerializedSize());
            ScopedWriteBuffer.serialize(keybb.getSlice(), key, MyBuffer.DEFAULT_SERIALIZER);
            newCell.key.set(keybb);
            allocator.allocate(valuebb.getSlice(), value.calculateSerializedSize());
            ScopedWriteBuffer.serialize(valuebb.getSlice(), value, MyBuffer.DEFAULT_SERIALIZER);
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb.getSlice());
            }
        } else {
            if (prevValue.value.get() == null) {
                ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
                allocator.allocate(valuebb.getSlice(), value.calculateSerializedSize());
                ScopedWriteBuffer.serialize(valuebb.getSlice(), value, MyBuffer.DEFAULT_SERIALIZER);
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.free(valuebb.getSlice());
                }
            } else {
                synchronized (prevValue.value) {
                    ScopedWriteBuffer.serialize(prevValue.value.get().getSlice(), value, MyBuffer.DEFAULT_SERIALIZER);
                }
            }
        }
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        //TODO YONIGO - this wont work with puts together.
        Cell newCell = new Cell();

        newCell.key.set(key);
        Cell prevValue = skipListMap.putIfAbsent(newCell, newCell);
        if (prevValue == null) {
            ScopedReadBuffer keybb = new ScopedReadBuffer(mm.getEmptySlice());
            ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
            allocator.allocate(keybb.getSlice(), key.calculateSerializedSize());
            ScopedWriteBuffer.serialize(keybb.getSlice(), key, MyBuffer.DEFAULT_SERIALIZER);
            newCell.key.set(keybb);
            allocator.allocate(valuebb.getSlice(), value.calculateSerializedSize());
            ScopedWriteBuffer.serialize(valuebb.getSlice(), value, MyBuffer.DEFAULT_SERIALIZER);
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb.getSlice());
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void removeOak(K key) {
        Cell val = skipListMap.remove(key);
        if (val == null) {
            return;
        }
        allocator.free(((ScopedReadBuffer) val.key.get()).getSlice());
        allocator.free(val.value.get().getSlice());
        // TODO YONIGO - need some sync here!
    }

    @Override
    public boolean computeIfPresentOak(K key) {
        return false;
    }

    @Override
    public void computeOak(K key) {

    }

    @Override
    public boolean ascendOak(K from, int length) {
        Iterator<Map.Entry<Object, Cell>> iter = skipListMap.tailMap(from, true).entrySet().iterator();
        return iterate(iter, length);
    }

    @Override
    public boolean descendOak(K from, int length) {
        Iterator<Map.Entry<Object, Cell>> iter = skipListMap.descendingMap().tailMap(from, true).entrySet().iterator();
        return iterate(iter, length);
    }

    private boolean iterate(Iterator<Map.Entry<Object, Cell>> iter, int length) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            Map.Entry<Object, Cell> cell = iter.next();
            //only if cell is not null value is not deleted or not set yet.
            if (cell.getValue().value.get() != null) {
                if (!Parameters.confZeroCopy) {
                    MyBuffer des = MyBuffer.deserialize(cell.getValue().value.get());
                    //YONIGO - I just do this so that hopefully jvm doesnt optimize out the deserialize
                    if (des != null) {
                        i++;
                    }
                } else {
                    i++;
                }

            }
        }
        return i == length;
    }

    @Override
    public void clear() {

        skipListMap.values().forEach(cell -> {
            allocator.free(((ScopedReadBuffer) cell.key.get()).getSlice());
            allocator.free(cell.value.get().getSlice());
        });
        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator.close();
        allocator = new NativeMemoryAllocator((long) Integer.MAX_VALUE * 16);
        System.gc();
    }

    @Override
    public int size() {
        return skipListMap.size();
    }


    @Override
    public void putIfAbsentComputeIfPresentOak(K key, V value) {


        Consumer<OakScopedWriteBuffer> computeFunction = writeBuffer -> {
            OakUnsafeDirectBuffer buffer = (OakUnsafeDirectBuffer) writeBuffer;
            ByteBuffer buf = buffer.getByteBuffer();
            buf.putLong(1, ~buf.getLong(1));
        };

        BiFunction<Object, Cell, Cell> fun = (prevValueO, v) -> {
            Cell prevValue = (Cell) prevValueO;
            // cell is in map but maybe not initialized yet
            if (prevValue.value.get() == null) {
                ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
                allocator.allocate(valuebb.getSlice(), value.calculateSerializedSize());
                ScopedWriteBuffer.serialize(valuebb.getSlice(), value, MyBuffer.DEFAULT_SERIALIZER);
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.free(valuebb.getSlice());
                    synchronized (prevValue.value) {
                        ScopedWriteBuffer.compute(prevValue.value.get().getSlice(), computeFunction);
                    }
                }
            } else {
                synchronized (prevValue.value) {
                    ScopedWriteBuffer.compute(prevValue.value.get().getSlice(), computeFunction);
                }
            }
            return prevValue;
        };


        Cell newCell = new Cell();
        newCell.key.set(key);

        boolean in = skipListMap.containsKey(newCell);

        Cell retval = skipListMap.merge(newCell, newCell, fun);

        // If we only added and didnt do any compute, still have to init cell
        if (retval.value.get() == null) {
            ScopedReadBuffer keybb = new ScopedReadBuffer(mm.getEmptySlice());
            ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
            allocator.allocate(keybb.getSlice(), key.calculateSerializedSize());
            ScopedWriteBuffer.serialize(keybb.getSlice(), key, MyBuffer.DEFAULT_SERIALIZER);
            retval.key.set(keybb);
            allocator.allocate(valuebb.getSlice(), value.calculateSerializedSize());
            ScopedWriteBuffer.serialize(valuebb.getSlice(), value, MyBuffer.DEFAULT_SERIALIZER);
            if (!retval.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb.getSlice());
                synchronized (retval.value) {
                    ScopedWriteBuffer.compute(retval.value.get().getSlice(), computeFunction);
                }
            }
        }

    }

    private static class Cell {
        final AtomicReference<Object> key;
        final AtomicReference<ScopedReadBuffer> value;

        Cell() {
            key = new AtomicReference<>();
            value = new AtomicReference<>();
        }
    }
}

