/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import com.yahoo.oak.synchrobench.MyBuffer;

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
    private Comparator<Object> comparator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;

    public OffHeapList() {

        comparator = (o1, o2) ->
        {
            //TODO YONIGO - what if key gets dfeleted?
            if (o1 instanceof MyBuffer) {

                //o2 is a node and the key is either mybuffer or bytebuffer:
                Cell cell2 = (Cell) o2;
                Object key2 = cell2.key.get();
                if (key2 instanceof MyBuffer) {
                    return MyBuffer.compareBuffers((MyBuffer) o1, (MyBuffer) key2);
                } else {
                    return MyBuffer.compareBuffers((MyBuffer) o1, (Slice) key2);
                }

            } else if (o2 instanceof MyBuffer) {
                Cell cell1 = (Cell) o1;
                Object key1 = cell1.key.get();
                if (key1 instanceof MyBuffer) {
                    return MyBuffer.compareBuffers((MyBuffer) key1, (MyBuffer) o2);
                } else {
                    return -1 * MyBuffer.compareBuffers((MyBuffer) o2, (Slice) key1);
                }
            } else if (o1 instanceof OffHeapList.Cell && o2 instanceof OffHeapList.Cell) {
                Cell cell1 = (Cell) o1;
                Object key1 = cell1.key.get();
                Cell cell2 = (Cell) o2;
                Object key2 = cell2.key.get();

                if (key1 instanceof MyBuffer && key2 instanceof MyBuffer) {
                    return MyBuffer.compareBuffers((MyBuffer) key1, (MyBuffer) key2);
                } else if (key1 instanceof Slice && key2 instanceof Slice) {
                    return MyBuffer.compareBuffers((Slice) key1, (Slice) key2);
                } else if (key1 instanceof MyBuffer && key2 instanceof Slice) {
                    return MyBuffer.compareBuffers((MyBuffer) key1, (Slice) key2);
                } else {
                    return -1 * MyBuffer.compareBuffers((MyBuffer) key2, (Slice) key1);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        };

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
    }

    @Override
    public boolean getOak(K key) {
        Cell value = skipListMap.get(key);
        if (Parameters.zeroCopy) {
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
            Slice keybb = new Slice();
            Slice valuebb = new Slice();
            allocator.allocate(keybb, key.calculateSerializedSize(),
                MemoryManager.Allocate.KEY);
            keybb.duplicateBuffer();
            MyBuffer.serialize(key, keybb);
            newCell.key.set(keybb);
            allocator.allocate(valuebb, value.calculateSerializedSize(),
                MemoryManager.Allocate.VALUE);
            valuebb.duplicateBuffer();
            MyBuffer.serialize(value, valuebb);
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb);
            }
        } else {
            if (prevValue.value.get() == null) {
                Slice valuebb = new Slice();
                allocator.allocate(valuebb, value.calculateSerializedSize(),
                    MemoryManager.Allocate.VALUE);
                valuebb.duplicateBuffer();
                MyBuffer.serialize(value, valuebb);
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.free(valuebb);
                }
            } else {
                synchronized (prevValue.value) {
                    MyBuffer.serialize(value, prevValue.value.get());
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
            Slice keybb = new Slice();
            Slice valuebb = new Slice();
            allocator.allocate(keybb, key.calculateSerializedSize(),
                MemoryManager.Allocate.KEY);
            keybb.duplicateBuffer();
            MyBuffer.serialize(key, keybb);
            newCell.key.set(keybb);
            allocator.allocate(valuebb, value.calculateSerializedSize(),
                MemoryManager.Allocate.VALUE);
            valuebb.duplicateBuffer();
            MyBuffer.serialize(value, valuebb);
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb);
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
        allocator.free((Slice) val.key.get());
        allocator.free(val.value.get());
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
                if (!Parameters.zeroCopy) {
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
            allocator.free((Slice) cell.key.get());
            allocator.free(cell.value.get());
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


        Consumer<OakUnsafeDirectBuffer> computeFunction = buffer -> {
            ByteBuffer buf = buffer.getByteBuffer();
            int pos = buffer.getOffset();
            buf.putLong(pos + 1, ~buf.getLong(pos + 1));
        };

        BiFunction<Object, Cell, Cell> fun = (prevValueO, v) -> {
            Cell prevValue = (Cell) prevValueO;
            // cell is in map but maybe not initialized yet
            if (prevValue.value.get() == null) {
                Slice valuebb = new Slice();
                allocator.allocate(valuebb, value.calculateSerializedSize(),
                    MemoryManager.Allocate.VALUE);
                valuebb.duplicateBuffer();
                MyBuffer.serialize(value, valuebb);
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.free(valuebb);
                    synchronized (prevValue.value) {
                        computeFunction.accept(prevValue.value.get());
                    }
                }
            } else {
                synchronized (prevValue.value) {
                    computeFunction.accept(prevValue.value.get());
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
            Slice keybb = new Slice();
            Slice valuebb = new Slice();
            allocator.allocate(keybb, key.calculateSerializedSize(),
                MemoryManager.Allocate.KEY);
            keybb.duplicateBuffer();
            MyBuffer.serialize(key, keybb);
            retval.key.set(keybb);
            allocator.allocate(valuebb, value.calculateSerializedSize(),
                MemoryManager.Allocate.VALUE);
            valuebb.duplicateBuffer();
            MyBuffer.serialize(value, valuebb);
            if (!retval.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb);
                synchronized (retval.value) {
                    computeFunction.accept(retval.value.get());
                }
            }
        }

    }

    private static class Cell {
        final AtomicReference<Object> key;
        final AtomicReference<Slice> value;

        Cell() {
            key = new AtomicReference<>();
            value = new AtomicReference<>();
        }
    }
}

