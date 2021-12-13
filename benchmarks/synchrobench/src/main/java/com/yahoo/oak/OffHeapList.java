/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.synchrobench.contention.abstractions.BenchKey;
import com.yahoo.oak.synchrobench.contention.abstractions.BenchValue;
import com.yahoo.oak.synchrobench.contention.abstractions.KeyGenerator;
import com.yahoo.oak.synchrobench.contention.abstractions.ValueGenerator;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
import com.yahoo.oak.synchrobench.maps.BenchMap;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class OffHeapList extends BenchMap {
    private static final long MAX_OFF_MEMORY = 256 * GB;

    private final Comparator<Object> comparator;

    private ConcurrentSkipListMap<Object, Cell> skipListMap;
    private BlockMemoryAllocator allocator;
    private MemoryManager mm;


    public OffHeapList(KeyGenerator keyGen, ValueGenerator valueGen) {
        super(keyGen, valueGen);

        comparator = (o1, o2) -> {
            if (o1 instanceof OffHeapList.Cell) {
                o1 = ((Cell) o1).key.get();
            }

            if (o2 instanceof OffHeapList.Cell) {
                o2 = ((Cell) o2).key.get();
            }

            if (o1 instanceof OakScopedReadBuffer) {
                if (o2 instanceof OakScopedReadBuffer) {
                    return keyGen.compareSerializedKeys((OakScopedReadBuffer) o1, (OakScopedReadBuffer) o2);
                } else {
                    // Note the inversion of arguments, hence sign flip
                    return (-1) * keyGen.compareKeyAndSerializedKey((BenchKey) o2, (OakScopedReadBuffer) o1);
                }
            } else {
                if (o2 instanceof OakScopedReadBuffer) {
                    return keyGen.compareKeyAndSerializedKey((BenchKey) o1, (OakScopedReadBuffer) o2);
                } else {
                    return keyGen.compareKeys((BenchKey) o1, (BenchKey) o2);
                }
            }
        };
    }

    /** {@inheritDoc} **/
    @Override
    public void init() {
        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new NativeMemoryAllocator(MAX_OFF_MEMORY);
        // We use memory-manager in this context only to generate the slice object.
        // OffHeapList doesn't use the slice's header, so we choose a header-less memory-manager.
        mm = new SeqExpandMemoryManager(allocator);
    }

    /** {@inheritDoc} **/
    @Override
    public void close() {
        skipListMap.values().forEach(cell -> {
            allocator.free(((ScopedReadBuffer) cell.key.get()).getSlice());
            allocator.free(cell.value.get().getSlice());
        });
        allocator.close();

        skipListMap = null;
        allocator = null;
        mm = null;
    }

    /** {@inheritDoc} **/
    @Override
    public boolean getOak(BenchKey key, Blackhole blackhole) {
        Cell value = skipListMap.get(key);
        if (value == null || value.value == null) {
            return false;
        }

        if (Parameters.confZeroCopy) {
            if (Parameters.confConsumeValues && blackhole != null) {
                valueGen.consumeSerializedValue(value.value.get(), blackhole);
            }
        } else {
            BenchValue res = valueGen.deserialize(value.value.get());
            if (res == null) {
                return false;
            }
            if (Parameters.confConsumeValues && blackhole != null) {
                valueGen.consumeValue(res, blackhole);
            }
        }

        return true;
    }

    /** {@inheritDoc} **/
    @Override
    public void putOak(BenchKey key, BenchValue value) {
        Cell newCell = new Cell();
        newCell.key.set(key);
        Cell prevValue = skipListMap.putIfAbsent(newCell, newCell);

        if (prevValue == null) {
            ScopedReadBuffer keybb = new ScopedReadBuffer(mm.getEmptySlice());
            ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
            allocator.allocate(keybb.getSlice(), keyGen.calculateSize(key));
            ScopedWriteBuffer.serialize(keybb.getSlice(), key, keyGen);
            newCell.key.set(keybb);
            allocator.allocate(valuebb.getSlice(), valueGen.calculateSize(value));
            ScopedWriteBuffer.serialize(valuebb.getSlice(), value, valueGen);
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb.getSlice());
            }
        } else {
            if (prevValue.value.get() == null) {
                ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
                allocator.allocate(valuebb.getSlice(), valueGen.calculateSize(value));
                ScopedWriteBuffer.serialize(valuebb.getSlice(), value, valueGen);
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.free(valuebb.getSlice());
                }
            } else {
                synchronized (prevValue.value) {
                    ScopedWriteBuffer.serialize(prevValue.value.get().getSlice(), value, valueGen);
                }
            }
        }
    }

    /** {@inheritDoc} **/
    @Override
    public boolean putIfAbsentOak(BenchKey key, BenchValue value) {
        //TODO YONIGO - this wont work with puts together.
        Cell newCell = new Cell();

        newCell.key.set(key);
        Cell prevValue = skipListMap.putIfAbsent(newCell, newCell);
        if (prevValue == null) {
            ScopedReadBuffer keybb = new ScopedReadBuffer(mm.getEmptySlice());
            ScopedReadBuffer valuebb = new ScopedReadBuffer(mm.getEmptySlice());
            allocator.allocate(keybb.getSlice(), keyGen.calculateSize(key));
            ScopedWriteBuffer.serialize(keybb.getSlice(), key, keyGen);
            newCell.key.set(keybb);
            allocator.allocate(valuebb.getSlice(), valueGen.calculateSize(value));
            ScopedWriteBuffer.serialize(valuebb.getSlice(), value, valueGen);
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb.getSlice());
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    /** {@inheritDoc} **/
    @Override
    public void removeOak(BenchKey key) {
        Cell val = skipListMap.remove(key);
        if (val == null) {
            return;
        }
        allocator.free(((ScopedReadBuffer) val.key.get()).getSlice());
        allocator.free(val.value.get().getSlice());
        // TODO YONIGO - need some sync here!
    }

    /** {@inheritDoc} **/
    @Override
    public boolean computeIfPresentOak(BenchKey key) {
        return false;
    }

    /** {@inheritDoc} **/
    @Override
    public void computeOak(BenchKey key) {

    }

    /** {@inheritDoc} **/
    @Override
    public boolean ascendOak(BenchKey from, int length, Blackhole blackhole) {
        Iterator<Map.Entry<Object, Cell>> iter = skipListMap.tailMap(from, true).entrySet().iterator();
        return iterateOffHeap(iter, length, blackhole);
    }

    /** {@inheritDoc} **/
    @Override
    public boolean descendOak(BenchKey from, int length, Blackhole blackhole) {
        Iterator<Map.Entry<Object, Cell>> iter = skipListMap.descendingMap().tailMap(from, true).entrySet().iterator();
        return iterateOffHeap(iter, length, blackhole);
    }

    private boolean iterateOffHeap(Iterator<Map.Entry<Object, Cell>> iter, int length, Blackhole blackhole) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            Map.Entry<Object, Cell> entry = iter.next();
            Cell cell = entry.getValue();

            OakScopedReadBuffer valueBuffer = cell.value.get();
            Object keyObj = cell.key.get();

            //only if cell is not null value is not deleted or not set yet.
            if (valueBuffer == null || keyObj == null) {
                continue;
            }
            i++;

            if (!Parameters.confZeroCopy) {
                BenchKey key;
                if (keyObj instanceof OakScopedReadBuffer) {
                    key = keyGen.deserialize((OakScopedReadBuffer) keyObj);
                } else {
                    assert keyObj instanceof BenchKey;
                    key = (BenchKey) keyObj;
                }

                if (Parameters.confConsumeKeys && blackhole != null) {
                    keyGen.consumeKey(key, blackhole);
                }

                BenchValue value = valueGen.deserialize(valueBuffer);
                if (Parameters.confConsumeValues && blackhole != null) {
                    valueGen.consumeValue(value, blackhole);
                }
            } else {
                if (Parameters.confConsumeKeys && blackhole != null) {
                    if (keyObj instanceof OakScopedReadBuffer) {
                        keyGen.consumeSerializedKey((OakScopedReadBuffer) keyObj, blackhole);
                    } else {
                        assert keyObj instanceof BenchKey;
                        keyGen.consumeKey((BenchKey) keyObj, blackhole);
                    }
                }

                if (Parameters.confConsumeValues && blackhole != null) {
                    valueGen.consumeSerializedValue(valueBuffer, blackhole);
                }
            }
        }
        return i == length;
    }

    /** {@inheritDoc} **/
    @Override
    public int size() {
        return skipListMap.size();
    }

    /** {@inheritDoc} **/
    @Override
    public float nonHeapAllocatedGB() {
        return (float) allocator.allocated() / (float) GB;
    }

    /** {@inheritDoc} **/
    @Override
    public void putIfAbsentComputeIfPresentOak(BenchKey key, BenchValue value) {
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
                allocator.allocate(valuebb.getSlice(), valueGen.calculateSize(value));
                ScopedWriteBuffer.serialize(valuebb.getSlice(), value, valueGen);
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
            allocator.allocate(keybb.getSlice(), keyGen.calculateSize(key));
            ScopedWriteBuffer.serialize(keybb.getSlice(), key, keyGen);
            retval.key.set(keybb);
            allocator.allocate(valuebb.getSlice(), valueGen.calculateSize(value));
            ScopedWriteBuffer.serialize(valuebb.getSlice(), value, valueGen);
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

