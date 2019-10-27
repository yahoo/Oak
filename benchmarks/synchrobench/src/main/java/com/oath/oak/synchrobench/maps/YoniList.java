package com.oath.oak.synchrobench.maps;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.OakBlockMemoryAllocator;
import com.oath.oak.Slice;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.oath.oak.synchrobench.contention.benchmark.Parameters;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class YoniList<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private ConcurrentSkipListMap<Object, Cell> skipListMap;
    private OakBlockMemoryAllocator allocator;
    private Comparator<Object> comparator;

    public YoniList() {

        comparator = (o1, o2) ->
        {
            //TODO YONIGO - what if key gets dfeleted?
            if (o1 instanceof MyBuffer) {

                //o2 is a node and the key is either mybuffer or bytebuffer:
                Cell cell2 = (Cell) o2;
                Object key2 = cell2.key.get();
                if (key2 instanceof MyBuffer) {
                    return MyBufferOak.keysComparator.compareKeys((MyBuffer) o1, (MyBuffer) key2);
                } else {
                    return MyBufferOak.keysComparator.compareKeyAndSerializedKey((MyBuffer) o1,
                            ((Slice) key2).getByteBuffer());
                }

            } else if (o2 instanceof MyBuffer) {
                Cell cell1 = (Cell) o1;
                Object key1 = cell1.key.get();
                if (key1 instanceof MyBuffer) {
                    return MyBufferOak.keysComparator.compareKeys((MyBuffer) key1, (MyBuffer) o2);
                } else {
                    return -1 * MyBufferOak.keysComparator.compareKeyAndSerializedKey((MyBuffer) o2,
                            ((Slice) key1).getByteBuffer());
                }
            } else if (o1 instanceof YoniList.Cell && o2 instanceof YoniList.Cell) {
                Cell cell1 = (Cell) o1;
                Object key1 = cell1.key.get();
                Cell cell2 = (Cell) o2;
                Object key2 = cell2.key.get();

                if (key1 instanceof MyBuffer && key2 instanceof MyBuffer) {
                    return MyBufferOak.keysComparator.compareKeys((MyBuffer) key1, (MyBuffer) key2);
                } else if (key1 instanceof Slice && key2 instanceof Slice) {
                    return MyBufferOak.keysComparator.compareSerializedKeys(((Slice) key1).getByteBuffer(),
                            ((Slice) key2).getByteBuffer());
                } else if (key1 instanceof MyBuffer && key2 instanceof Slice) {
                    return MyBufferOak.keysComparator.compareKeyAndSerializedKey((MyBuffer) key1,
                            ((Slice) key2).getByteBuffer());
                } else {
                    return -1 * MyBufferOak.keysComparator.compareKeyAndSerializedKey((MyBuffer) key2,
                            ((Slice) key1).getByteBuffer());
                }
            } else {
                throw new UnsupportedOperationException();
            }
        };

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new OakNativeMemoryAllocator((long) Integer.MAX_VALUE * 16);
    }

    @Override
    public boolean getOak(K key) {
        Cell value = skipListMap.get(key);
        if (Parameters.zeroCopy) {
            return value != null && value.value != null;
        } else {
            if (value != null && value.value != null) {
                MyBuffer des = MyBufferOak.serializer.deserialize(value.value.get().getByteBuffer());
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
            Slice keybb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(key), true).duplicate();
            MyBufferOak.serializer.serialize(key, keybb.getByteBuffer());
            newCell.key.set(keybb);
            Slice valuebb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(value), false);
            MyBufferOak.serializer.serialize(value, valuebb.getByteBuffer());
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.freeSlice(valuebb);
            }
        } else {
            if (prevValue.value.get() == null) {
                Slice valuebb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(value), false);
                MyBufferOak.serializer.serialize(value, valuebb.getByteBuffer());
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.freeSlice(valuebb);
                }
            } else {
                synchronized (prevValue.value) {
                    MyBufferOak.serializer.serialize(value, prevValue.value.get().getByteBuffer());
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
            Slice keybb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(key), true).duplicate();
            MyBufferOak.serializer.serialize(key, keybb.getByteBuffer());
            newCell.key.set(keybb);
            Slice valuebb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(value), false);
            MyBufferOak.serializer.serialize(value, valuebb.getByteBuffer());
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.freeSlice(valuebb);
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
        allocator.freeSlice((Slice) val.key.get());
        allocator.freeSlice(val.value.get());
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
        Iterator<Cell> iter = skipListMap.tailMap(from, true).values().iterator();
        return iterate(iter, length);
    }

    @Override
    public boolean descendOak(K from, int length) {
        Iterator<Cell> iter = skipListMap.descendingMap().tailMap(from, true).values().iterator();
        return iterate(iter, length);
    }


    private boolean iterate(Iterator<Cell> iter, int length) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            Cell cell = iter.next();
            //only if cell is not null value is not deleted or not set yet.
            if (cell.value.get() != null) {
                if (!Parameters.zeroCopy) {
                    MyBuffer des = MyBufferOak.serializer.deserialize(cell.value.get().getByteBuffer());
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
            allocator.freeSlice((Slice) cell.key.get());
            allocator.freeSlice(cell.value.get());
        });
        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator.close();
        allocator = new OakNativeMemoryAllocator((long) Integer.MAX_VALUE * 16);
        System.gc();
    }

    @Override
    public int size() {
        return skipListMap.size();
    }


    @Override
    public void putIfAbsentComputeIfPresentOak(K key, V value) {


        Consumer<ByteBuffer> computeFunction = (ByteBuffer buffer) -> buffer.putLong(1, ~buffer.getLong(1));

        BiFunction<Object, Cell, Cell> fun = (prevValueO, v) -> {
            Cell prevValue = (Cell) prevValueO;
            // cell is in map but maybe not initialized yet
            if (prevValue.value.get() == null) {
                Slice valuebb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(value), false);
                MyBufferOak.serializer.serialize(value, valuebb.getByteBuffer());
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.freeSlice(valuebb);
                    synchronized (prevValue.value) {
                        computeFunction.accept(prevValue.value.get().getByteBuffer());
                    }
                }
            } else {
                synchronized (prevValue.value) {
                    computeFunction.accept(prevValue.value.get().getByteBuffer());
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
            Slice keybb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(key), true).duplicate();
            MyBufferOak.serializer.serialize(key, keybb.getByteBuffer());
            retval.key.set(keybb);
            Slice valuebb = allocator.allocateSlice(MyBufferOak.serializer.calculateSize(value), false);
            MyBufferOak.serializer.serialize(value, valuebb.getByteBuffer());
            if (!retval.value.compareAndSet(null, valuebb)) {
                allocator.freeSlice(valuebb);
                synchronized (retval.value) {
                    computeFunction.accept(retval.value.get().getByteBuffer());
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

