# Oak
Oak (Off-heap Allocated Keys) is a scalable concurrent KV-map for real-time analytics.
Oak implements the industry standard Java NavigableMap API. It provides strong (atomic) semantics for read, write, read-modify-write, and range query (scan) operations (forward and backward). Oak is optimized for big keys and values, in particular for incremental maintenance of objects (e.g., aggregation). It is faster and scales better with the number of CPU cores than popular NavigableMap implementations, e.g., Doug Lee’s ConcurrentSkipListMap (Java’s default).

## Why Oak?
1. Oak provides great performance, has fine synchronization, and thus scales well with numbers of threads
2. Oak takes (can take) all the keys and the data off-heap, thus allows working with huge heap (RAM) above 50G, without JVM GC overheads.
3. Oak provides rich **atomic** API. For example, in the current Java NavigableMap implementation, compute is not guaranteed to be atomic. Oak’s update operations (such as put and compute) take user-provided lambda functions for easy integration in a range of use cases. 
4. Descending Scans: Oak is expediting descending scans without the complexity of managing a doubly-linked list. In our experiments, Oak’s descending scans are 4.8x faster than ConcurrentSkipListMap’s. Bottom line the scans in both directions are similarly fast.

## Oak Design Points
1. Oak’s internal index is built on contiguous chunks of memory, which speeds up the search through the index due to locality of access.
2. Oak provides an efficient implementation of the NavigableMap.compute(key, updateFunction) API – an atomic, zero-copy update in-place. Specifically, Oak allows user to find an old value associated with the key and to update it (in-place) to updateFunction(old value). 
This allows the Oak user to focus on business logic without taking care of the hard issues of data layout and concurrency control.
3. Further on, Oak supports atomic putIfAbsentComputeIfPresent(key, buildFunction, updateFunction) interface. That allows to look for the key, if key is not yet exists the new key-->buildFunction(place to update) mapping is added, otherwise the key’s value is updated to update(old value). 
The above interface works concurrently with other updates and requires only one search traversal.
4. Oak works off-heap and on-heap. In the off-heap case the keys and the values are copied and stored in a self-managed off-heap ByteBuffer. For Oak, the use of off-heap memory is simple and efficient thanks to its use of uniform-sized chunks. Its epoch-based internal garbage collection has negligible overhead.
5. Oak’s forward and reverse scans are equally fast (interestingly, prior algorithms as Java’s ConcurrentSkipListMap did not focus on reverse scans, and provided grossly inferior performance).

## Instalation
Oak is a library to be used in your code. After downloading Oak use `mvn install` to compile and install. Then update dependencies, like:
```
  <dependency>
      <groupId>oak</groupId>
      <artifactId>oak</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
```
Finally, import the relevant classes and use Oak according to the description below. 

## Init
When constructing **off** heap Oak the capacity needs to be specified.
Oak will allocate off heap memory (with the requested capacity) at construction.

The keys and values are of `ByteBuffer` type.
Therefor, Oak can also be constructed with a specified comparator that is a `Comparator<ByteBuffer>` 
(will be used to compare keys) and a minimum key (also a `ByteBuffer`).
Although it is highly recommended to use Oak without a special `Comparator<ByteBuffer>`.

### Code Example

```java
OakMapOnHeapImpl oakOnHeap = new OakMapOnHeapImpl();
OakMapOffHeapImpl oakOffHeap = new OakMapOffHeapImpl();
```

```java
Comparator<ByteBuffer> comparator = new IntComparator();
ByteBuffer min = ByteBuffer.allocate(10);
min.putInt(0,Integer.MIN_VALUE);
OakMapOffHeapImpl oakInt = new OakMapOffHeapImpl(comparator, min);
```

```java
public class IntComparator implements Comparator<ByteBuffer> {

    @Override
    public int compare(ByteBuffer bb1, ByteBuffer bb2) {
        int i1 = bb1.getInt(bb1.position());
        int i2 = bb2.getInt(bb2.position());
        if (i1 > i2) {
            return 1;
        } else if (i1 < i2) {
            return -1;
        } else {
            return 0;
        }
    }

}
```

## OakMap Methods

Oak supports several methods:
```java
void put(ByteBuffer key, ByteBuffer value);
boolean putIfAbsent(ByteBuffer key, ByteBuffer value);
void remove(ByteBuffer key);
OakBuffer getHandle(ByteBuffer key);
boolean computeIfPresent(ByteBuffer key, Consumer<WritableOakBuffer> updatingFunction);
OakMap subMap(ByteBuffer fromKey, boolean fromInclusive, ByteBuffer toKey, boolean toInclusive);
OakMap headMap(ByteBuffer toKey, boolean inclusive);
OakMap tailMap(ByteBuffer fromKey, boolean inclusive);
OakMap descendingMap();
CloseableIterator<OakBuffer> valuesIterator();
CloseableIterator<Map.Entry<ByteBuffer, OakBuffer>> entriesIterator();
CloseableIterator<ByteBuffer> keysIterator(); 
```

Off heap oak also supports:
```java
long size();
void close();
```

### Code Examples

```java
ByteBuffer bb = ByteBuffer.allocate(4);
bb.putInt(0,0);
```

#### Put
```java
oak.put(bb,bb);
```

#### PutIfAbsent
```java
boolean res = oak.putIfAbsent(bb,bb);
```

#### Remove
```java
oak.remove(bb);
```

#### Get
```java
OakBuffer buffer = oak.getHandle(bb);
if(buffer != null) {
    try {
        int get = buffer.getInt(0);
    } catch (NullPointerException e){
    }
}
```

#### Compute
```java
Consumer<WritableOakBuffer> func = buf -> {
    if (buf.getInt(0) == 1) {
        buf.putInt(1);
        buf.putInt(1);
    }
};
oak.computeIfPresent(bb, func);
```

#### Iterator
```java
try (CloseableIterator<ByteBuffer> iterator = oak.keysIterator()) {
    while (iter.hasNext()) {
        ByteBuffer buffer = iter.next();
    }
}
```

#### Descending Iterator
```java
try (CloseableIterator iter = oak.descendingMap().entriesIterator()) {
    while (iter.hasNext()) {
        Map.Entry<ByteBuffer, OakBuffer> e = (Map.Entry<ByteBuffer, OakBuffer>) iter.next();
    }
}
```

#### Range Iterator
```java
ByteBuffer from = ByteBuffer.allocate(4);
from.putInt(0,1);
ByteBuffer to = ByteBuffer.allocate(4);
to.putInt(0,4);

OakMap sub = oak.subMap(from, false, to, true);
try (CloseableIterator<OakBuffer>  iter = sub.valuesIterator()) {
    while (iter.hasNext()) {
        OakBuffer buffer = iter.next();
    }
}
```
