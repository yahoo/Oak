# Oak
> Oak (Off-heap Allocated Keys) is a scalable concurrent Key Value (KV) map for real-time analytics.

Oak implements the industry standard Java NavigableMap API. It provides strong (atomic) semantics for read, write, read-modify-write, and range query (scan) operations, both forward and backward. Oak is optimized for big keys and values, in particular for incremental maintenance of objects (e.g., aggregation). It's faster and scales better with additional CPU cores than popular NavigableMap implementations, such as Doug Lee’s ConcurrentSkipListMap, which is Java’s default.

## Why Oak?
1. Oak provides great performance, has fine synchronization, and thus scales well with numbers of threads
2. Oak takes (can take) all the keys and the data off-heap, thus allows working with huge heap (RAM) above 50G, without JVM GC overheads.
3. Oak provides rich **atomic** API. For example, in the current Java NavigableMap implementation, compute is not guaranteed to be atomic. Oak’s update operations (such as put and compute) take user-provided lambda functions for easy integration in a range of use cases. 
4. Descending Scans: Oak is expediting descending scans without the complexity of managing a doubly-linked list. In our experiments, Oak’s descending scans are 4.8x faster than ConcurrentSkipListMap’s. Bottom line the scans in both directions are similarly fast.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Configuration](#configuration)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Background
- Oak’s internal index is built on contiguous chunks of memory; this speeds up searches through the index due to access locality.
- Oak provides an efficient implementation of the NavigableMap.compute(key, updateFunction) API; an atomic, zero-copy update in-place. Specifically, Oak allows user to find an old value associated with the key and to update it in place to updateFunction(old value). This allows the Oak users to focus on business logic without dealing with the hard problems data layout and concurrency control presents.
- Further on, Oak supports an atomic putIfAbsentComputeIfPresent(key, buildFunction, updateFunction) interface. This provides the ability to look for a key, and if the key doesn't exist, the new key-->buildFunction(place to update) mapping is added, otherwise the key’s value is updated with update(old value). This interface works concurrently with other updates and requires only one search traversal.
- Oak works off-heap and on-heap. In the case of off-heap, the keys and the values are copied and stored in a self-managed, off-heap ByteBuffer. With Oak, the use of off-heap memory is simple and efficient thanks to its use of uniform-sized chunks, and its epoch-based internal garbage collection has negligible overhead.
- Oak’s forward and reverse scans are equally fast. Interestingly, prior algorithms like Java’s ConcurrentSkipListMap did not focus on reverse scans, and provided grossly inferior performance as a result.

## Installation
Oak is a library to be used in your code. After downloading Oak use `mvn install` to compile and install. Then update dependencies, like:
```
  <dependency>
      <groupId>oak</groupId>
      <artifactId>oak</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
```
Finally, import the relevant classes and use Oak according to the description below. 

When constructing **off**-heap with Oak, the memory capacity needs to be specified.
Oak will allocate off-heap memory with the requested capacity at construction.

The keys and values are of `ByteBuffer` type. Therefore, Oak can also be constructed with a specified comparator that is a `Comparator<ByteBuffer>` to compare keys, and a minimum key that's also a `ByteBuffer`. However, it is highly recommended to use Oak without a special `Comparator<ByteBuffer>`.

## Usage

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

### OakMap Methods

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

#### Code Examples

```java
ByteBuffer bb = ByteBuffer.allocate(4);
bb.putInt(0,0);
```

##### Put
```java
oak.put(bb,bb);
```

##### PutIfAbsent
```java
boolean res = oak.putIfAbsent(bb,bb);
```

##### Remove
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

##### Iterator
```java
try (CloseableIterator<ByteBuffer> iterator = oak.keysIterator()) {
    while (iter.hasNext()) {
        ByteBuffer buffer = iter.next();
    }
}
```

##### Descending Iterator
```java
try (CloseableIterator iter = oak.descendingMap().entriesIterator()) {
    while (iter.hasNext()) {
        Map.Entry<ByteBuffer, OakBuffer> e = (Map.Entry<ByteBuffer, OakBuffer>) iter.next();
    }
}
```

##### Range Iterator
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
## Contribute

Please refer to [the contributing.md file](Contributing.md) for information about how to get involved. We welcome issues, questions, and pull requests. Pull Requests are welcome


## License

This project is licensed under the terms of the [Apache 2.0](LICENSE-Apache-2.0) open source license.