# Oak
> Oak (Off-heap Allocated Keys) is a scalable, concurrent, in-memory Key Value (KV) map.

Oak is a concurrent Key-Value Map that may keep all keys and values off-heap enabling working with bigger heap sizes comparing to JVM managed heap.
Oak implements API similar to the industry standard Java8 ConcurrentNavigableMap API. It provides strong (atomic) semantics for read, write, read-modify-write, and (non-atomic) range query (scan) operations, both forward and backward.
Oak is optimized for big keys and values, in particular for incremental maintenance of objects (update in-place).
It's faster and scales better with additional CPU cores than popular Java's ConcurrentNavigableMap [ConcurrentSkipListMap](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentSkipListMap.html).

## Why Oak?
1. Oak provides great performance, has fine synchronization, and thus scales well with numbers of threads. [Performance evaluation](https://git.ouroath.com/anastas/oak/wiki/Performance).
2. Oak takes all the keys and the data off-heap, thus allows working with huge heap (RAM) above 50G, without JVM GC overheads.
	- To support off-heap, Oak has embedded, efficient, epoch-based memory management that mostly eliminates JVM GC overheads.
4. Oak provides rich **atomic** API. For example, in the current Java ConcurrentSkipListMap implementation, compute() is not guaranteed to be atomic. Oak’s update operations (such as put and compute) take user-provided lambda functions for easy integration in a range of use cases.
5. Descending Scans: Oak is expediting descending scans without an additional complexity. In our experiments, Oak’s descending scans are 4.8x faster than ConcurrentSkipListMap’s. Bottom line, in Oak, the scans in both directions are similarly fast. [Performance evaluation](https://git.ouroath.com/anastas/oak/wiki/Performance).

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Builder](#builder)
- [API](#api)
- [Usage](#usage)
- [Oak Views](#views)
- [Contribute](#contribute)
- [License](#license)

## Background

### Design Points
- Oak index is built on contiguous chunks of memory; this speeds up searches through the index due to access locality. Read more about [Oak design](https://git.ouroath.com/anastas/oak/wiki/Design).
- Oak works off-heap, thus the keys and the values are copied and stored in a self-managed, off-heap byte arrays.

### Design Requirements
To efficiently manage its content Oak requires that the user defines two auxiliary tools: a Serializer and a Comparator; both are passed during construction.
1. *Serializer:* The keys and the values are requested to provide a (1)serializer, (2)deserializer, and (3)serialized size calculator. All three are parts of [Serializer](#serializer).
	- For better performance, Oak allocates the space for a key/value and uses the given serializer to write the key/value directly to the allocated space. Oak requests key/value size calculator to know the amount of space to be allocated. Both the keys and the values are variable sized.
2. *Comparator:* In order to compare the internally kept, serialized keys with the deserialized key given for the search, Oak requires a special comparator. The comparator should be able to compare between keys in their serialized and deserialized (object) variants.

## Install
Oak is a library to be used in your code. After downloading Oak, compile it using `mvn install package` to compile and install. Then update your project's pom.xml file dependencies, like:
```
  <dependency>
      <groupId>oak</groupId>
      <artifactId>oak</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
```
Finally, import the relevant classes and use Oak according to the description below. 

## Builder

In order to build Oak the user should first create the builder, after that the Oak construction is easy:.
```java
OakMapBuilder<K,V> builder = ... \\ create a builder with the following details
OakMap<K,V> oak = builder.build();
```

Oak requires multiple parameters to be defined for Oak's builder, those parameters will be explained below.
When constructing off-heap Oak, the memory capacity (per Oak instance) needs to be specified. Oak will allocate the off-heap memory with the requested capacity at construction (and later manage this memory).

### Serializer
As explained above, OakMap<K,V> is given key 'K' and value 'V', which are requested to come with a serializer, deserializer and size calculator. Oak user is requested to implement the following interface that can be found in the Oak project.

```java
public interface Serializer<T> {

  // serializes the object
  void serialize(T source, ByteBuffer targetBuffer);

  // deserializes the given byte buffer
  T deserialize(ByteBuffer byteBuffer);

  // returns the number of bytes needed for serializing the given object
  int calculateSize(T object);
}
```

This is how to create those classes in your code:

```java
public class OakKeySerializerImplementation implements Serializer<K>
{...}

public class OakValueSerializerImplementation implements Serializer<V>
{...}
```

### Minimal Key
Oak requires a key that can represent a negative infinity according to the user-defined comparision among the keys. The requested minimal key is a key of type 'K' considered by the given comparators smaller then any other key (serialized or in the object mode). Minimal key is requested to be passed for the builder creation.

### Comparator
After a Key-Value pair is inserted into Oak, it is kept in a serialized (buffered) state. However, Oak gets input key as an object, serialization of which is delayed until proved as needed.
Thus, while searching through the map, Oak might compare between keys in their Object and Serialized modes. Oak provides the following interface for a special comparator:
```java
public interface OakComparator<K> {

  int compareKeys(K key1, K key2);

  int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2);

  int compareSerializedKeyAndKey(ByteBuffer serializedKey, K key);
}
```

This is how to create the comparator class in your code:

```java
public class OakKeyComparatorImplementation implements OakComparator<K>
{...}
```

### Builder
Below please find an example how to create OakMapBuilder and OakMap. For more comprehensive code example please refer to [Usage](#usage) section.

```java
OakMapBuilder<K,V> builder = new OakMapBuilder()
            .setKeySerializer(new OakKeySerializerImplementation(...))
            .setValueSerializer(new OakValueSerializerImplementation(...))
            .setMinKey(...)
            .setKeysComparator(new OakKeyComparatorImplementation(...))
            .setMemoryCapacity(...);

OakMap<K,V> oak = builder.build();
```

## API

### OakMap Methods
You are welcome to take a look on the Oak's [full API](https://git.ouroath.com/anastas/oak/wiki/Full-API)
Oak supports similar to ConcurrentNavigableMap API, unusual API methods and special cases are going to be further discussed below.

### OakBuffers
Oak provides two types of memory buffers: *OakRBuffer* (read-only) and *OakWBuffer* (read and write). Those buffers support API identical to **read-only** Java ByteBuffers for OakRBuffer and writable Java ByteBuffer for OakWBuffer.

Oak buffers allow the user a direct access to the underlying serialized key-value pairs, without caring for concurrent accesses and memory management. Oak buffers help to avoid the unnecessary copies and deserialization of the underlying mappings.
However, since the value updates happen in-place and all accesses share the same underlying memory, reader may evidence different values or even value deletion associated with the same key. This is normal behavior for concurrent map that doesn't use a copies of the objects.

OakRBuffer can represent either key or value. OakRBuffer's user can use the same interface as *read-only* ByteBuffer, like `int getInt(int index)`, `char getChar(int index)`, `limit()`, etc. Notice that ConcurrentModificationException can be thrown as a a result of any OakRBuffer method in case the mapping was concurrently deleted.

### Notes for data retrieval
1. For better performance of data retrieval, Oak supplies OakBufferView of the OakMap. The OakBufferView provides the following four methods for data retrieval, the output is presented as OakRBuffer, namely:
	- `OakRBuffer get(K key)`
	- `CloseableIterator<OakRBuffer> valuesIterator()`
	- `CloseableIterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesIterator()`
	- `CloseableIterator<OakRBuffer> keysIterator()`
2. Without OakBufferView OakMap's data can be directly retrieved via the following four methods:
	- `V get(K key)`
	- `CloseableIterator<V> valuesIterator()`
	- `CloseableIterator<Map.Entry<K, V>> entriesIterator()`
	- `CloseableIterator<K> keysIterator()`
3. However, those four direct methods return keys and/or values using deseriliazation (copy) and creating the Objects of the requested type. This is costly, and we strongly advice to use OakBufferView or OakTransformView to operate directly on the internal data.
4. For further understanding of the data retrieval via OakTransformView, please refer to [Oak Views](#views) section.

### Notes for data ingestion
1. The data can be ingested and updated via the following five methods:
 	- `void put(K key, V value)`
 	- `boolean putIfAbsent(K key, V value)`
 	- `void remove(K key)`
 	- `boolean computeIfPresent(K key, Consumer<OakWBuffer> computer)`
 	- `void putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer)`
2. In contrast to ConcurrentNavigableMap API `void put(K key, V value)` doesn't return the value previously associated with the key, if key existed. Similarly `void remove(K key)` doesn't return boolean explaining whether key was really deleted, if key existed.
3. `boolean computeIfPresent(K key, Consumer<OakWBuffer> computer)` gets the user-defined computer function. The computer is invoked in case the key exists.
The computer is provided with OakWBuffer representing the serialized value associated with the key. The computer effect is atomic, meaning either all updates are sean to the concurrent readers or none.
The compute functionality gives the Oak user an efficient zero-copy update in place, which allows the Oak users to focus on business logic without dealing with the hard problems data layout and concurrency control presents.
5. Further on, Oak supports an atomic `void putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer)` interface (not part of ConcurrentNavigableMap).
	- This provides the ability to look for a key, and if the key doesn't exist, the new key-->serializeValue(place to update) mapping is added, otherwise the key’s value is updated with computer(old value). This interface works concurrently with other updates and requires only one search traversal.

## Usage

Integer to Integer build example can be seen in [Code Examples](https://git.ouroath.com/anastas/oak/wiki/Code-Examples).

### Code Examples

##### Simple Put and Get
```java
oak.put(Integer(10),Integer(100));
Integer i = oak.get(Integer(10));
```

##### PutIfAbsent
```java
boolean res = oak.putIfAbsent(Integer(11),Integer(110));
```

##### Remove
```java
oak.remove(Integer(11));
```

##### Get OakRBuffer
```java
OakBufferView oakView = oak.createBufferView();
OakRBuffer buffer = oakView.get(Integer(10));
if(buffer != null) {
    try {
        int get = buffer.getInt(0);
    } catch (ConcurrentModificationException e){
    }
}
```

#### Compute
```java
Consumer<OakWBuffer> func = buf -> {
    if (buf.getInt(0) == 1) {
        buf.putInt(1);
        buf.putInt(1);
    }
};
oak.computeIfPresent(Integer(10), func);
```

##### Simple Iterator
```java
try (CloseableIterator<Integer> iterator = oak.keysIterator()) {
    while (iter.hasNext()) {
        Integer i = iter.next();
    }
}
```

##### Simple Descending Iterator
```java
try (CloseableIterator<Integer, Integer> iter = oak.descendingMap().entriesIterator()) {
    while (iter.hasNext()) {
        Map.Entry<Integer, Integer> e = iter.next();
    }
}
```

##### Simple Range Iterator
```java
Integer from = Integer(4);
Integer to = Integer(6);

OakMap sub = oak.subMap(from, false, to, true);
try (CloseableIterator<Integer>  iter = sub.valuesIterator()) {
    while (iter.hasNext()) {
        Integer i = iter.next();
    }
}
```

## Views

In addition to OakBufferView explained above, Oak supplies OakTransformView, allowing manipulating on ByteBuffers instead on OakRBuffer. It might be preferable for those who prefer to directly retrieve the modified (transformed) data from OakMap. Transform view can be create via `OakTransformView createTransformView(Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer)`.
It requires a transform function `Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer` that may transform key-value pairs given as **read-only** ByteBuffers into any `T` object. The first ByteBuffer parameter (of the Entry) is the key and the second is the value. OakTransformView API is the same as of OakBufferView, but `T` object is the return value, namely:
	- `T get(K key)`,
	- `CloseableIterator<T> valuesIterator()`,
	- `CloseableIterator<Map.Entry<T, T>> entriesIterator()`,
	- `CloseableIterator<T> keysIterator()`

### Code example

```java
Function<Map.Entry<ByteBuffer, ByteBuffer>, Integer> func = (e) -> {
    if (e.getKey().getInt(0) == 1) {
        return e.getKey().getInt(0)*e.getValue().getInt(0);
    } else return 0;
};

OakTransformView oakView = oak.createTransformView(func);

try (CloseableIterator<Integer> iter = oakView.entriesIterator()) {
    while (iter.hasNext()) {
        Integer i = iter.next();
    }
}
```

## Contribute

Please refer to [the contributing.md file](Contributing.md) for information about how to get involved. We welcome issues, questions, and pull requests. Pull Requests are welcome


## License

This project is licensed under the terms of the [Apache 2.0](LICENSE-Apache-2.0) open source license.
