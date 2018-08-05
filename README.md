# Oak
> Oak (Off-heap Allocated Keys) is a scalable concurrent Key Value (KV) map for real-time analytics.

Oak is a concurrent Key-Value Map that may keep all keys and values off-heap enabling working with bigger heap sizes comparing to JVM managed heap.
Oak implements API similar to the industry standard Java8 ConcurrentNavigableMap API. It provides strong (atomic) semantics for read, write, read-modify-write, and (non-atomic) range query (scan) operations, both forward and backward.
Oak is optimized for big keys and values, in particular for incremental maintenance of objects (e.g., aggregation).
It's faster and scales better with additional CPU cores than popular ConcurrentNavigableMap implementations, such as Doug Lee’s ConcurrentSkipListMap, which is Java’s default.

## Why Oak?
1. Oak provides great performance, has fine synchronization, and thus scales well with numbers of threads.
2. Oak's embedded, efficient, epoch-based memory management mostly eliminates JVM GC overheads.
3. Oak takes all the keys and the data off-heap, thus allows working with huge heap (RAM) above 50G, without JVM GC overheads.
4. Oak provides rich **atomic** API. For example, in the current Java ConcurrentSkipListMap implementation, compute() is not guaranteed to be atomic. Oak’s update operations (such as put and compute) take user-provided lambda functions for easy integration in a range of use cases.
5. Descending Scans: Oak is expediting descending scans without the complexity of managing a doubly-linked list. In our experiments, Oak’s descending scans are 4.8x faster than ConcurrentSkipListMap’s. Bottom line the scans in both directions are similarly fast.

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
- Oak’s internal index is built on contiguous chunks of memory; this speeds up searches through the index due to access locality.
- Oak provides an efficient implementation of the ConcurrentNavigableMap.compute(key, updateFunction) API; an atomic, zero-copy update in-place. Specifically, Oak allows user to find an old value associated with the key and to update it in place to updateFunction(old value). This allows the Oak users to focus on business logic without dealing with the hard problems data layout and concurrency control presents.
- Further on, Oak supports an atomic putIfAbsentComputeIfPresent(key, buildFunction, updateFunction) interface. This provides the ability to look for a key, and if the key doesn't exist, the new key-->buildFunction(place to update) mapping is added, otherwise the key’s value is updated with update(old value). This interface works concurrently with other updates and requires only one search traversal.
- Oak works off-heap. As a future work it will be extended on-heap. In the case of off-heap, the keys and the values are copied and stored in a self-managed, off-heap ByteBuffer. With Oak, the use of off-heap memory is simple and efficient thanks to its use of uniform-sized chunks, and its epoch-based internal garbage collection has negligible overhead.

### Oak Design Requests
1. *Serializer:* as explained, Oak takes the keys and the values off-heap, thus the keys and the values are going to be serialized and written (copied) to buffers. Therefore, for the keys and the values it is requested to provide a serializer, deserializer, and serialized size calculator. [example] (### Key/Value Serializer)
	- A possible way to eliminate the serializer request is to ask for an input key/value as a buffer (e.g. ByteBuffer). However, this requires double copy, if Oak user has keys/values as an objects. Thus first copy is when the user creates a buffer from object, and second copy is when Oak copies the given buffer to its internally manageable memory. For better performance, Oak uses only the second copy. Oak allocates a space for a key/value and uses the given serializer to write the key/value directly to the allocated space. Oak requests key/value size calculator to know the amount of space to be allocated. Both the keys and the values are variable sized.
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

In order to build Oak the user should first create `OakMapBuilder builder`, after that the Oak construction is easy `oak = builder.build()`.
Oak requires multiple parameters to be defined for Oak's construction, those parameters will be explained below.
When constructing off-heap with Oak, the memory capacity (per Oak instance) needs to be specified. Oak will allocate the off-heap memory with the requested capacity at construction (and later manage this memory).

### Key/Value Serializer
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
Oak requires a key that can represent a negative infinity according to the user-defined comparision among the keys. The requested minimal key is a key of type 'K' considered by the given comparators smaller then any other key (serialized or in the object mode).

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
Below please find an example how to create OakMapBuilder. For more comprehensive code example please refer to [Usage](#usage) section.

```java
OakMapBuilder builder = new OakMapBuilder()
            .setKeySerializer(new OakKeySerializerImplementation(...))
            .setValueSerializer(new OakValueSerializerImplementation(...))
            .setMinKey(...)
            .setKeysComparator(new OakKeyComparatorImplementation(...))
            .setMemoryCapacity(...);
```

## API

### OakMap Methods

Oak supports similar to ConcurrentNavigableMap API, unusual API methods are going to be further discussed below:
```java
public void close(); // Cleans off heap memory
public long memorySize(); // Returns current off heap memory usage in bytes
public int entries(); // Number of key-value pairs inside the map
void put(K key, V value); // If the key exists, the old value is replaced
boolean putIfAbsent(K key, V value); // If the key doesn’t exist, associate it with the given value
void remove(K key); // Removes the mapping for the key, if it is present
V get(K key); // Returns deserialized copy of the value to which the key is mapped
K getMinKey(); // The minimal key in the map
K getMaxKey(); // The maximal key in the map
// Updates the value for the key, using the user-supplied computer function
boolean computeIfPresent(K key, Consumer<OakWBuffer> computer);
// If the key doesn’t exist, associate it with the given value, otherwise update the value for the key
void putIfAbsentComputeIfPresent(K key, V value, Consumer<ByteBuffer> computer);

// Returns a view of the portion of the map with keys range from fromKey to toKey
OakMap subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);
OakMap headMap(K toKey, boolean inclusive); // Returns a view of the portion of the map with keys less than toKey
OakMap tailMap(K fromKey, boolean inclusive); // Returns a view of the portion of the map with keys greater than toKey
OakMap descendingMap(); // Returns a reverse order view of the mappings contained in the map

CloseableIterator<V> valuesIterator(); // Iterates over the values contained in the map
CloseableIterator<Map.Entry<K, V>> entriesIterator(); // Iterates over the mappings contained in the map
CloseableIterator<K> keysIterator(); // Iterates over the keys contained in the map

OakBufferView createBufferView(); // get the mappings as OakBuffers without costly deserialization
// get the transformed mappings, using user-supplied transform function
OakTransformView createTransformView(BiFunction<ByteBuffer, ByteBuffer, T> transformer);
```

### OakBuffers
Oak provides two types of memory buffers: *OakRBuffer* (read-only) and *OakWBuffer* (read and write). Those buffers support API identical to read-only Java ByteBuffers for OakRBuffer and writable Java ByteBuffer for OakWBuffer.
Unfortunately, direct extension of the ByteBuffer class is impossible outside of the ByteBuffer package. Oak buffers allow the user a direct access to the underlying serialized key-value pairs, without caring for concurrent accesses and memory management. Oak buffers help to avoid the unnecessary copies and deserialization of the underlying mappings.
However, since the value updates happen in-place and all accesses share the same underlying memory, reader may evidence different values or even value deletion associated with the same key.

### Notes for data retrieval
1. The data can be retrieved via the following four methods:
	- `V get(K key)`,
	- `CloseableIterator<V> valuesIterator()`,
	- `CloseableIterator<Map.Entry<K, V>> entriesIterator()`,
	- `CloseableIterator<K> keysIterator()`
2. Those four methods returns keys and/or values using deseriliazation (copy) and creating the Objects of the requested type. This is costly, and we strongly advice to use Oak provided Buffers or Transformations to operate directly on the internal data.
3. For better performance of data retrieval, Oak supplies OakBufferView of the OakMap. The OakBufferView provides the same four methods for data retrieval, but the output is presented as OakRBuffer, namely:
	- `OakRBuffer get(K key)`,
	- `CloseableIterator<OakRBuffer> valuesIterator()`,
	- `CloseableIterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesIterator()`,
	- `CloseableIterator<OakRBuffer> keysIterator()`
4. OakRBuffer can represent either key or value. After getting OakRBuffer user can use the same interface as *read-only* ByteBuffer, like `int getInt(int index)`, `char getChar(int index)`, `limit()`, etc. Notice that ConcurrentModificationException can be thrown as a a result of any OakRBuffer method in case the mapping was concurrently deleted.
5. For further understanding of the data retrieval via Oak transform view, please refer to [Oak Views](#views) section.

### Notes for data ingestion
1. The data can be ingested and updated via the following four methods:
 	- `void put(K key, V value)`,
 	- `boolean putIfAbsent(K key, V value)`,
 	- `void remove(K key)`,
 	- `boolean computeIfPresent(K key, Consumer<OakWBuffer> computer)`,
 	- `void putIfAbsentComputeIfPresent(K key, V value, Consumer<ByteBuffer> computer)`
2. In contrast to ConcurrentNavigableMap API `void put(K key, V value)` doesn't return the value previously associated with the key, if key existed. Similarly `void remove(K key)` doesn't remove boolean explaining whether key was really deleted, if key existed.
3. `boolean computeIfPresent(K key, Consumer<OakWBuffer> computer)` gets the user-defined computer function. The computer is invoked in case the key exists. The computer is provided with OakWBuffer representing the serialized value associated with the key. The computer effect is atomic, meaning either all updates are sean to the concurrent readers or none.

## Usage

### Integer to Integer build example

```java
    KeySerializer<Integer> keySerializer = new KeySerializer<Integer>() {

      @Override
      public void serialize(Integer key, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), key);
      }

      @Override
      public Integer deserialize(ByteBuffer serializedKey) {
        return serializedKey.getInt(serializedKey.position());
      }

			@Override
      public int calculateSize(Integer object) {
        return Integer.BYTES;
      }
    };

    ValueSerializer<Integer, Integer> valueSerializer = new ValueSerializer<Integer, Integer>() {
      @Override
      public void serialize(Integer key, Integer value, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), value);
      }

      @Override
      public Integer deserialize(ByteBuffer serializedKey, ByteBuffer serializedValue) {
        return serializedValue.getInt(serializedValue.position());
      }

      @Override
      public int calculateSize(Integer object) {
        return Integer.BYTES;
      }
    };

    OakComparator<Integer> keysComparator = new OakComparator<Integer>() {

      @Override
      public int compareKeys(Integer int1, Integer int2) {
        return intsCompare(int1, int2);
      }

      @Override
      public int compareSerializedKeys(ByteBuffer buff1, ByteBuffer buff2) {
        int int1 = buff1.getInt(buff1.position());
        int int2 = buff2.getInt(buff2.position());
        return intsCompare(int1, int2);
      }

      @Override
      public int compareSerializedKeyAndKey(ByteBuffer buff1, Integer int2) {
        int int1 = buff1.getInt(buff1.position());
        return intsCompare(int1, int2);
      }
     };

     OakMapBuilder builder = new OakMapBuilder<Integer, Integer>()
                 .setKeySerializer(keySerializer)
                 .setValueSerializer(valueSerializer)
                 .setMinKey(new Integer(Integer.MIN_VALUE))
                 .setKeysComparator(keysComparator)
                 .setMemoryCapacity(1048576); // 1MB in bytes

     OakMap<Integer,Integer> oak = (OakMap<Integer, Integer>) builder.build();
```

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

#### Get OakRBuffer
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

In addition to OakBufferView explained above, Oak supplies OakTransformView, allowing manipulating on ByteBuffers instead on OakRBuffer. It might be preferable for those who prefer to directly retrieve the modified (transformed) data from OakMap. Transform view can be create via `OakTransformView createTransformView(BiFunction<ByteBuffer, ByteBuffer, T> transformer)`.
It requires a transform function `BiFunction<ByteBuffer, ByteBuffer, T> transformer)` that may transform key-value pairs given as **read-only** ByteBuffers into any `T` object. The first ByteBuffer parameter is the key and the second is the value. OakTransformView API is the same as of OakBufferView, but `T` object is the return value, namely:
	- `T get(K key)`,
	- `CloseableIterator<T> valuesIterator()`,
	- `CloseableIterator<Map.Entry<T, T>> entriesIterator()`,
	- `CloseableIterator<T> keysIterator()`

### Code example

```java
BiFunction<ByteBuffer,ByteBuffer, Integer> func = (k,v) -> {
    if (k.getInt(0) == 1) {
        return k.getInt(0)*y.getInt(0);
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
