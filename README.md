# Oak
> Oak (Off-heap Allocated Keys) is a scalable concurrent Key Value (KV) map for real-time analytics.

Oak is a concurrent Key-Value Map that may keep all keys and values off-heap enabling working with bigger heap sizes comparing to JVM managed heap.
Oak implements API similar to the industry standard Java8 ConcurrentNavigableMap API. It provides strong (atomic) semantics for read, write, read-modify-write, and range query (scan) operations, both forward and backward.
Oak is optimized for big keys and values, in particular for incremental maintenance of objects (e.g., aggregation).
It's faster and scales better with additional CPU cores than popular ConcurrentNavigableMap implementations, such as Doug Lee’s ConcurrentSkipListMap, which is Java’s default.

## Why Oak?
1. Oak provides great performance, has fine synchronization, and thus scales well with numbers of threads.
2. Oak has an embedded efficient epoch-based memory management and mostly eliminates JVM GC overheads.
3. Oak takes (can take) all the keys and the data off-heap, thus allows working with huge heap (RAM) above 50G, without JVM GC overheads.
4. Oak provides rich **atomic** API. For example, in the current Java ConcurrentSkipListMap implementation, compute() is not guaranteed to be atomic. Oak’s update operations (such as put and compute) take user-provided lambda functions for easy integration in a range of use cases.
5. Descending Scans: Oak is expediting descending scans without the complexity of managing a doubly-linked list. In our experiments, Oak’s descending scans are 4.8x faster than ConcurrentSkipListMap’s. Bottom line the scans in both directions are similarly fast.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Builder](#builder)
- [API](#api)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Background
- Oak’s internal index is built on contiguous chunks of memory; this speeds up searches through the index due to access locality.
- Oak provides an efficient implementation of the ConcurrentNavigableMap.compute(key, updateFunction) API; an atomic, zero-copy update in-place. Specifically, Oak allows user to find an old value associated with the key and to update it in place to updateFunction(old value). This allows the Oak users to focus on business logic without dealing with the hard problems data layout and concurrency control presents.
- Further on, Oak supports an atomic putIfAbsentComputeIfPresent(key, buildFunction, updateFunction) interface. This provides the ability to look for a key, and if the key doesn't exist, the new key-->buildFunction(place to update) mapping is added, otherwise the key’s value is updated with update(old value). This interface works concurrently with other updates and requires only one search traversal.
- Oak works off-heap. As a future work it will be extended on-heap. In the case of off-heap, the keys and the values are copied and stored in a self-managed, off-heap ByteBuffer. With Oak, the use of off-heap memory is simple and efficient thanks to its use of uniform-sized chunks, and its epoch-based internal garbage collection has negligible overhead.
- Oak’s forward and reverse scans are equally fast. Interestingly, prior algorithms like Java’s ConcurrentSkipListMap did not focus on reverse scans, and provided grossly inferior performance as a result.

### Oak Design Requests
1. Oak takes the keys and the values off-heap, thus the keys and the values are going to be serialized and written to buffers. Therefore, the Objects defining the keys and the value types are requested to provide serializer, deserializer, and serialized size calculator.
2. A possible way to eliminate the request for key/value serializer/deserializer is to ask for a key/value as a buffer (e.g. ByteBuffer) already as an input. However, this way requires double copy, in case Oak user has keys/values as an objects. Thus first copy is when the user creates a buffer from object, and second copy is when Oak copies the given buffer to its internally manageable memory. For better performance, Oak uses only the second copy. Oak allocates a space for a key/value and uses the given serializer to write the key/value directly to the allocated space. Therefore, Oak requests key/value size calculator to know the amount of space to be allocated. Both the keys and the values are variable sized.

## Install
Oak is a library to be used in your code. After downloading Oak use `mvn install package` to compile and install. Then update the pom.xml file dependencies, like:
```
  <dependency>
      <groupId>oak</groupId>
      <artifactId>oak</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
```
Finally, import the relevant classes and use Oak according to the description below. 

## Builder

In order to build Oak the user should first create `OakMapBuilder builder`, after that the Oak construction is easy `oak = builder.buildOffHeapOakMap()`.
Oak requires quite big amount of parameters to be defined for Oak's construction, those parameters will be now explained.
When constructing off-heap with Oak, the memory capacity (per Oak instance) needs to be specified. Oak will allocate the off-heap memory with the requested capacity at construction (and later manage this memory).

### Key/Value Serializer and Size Calculator
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
A key of type 'K' considered by the given constructors smaller then any other key (serialized or in the object mode).

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
Below please find an example how to create OakMapBuilder.

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

Oak supports several methods, unusual of the methods are going to be further discussed below:
```java
public void close(); // cleans off heap memory
public long memorySize(); // current off heap memory usage in bytes
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
OakTransformView createTransformView(Function<ByteBuffer, ByteBufferT> transformer);
```


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
