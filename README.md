# Oak
> Oak (Off-heap Allocated Keys) is a scalable, concurrent, in-memory Key Value (KV) map.

OakMap is a concurrent Key-Value Map that may keep all keys and values off-heap. This enables working with bigger heap sizes than JVM's managed heap.
OakMap implements the industry standard Java8 ConcurrentNavigableMap API. It provides strong (atomic) semantics for read, write, and read-modify-write, as well as (non-atomic) range query (scan) operations, both forward and backward.
OakMap is optimized for big keys and values, in particular, for incremental maintenance of objects (update in-place).
It is faster and scales better with additional CPU cores than the popular Java ConcurrentNavigableMap [ConcurrentSkipListMap](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentSkipListMap.html).

## Why OakMap?
1. OakMap provides great performance: it employs fine-grain synchronization, and thus scales well with numbers of threads; it also achieves cache-friendliness by avoiding memory fragmentation. [Performance evaluation](https://github.com/yahoo/Oak/wiki/Performance).
2. OakMap takes keys and the data off-heap, and thus allows working with a huge heap (RAM) -- even more than 50G -- without JVM GC overheads.
	- To support off-heap, OakMap has embedded, efficient, epoch-based memory management that mostly eliminates JVM GC overheads.
4. OakMap provides a rich API for **atomic** accesses to data. For example, OakMap supports atomic compute() -- in place computations on existing keys -- whereas the current Java ConcurrentSkipListMap implementation does not guarantee the atomicity of compute(). OakMap’s update operations (such as put and compute) take user-provided lambda functions for easy integration in diverse use cases.
5. Descending Scans: OakMap expedites descending scans without additional complexity. In our experiments, OakMap’s descending scans are 4.8x faster than ConcurrentSkipListMap’s, and perform similarly to their ascending counterparts. [Performance evaluation](https://github.com/yahoo/Oak/wiki/Performance).

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Builder](#builder)
- [API](#api)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Background

### Design Points
- OakMap consists of an on-heap index to off-heap keys and values. OakMap's index is structured as a list of contiguous chunks of memory; this speeds up searches through the index due to access locality and cache-friendliness. Read more about [OakMap design](https://github.com/yahoo/Oak/wiki/Design).
- OakMap's keys and the values are copied and stored in self-managed off-heap byte arrays.

### Design Requirements
In order to efficiently manage its content, OakMap requires that the user define two auxiliary tools: an OakSerializer and an OakComparator; both are passed during construction.
1. *OakSerializer:* Both keys and values need to provide a (1) serializer, (2) deserializer, and (3) serialized size calculator. All three are parts of [OakSerializer](#oakserializer).
	- For boosting performance, OakMap allocates space for a given key/value and then uses the given serializer to write the key/value directly to the allocated space. OakMap uses the appropriate size calculator to deduce the amount of space to be allocated. Note that both keys and the values are variable-sized.
2. *OakComparator:* In order to compare the internally-kept serialized keys with the deserialized key provided by the API, OakMap requires a (key) comparator. The comparator compares two keys, each of which may be provided either as a deserialized object or as a serialized one, determining whether they are equal, and if not, which is bigger.

## Install
OakMap is a library. After downloading Oak, compile it using `mvn install package` to compile and install. Then update your project's pom.xml file dependencies, as follows:
```
  <dependency>
      <groupId>oak</groupId>
      <artifactId>oak</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
```
Finally, import the relevant classes and use OakMap according to the description below.

## Builder

In order to build OakMap the user should first create the builder, and then use it to construct OakMap:.
```java
OakMapBuilder<K,V> builder = ... \\ create a builder; details provided below
OakMap<K,V> oak = builder.build();
```

OakMap requires multiple parameters to be defined for the builder, as shall be explained below.
When constructing off-heap OakMap, the memory capacity (per OakMap instance) needs to be specified. OakMap allocates the off-heap memory with the requested capacity at construction time, and later manages this memory.

### OakSerializer
As explained above, OakMap<K,V> is given key 'K' and value 'V', which are requested to come with a serializer, deserializer and size calculator. OakMap user is requested to implement the following interface that can be found in the Oak project.

```java
public interface OakSerializer<T> {

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
public class OakKeySerializerImplementation implements OakSerializer<K>
{...}

public class OakValueSerializerImplementation implements OakSerializer<V>
{...}
```

### Minimal Key
OakMap requires a minimal key that can represent negative infinity according in the user-defined comparision among the keys. The requested minimal key is of type 'K', and is considered by the given comparator to be smaller than every other key (serialized or not). The minimal key is passed as a parameter during builder creation.

### Comparator
After a Key-Value pair is inserted into OakMap, it is kept in a serialized (buffered) state. However, OakMap's API gets the input key as an object, the serialization of which is deferred until it proves to be required.
Thus, while searching through the map, OakMap might compare between keys in their Object and Serialized modes. OakMap provides the following interface for such a comparator:
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
We provide an example how to create OakMapBuilder and OakMap. For a more comprehensive code example please refer to the [Usage](#usage) section.

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
You are welcome to take a look on the OakMap's [full API](https://github.com/yahoo/Oak/wiki/Full-API).
OakMap's API implements the ConcurrentNavigableMap interface. For improved performance, it offers additional non-standard zero-copy API methods that are discussed below.

### OakBuffers
OakMap provides two types of memory buffers: *OakRBuffer* (read-only) and *OakWBuffer* (read and write). These buffers support a similar API for read-only Java ByteBuffers and writable Java ByteBuffers, respectively.

OakMap buffers allow the user direct access to the underlying serialized key-value pairs, without needing to worry about  concurrent accesses and memory management. This access reduces unnecessary copies and deserialization of the underlying mappings.
Note, however, that since OakMap's get method avoids copying the value and instead returns access to the same underlying memory buffer that compute operations update in-place, the reader may encounter different values -- and even value deletions -- when accessing the buffer returned from get multiple times. This is of course normal behavior for a concurrent map that avoids copying.

An OakRBuffer can represent either a key or a value. The OakRBuffer's user can use the standard interface of a *read-only* ByteBuffer, for example, `int getInt(int index)`, `char getChar(int index)`, `limit()`, etc. Note that ConcurrentModificationException can be thrown as a result of any OakRBuffer method in case the mapping is concurrently deleted.

For backward compatibility with applications that are already based on the use of ByteBuffers, Oak Buffers provide the transform method that atomically applies a transformation to a *read-only* instance of the underlying ByteBuffer. For a more comprehensive code example please refer to the [usage](#usage) section. 


### Notes on data retrieval
1. For better performance of data retrieval, OakMap supplies a ZeroCopyMap interface of the OakMap:
    `ZeroCopyMap<K, V> zc()` 
    
    The ZeroCopyMap interface provides the following four methods for data retrieval, whose result is presented as an OakRBuffer:
	- `OakRBuffer get(K key)`
	- `Collection<OakRBuffer> values()`
	- `Set<Map.Entry<OakRBuffer, OakRBuffer>> entrySet()`
	- `Set<OakRBuffer> keySet()`
2. Without ZeroCopyMap, OakMap's data can be directly retrieved via the following four methods:
	- `V get(Object key)`
	- `Collection<V> values()`
	- `Set<Map.Entry<K, V>> entrySet()`
	- `NavigableSet<K> keySet()`
	
	However, these direct methods return keys and/or values as Objects by applying deseriliazation (copy). This is costly,  and we strongly advice to use ZeroCopyMap to operate directly on the internal data representation.
3. For examples of direct data manipulations, please refer to the [usage](#usage) section.

### Notes on data ingestion
1. Data can be ingested via the standard ConcurrentNavigableMap API.
2. For improved performance, data can be also ingested and updated via the following five methods provided by the ZeroCopyMap interface:
 	- `void put(K key, V value)`
 	- `boolean putIfAbsent(K key, V value)`
 	- `void remove(K key)`
 	- `boolean computeIfPresent(K key, Consumer<OakWBuffer> computer)`
 	- `boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer)`
3. In contrast to the ConcurrentNavigableMap API, the zero-copy method `void put(K key, V value)` does not return the value previously associated with the key, if key existed. Likewise, `void remove(K key)` does not return a boolean indicating whether key was actually deleted, if key existed.
4. `boolean computeIfPresent(K key, Consumer<OakWBuffer> computer)` gets the user-defined computer function. The computer is invoked in case the key exists.
The computer is provided with a mutable OakWBuffer, representing the serialized value associated with the key. The computer's effect is atomic, meaning that either all updates are seen by concurrent readers, or none are.
The compute functionality offers the OakMap user an efficient zero-copy update-in-place, which allows OakMap users to focus on business logic without dealing with the hard problems that data layout and concurrency control present.
5. OakMap additionally supports an atomic `boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer)` interface, (which is not part of ConcurrentNavigableMap).
This API looks for a key. If the key does not exist, it adds a new Serialized key --> Serialized value mapping. Otherwise, the value associated with the key is updated with computer(old value). This interface works concurrently with other updates and requires only one search traversal. This interface returns true if a new key was added, false otherwise.

## Memory Management
As explained above, when constructing off-heap OakMap, the memory capacity (per OakMap instance) needs to be specified. OakMap allocates the off-heap memory with the requested capacity at construction time, and later manages this memory.
This memory (the entire given capacity) needs to be released later, thus OakMap implements AutoClosable. Be sure to use it within try-statement or better invoke OakMap's close() method when OakMap is no longer in use.

Please pay attention that multiple Oak sub-maps can reference the same underlying memory of OakMap. The memory will be released only when last of those sub-maps is closed.
However, note that each sub-map is in particular an OakMap and thus AutoCloseable and needs to be closed (explicitly or implicitly). Again, close() can be invoked on different objects referring to the same underlying memory, but the final release will happen only once.

## Usage

An Integer to Integer build example can be seen in [Code Examples](https://github.com/yahoo/Oak/wiki/Code-Examples). Here we illustrate individual operations.

### Code Examples

We show some examples of Oak ZeroCopyMap interface usage below. These examples assume `OakMap<Integer, Integer> oak` is defined and constructed as described in the [Builder](#builder) section.

##### Simple Put and Get
```java
oak.put(10,100);
Integer i = oak.get(10);
```

##### Remove
```java
oak.zc().remove(11);
```

##### Get OakRBuffer
```java
OakRBuffer buffer = oak.zc().get(10);
if(buffer != null) {
    try {
        int get = buffer.getInt(0);
    } catch (ConcurrentModificationException e) {
    }
}
```

##### Scan&Copy
```java
Integer targetBuffer[] = new Integer[oak.size()]; // might not be correct with multiple threads
Iterator<Integer> iter = oak.values().iterator();
		int i = 0;
		while (iter.hasNext()) {
            targetBuffer[i++] = iter.next();
    }
}
```

##### Compute
```java
Consumer<OakWBuffer> func = buf -> {
    Integer cnt = buf.getInt(0);    // read integer from position 0
    buf.putInt(0, (cnt+1));			// accumulate counter, position back to 0
};
oak.zc().computeIfPresent(10, func);
```

##### Conditional Compute
```java
Consumer<OakWBuffer> func = buf -> {
    if (buf.getInt(0) == 0) {	    // check integer at position 0
        buf.putInt(1);				// position in the buffer is promoted
        buf.putInt(1);
    }
};
oak.zc().computeIfPresent(10, func);
```

##### Simple Iterator
```java
Iterator<Integer> iterator = oak.keySet().iterator();
while (iter.hasNext()) {
    Integer i = iter.next();
}
```

##### Simple Descending Iterator
```java
try (OakMap<Integer, Integer> oakDesc = oak.descendingMap()) {
    Iterator<Integer, Integer>> iter = oakDesc.entrySet().iterator();
    while (iter.hasNext()) {
        Map.Entry<Integer, Integer> e = iter.next();
    }
}
```

##### Simple Range Iterator
```java
Integer from = (Integer)4;
Integer to = (Integer)6;

try (OakMap sub = oak.subMap(from, false, to, true)) {
    Iterator<Integer>  iter = sub.values().iterator();
    while (iter.hasNext()) {
        Integer i = iter.next();
    }
}
```

##### Direct ByteBuffer Transformations

```java
Function<ByteBuffer, String> toStrings = e -> String.valueOf(e.getInt(0));

Iterator<String> iter = oak.zc().values().stream().map(v -> v.transform(toStrings)).iterator();
while (iter.hasNext()) {
    String s = iter.next();
}
```

## Contribute

Please refer to the [contributing file](./CONTRIBUTING.md) for information about how to get involved. We welcome issues, questions, and pull requests.  


## License

This project is licensed under the terms of the [Apache 2.0](LICENSE-Apache-2.0) open source license.
