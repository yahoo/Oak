# Oak
> Oak (Off-heap Allocated Keys) is a scalable, concurrent, in-memory Key-Value (KV) map.

OakMap is a concurrent Key-Value Map that keeps all keys and values off-heap. This allows storing more data (up to 3 times more data) compare to using the standard JVM heap management, albeit using the same memory footprint.
OakMap implements the industry-standard Java8 ConcurrentNavigableMap API. It provides strong (atomic) semantics for read, write, and read-modify-write operations, as well as (non-atomic) range query (scan) operations, both forward and backward.
OakMap is optimized for big keys and values, in particular, for incremental maintenance of objects (update in-place).
It is faster and scales better with additional CPU cores than the popular Java ConcurrentNavigableMap [ConcurrentSkipListMap](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentSkipListMap.html).

## Why OakMap?
1. OakMap provides great performance: it employs fine-grain synchronization, and thus scales well with numbers of threads; it also achieves cache-friendliness by avoiding memory fragmentation (see [performance evaluation](https://github.com/yahoo/Oak/wiki/Performance)).
2. OakMap takes keys and the data off-heap, and thus allows working with a huge heap (RAM) -- even more than 50G -- without JVM GC overheads.
   - To support off-heap, OakMap has embedded, efficient, epoch-based memory management that mostly eliminates JVM GC overheads.
4. OakMap provides a rich API for **atomic** accesses to data. For example, OakMap supports atomic compute() -- in place computations on existing keys -- whereas the current Java ConcurrentSkipListMap implementation does not guarantee the atomicity of `compute()`. OakMap’s update operations (such as put and compute) take user-provided lambda functions for easy integration in diverse use cases.
5. Descending Scans: OakMap expedites descending scans without additional complexity. In our experiments, OakMap’s descending scans are 4.8x faster than ConcurrentSkipListMap’s, and perform similarly to their ascending counterparts (see [performance evaluation](https://github.com/yahoo/Oak/wiki/Performance-Evaluation)).

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
- OakMap's keys and values are copied and stored in self-managed off-heap byte arrays.

### Design Requirements
To efficiently manage its content, OakMap requires that the user define two auxiliary tools: an OakSerializer and an OakComparator; both are passed during construction.
1. *OakSerializer:* Both keys and values need to provide a (1) serializer, (2) deserializer, and (3) serialized size calculator. All three are parts of [OakSerializer](#oakserializer).
   - For boosting performance, OakMap allocates space for a given key/value and then uses the given serializer to write the key/value directly to the allocated space. OakMap uses the appropriate size calculator to deduce the amount of space to be allocated. Note that both keys and values are variable-sized.
2. *OakComparator:* To compare the internally-kept serialized keys with the deserialized key provided by the API, OakMap requires a (key) comparator. The comparator compares two keys, each of which may be provided either as a deserialized object or as a serialized one, determining whether they are equal, and if not, which is bigger.

## Install
OakMap is a library. After downloading Oak, compile it using `mvn install package` to compile and install. Then update your project's pom.xml file dependencies, as follows:
```xml
  <dependency>
      <groupId>oak</groupId>
      <artifactId>oak</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
```
Finally, import the relevant classes and use OakMap according to the description below.

## Builder

To build `OakMap`, the user should first create the builder, and then use it to construct `OakMap`:
```java
OakMapBuilder<K,V> builder = ... // create a builder; details provided below
OakMap<K,V> oak = builder.build();
```

OakMap requires multiple parameters to be defined for the builder, as shall be explained below.
When constructing off-heap OakMap, the memory capacity (per OakMap instance) needs to be specified. OakMap allocates the off-heap memory with the requested capacity at construction time and later manages this memory.

### OakSerializer
As explained above, `OakMap<K, V>` is given key `K` and value `V`, which are requested to come with a serializer, deserializer, and size calculator. OakMap user needs to implement the following interface that can be found in the Oak project.

```java
public interface OakSerializer<T> {
  // serializes the data
  void serialize(T data, OakScopedWriteBuffer serializedData);

  // deserializes the data
  T deserialize(OakScopedReadBuffer serializedData);

  // returns the number of bytes needed for serializing the given data
  int calculateSize(T data);
}
```

*Note 1*: Oak use dedicated objects to access off-heap memory: `OakScopedReadBuffer` and `OakScopedWriteBuffer`.
See [Oak Buffers](#oak-buffers) for more information.

*Note 2*: `OakScopedReadBuffer` and `OakScopedWriteBuffer` should not be stored for future use.
They are valid only in the context of these methods (`serialize()`/`deserialize()`).
Using these buffers outside their intended context may yield unpredicted results.

For example, the implementation of key serializer for an application that use integer as keys might look like this:

```java
public class MyAppKeySerializer implements OakSerializer<Integer> {
  void serialize(Integer key, OakScopedWriteBuffer serializedKey) {
    // We store the value at the first position of the off-heap buffer.
    serializedKey.putInt(0, value);
  }
    
  Integer deserialize(OakScopedReadBuffer serializedKey) {
    return serializedKey.getInt(0);
  }
    
  int calculateSize(Integer key) {
    // We only store one integer
    return Integer.BYTES;
  }
}

public class MayAppValueSerializer implements OakSerializer<V>
{...}
```

### Minimal Key
OakMap requires a minimal key that can represent negative infinity according to the user-defined comparison among the keys. The requested minimal key is of type `K` and is considered by the given comparator to be smaller than every other key (serialized or not). The minimal key is passed as a parameter during builder creation.

### Comparator
After a Key-Value pair is inserted into OakMap, it is kept in a serialized (buffered) state. However, OakMap's API gets the input key as an object, the serialization of which is deferred until it proves to be required.
Thus, while searching through the map, OakMap might compare between two keys in their Object and Serialized modes. OakMap provides the following interface for such a comparator:
```java
public interface OakComparator<K> {
  int compareKeys(K key1, K key2);

  int compareSerializedKeys(OakScopedReadBuffer serializedKey1, OakScopedReadBuffer serializedKey2);

  int compareSerializedKeyAndKey(OakScopedReadBuffer serializedKey1, K key2);
}
```

*Note 1*: Oak use dedicated objects to access off-heap memory: `OakScopedReadBuffer` and `OakScopedWriteBuffer`.
See [Oak Buffers](#oak-buffers) for more information.

*Note 2*: `OakScopedReadBuffer` and `OakScopedWriteBuffer` should not be stored for future use.
They are valid only in the context of these methods (`compareKeys()`/`compareSerializedKeys()/compareSerializedKeyAndKey()`).
Using these buffers outside their intended context may yield unpredicted results.

For example, the implementation of a key comparator for an application that uses integer as keys might look like this:

```java
public class MyAppKeyComparator implements OakComparator<Integer>
{
  int compareKeys(Integer key1, Integer key2) {
    return Integer.compare(key1, key2);
  }

  int compareSerializedKeys(OakReadBuffer serializedKey1, OakReadBuffer serializedKey2) {
    return Integer.compare(serializedKey1.getInt(0), serializedKey2.getInt(0)); 
  }

  int compareSerializedKeyAndKey(OakReadBuffer serializedKey1, Integer key2) {
    return Integer.compare(serializedKey1.getInt(0), key2);
  }
}
```

### Builder
We provide an example of how to create OakMapBuilder and OakMap. For a more comprehensive code example please refer to the [Usage](#usage) section.

```java
OakMapBuilder<K,V> builder = new OakMapBuilder()
            .setKeySerializer(new MyAppKeySerializer())
            .setValueSerializer(new MyAppValueSerializer())
            .setMinKey(...)
            .setKeysComparator(new MyAppKeyComparator())
            .setMemoryCapacity(...);

OakMap<K,V> oak = builder.build();
```

## API

### OakMap Methods
OakMap's API implements the ConcurrentNavigableMap interface. For improved performance, it offers additional non-standard zero-copy API methods that are discussed below.

You are welcome to take a look at the OakMap's [full API](https://github.com/yahoo/Oak/wiki/Full-API).
For a more comprehensive code example please refer to the [usage](#usage) section. 

### Oak Buffers
Oak uses dedicated buffer objects to access off-heap memory. 
These buffers cannot be instantiated by the user and are always supplied to the user by Oak.
Their interfaces are:
<pre>
Buffer                    Access      Usage
------------------------  ----------  ------------------------------
<a href="./core/src/main/java/com/yahoo/oak/OakBuffer.java" title="OakBuffer">OakBuffer</a>                 read-only   base class for all the buffers
├── <a href="./core/src/main/java/com/yahoo/oak/OakScopedReadBuffer.java" title="OakScopedReadBuffer">OakScopedReadBuffer</a>   read-only   attached to a specific scope
├── <a href="./core/src/main/java/com/yahoo/oak/OakScopedWriteBuffer.java" title="OakScopedWriteBuffer">OakScopedWriteBuffer</a>  read/write  attached to a specific scope
└── <a href="./core/src/main/java/com/yahoo/oak/OakUnscopedBuffer.java" title="OakUnscopedBuffer">OakUnscopedBuffer</a>     read-only   can be used in any scope
</pre>

These buffers may represent either a key or a value.
They mimic the standard interface of Java's `ByteBuffer`, for example, `int getInt(int index)`, `char getChar(int index)`, `capacity()`, etc. 

The scoped buffers (`OakScopedReadBuffer` and `OakScopedWriteBuffer`) are attached to the scope of the callback method they were first introduced to the user. The behavior of these buffers outside their attached scope is undefined.
Such a callback method might be the application's serializer and comparator, or a lambda function that can read/store/update the data.
This access reduces unnecessary copies and deserialization of the underlying data.
In their intended context, the user does not need to worry about concurrent accesses and memory management.
Using these buffers outside their intended context may yield unpredicted results, e.g., reading non-consistent data and/or irrelevant data.

The un-scoped buffer (`OakUnscopedBuffer`) is detached from any specific scope, i.e., it may be stored for future use.
The zero-copy methods of `OakMap` return this buffer to avoid copying the data and instead the user can access the underlying memory buffer directly (lazy evaluation).
While the scoped buffers' data accesses are synchronized, when using `OakUnscopedBuffer`, the same memory might be access by concurrent update operations.
Thus, the reader may encounter different values -- and even value deletions -- when accessing `OakUnscopedBuffer` multiple times.
Specifically, when trying to access a deleted mapping via an `OakUnscopedBuffer`, `ConcurrentModificationException` will be thrown.
This is of course normal behavior for a concurrent map that avoids copying.
To allow complex, multi-value atomic operations on the data, `OakUnscopedBuffer` provides a `transform()` method that allows the user to apply a transformation function atomically on a read-only, scoped version of the buffer (`OakScopedReadBuffer`). 
See the [Data Retrieval](#data-retrieval) for more information.

For performance and backward compatibility with applications that are already based on the use of `ByteBuffer`, Oak's buffers also implement a dedicated unsafe interface `OakUnsafeDirectBuffer`.
This interface allows high-performance access to the underlying data of Oak.
To achieve that, it sacrifices safety, so it should be used only if you know what you are doing.
Misuse of this interface might result in corrupted data, a crash or a deadlock.

Specifically, the developer should be concerned with two issues:
 1. _Concurrency_: using this interface inside the context of `serialize()`, `compute(), `compare()` and `transform()` is thread-safe.
    In other contexts (e.g., `get()` output), the developer should ensure that there is no concurrent access to this data. Failing to ensure that might result in corrupted data.
 2. _Data boundaries_: when using this interface, Oak will not alert the developer regarding any out of boundary access.
    Thus, the developer should use `getOffset()` and `getLength()` to obtain the data boundaries and carefully access the data. Writing data out of these boundaries might result in corrupted data, a crash, or a deadlock.

To use this interface, the developer should cast Oak's buffer (`OakScopedReadBuffer` or `OakScopedWriteBuffer`) to this interface,
similarly to how Java's internal DirectBuffer is used. For example:
```java
int foo(OakScopedReadBuffer b) {
  OakUnsafeDirectBuffer ub = (OakUnsafeDirectBuffer) b;
  ByteBuffer bb = ub.getByteBuffer();
  return bb.getInt(ub.getOffset());
}
```

*Note 1*: in the above example, the following will throw a `ReadOnlyBufferException` because the buffer mode is read-only:
```java
bb.putInt(ub.getOffset(), someInteger);
```

*Note 2*: the user should never change the buffer's state, namely the position and limit (`bb.limit(i)` or `bb.position(i)`).
Changing the buffer's state will make some data inaccessible to the user in the future.   


### Data Retrieval
1. For best performance of data retrieval, `OakMap` supplies a `ZeroCopyMap` interface of the map:
    `ZeroCopyMap<K, V> zc()` 
    
    The `ZeroCopyMap` interface provides the following four methods for data retrieval, whose result is presented as an `OakUnscopedBuffer`:
   - `OakUnscopedBuffer get(K key)`
   - `Collection<OakUnscopedBuffer> values()`
   - `Set<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entrySet()`
   - `Set<OakUnscopedBuffer> keySet()`
    - `Set<OakUnscopedBuffer> keyStreamSet()`
    - `Collection<OakUnscopedBuffer> valuesStream()`
    - `Set<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> entryStreamSet()`
   
    Note that in addition to the `ConcurrentNavigableMap` style sets, we introduce a new type of stream sets.
    When a stream-set is iterated it gives a "stream" view of the elements, meaning only one element can be observed at a time.
    It is preferred to use the stream iterators when possible as they instantiate significantly fewer objects, which improve performance. 
2. Without `ZeroCopyMap`, `OakMap`'s data can be directly retrieved via the following four methods:
   - `V get(Object key)`
   - `Collection<V> values()`
   - `Set<Map.Entry<K, V>> entrySet()`
   - `NavigableSet<K> keySet()`
   
   However, these direct methods return keys and/or values as Objects by applying deserialization (copy). This is costly, and we strongly advise to use `ZeroCopyMap` to operate directly on the internal data representation.
3. For examples of direct data manipulations, please refer to the [usage](#usage) section.

### Data Ingestion
1. Data can be ingested via the standard `ConcurrentNavigableMap` API.
2. For improved performance, data can be also ingested and updated via the following five methods provided by the `ZeroCopyMap` interface:
   - `void put(K key, V value)`
   - `boolean putIfAbsent(K key, V value)`
   - `void remove(K key)`
   - `boolean computeIfPresent(K key, Consumer<OakScopedWriteBuffer> computer)`
   - `boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakScopedWriteBuffer> computer)`
3. In contrast to the `ConcurrentNavigableMap` API, the zero-copy method `void put(K key, V value)` does not return the value previously associated with the key, if key existed. Likewise, `void remove(K key)` does not return a boolean indicating whether key was actually deleted, if key existed.
4. `boolean computeIfPresent(K key, Consumer<OakScopedWriteBuffer> computer)` gets the user-defined computer function. The computer is invoked in case the key exists.
The computer is provided with a mutable `OakScopedWriteBuffer`, representing the serialized value associated with the key. The computer's effect is atomic, meaning either all updates are seen by concurrent readers, or none are.
The `compute()` functionality offers the `OakMap` user an efficient zero-copy update-in-place, which allows `OakMap` users to focus on business logic without dealing with the hard problems that data layout and concurrency control present.
5. Additionally, `OakMap` supports an atomic `boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakScopedWriteBuffer> computer)` interface, (which is not part of `ConcurrentNavigableMap`).
This API looks for a key. If the key does not exist, it adds a new Serialized key --> Serialized value mapping. Otherwise, the value associated with the key is updated with `computer(old value)`. This interface works concurrently with other updates and requires only one search traversal. This interface returns true if a new key was added, false otherwise.

## Memory Management
As explained above, when constructing off-heap `OakMap`, the memory capacity (per `OakMap` instance) needs to be specified. `OakMap` allocates the off-heap memory with the requested capacity at construction time, and later manages this memory.
This memory (the entire given capacity) needs to be released later, thus `OakMap` implements `AutoClosable`. Be sure to use it within try-statement or better invoke `OakMap.close()` method when `OakMap` is no longer in use.

Please pay attention that multiple Oak sub-maps can reference the same underlying memory of `OakMap`. The memory will be released only when the last of those sub-maps are closed.
However, note that each sub-map is in particular an `OakMap` and thus `AutoCloseable` and needs to be closed (explicitly or implicitly). Again, `close()` can be invoked on different objects referring to the same underlying memory, but the final release will happen only once.

## Usage

An Integer to Integer build example can be seen in [Code Examples](https://github.com/yahoo/Oak/wiki/Code-Examples). Here we illustrate individual operations.

### Code Examples

We show some examples of Oak `ZeroCopyMap` interface usage below. These examples assume `OakMap<Integer, Integer> oak` is defined and constructed as described in the [Builder](#builder) section.

##### Simple Put and Get
```java
oak.put(10,100);
Integer i = oak.get(10);
```

##### Remove
```java
oak.zc().remove(11);
```

##### Get OakUnscopedBuffer
```java
OakUnscopedBuffer buffer = oak.zc().get(10);
if(buffer != null) {
    try {
        int get = buffer.getInt(0);
    } catch (ConcurrentModificationException e) {
    }
}
```

##### Scan & Copy
```java
Integer[] targetBuffer = new Integer[oak.size()]; // might not be correct with multiple threads
Iterator<Integer> iter = oak.values().iterator();
int i = 0;
while (iter.hasNext()) {
    targetBuffer[i++] = iter.next();
}
```

##### Compute
```java
Consumer<OakScopedWriteBuffer> func = buf -> {
    Integer cnt = buf.getInt(0);   // read integer from position 0
    buf.putInt(0, (cnt+1));        // accumulate counter, position back to 0
};
oak.zc().computeIfPresent(10, func);
```

##### Conditional Compute
```java
Consumer<OakScopedWriteBuffer> func = buf -> {
    if (buf.getInt(0) == 0) {     // check integer at position 0
        buf.putInt(1);             // position in the buffer is promoted
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

##### Transformations

```java
Function<OakScopedReadBuffer, String> intToStrings = e -> String.valueOf(e.getInt(0));

Iterator<String> iter = oak.zc().values().stream().map(v -> v.transform(intToStrings)).iterator();
while (iter.hasNext()) {
    String s = iter.next();
}
```

##### Unsafe buffer access

```java
Function<OakScopedReadBuffer, String> intToStringsDirect = b -> {
  OakUnsafeDirectBuffer ub = (OakUnsafeDirectBuffer) b;
  ByteBuffer bb = ub.getByteBuffer();
  return bb.getInt(ub.getOffset());
};

Iterator<String> iter = oak.zc().values().stream().map(v -> v.transform(intToStringsDirect)).iterator();
while (iter.hasNext()) {
    String s = iter.next();
}
```

##### Unsafe direct buffer access (address)
Oak support accessing its keys/values using direct memory address.
`DirectUtils` can be used to access the memory address data.

```java
Function<OakScopedReadBuffer, String> intToStringsDirect = b -> {
  OakUnsafeDirectBuffer ub = (OakUnsafeDirectBuffer) b;
  return DirectUtils.getInt(ub.getAddress());
};

Iterator<String> iter = oak.zc().values().stream().map(v -> v.transform(intToStringsDirect)).iterator();
while (iter.hasNext()) {
    String s = iter.next();
}
```

Note: in the above example, the following will not throw any exception even if the buffer mode is read-only:
```java
DirectUtils.putInt(ub.getAddress(), someInteger);
```

## Contribute

Please refer to the [contributing file](./CONTRIBUTING.md) for information about how to get involved. We welcome issues, questions, and pull requests.  


## License

This project is licensed under the terms of the [Apache 2.0](LICENSE-Apache-2.0) open source license.
