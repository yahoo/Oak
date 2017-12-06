# Oak
Oak map implementation

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