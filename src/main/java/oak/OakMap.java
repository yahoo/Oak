package oak;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface OakMap {

    /* ------ Map API methods ------ */

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @throws NullPointerException if the specified key is null
     */
    void put(ByteBuffer key, ByteBuffer value);

    /**
     * If the specified key is not already associated
     * with a value, associate it with the given value.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return {@code true} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    boolean putIfAbsent(ByteBuffer key, ByteBuffer value);

    /**
     * If the specified key is not already associated
     * with a value, associate it with the given value.
     * Creates a copy of the value in the map.
     *
     * @param key   key with which the specified value is to be associated
     * @param keyCreator            writes the key on the given ByteBuffer, according to the given Object
     * @param keyCapacityCalculator returns the key capacity (for allocation), according to the given key object
     * @param valueCreator          the function to construct a value
     * @param valueCapacity         the value ByteBuffer size
     * @return {@code true} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    boolean putIfAbsent(
            Object key,
            Consumer<Entry<Entry<ByteBuffer, Integer>, Object>> keyCreator,
            Function<Object, Integer> keyCapacityCalculator,
            Consumer<ByteBuffer> valueCreator,
            int valueCapacity);

    /**
     * Removes the mapping for a key from this map if it is present.
     *
     * @param key key whose mapping is to be removed from the map
     * @throws NullPointerException if the specified key is null
     */
    void remove(Object key);

    /**
     * Returns a read only view of the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return a read only view of the value to which the specified key is mapped, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException if the specified key is null
     */
    OakBuffer get(Object key);

    /**
     * Returns a transformation of the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @param transformer the function that executes the transformation
     * @return a transformation of the value to which the specified key is mapped, or
     * {@code null} if this map contains no mapping for the key
     * @throws NullPointerException if the specified key is null
     */
    <T> T getTransformation(Object key, Function<ByteBuffer,T> transformer);

    /**
     * Returns a read only view of the minimal key in the map,
     * or {@code null} if this map contains no keys.
     *
     * @return a read only view of the minimal key in the map,
     * or {@code null} if this map contains no keys.
     */
    //ByteBuffer getMinKey();

    /**
     * Updates the value for the specified key
     *
     * @param key      key with which the specified value is to be associated
     * @param function the function to update a value
     * @return {@code false} if there was no mapping for the key
     * @throws NullPointerException if the specified key or the function is null
     */
    boolean computeIfPresent(Object key, Consumer<WritableOakBuffer> function);

    /**
     * If the specified key is not already associated
     * with a value, associate it with a constructed value.
     * Else, updates the value for the specified key.
     *
     * @param key         key with which the specified value is to be associated
     * @param constructor the function to construct a value
     * @param function    the function to update a value
     */
    void putIfAbsentComputeIfPresent(ByteBuffer key, Supplier<ByteBuffer> constructor, Consumer<WritableOakBuffer> function);

    /**
     * If the specified key is not already associated
     * with a value, associate it with a constructed value.
     * Else, updates the value for the specified key.
     *
     * @param key                   key with which the specified value is to be associated
     * @param keyCreator            writes the key on the given ByteBuffer, according to the given Object
     * @param keyCapacityCalculator returns the key capacity (for allocation), according to the given key object
     * @param valueCreator          the function to construct a value
     * @param valueCapacity         the value ByteBuffer size
     * @param function              the function to update a value
     */
    void putIfAbsentComputeIfPresent(
            Object key,
            Consumer<Entry<Entry<ByteBuffer, Integer>, Object>> keyCreator,
            Function<Object, Integer> keyCapacityCalculator,
            Consumer<ByteBuffer> valueCreator,
            int valueCapacity,
            Consumer<WritableOakBuffer> function);

    /*-------------- SubMap --------------*/

    /**
     * Returns a view of the portion of this map whose keys range from
     * {@code fromKey} to {@code toKey}.  If {@code fromKey} and
     * {@code toKey} are equal, the returned map is empty unless
     * {@code fromInclusive} and {@code toInclusive} are both true.  The
     * returned map is backed by this map, so changes in the returned map are
     * reflected in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>
     * <p>The returned map will throw an {@code IllegalArgumentException}
     * on an attempt to insert a key outside of its range, or to construct a
     * submap either of whose endpoints lie outside its range.
     *
     * @param fromKey       low endpoint of the keys in the returned map
     * @param fromInclusive {@code true} if the low endpoint
     *                      is to be included in the returned view
     * @param toKey         high endpoint of the keys in the returned map
     * @param toInclusive   {@code true} if the high endpoint
     *                      is to be included in the returned view
     * @return a view of the portion of this map whose keys range from
     * {@code fromKey} to {@code toKey}
     * @throws NullPointerException     if {@code fromKey} or {@code toKey}
     *                                  is null and this map does not permit null keys
     * @throws IllegalArgumentException if {@code fromKey} is greater than
     *                                  {@code toKey}; or if this map itself has a restricted
     *                                  range, and {@code fromKey} or {@code toKey} lies
     *                                  outside the bounds of the range
     */
    OakMap subMap(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive);

    /**
     * Returns a view of the portion of this map whose keys are less than (or
     * equal to, if {@code inclusive} is true) {@code toKey}.  The returned
     * map is backed by this map, so changes in the returned map are reflected
     * in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>
     * <p>The returned map will throw an {@code IllegalArgumentException}
     * on an attempt to insert a key outside its range.
     *
     * @param toKey     high endpoint of the keys in the returned map
     * @param inclusive {@code true} if the high endpoint
     *                  is to be included in the returned view
     * @return a view of the portion of this map whose keys are less than
     * (or equal to, if {@code inclusive} is true) {@code toKey}
     * @throws NullPointerException     if {@code toKey} is null
     *                                  and this map does not permit null keys
     * @throws IllegalArgumentException if this map itself has a
     *                                  restricted range, and {@code toKey} lies outside the
     *                                  bounds of the range
     */
    OakMap headMap(Object toKey, boolean inclusive);

    /**
     * Returns a view of the portion of this map whose keys are greater than (or
     * equal to, if {@code inclusive} is true) {@code fromKey}.  The returned
     * map is backed by this map, so changes in the returned map are reflected
     * in this map, and vice-versa.  The returned map supports all
     * map operations that this map supports.
     * <p>
     * <p>The returned map will throw an {@code IllegalArgumentException}
     * on an attempt to insert a key outside its range.
     *
     * @param fromKey   low endpoint of the keys in the returned map
     * @param inclusive {@code true} if the low endpoint
     *                  is to be included in the returned view
     * @return a view of the portion of this map whose keys are greater than
     * (or equal to, if {@code inclusive} is true) {@code fromKey}
     * @throws NullPointerException     if {@code fromKey} is null
     *                                  and this map does not permit null keys
     * @throws IllegalArgumentException if this map itself has a
     *                                  restricted range, and {@code fromKey} lies outside the
     *                                  bounds of the range
     */
    OakMap tailMap(Object fromKey, boolean inclusive);

    /* ---------------- View methods -------------- */

    /**
     * Returns a reverse order view of the mappings contained in this map.
     * The descending map is backed by this map, so changes to the map are
     * reflected in the descending map, and vice-versa.
     * <p>
     * <p>The expression {@code m.descendingMap().descendingMap()} returns a
     * view of {@code m} essentially equivalent to {@code m}.
     *
     * @return a reverse order view of this map
     */
    OakMap descendingMap();

    /**
     * Returns a {@link CloseableIterator} of the values contained in this map
     * in ascending order of the corresponding keys.
     */
    CloseableIterator<OakBuffer> valuesIterator();

    /**
     * Returns a {@link CloseableIterator} of the mappings contained in this map in ascending key order.
     */
    CloseableIterator<Entry<ByteBuffer, OakBuffer>> entriesIterator();

    /**
     * Returns a {@link CloseableIterator} of the keys contained in this map in ascending order.
     */
    CloseableIterator<ByteBuffer> keysIterator();

    /**
     * Returns a {@link CloseableIterator} of transformations on the values contained in this map
     * in ascending order of the corresponding keys.
     */
    <T> CloseableIterator<T> valuesTransformIterator(Function<ByteBuffer,T> transformer);

    /**
     * Returns a {@link CloseableIterator} of transformations on the mappings contained in this map
     * in ascending key order.
     */
    <T> CloseableIterator<T> entriesTransformIterator(Function<Entry<ByteBuffer, ByteBuffer>,T> transformer);

    /**
     * Returns a {@link CloseableIterator} of transformations on the keys contained in this map in ascending order.
     */
    <T> CloseableIterator<T> keysTransformIterator(Function<ByteBuffer,T> transformer);

    enum Operation {
        NO_OP,
        PUT,
        PUT_IF_ABSENT,
        REMOVE,
        COMPUTE
    }

}