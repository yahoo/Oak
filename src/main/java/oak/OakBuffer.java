package oak;

import java.nio.ByteOrder;

public abstract class OakBuffer {

    /**
     * Returns this buffer's capacity.
     *
     * @return The capacity of this buffer
     */
    public abstract int capacity() throws NullPointerException;

    /**
     * Returns this buffer's position.
     *
     * @return The position of this buffer
     */
    public abstract int position() throws NullPointerException;

    /**
     * Returns this buffer's limit.
     *
     * @return The limit of this buffer
     */
    public abstract int limit() throws NullPointerException;

    /**
     * Returns the number of elements between the current position and the
     * limit.
     *
     * @return The number of elements remaining in this buffer
     */
    public abstract int remaining() throws NullPointerException;

    /**
     * Tells whether there are any elements between the current position and
     * the limit.
     *
     * @return <tt>true</tt> if, and only if, there is at least one element
     * remaining in this buffer
     */
    public abstract boolean hasRemaining() throws NullPointerException;

    /**
     * Absolute <i>get</i> method.  Reads the byte at the given
     * index.
     *
     * @param index The index from which the byte will be read
     * @return The byte at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit
     */
    public abstract byte get(int index) throws NullPointerException;

    /**
     * Retrieves this buffer's byte order.
     * <p>
     * <p> The byte order is used when reading or writing multibyte values, and
     * when creating buffers that are views of this byte buffer.  The order of
     * a newly-created byte buffer is always {@link ByteOrder#BIG_ENDIAN
     * BIG_ENDIAN}.  </p>
     *
     * @return This buffer's byte order
     */
    public abstract ByteOrder order() throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a char value.
     * <p>
     * <p> Reads two bytes at the given index, composing them into a
     * char value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The char value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     */
    public abstract char getChar(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a short value.
     * <p>
     * <p> Reads two bytes at the given index, composing them into a
     * short value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The short value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus one
     */
    public abstract short getShort(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading an int value.
     * <p>
     * <p> Reads four bytes at the given index, composing them into a
     * int value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The int value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     */
    public abstract int getInt(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a long value.
     * <p>
     * <p> Reads eight bytes at the given index, composing them into a
     * long value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The long value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     */
    public abstract long getLong(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a float value.
     * <p>
     * <p> Reads four bytes at the given index, composing them into a
     * float value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The float value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus three
     */
    public abstract float getFloat(int index) throws NullPointerException;

    /**
     * Absolute <i>get</i> method for reading a double value.
     * <p>
     * <p> Reads eight bytes at the given index, composing them into a
     * double value according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The double value at the given index
     * @throws IndexOutOfBoundsException If <tt>index</tt> is negative
     *                                   or not smaller than the buffer's limit,
     *                                   minus seven
     */
    public abstract double getDouble(int index) throws NullPointerException;
}
