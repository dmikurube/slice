/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.slice;

import sun.misc.Unsafe;

import java.io.IOException;

import static io.airlift.slice.JvmUtils.unsafe;
import static io.airlift.slice.Preconditions.checkNotNull;
import static io.airlift.slice.Preconditions.checkPositionIndexes;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class Slice
{
    /**
     * Base object for relative addresses.  If null, the address is an
     * absolute location in memory.
     */
    private final Object base;

    /**
     * If base is null, address is the absolute memory location of data for
     * this slice; otherwise, address is the offset from the base object.
     * This base plus relative offset addressing is taken directly from
     * the Unsafe interface.
     * <p/>
     * Note: if base object is a byte array, this address ARRAY_BYTE_BASE_OFFSET,
     * since the byte array data starts AFTER the byte array object header.
     */
    private final long address;

    /**
     * Size of the slice
     */
    private final int size;

    /**
     * Reference is typically a ByteBuffer object, but can be any object this
     * slice must hold onto to assure that the underlying memory is not
     * freed by the garbage collector.
     */
    private final Object reference;

    /**
     * Creates an empty slice.
     */
    Slice()
    {
        this.base = null;
        this.address = 0;
        this.size = 0;
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array range.
     */
    Slice(byte[] base, int offset, int length)
    {
        checkNotNull(base, "base is null");
        checkPositionIndexes(offset, offset + length, base.length);

        this.base = base;
        this.address = ARRAY_BYTE_BASE_OFFSET + offset;
        this.size = length;
        this.reference = null;
    }

    /**
     * Length of this slice.
     */
    public int length()
    {
        return size;
    }

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public byte getByte(int index)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        return unsafe.getByte(base, address + index);
    }

    /**
     * Gets a 32-bit integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public int getInt(int index)
    {
        checkIndexLength(index, SIZE_OF_INT);
        return unsafe.getInt(base, address + index);
    }

    /**
     * Gets a 64-bit long integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public long getLong(int index)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        return unsafe.getLong(base, address + index);
    }

    /**
     * Gets a 64-bit double at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public double getDouble(int index)
    {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        return unsafe.getDouble(base, address + index);
    }

    /**
     * Transfers portion of data from this slice into the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        checkIndexLength(index, length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);

        copyMemory(base, address + index, destination, (long) ARRAY_BYTE_BASE_OFFSET + destinationIndex, length);
    }

    /**
     * Sets the specified byte at the specified absolute {@code index} in this
     * buffer.  The 24 high-order bits of the specified value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public void setByte(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        unsafe.putByte(base, address + index, (byte) (value & 0xFF));
    }

    /**
     * Sets the specified 32-bit integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public void setInt(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_INT);
        unsafe.putInt(base, address + index, value);
    }

    /**
     * Sets the specified 64-bit long integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public void setLong(int index, long value)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        unsafe.putLong(base, address + index, value);
    }

    /**
     * Sets the specified 64-bit double at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public void setDouble(int index, double value)
    {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        unsafe.putDouble(base, address + index, value);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0}, or
     * if {@code index + source.length} is greater than {@code this.length()}
     */
    public void setBytes(int index, byte[] source)
    {
        setBytes(index, source, 0, source.length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        copyMemory(source, (long) ARRAY_BYTE_BASE_OFFSET + sourceIndex, base, address + index, length);
    }

    private static void copyMemory(Object src, long srcAddress, Object dest, long destAddress, int length)
    {
        // The Unsafe Javadoc specifies that the transfer size is 8 iff length % 8 == 0
        // so ensure that we copy big chunks whenever possible, even at the expense of two separate copy operations
        int bytesToCopy = length - (length % 8);
        unsafe.copyMemory(src, srcAddress, dest, destAddress, bytesToCopy);
        unsafe.copyMemory(src, srcAddress + bytesToCopy, dest, destAddress + bytesToCopy, length - bytesToCopy);
    }

    private void checkIndexLength
    (int index, int length)
    {
        checkPositionIndexes(index, index + length, length());
    }
}
