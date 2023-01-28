package com.lss233.minidb.engine.index.bptree

import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

/**
 *
 * Class for our Internal nodes
 *
 * Create an internal node
 *
 * @param nodeType the node type parameter
 * @param pageIndex the index of the page
 */
@Suppress("unused")
class TreeInternalNode(nodeType: TreeNodeType, pageIndex: Long):TreeNode(nodeType, pageIndex) {

    private val pointerArray: LinkedList<Long> = LinkedList()

    fun removePointerAt(index: Int) {
        pointerArray.removeAt(index)
    }

    fun getPointerAt(index: Int): Long {
        return if (index < 0 || index >= pointerArray.size) -1 else pointerArray[index]
    }

    fun popPointer(): Long {
        return pointerArray.pop()
    }

    fun removeLastPointer(): Long {
        return pointerArray.removeLast()
    }

    fun addPointerAt(index: Int, `val`: Long) {
        pointerArray.add(index, `val`)
    }

    fun addPointerLast(`val`: Long) {
        pointerArray.addLast(`val`)
    }

    fun setPointerAt(index: Int, `val`: Long) {
        pointerArray[index] = `val`
    }

    val pointerListSize: Int
        get() = pointerArray.size

    fun pushToPointerArray(`val`: Long) {
        pointerArray.push(`val`)
    }

    /**
     * @param r pointer to *opened* B+ tree file
     * @throws IOException is thrown when an I/O exception is captured.
     */
    @Throws(IOException::class)
    override fun writeNode(r: RandomAccessFile, conf: BPlusConfiguration) {

        // update root index in the file
        if (isRoot()) {
            r.seek(conf.headerSize - 16L)
            r.writeLong(pageIndex)
        }

        // account for the header page as well.
        r.seek(pageIndex)
        val buffer = ByteArray(conf.pageSize)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)
        // write the node type
        bbuffer.putShort(getPageType())

        // write current capacity
        bbuffer.putInt(getCurrentCapacity())

        // now write Key/Pointer pairs
        for (i in 0 until getCurrentCapacity()) {
            bbuffer.putLong(getPointerAt(i)) // Pointer
            conf.writeKey(bbuffer, getKeyAt(i))
        }
        // final pointer.
        bbuffer.putLong(getPointerAt(getCurrentCapacity()))
        r.write(buffer)
    }
}
