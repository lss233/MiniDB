package com.lss233.minidb.engine.index.bptree

import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

/**
 * Class for our Tree leafs
 *
 * Constructor for our Internal node
 * @param nextPagePointer the next leaf pointer
 * @param prevPagePointer the previous leaf pointer
 * @param nodeType the node type
 * @param pageIndex the index of the page
 */
@Suppress("unused")
class TreeLeaf constructor (
    var nextPagePointer: Long, var prevPagePointer: Long,
    nodeType: TreeNodeType, pageIndex: Long
) : TreeNode(nodeType, pageIndex) {

    var valueList: LinkedList<Long> // satellite data list

    private val overflowList: LinkedList<Long> // overflow pointer list

    init {
        require(!(nodeType === TreeNodeType.TREE_ROOT_LEAF && nextPagePointer > 0)) {
            "Can't have leaf root with non-null next pointer"
        }
        overflowList = LinkedList()
        valueList = LinkedList()
    }

    fun addToOverflowList(index: Int, value: Long) {
        overflowList.add(index, value)
    }

    fun addLastToOverflowList(value: Long) {
        overflowList.addLast(value)
    }

    fun addLastToValueList(value: Long) {
        valueList.addLast(value)
    }

    fun getOverflowPointerAt(index: Int): Long {
        return overflowList[index]
    }

    fun pushToOverflowList(overflowPointer: Long) {
        overflowList.push(overflowPointer)
    }

    fun popOverflowPointer(): Long {
        return overflowList.pop()
    }

    fun setOverflowPointerAt(index: Int, value: Long) {
        overflowList[index] = value
    }

    fun removeLastOverflowPointer(): Long {
        return overflowList.removeLast()
    }

    val lastOverflowPointer: Long
        get() = overflowList.last

    fun addToValueList(index: Int, value: Long) {
        valueList.add(index, value)
    }

    fun getValueAt(index: Int): Long {
        return valueList[index]
    }

    fun pushToValueList(value: Long) {
        valueList.push(value)
    }

    fun popValue(): Long {
        return valueList.pop()
    }

    fun removeLastValue(): Long {
        return valueList.removeLast()
    }

    fun getNextPagePointer(): Long {
        return nextPagePointer
    }

    fun setNextPagePointer(next: Long) {
        nextPagePointer = next
    }


    fun removeEntryAt(index: Int, conf: BPlusConfiguration): Long {
        keyArray!!.removeAt(index)
        overflowList.removeAt(index)
        val s = valueList.removeAt(index)
        decrementCapacity(conf)
        return s
    }

    /**
     * @param r pointer to *opened* B+ tree file
     * @param conf configuration parameter
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class)
    override fun writeNode(r: RandomAccessFile, conf: BPlusConfiguration) {

        // update root index in the file
        if (isRoot()) {
            r.seek(conf.headerSize - 16L)
            r.writeLong(getPageIndex())
        }

        // account for the header page as well.
        r.seek(getPageIndex())
        val buffer = ByteArray(conf.pageSize)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)

        // now write the node type
        bbuffer.putShort(getPageType())

        // write the prev pointer
        bbuffer.putLong(prevPagePointer)

        // write the next pointer
        bbuffer.putLong(nextPagePointer)

        // then write the current capacity
        bbuffer.putInt(getCurrentCapacity())

        // now write the Key/Value pairs
        for (i in 0 until getCurrentCapacity()) {
            conf.writeKey(bbuffer, getKeyAt(i))
            bbuffer.putLong(valueList[i])
            bbuffer.putLong(getOverflowPointerAt(i))
        }
        r.write(buffer)
    }
}
