package com.lss233.minidb.engine.index.bptree

import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

/**
 * Constructor which takes into the node type as well as the
 * page index
 *
 * @param nextPagePointer the next overflow pointer
 * @param prevPagePointer the previous leaf or overflow pointer
 * @param pageIndex the page index in the file
 */
@SuppressWarnings("unused")
class TreeOverflow(private var nextPagePointer: Long, private var prevPagePointer: Long, pageIndex: Long): TreeNode(TreeNodeType.TREE_LEAF_OVERFLOW, pageIndex) {

    val valueList: LinkedList<Long> = LinkedList()


    fun pushToValueList(value: Long) {
        valueList.push(value)
    }

    fun addToValueList(index: Int, value: Long) {
        valueList.add(index, value)
    }

    fun getValueAt(index: Int): Long {
        return valueList[index]
    }

    fun getNextPagePointer(): Long {
        return nextPagePointer
    }

    fun setNextPagePointer(next: Long) {
        nextPagePointer = next
    }

    private fun getPrevPagePointer(): Long {
        return prevPagePointer
    }

    fun setPrevPagePointer(prevPagePointer: Long) {
        this.prevPagePointer = prevPagePointer
    }

    /**
     * @param r pointer to *opened* B+ tree file
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class)
    override fun writeNode(r: RandomAccessFile, conf: BPlusConfiguration) {
        // account for the header page as well.
        r.seek(pageIndex)
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
        conf.writeKey(bbuffer, getKeyAt(0))

        // now write the values
        for (i in 0 until getCurrentCapacity()) {
            bbuffer.putLong(valueList[i])
        }
        r.write(buffer)
    }
}