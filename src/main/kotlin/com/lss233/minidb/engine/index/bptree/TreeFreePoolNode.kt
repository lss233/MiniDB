package com.lss233.minidb.engine.index.bptree

import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Constructor which takes into the node type as well as the
 * page index
 *
 * @param pageIndex the page index in the file
 */
@Suppress("unused")
class TreeFreePoolNode(pageIndex: Long, var nextPointer: Long):TreeNode(TreeNodeType.TREE_FREE_POOL, pageIndex) {

    /**
     * @param r     an *already* open pointer which points to our B+ Tree file
     * @param conf  B+ Tree configuration
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class)
    override fun writeNode(r: RandomAccessFile, conf: BPlusConfiguration) {

        // account for the header page as well
        r.seek(pageIndex)
        val buffer = ByteArray(conf.pageSize)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)

        // write the node type
        bbuffer.putShort(getPageType())

        // write the next pointer
        bbuffer.putLong(nextPointer)

        // write current capacity
        bbuffer.putInt(getCurrentCapacity())

        // now write the index values
        for (i in 0 until getCurrentCapacity()) {
            bbuffer.putLong((getKeyAt(i)[0] as Long))
        }
        r.write(buffer)
    }
}
