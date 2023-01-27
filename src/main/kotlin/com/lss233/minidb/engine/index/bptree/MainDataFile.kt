package com.lss233.minidb.engine.index.bptree

import com.lss233.minidb.exception.MiniDBException
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*
import java.util.function.Function


class MainDataFile(
    var conf: MainDataConfiguration, mode: String,
    filePath: String, var rowID2position: BPlusTree
) {
    private val file: RandomAccessFile
    private val freeSlots: TreeSet<Long> // each element reflects a free slot
    var elementCount = 0L   // number of elements
        private set
    private var totalPages = 1L    // number of pages in the file, can be counted from file length

    init {
        freeSlots = TreeSet()
        val f = File(filePath)
        val stmode = mode.substring(0, 2)
        file = RandomAccessFile(filePath, stmode)
        if (f.exists() && !mode.contains("+")) {
            file.seek(0)
            totalPages = file.length() / conf.pageSize
            elementCount = file.readLong()
            var pindex = file.readLong()
            val buffer = ByteArray(conf.pageSize)
            // read the free pages
            while (pindex != -1L) {
                freeSlots.add(pindex)
                file.seek(pindex)
                file.read(buffer, 0, buffer.size)
                val bbuffer = ByteBuffer.wrap(buffer)
                bbuffer.order(ByteOrder.BIG_ENDIAN)
                pindex = bbuffer.long
                for (i in 0 until conf.nValidPointerInFreePage) {
                    val index = bbuffer.long
                    if (index != -1L) {
                        freeSlots.add(index)
                    } else {
                        break
                    }
                }
            }
        } else {
            file.setLength(conf.pageSize.toLong()) // initial tree have 2 pages. head page and root page
            file.seek(0)
            file.writeLong(elementCount)
            file.writeLong(-1L)
        }
    }

    @get:Throws(IOException::class)
    private val firstAvailablePageIndex: Long
        private get() {
            // check if we have unused pages
            if (freeSlots.size == 0) { // file length == conf.pageSize * totalPages, allocate new pages
                val ALLOCATE_NEW_PAGES = 10L
                val tmp = totalPages
                totalPages += ALLOCATE_NEW_PAGES
                file.setLength(conf.pageSize * totalPages)
                for (i in tmp until tmp + ALLOCATE_NEW_PAGES) {
                    freeSlots.add(i * conf.pageSize)
                }
            }
            return freeSlots.pollFirst()
        }

    fun insertRow(key: ArrayList<Any?>, rowID: Long) {
        val position = firstAvailablePageIndex
        file.seek(position)
        val buffer = ByteArray(conf.pageSize)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)
        bbuffer.putLong(rowID)
        conf.writeKey(bbuffer, key)
        rowID2position.insertPair(ArrayList<Any>(Arrays.asList(rowID)), position)
        elementCount += 1
        file.write(buffer)
    }

    @Throws(IOException::class)
    fun deleteRow(rowID: Long) {
        val position: Long = rowID2position.search(ArrayList<Any>(Arrays.asList(rowID))).get(0)
        if (freeSlots.contains(position)) {
            throw MiniDBException(String.format("The row %d to delete does not exist!", rowID))
        }
        freeSlots.add(position)
        rowID2position.deletePair(ArrayList<Any>(Arrays.asList(rowID)), position)
        elementCount -= 1
    }

    @Throws(IOException::class, MiniDBException::class)
    fun readRow(rowID: Long): ArrayList<Any> {
        val position: Long = rowID2position.search(ArrayList<Any>(Arrays.asList(rowID))).get(0)
        if (freeSlots.contains(position)) {
            throw MiniDBException(String.format("The row %d does not exist!", rowID))
        }
        file.seek(position)
        val buffer = ByteArray(conf.pageSize)
        file.read(buffer)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)
        bbuffer.long
        return conf.readKey(bbuffer)
    }

    @Throws(IOException::class, MiniDBException::class)
    fun updateRow(rowID: Long, newKey: ArrayList<Any?>) {
        val position: Long = rowID2position.search(ArrayList<Any>(listOf(rowID)))[0]
        val buffer = ByteArray(conf.pageSize)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)
        bbuffer.putLong(rowID)
        conf.writeKey(bbuffer, newKey)
        file.seek(position)
        file.write(buffer)
    }

    class SearchResult {
        var key: ArrayList<Any>?
        var rowID: Long

        constructor(key: ArrayList<Any>, rowID: Long) {
            this.key = key
            this.rowID = rowID
        }

        constructor() {
            key = null
            rowID = 0
        }
    }

    // linear scan
    @Throws(IOException::class)
    fun searchRows(pred: Function<SearchResult?, Boolean?>): LinkedList<SearchResult> {
        val length = file.length()
        val positions = LongArray(length.toInt() / conf.pageSize)
        var index = 0
        run {
            var i = conf.pageSize.toLong()
            while (i < length) {
                if (!freeSlots.contains(i)) {
                    positions[index++] = i
                }
                i += conf.pageSize
            }
        }
        val ans = LinkedList<SearchResult>()
        for (i in 0 until index) {
            val position = positions[i]
            file.seek(position)
            val buffer = ByteArray(conf.pageSize)
            file.read(buffer)
            val bbuffer = ByteBuffer.wrap(buffer)
            bbuffer.order(ByteOrder.BIG_ENDIAN)
            val each = SearchResult()
            each.rowID = bbuffer.long
            each.key = conf.readKey(bbuffer)
            if (pred.apply(each) == true) {
                ans.add(each)
            }
        }
        return ans
    }

    @Throws(IOException::class, MiniDBException::class)
    fun close() {
        // allocate space when there are no free slots
        val position = firstAvailablePageIndex
        freeSlots.add(position)

        // trim file
        val tailFreePages = TreeSet<Long>()
        var lastPos = file.length() - conf.pageSize
        while (!freeSlots.isEmpty()) {
            val last = freeSlots.last()
            lastPos -= if (lastPos == last) {
                freeSlots.pollLast()
                tailFreePages.add(last)
                conf.pageSize
            } else {
                break
            }
        }
        if (tailFreePages.size < 20) { // waste some pages, ok
            freeSlots.addAll(tailFreePages)
        } else { // too many free pages at the last, trim the file
            for (i in 0..19) {
                freeSlots.add(tailFreePages.pollFirst())
            }
            file.setLength(file.length() - conf.pageSize * tailFreePages.size)
        }
        val positions = arrayOfNulls<Long>(freeSlots.size + 1)
        freeSlots.toArray(positions)
        positions[positions.size - 1] = -1L
        var pages = positions.size / conf.nValidPointerInFreePage
        if (freeSlots.size % conf.nValidPointerInFreePage !== 0) {
            pages += 1
        }
        val freePagePositions = arrayOfNulls<Long>(pages)
        System.arraycopy(positions, 0, freePagePositions, 0, pages)
        file.seek(0)
        file.writeLong(elementCount)
        file.writeLong((if (pages == 0) -1L else freePagePositions[0])!!)
        val buffer = ByteArray(conf.pageSize)
        var ipage = 0
        var ifree = 1 // the first position is written in the file header
        while (ipage < pages && ifree < positions.size) {
            file.seek(freePagePositions[ipage++]!!)
            val bbuffer = ByteBuffer.wrap(buffer)
            bbuffer.order(ByteOrder.BIG_ENDIAN)
            if (ipage == pages) { // end
                bbuffer.putLong(-1L)
            } else {
                bbuffer.putLong(freePagePositions[ipage]!!)
            }
            for (i in 0 until conf.nValidPointerInFreePage) {
                if (ifree >= positions.size) {
                    bbuffer.putLong(-1L)
                } else {
                    bbuffer.putLong(positions[ifree++]!!)
                }
            }
            file.write(buffer)
        }
        file.close()
        rowID2position.commitTree()
    }
}
