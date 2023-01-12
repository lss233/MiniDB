package com.lss233.minidb.engine.index.bptree

import com.lss233.minidb.exception.MiniDBException
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

/**
 * On-disk BPlus Tree implementation.
 * values are unique. keys are unique. But one key can have multiple corresponding values,
 * depending on the `unique` flag in the `BPlusConfiguration` parameter.
 * The tree is used to represent a mapping from index to rowID.
 * internal node:
 *  k_{-1}=-inf     k0          k1         k2        k_{n}=+inf
 *            [p0)      [p1)         [p2)       [p3)
 *              k_{i-1} <= pi < k_{i}, 0 <= i <= n
 *
 *  leaf node:
 *  k_{-1}=-inf     k0          k1         k2        k_{n}=+inf
 *                  v0          v1         v2
 *                  vi, 0 <= i < n
 *
 *  node split:
 *          [ k1 k2 k3 k4 ]
 *
 *  This split would result in the following:
 *
 *              [ k3 ]
 *              /   \
 *            /      \
 *          /         \
 *     [ k1 k2 ]   [ k3 k4 ]
 *
 * Constructor BPlusTree
 * @param conf B+ Tree configuration instance
 * @param mode I/O mode
 * @param treeFilePath file path for the file
 * @throws IOException is thrown when we fail to open/create the binary tree file
 * */

@Suppress("unused")
class BPlusTree constructor(var conf: BPlusConfiguration, mode: String, treeFilePath: String) {

    private var root: TreeNode? = null
    private var aChild: TreeNode? = null
    private val treeFile: RandomAccessFile
    private val freeSlots: LinkedList<Long> // each element reflects a free page
    private var firstFreePoolPointer: Long
    private var usedPages: Long = 2L // number of used pages (>= 1, header page)
    private var totalPages: Long = 2L // number of pages in the file (>= 1, used pages + free pages), can be counted from file length
    private var deleteIterations: Int = 0

    init {
        usedPages = 2L
        totalPages = 2L
        deleteIterations = 0
        firstFreePoolPointer = -1L
        freeSlots = LinkedList()
        val f = File(treeFilePath)
        val stmode = mode.substring(0, 2)
        treeFile = RandomAccessFile(treeFilePath, stmode)
        // check if the file already exists
        if (f.exists() && !mode.contains("+")) {
            totalPages = treeFile.length() / conf.pageSize
            readFileHeader(treeFile)
            // read the free pages
            var pindex = firstFreePoolPointer
            var fpn: TreeFreePoolNode
            while (pindex != -1L) {
                freeSlots.add(pindex)
                fpn = readNode(pindex) as TreeFreePoolNode
                for (each: ArrayList<Any> in fpn.keyArray!!) {
                    freeSlots.addLast(each[0] as Long)
                }
                pindex = fpn.nextPointer
            }
        } else {
            treeFile.setLength(2L * conf.pageSize) // initial tree have 2 pages. head page and root page
            root = TreeLeaf(
                -1, -1,
                TreeNode.TreeNodeType.TREE_ROOT_LEAF, conf.pageSize.toLong()
            )
            root!!.writeNode(treeFile, conf)
            writeFileHeader(conf)
        }
    }

    // Is this a unique index?
    fun isUnique(): Boolean {
        return conf.unique
    }

    /**
     * Insert the (key, value) pair into the tree
     * @param key key to add
     * @param value value of the key
     * @throws IOException is thrown when any of the read/write ops fail.
     * @throws IllegalStateException is thrown we have a null tree
     * @throws MiniDBException
     */
    @Suppress("unused")
    @Throws(IOException::class, MiniDBException::class, IllegalStateException::class)
    fun insertPair(key: ArrayList<Any>, value: Long) {
        if (root == null) {
            throw IllegalStateException("Can't insert to null tree")
        }
        conf.padKey(key)

        // check if our root is full
        if (root!!.isFull(conf)) {
            // allocate a new *internal* node, to be placed as the
            // *left* child of the new root
            aChild = this.root
            val nodeBuf = TreeInternalNode(
                TreeNode.TreeNodeType.TREE_ROOT_INTERNAL,
                generateFirstAvailablePageIndex(conf)
            )
            nodeBuf.addPointerAt(0, aChild!!.getPageIndex())
            this.root = nodeBuf

            // split root.
            splitTreeNode(nodeBuf, 0)
            writeFileHeader(conf)
            insertNonFull(nodeBuf, key, value)
        } else {
            insertNonFull(root!!, key, value)
        }
    }

    /**
     * This function is inspired from the one given in CLRS for inserting a key to
     * a B-Tree but as splitTreeNode has been (heavily) modified in order to be used
     * in our B+ Tree. It supports handling duplicate keys (if enabled) as well.
     *
     * It is able to insert the (Key, Value) pairs using only one pass through the tree.
     *
     * @param n current node
     * @param key key to add
     * @param value value paired with the key
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun insertNonFull(n: TreeNode, key: ArrayList<Any>, value: Long) {
        var useChild = true
        val i = binSearchBlock(n, key, Rank.PlusOne)
        // check if we have a leaf
        if (n.isLeaf()) {
            val l = n as TreeLeaf

            // before we add it, let's check if the key already exists
            // and if it does pull up (or create) the overflow page and
            // add the value there.
            //
            // Not that we do *not* add the key if we have a true unique flag


            // this is to adjust for a corner case due to indexing
            val iadj = if (n.getCurrentCapacity() > 0 &&
                i == 0 && conf.gt(n.getFirstKey(), key)) i else i - 1
            if (n.getCurrentCapacity() > 0 && conf.eq(n.getKeyAt(iadj), key)) { // duplicate value for the same key
                if (conf.unique) {
                    throw MiniDBException(
                        java.lang.String.format(
                            MiniDBException.DuplicateValue, value.toString(),
                            conf.keyToString(key)
                        )
                    )
                }

                // overflow page does not exist, yet; time to create it!
                if (l.getOverflowPointerAt(iadj) < 0) {
                    createOverflowPage(l, iadj, value)
                } else {
                    var ovf = readNode(l.getOverflowPointerAt(iadj)) as TreeOverflow?
                    while (ovf!!.isFull(conf)) {
                        // check if we have more, if not create
                        if (ovf.getNextPagePointer() < 0) {
                            // create page and return
                            createOverflowPage(ovf, -1, value)
                            return
                        } else {
                            ovf = readNode(ovf.getNextPagePointer()) as TreeOverflow?
                        }
                    }

                    // if the loaded page is not full then add it.
                    ovf.pushToValueList(value)
                    ovf.incrementCapacity(conf)
                    ovf.writeNode(treeFile, conf)
                }
            } else {
                // we have a new key insert
                l.addToValueList(i, value)
                l.addToKeyArrayAt(i, key)
                // also create a NULL overflow pointer
                l.addToOverflowList(i, -1L)
                l.incrementCapacity(conf)
                // commit the changes
                l.writeNode(treeFile, conf)
            }
        } else {

            // This requires a bit of explanation; the above while loop
            // starts initially from the *end* key parsing the *right*
            // child, so initially it's like this:
            //
            //  Step 0:
            //
            //  Key Array:          | - | - | - | x |
            //  Pointer Array:      | - | - | - | - | x |
            //
            // Now if the while loop stops there, we have a *right* child
            // pointer, but should it continues we get the following:
            //
            // Step 1:
            //
            //  Key Array:          | - | - | x | - |
            //  Pointer Array:      | - | - | - | x | - |
            //
            //  and finally we reach the special case where we have the
            //  following:
            //
            // Final step:
            //
            //  Key Array:          | x | - | - | - |
            //  Pointer Array:      | x | - | - | - | - |
            //
            //
            // In this case we have a *left* pointer, which can be
            // quite confusing initially... hence the changed naming.
            //
            //
            val inode = n as TreeInternalNode
            aChild = readNode(inode.getPointerAt(i))
            if (aChild!!.isOverflow() || aChild!!.isFreePoolNode()) {
                // "aChild can't be overflow node"
                throw MiniDBException(MiniDBException.InvalidBPTreeState)
            }
            var nextAfterAChild: TreeNode? = null
            if (aChild!!.isFull(conf)) {
                splitTreeNode(inode, i)
                if (conf.ge(key, n.getKeyAt(i))) {
                    useChild = false
                    nextAfterAChild = readNode(inode.getPointerAt(i + 1))
                }
            }
            (if (useChild) aChild else nextAfterAChild)?.let { insertNonFull(it, key, value) }
        }
    }

    /**
     *
     * This function is based on the similar function prototype that
     * is given by CLRS for B-Tree but is altered (quite a bit) to
     * be able to be used for B+ Trees.
     * The main difference is that when the split happens *all* keys
     * are preserved and the first key of the right node is moved up.
     * For example say we have the following (order is assumed to be 3):
     *          [ k1 k2 k3 k4 ]
     * This split would result in the following:
     *              [ k3 ]
     *              /   \
     *            /      \
     *          /         \
     *     [ k1 k2 ]   [ k3 k4 ]
     * This function requires at least *3* page writes plus the commit of
     * the updated page count to the file header. In the case that after
     * splitting we have a new root we must commit the new root index
     * to the file header as well; this happens transparently inside
     * writeNode method and is not explicitly done here.
     *
     * @param n internal node "parenting" the split
     * @param index index in the node n that we need to add the median
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun splitTreeNode(n: TreeInternalNode, index: Int) {
        val setIndex: Int
        val znode: TreeNode
        val keyToAdd: ArrayList<Any>
        val ynode = aChild // x.c_{i}
        if (ynode!!.isInternalNode()) {
            val yInternal = ynode as TreeInternalNode
            val zInternal: TreeInternalNode = TreeInternalNode(
                TreeNode.TreeNodeType.TREE_INTERNAL_NODE,
                generateFirstAvailablePageIndex(conf)
            )
            val oldCapacity = ynode.getCurrentCapacity()
            setIndex = oldCapacity / 2
            var i = 0
            while (i < setIndex) {
                zInternal.addToKeyArrayAt(i, yInternal.popKey())
                zInternal.addPointerAt(i, yInternal.popPointer())
                i++
            }
            zInternal.addPointerAt(i, yInternal.popPointer())
            //keyToAdd = ynode.getFirstKey();
            keyToAdd = ynode.popKey()
            zInternal.setCurrentCapacity(setIndex)
            yInternal.setCurrentCapacity(oldCapacity - setIndex - 1)

            // it is the root, invalidate it and make it a regular internal node
            if (yInternal.isRoot()) {
                yInternal.setNodeType(TreeNode.TreeNodeType.TREE_INTERNAL_NODE)
            }

            // update pointer at n_{index+1}
            n.addPointerAt(index, zInternal.getPageIndex())
            // update key value at n[index]
            n.addToKeyArrayAt(index, keyToAdd)
            // adjust capacity
            n.incrementCapacity(conf)
            // update shared child pointer.
            aChild = zInternal
            // update reference
            znode = zInternal
        } else {
            val zLeaf: TreeLeaf
            val yLeaf = ynode as TreeLeaf
            val afterLeaf: TreeLeaf
            zLeaf = TreeLeaf(
                yLeaf.getNextPagePointer(),
                yLeaf.getPageIndex(), TreeNode.TreeNodeType.TREE_LEAF,
                generateFirstAvailablePageIndex(conf)
            )

            // update the previous pointer from the node after ynode
            if (yLeaf.nextPagePointer != (-1).toLong()) {
                afterLeaf = (readNode(yLeaf.getNextPagePointer()) as TreeLeaf)
                afterLeaf.prevPagePointer = zLeaf.getPageIndex()
                afterLeaf.writeNode(treeFile, conf)
            }

            // update pointers in ynode, only have to update next pointer
            yLeaf.setNextPagePointer(zLeaf.getPageIndex())
            val oldCapacity = yLeaf.getCurrentCapacity()
            setIndex = oldCapacity / 2
            for (i in 0 until setIndex) {
                //long fk = ynode.getLastKey();
                //long ovf1 = ((TreeLeaf)ynode).getLastOverflowPointer();
                zLeaf.pushToKeyArray(yLeaf.removeLastKey())
                zLeaf.pushToValueList(yLeaf.removeLastValue())
                zLeaf.pushToOverflowList(yLeaf.removeLastOverflowPointer())
                zLeaf.incrementCapacity(conf)
                yLeaf.decrementCapacity(conf)
            }

            // it is the root, invalidate it and make it a regular leaf
            if (yLeaf.isRoot()) {
                yLeaf.setNodeType(TreeNode.TreeNodeType.TREE_LEAF)
            }

            // update pointer at n_{index+1}
            n.addPointerAt(index + 1, zLeaf.getPageIndex())
            // update key value at n[index]
            n.addToKeyArrayAt(index, zLeaf.getKeyAt(0))
            // adjust capacity
            n.incrementCapacity(conf)
            // update reference
            znode = zLeaf
        }
        znode.setBeingDeleted(false)
        // commit the changes
        znode.writeNode(treeFile, conf)
        ynode.writeNode(treeFile, conf)
        n.writeNode(treeFile, conf)
    }

    /**
     * @param n node to add the page
     * @param index this is only used in the case of a leaf
     * @param value value to push in the new page
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun createOverflowPage(n: TreeNode, index: Int, value: Long) {
        val novf: TreeOverflow
        if (n.isOverflow()) {
            val ovf = n as TreeOverflow
            novf = TreeOverflow(
                -1L, ovf.getPageIndex(),
                generateFirstAvailablePageIndex(conf)
            )
            // push the first value
            novf.pushToValueList(value)
            novf.incrementCapacity(conf)
            // update overflow pointer to parent node
            ovf.setNextPagePointer(novf.getPageIndex())
            // set being deleted to false
            novf.setBeingDeleted(false)
            novf.addToKeyArrayAt(0, ovf.getKeyAt(0)) // record the corresponding key
            // commit changes to new overflow page
            novf.writeNode(treeFile, conf)
            // commit changes to old overflow page
            ovf.writeNode(treeFile, conf)
        } else if (n.isLeaf()) {
            val l = n as TreeLeaf
            novf = TreeOverflow(
                -1L, l.getPageIndex(),
                generateFirstAvailablePageIndex(conf)
            )
            novf.addToKeyArrayAt(0, n.getKeyAt(index)) // record the corresponding key
            // push the first value
            novf.pushToValueList(value)
            novf.incrementCapacity(conf)
            // update overflow pointer to parent node
            l.setOverflowPointerAt(index, novf.getPageIndex())
            // set being deleted to false
            novf.setBeingDeleted(false)
            // commit changes to overflow page
            novf.writeNode(treeFile, conf)
            // commit changes to leaf page
            l.writeNode(treeFile, conf)
            // commit page counts
        } else {
            // "Expected Leaf or Overflow, got instead: " + n.getNodeType().toString()
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }
    }

    /**
     * Binary search implementation for tree blocks; if not found returns the lower/upper bound
     * position instead based on `rank`.
     * @param n node to search
     * @param key key to search
     * @param rank rank of the search (for lower/upper bound)
     * @return the index of the bound or found key.
     */
    private fun binSearchBlock(n: TreeNode, key: ArrayList<Any>, rank: Rank): Int {
        val ans = binarySearch(n, 0, n.getCurrentCapacity(), key)
        return when (rank) {
            Rank.PlusOne -> ans + 1
            Rank.Succ -> if (ans == -1) { // not found
                0
            } else {
                // and can be used as array index ([0, n))
                if (conf.eq(key, n.getKeyAt(ans))) { // found
                    ans
                } else (ans + 1).coerceAtMost(n.getCurrentCapacity() - 1)
                // not found
            }
            else -> ans
        }
    }

    /**
     * binary search by Junhui Deng
     * search in [lo, hi). 0 <= lo <= hi <= n
     * the common usage is binarySearch(node, 0, n).
     * if key is very small, -1 is returned.
     * if key is very large, n - 1 is returned.
     */
    private fun binarySearch(node: TreeNode, lo: Int, hi: Int, key: ArrayList<Any>): Int {
        var lo = lo
        var hi = hi
        while (lo < hi) {
            val mi = (lo + hi) / 2
            if (conf.lt(key, node.getKeyAt(mi))) {
                hi = mi
            } else {
                lo = mi + 1
            }
        }
        return lo - 1
    }

    /**
     * read the values for the specified key. overflow values are read as well.
     *
     * @param l leaf which contains the key with the overflow page
     * @param i index of the key
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class)
    private fun getValues(l: TreeLeaf, i: Int): LinkedList<Long> {
        val ans = LinkedList<Long>()
        ans.addLast(l.valueList[i])
        if (conf.unique) {
            return ans
        }
        // non-unique index, find values in the overflow pages
        if (l.getOverflowPointerAt(i) != (-1).toLong()) {
            var povf = readNode(l.getOverflowPointerAt(i)) as TreeOverflow?
            while (true) {
                ans.addAll(povf!!.valueList)
                if (povf.getNextPagePointer() != -1L) {
                    povf = readNode(povf.getNextPagePointer()) as TreeOverflow?
                } else {
                    break
                }
            }
        }
        return ans
    }

    /**
     * @param minKey min key of the range
     * @param maxKey max key of the range
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    fun rangeSearch(minKey: ArrayList<Any>, maxKey: ArrayList<Any>, includeMin: Boolean, includeMax: Boolean): LinkedList<Long> {
        val ans = LinkedList<Long>()
        conf.padKey(minKey)
        conf.padKey(maxKey)
        if (conf.eq(minKey, maxKey) && (!includeMax || !includeMin)) {
            // the maxKey == minKey, but the interval is open in either side
            return ans
        }
        val minAns = findKey(minKey)

        // read up until we find a key that's greater than maxKey
        // or the last entry.
        var leaf: TreeLeaf? = minAns.leafLoc
        var i = minAns.index

        // shit corner case. just move to one key left to assure leaf.ki <= minKey
        if (i > 0) {
            --i
        } else {
            if (leaf != null) {
                if (leaf.prevPagePointer >= 0) // check if we have a previous node to load.
                {
                    leaf = readNode(leaf.prevPagePointer) as TreeLeaf?
                    i = leaf!!.getCurrentCapacity() - 1
                }
            }
        }

        // left part, iterate while leaf.ki < minKey
        while (conf.lt(leaf!!.getKeyAt(i), minKey)) {
            i++
            if (i == leaf.getCurrentCapacity()) {
                // check if we need to read the next block
                if (leaf.getNextPagePointer() < 0) {
                    // check if we have a next node to load.
                    return ans
                }
                leaf = readNode(leaf.getNextPagePointer()) as TreeLeaf?
                i = 0
            }
        }

        // now leaf.ki >= minKey

        // left corner case
        if (includeMin && conf.eq(leaf.getKeyAt(i), minKey)) {
            ans.addAll(getValues(leaf, i))
            ++i
            if (i == leaf.getCurrentCapacity()) {
                // check if we need to read the next block
                if (leaf.getNextPagePointer() < 0) {
                    // check if we have a next node to load.
                    return ans
                }
                leaf = readNode(leaf.getNextPagePointer()) as TreeLeaf?
                i = 0
            }
        }

        // middle part, left open and right open
        while (conf.lt(leaf!!.getKeyAt(i), maxKey)) {
            ans.addAll(getValues(leaf, i))
            i++
            if (i == leaf.getCurrentCapacity()) {
                // check if we need to read the next block
                if (leaf.getNextPagePointer() < 0) {
                    // check if we have a next node to load.
                    return ans
                }
                leaf = readNode(leaf.getNextPagePointer()) as TreeLeaf?
                i = 0
            }
        }

        // right corner case
        if (includeMax && conf.eq(leaf.getKeyAt(i), maxKey)) {
            ans.addAll(getValues(leaf, i))
        }
        return ans
    }

    /**
     * @param key the key to find
     * @return if `key` is not found, `the last leaf node that is searched` is returned.
     * else, return Pair(leaf node that contains the given key, the index of the key)
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun findKey(key: ArrayList<Any>): SearchResult {
        return _findKey(root!!, null, -1, -1, key)
    }

    @Throws(IOException::class, MiniDBException::class)
    private fun _findKey(currentArg: TreeNode, parent: TreeInternalNode?, parentPointerIndex: Int, parentKeyIndex: Int, key: ArrayList<Any>): SearchResult {
        // check if we need to consolidate
        var current = currentArg
        if (current.isTimeToMerge(conf)) {
            //System.out.println("Parent needs merging (internal node)");
            val mres = mergeOrRedistributeTreeNodes(
                current, parent,
                parentPointerIndex, parentKeyIndex
            )
            if (mres != null) {
                current = mres
            }
        }

        // search for the key
        val i = binSearchBlock(current, key, Rank.Succ)
        // check if it's an internal node
        if (current.isInternalNode()) {
            // if it is, descend to a leaf
            val inode = current as TreeInternalNode?
            var idx = i
            // check if we are at the end
            if (conf.ge(key, current.getKeyAt(i))) {
                idx++
            }
            // read the next node
            val next = readNode(inode!!.getPointerAt(idx))
            // finally return the resulting set
            return _findKey(next!!, inode, idx, i, key)
        } else if (current.isLeaf()) {
            val l = current as TreeLeaf?
            // check if we actually found the key
            return if (i == l!!.getCurrentCapacity() || conf.neq(key, l.getKeyAt(i))) {
                //key not found
                SearchResult(l, i, false)
            } else {
                // key found!
                SearchResult(l, i, true)
            }
        }
        throw MiniDBException(MiniDBException.BadNodeType)
    }

    /**
     * delete or update an existing pair.
     * It makes update faster than deleting and inserting, as well as sharing much code with deletion.
     * @param delete whether to delete or update.
     * @param value useful if `not conf.unique`. (if `conf.unique`, the `key` is enough to identify the pair)
     * @param newValue useful only when `delete == false` (which means update).
     * @return whether the deletion or update is successful
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun deleteOrUpdatePair(key: ArrayList<Any>, value: Long, newValue: Long, delete: Boolean): Boolean {
        conf.padKey(key)
        val ans = findKey(key)
        if (!ans.found) {
            return false
        }
        val l = ans.leafLoc
        val i = ans.index
        if (conf.unique) { // the key is enough to identify the pair. `value` is useless
            if (delete) {
                l.removeEntryAt(i, conf)
            } else {
                l.valueList[i] = newValue
            }
            l.writeNode(treeFile, conf)
            return true
        }
        val savedValue = l.getValueAt(i)
        if (savedValue == value) { // the value is found in this node
            if (delete) {
                val ovfpointer = l.getOverflowPointerAt(i)
                if (ovfpointer == -1L) { //there is only one value for the key. delete the (key, value) pair.
                    l.removeEntryAt(i, conf)
                } else { // the index is not unique and there are multiple values.
                    // delete one value and read a value from the overflow page.
                    val povf = readNode(ovfpointer) as TreeOverflow
                    l.valueList.set(i, povf.valueList.remove())
                    povf.decrementCapacity(conf)
                    // persist
                    povf.writeNode(treeFile, conf)
                }
            } else {
                l.valueList[i] = newValue
            }
            l.writeNode(treeFile, conf)
            return true
        }

        // check if we have an overflow page
        if (l.getOverflowPointerAt(i) != (-1).toLong()) {
            var ovf: TreeOverflow? = null // parent of `pre ovf`
            var povf = readNode(l.getOverflowPointerAt(i)) as TreeOverflow?
            var found = false
            while (true) {
                // find the value in one overflow page
                val it = povf!!.valueList.listIterator()
                while (it.hasNext() && !found) {
                    if (it.next() == value) {
                        found = true
                    }
                }
                if (found) {
                    if (delete) {
                        it.remove()
                        povf.decrementCapacity(conf) // decrease the page capacity
                    } else {
                        it.set(newValue)
                    }
                    if (povf.isEmpty()) // delete a value, may cause empty overflow page
                    {
                        // it's time to remove the page
                        if (ovf == null) {
                            l.setOverflowPointerAt(i, -1L)
                            l.writeNode(treeFile, conf)
                        } else {
                            ovf.setNextPagePointer(-1L)
                            ovf.writeNode(treeFile, conf)
                        }
                        deletePage(povf.getPageIndex(), false)
                    } else {
                        // delete a value but the page is not empty, or update a value. just update the page.
                        if (!delete && value == newValue) {
                            // small optimization. if the update is the same, no need to write the file.
                        } else {
                            povf.writeNode(treeFile, conf)
                        }
                    }
                    return true
                } else {
                    // find the value in the next page
                    if (povf.getNextPagePointer() != -1L) {
                        ovf = povf
                        povf = readNode(povf.getNextPagePointer()) as TreeOverflow?
                    } else {
                        // value not found
                        return false
                    }
                }
            }
        }
        // pair not found
        return false
    }

    @Throws(IOException::class, MiniDBException::class)
    fun deletePair(key: ArrayList<Any>, value: Long): Boolean {
        return deleteOrUpdatePair(key, value, -1L, true)
    }

    @Throws(IOException::class, MiniDBException::class)
    fun updatePair(key: ArrayList<Any>, value: Long, newValue: Long): Boolean {
        return deleteOrUpdatePair(key, value, newValue, false)
    }

    /**
     * query all the values with key.
     * @return not null. LinkedList with length >= 0.
     */
    @Throws(IOException::class, MiniDBException::class)
    fun search(key: ArrayList<Any>): LinkedList<Long> {
        val returnValue = LinkedList<Long>()
        conf.padKey(key)
        val ans = findKey(key)
        if (!ans.found) {
            return returnValue
        }
        val l = ans.leafLoc
        val i = ans.index
        returnValue.addAll(getValues(l, i))
        return returnValue
    }

    /**
     * Check if the node has the specified parent
     *
     * @param node node can be internal or leaf
     * @param parent parent is always internal node
     * @param pindex index to check
     * @return true if it is, false if it's not
     */
    private fun isParent(node: TreeNode, parent: TreeInternalNode, pindex: Int): Boolean {
        return parent.getCurrentCapacity() >= pindex && pindex >= 0 && node.getPageIndex() == parent.getPointerAt(pindex)
    }

    /**
     * Simple helper function to check if we can re-distribute the node
     * values.
     *
     * @param with node to check the capacity
     * @return the number of positions to check
     */
    @Throws(MiniDBException::class)
    private fun canRedistribute(with: TreeNode?): Int {
        if (with != null) {
            if (with.isInternalNode()) {
                if (isValidAfterRemoval(with as TreeInternalNode, 1)) {
                    return 1
                }
            } else if (with.isLeaf()) {
                if (isValidAfterRemoval(with as TreeLeaf, 1)) {
                    return 1
                }
            } else {
                //"Not leaf or internal node found"
                throw MiniDBException(MiniDBException.InvalidBPTreeState)
            }
        }
        return -1
    }

    /**
     * Check if the internal node fulfills the B+ Tree invariant after removing
     * `remove` number of elements
     *
     * @param node node to check
     * @param remove elements to be removed
     * @return true if it does, false if it fails the condition
     */
    private fun isValidAfterRemoval(node: TreeInternalNode, remove: Int): Boolean {
        return node.getCurrentCapacity() - remove >= conf.getMinInternalNodeCapacity()
    }

    /**
     * Check if the leaf node fulfills the B+ Tree invariant after removing
     * `remove` number of elements
     *
     * @param node node to check
     * @param remove elements to be removed
     * @return true if it does, false if it fails the condition
     */
    private fun isValidAfterRemoval(node: TreeLeaf, remove: Int): Boolean {
        return node.getCurrentCapacity() - remove >= conf.getMinLeafNodeCapacity()
    }

    /**
     * Function that is responsible to redistribute values among two leaf nodes
     * while updating the referring key of the parent node (always an internal node).
     *
     *
     * We have two distinct cases which are the following:
     *
     * This case is when we use the prev pointer:
     *
     * |--------|  <-----  |--------|
     * |  with  |          |   to   |
     * |--------|  ----->  |--------|
     *
     * In this case we  *remove* the *last* n elements from with
     * and *push* them (in the order removed) into the destination node
     *
     * The parent key-pointer is updated with the first value of the
     * receiving node.
     *
     * The other case is when we use the next pointer:
     *
     * |--------|  <-----  |--------|
     * |   to   |          |  with  |
     * |--------|  ----->  |--------|
     *
     * In this case we *remove* the *first* n elements from with and
     * add them *last* (in the order removed) into the destination node.
     *
     * The parent key-pointer is updated with the first value of the
     * node we retrieved the values.
     *
     *
     *
     * @param to node to receive (Key, Value) pairs
     * @param with node that we take the (Key, Value) pairs
     * @param left if left is true, then we use prev leaf else next
     * @param parent the internal node parenting both
     * @param parentKeyIndex index of the parent that refers to this pair
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun redistributeNodes(
        to: TreeLeaf?, with: TreeLeaf?,
        left: Boolean, parent: TreeInternalNode?,
        parentKeyIndex: Int
    ) {
        val key: ArrayList<Any>
        // handle the case when redistributing using prev
        if (left) {
            to!!.pushToOverflowList(with!!.removeLastOverflowPointer())
            to.pushToValueList(with.removeLastValue())
            to.pushToKeyArray(with.removeLastKey())
            to.incrementCapacity(conf)
            with.decrementCapacity(conf)
            // get the key from the left node
            key = to.getKeyAt(0)
        } else {
            to!!.addLastToOverflowList(with!!.popOverflowPointer())
            to.addLastToValueList(with.popValue())
            to.addLastToKeyArray(with.popKey())
            to.incrementCapacity(conf)
            with.decrementCapacity(conf)
            // get the key from the right node
            key = with.getKeyAt(0)
        }

        // in either case update parent pointer
        parent!!.setKeyArrayAt(parentKeyIndex, key)
        // finally write the changes
        to.writeNode(treeFile, conf)
        with.writeNode(treeFile, conf)
        parent.writeNode(treeFile, conf)
    }

    /**
     * Function that is responsible to redistribute values among two internal nodes
     * while updating the referring key of the parent node (always an internal node).
     *
     *
     * We have two distinct cases which are the following:
     *
     * This case is when we use the prev pointer:
     *
     * |--------|  <-----  |--------|
     * |  with  |          |   to   |
     * |--------|  ----->  |--------|
     *
     * In this case we  *remove* the *last* n elements from with
     * and *push* them (in the order removed) into the destination node
     *
     * The parent key-pointer is updated with the first value of the
     * receiving node.
     *
     * The other case is when we use the next pointer:
     *
     * |--------|  <-----  |--------|
     * |   to   |          |  with  |
     * |--------|  ----->  |--------|
     *
     * In this case we *remove* the *first* n elements from with and
     * add them *last* (in the order removed) into the destination node.
     *
     * The parent key-pointer is updated with the first value of the
     * node we retrieved the values.
     *
     *
     *
     * @param to node to receive (Key, Value) pairs
     * @param with node that we take the (Key, Value) pairs
     * @param left if left is true, then we use prev leaf else next
     * @param parent the internal node parenting both
     * @param parentKeyIndex index of the parent that refers to this pair
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun redistributeNodes(
        to: TreeInternalNode?, with: TreeInternalNode?,
        left: Boolean, parent: TreeInternalNode?,
        parentKeyIndex: Int
    ) {
        val key: ArrayList<Any>
        val pkey = parent!!.getKeyAt(parentKeyIndex)
        if (left) {
            to!!.pushToKeyArray(pkey)
            key = with!!.removeLastKey()
            to.pushToPointerArray(with.removeLastPointer())
            to.incrementCapacity(conf)
            with.decrementCapacity(conf)
        } else {
            to!!.addLastToKeyArray(pkey)
            key = with!!.popKey()
            to.addPointerLast(with.popPointer())
            to.incrementCapacity(conf)
            with.decrementCapacity(conf)
        }
        // in either case update the parent key
        parent.setKeyArrayAt(parentKeyIndex, key)
        // finally write the chances
        to.writeNode(treeFile, conf)
        with.writeNode(treeFile, conf)
        parent.writeNode(treeFile, conf)
    }

    /**
     * Function that merges two leaves together; in this case we *must*
     * have two leaves, left and right that are merged and their parent
     * that *must* be an internal node (or root).
     *
     * We also have the index of the parent that the pointers indicate
     * these two leaves
     *
     * it should be like this:
     *
     *            parent
     *      ... |  key  | ...
     *           /     \
     *      | left | right |
     *
     *
     * The merge happens from right -> left thus the final result would
     * be like this:
     *
     *            parent
     *      ... |  key  | ...
     *           /     \
     *      | result |  x
     *
     *
     * So basically we dump the values of right to the left while
     * updating the pointers.
     *
     * Constructor
     * @param left left-most leaf to merge
     * @param right right-most leaf to merge
     * @param other the other cached node
     * @param parent parent of both leaves (internal node)
     * @param parentPointerIndex index of parent that has these two pointers
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun mergeNodes(
        left: TreeLeaf, right: TreeLeaf, other: TreeLeaf,
        parent: TreeInternalNode, parentPointerIndex: Int,
        parentKeyIndex: Int, isLeftOfNext: Boolean,
        useNextPointer: Boolean
    ): TreeNode {
        if (left.getCurrentCapacity() + right.getCurrentCapacity() >
            conf.getMaxLeafNodeCapacity()
        ) {
            // "Leaf node capacity exceeded in merge"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }

        // flag the node for deletion
        right.setBeingDeleted(true)
        // join the two leaves together.
        val cap = right.getCurrentCapacity()
        joinLeaves(left, right, cap)

        // update the next pointers
        left.setNextPagePointer(right.getNextPagePointer())
        // now fix the top pointer
        fixTheTopPointer(
            other, parent, parentPointerIndex,
            parentKeyIndex, isLeftOfNext, useNextPointer
        )

        // update capacity as in both cases we remove a value
        parent.decrementCapacity(conf)
        // write parent node
        parent.writeNode(treeFile, conf)

        // update the prev pointer of right next node (if any)
        if (right.nextPagePointer != (-1).toLong()) {
            val rnext = readNode(right.getNextPagePointer()) as TreeLeaf?
            if (rnext != null) {
                rnext.prevPagePointer = left.getPageIndex()
            }
            rnext!!.writeNode(treeFile, conf)
        }

        // write the left node to disk
        left.writeNode(treeFile, conf)
        // remove the page
        deletePage(right.getPageIndex(), false)
        // finally return the node reference
        return left
    }

    private fun fixTheTopPointer(
        other: TreeNode, parent: TreeInternalNode,
        parentPointerIndex: Int, parentKeyIndex: Int,
        isLeftOfNext: Boolean, useNextPointer: Boolean
    ) {
        if (useNextPointer) {
            if (isLeftOfNext) {
                parent.removeKeyAt(parentKeyIndex + 1)
                parent.removePointerAt(parentPointerIndex + 1)
            } else {
                parent.removeKeyAt(parentKeyIndex)
                parent.removePointerAt(parentPointerIndex + 1)
            }
        } else {
            if (isLeftOfNext) {
                parent.removeKeyAt(parentKeyIndex)
                parent.removePointerAt(parentPointerIndex)
            } else {
                parent.removeKeyAt(parentKeyIndex)
                parent.removePointerAt(parentPointerIndex)
                parent.setKeyArrayAt(parentKeyIndex - 1, other.getFirstKey())
            }
        }
    }

    /**
     * @param left  left leaf
     * @param right right leaf
     * @param cap   max capacity of the leaf
     */
    @Throws(MiniDBException::class)
    private fun joinLeaves(left: TreeLeaf?, right: TreeLeaf?, cap: Int) {
        for (i in 0 until cap) {
            left!!.addLastToOverflowList(right!!.popOverflowPointer())
            left.addLastToValueList(right.popValue())
            left.addLastToKeyArray(right.popKey())
            left.incrementCapacity(conf)
            right.decrementCapacity(conf)
        }
    }

    /**
     * Naive merge of two leaf nodes from right to left.
     *
     * @param left node that merge ends
     * @param right node that is deleted during merge
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun mergeNodes(left: TreeLeaf, right: TreeLeaf) {
        right.setBeingDeleted(true)
        // join the two leaves together.
        val cap = right.getCurrentCapacity()
        joinLeaves(left, right, cap)

        // remove the page
        deletePage(right.getPageIndex(), false)
    }

    /**
     * Function that merges two internal nodes together; in this case
     * we *must* have two internal nodes, left and right that are merged
     * and their parent that *must* be an internal node (or root).
     *
     * We also have the index of the parent that the pointers
     * indicate these two internal nodes.
     *
     * it should be like this:
     *
     *            parent
     *      ... |  key  | ...
     *           /     \
     *      | left | right |
     *
     * @param left left-most internal node to merge
     * @param right right-most internal node to merge
     * @param parent parent of both internal nodes.
     * @param parentPointerIndex index of the parent that has these two pointers
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun mergeNodes(
        left: TreeInternalNode, right: TreeInternalNode,
        other: TreeInternalNode, parent: TreeInternalNode,
        parentPointerIndex: Int,
        parentKeyIndex: Int,
        isLeftOfNext: Boolean,
        useNextPointer: Boolean
    ): TreeNode {

        // check if we can actually merge
        if (left.getCurrentCapacity() + right.getCurrentCapacity() >
            conf.getMaxInternalNodeCapacity()
        ) {
            // "Internal node capacity exceeded in merge"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }

        // check if we are using a trickery
        if (isLeftOfNext && useNextPointer) {
            left.addLastToKeyArray(parent.getKeyAt(parentKeyIndex + 1))
        } else {
            left.addLastToKeyArray(parent.getKeyAt(parentKeyIndex))
        }

        // flag the node for deletion
        right.setBeingDeleted(true)
        val cap = right.getCurrentCapacity()
        joinInternalNodes(left, right, cap)
        // pump the last pointer as well
        left.addPointerLast(right.popPointer())
        // now increment the capacity as well
        left.incrementCapacity(conf)
        // now fix the top pointer.


        // now fix the top pointer
        fixTheTopPointer(
            other, parent, parentPointerIndex,
            parentKeyIndex, isLeftOfNext, useNextPointer
        )

        // update capacity as in both cases we remove a value
        parent.decrementCapacity(conf)
        // write parent node
        parent.writeNode(treeFile, conf)

        // write the node
        left.writeNode(treeFile, conf)
        // remove the page
        deletePage(right.getPageIndex(), false)
        // finally remove the node reference
        return left
    }

    /**
     * Naive merge of two internal nodes from right to left.
     *
     * @param left node that merge ends
     * @param right node that is deleted during merge
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun mergeNodes(left: TreeInternalNode, right: TreeInternalNode, midKey: ArrayList<Any>) {
        right.setBeingDeleted(true)
        left.addLastToKeyArray(midKey)
        val cap = right.getCurrentCapacity()
        joinInternalNodes(left, right, cap)

        // pump the last pointer as well
        left.addPointerLast(right.popPointer())
        left.incrementCapacity(conf)

        // finally remove the page
        deletePage(right.getPageIndex(), false)
    }

    /**
     * @param left  left node
     * @param right right node
     * @param cap   max capacity of the node
     */
    @Throws(MiniDBException::class)
    private fun joinInternalNodes(left: TreeInternalNode, right: TreeInternalNode, cap: Int) {
        for (i in 0 until cap) {
            left.addLastToKeyArray(right.popKey())
            left.addPointerLast(right.popPointer())
            left.incrementCapacity(conf)
            right.decrementCapacity(conf)
        }
    }

    /**
     *
     * This function handles the merging or redistribution of the nodes depending
     * on their capacity; usually we just redistribute, if not we merge.
     *
     * @param mnode current node (either leaf or internal node)
     * @param parent parent node *always* an internal node
     * @param parentPointerIndex pointer index to mnode in parent
     * @param parentKeyIndex key index that has mnode as a child in parent
     * @return the updated mnode
     * @throws IOException is thrown when an I/O operation fails
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(IOException::class, MiniDBException::class)
    fun mergeOrRedistributeTreeNodes(
        mnode: TreeNode, parent: TreeInternalNode?,
        parentPointerIndex: Int, parentKeyIndex: Int
    ): TreeNode? {

        // that index should not be present
        check(!(parent != null && parentPointerIndex < 0)) { "index < 0" }

        // this is the only case that the tree shrinks, from the root.
        if (parent == null) {
            val lChild = handleRootRedistributionOrMerging(mnode)
            if (lChild != null) return lChild
        } else return if (mnode.isLeaf()) {
            // merging a leaf requires the most amount of work, since
            // all leaves by definition are linked in a doubly-linked
            // linked-list; hence when we merge/remove a node we have
            // to make sure those links are consistent
            handleLeafNodeRedistributionOrMerging(
                mnode, parent,
                parentPointerIndex, parentKeyIndex
            )
        } else if (mnode.isInternalNode()) {
            // we have to merge internal nodes, this is the somewhat easy
            // case, since we do not have to update any more links than the
            // currently pulled nodes.
            handleInternalNodeRedistributionOrMerging(
                mnode, parent,
                parentPointerIndex, parentKeyIndex
            )
        } else {
            throw IllegalStateException("Read unknown or overflow page while merging")
        }

        // probably we didn't change something
        return null
    }


    /**
     * Handle the root node cases (leaf and internal node)
     *
     * @param node root node
     * @return the root node
     * @throws IOException is thrown when an I/O operation fails
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun handleRootRedistributionOrMerging(mnode: TreeNode): TreeNode? {
        if (mnode.isInternalNode()) {
            //System.out.println("\n -- Check if Consolidating Root required");
            if (mnode.getCurrentCapacity() > 1) {
                //System.out.println("\n -- Root size > 2, no consolidation");
                return root
            }
            val splitNode = mnode as TreeInternalNode
            // read up their pointers

            // load the pointers
            val lChild: TreeNode? = readNode(splitNode.getPointerAt(0))
            val rChild: TreeNode? = readNode(splitNode.getPointerAt(1))
            if (lChild == null || rChild == null) {
                // "Null root child found."
                throw MiniDBException(MiniDBException.InvalidBPTreeState)
            }
            val lcap = lChild.getCurrentCapacity()
            val rcap = rChild.getCurrentCapacity()

            // check their type
            if (lChild.isLeaf()) {

                // check if it's time to merge
                if (lcap > conf.getMinLeafNodeCapacity() && rcap > conf.getMinLeafNodeCapacity()) {
                    //System.out.println(" -- No need to consolidate root yet (to -> leaf)");
                    return mnode
                }
                val rNode = mnode
                val pLeaf = lChild as TreeLeaf
                val nLeaf = rChild as TreeLeaf

                // now merge them, and delete root page, while
                // updating its page number etc
                val nnum = canRedistribute(nLeaf)
                val pnum = canRedistribute(pLeaf)
                if (nnum > 0) {
                    redistributeNodes(pLeaf, nLeaf, false, rNode, 0)
                } else if (pnum > 0) {
                    redistributeNodes(nLeaf, pLeaf, true, rNode, 0)
                } else {
                    mergeNodes(pLeaf, nLeaf)
                    // update root page
                    pLeaf.setNodeType(TreeNode.TreeNodeType.TREE_ROOT_LEAF)
                    // update the leaf pointers
                    pLeaf.nextPagePointer = -1L
                    pLeaf.prevPagePointer = -1L
                    // delete previous root page
                    deletePage(root!!.getPageIndex(), false)
                    // set root page
                    root = lChild
                    // write root header
                    writeFileHeader(conf)
                    // write left leaf
                    lChild.writeNode(treeFile, conf)
                    // since we have a new root
                    return lChild
                }
            } else if (lChild.isInternalNode()) {
                // check if it's time to merge
                if (lcap + rcap >= conf.getMaxInternalNodeCapacity()) {
                    //System.out.println(" -- No need to consolidate root yet (to -> internal)");
                    return mnode
                }
                /*
                else {
                    System.out.println("-- Consolidating Root (internal -> internal)");
                }
                */
                val rNode = mnode
                val lIntNode = lChild as TreeInternalNode
                val rIntNode = rChild as TreeInternalNode
                val nnum = canRedistribute(rIntNode)
                val pnum = canRedistribute(lIntNode)
                if (nnum > 0) {
                    redistributeNodes(lIntNode, rIntNode, false, rNode, 0)
                } else if (pnum > 0) {
                    //System.out.println("\t -- Redistributing right with elements from left");
                    redistributeNodes(rIntNode, lIntNode, true, rNode, 0)
                } else {
                    //System.out.println("\t -- Merging leaf nodes");
                    mergeNodes(lIntNode, rIntNode, splitNode.getFirstKey())
                    // update root page
                    lIntNode.setNodeType(TreeNode.TreeNodeType.TREE_ROOT_INTERNAL)
                    // delete previous root page
                    deletePage(root!!.getPageIndex(), false)
                    // set root page
                    root = lChild
                    // write root header
                    writeFileHeader(conf)
                    // write left leaf
                    lChild.writeNode(treeFile, conf)
                    // since we have a new root
                    return lChild
                }
            } else {
                // "Unknown children type"
                throw MiniDBException(MiniDBException.InvalidBPTreeState)
            }
            return root
        } else if (!mnode.isLeaf()) {
            // "Invalid tree Root type."
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }
        return null
    }

    /**
     * Handle the leaf section of the redistribution/merging
     *
     * @param mnode node to process
     * @param parent the parent node
     * @param parentPointerIndex parent pointer index
     * @param parentKeyIndex parent key index that mnode is child
     * @return the updated node (if any updates are made)
     * @throws IOException is thrown when an I/O operation fails
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun handleLeafNodeRedistributionOrMerging(
        node: TreeNode,
        parent: TreeInternalNode,
        parentPointerIndex: Int,
        parentKeyIndex: Int
    ): TreeNode {
        var mnode = node
        val splitNode = mnode as TreeLeaf
        val nptr: TreeLeaf?
        val pptr: TreeLeaf?

        // load the pointers
        nptr = readNode(splitNode.nextPagePointer) as TreeLeaf?
        pptr = readNode(splitNode.prevPagePointer) as TreeLeaf?
        if (nptr == null && pptr == null) {
            // "Both children (leaves) can't null"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        } /*
        else {
            System.out.println("Leaf node merging/redistribution needs to happen");
        }*/

        // validate neighbouring nodes
        validateNeighbours(pptr!!, splitNode, nptr!!)
        val nnum = canRedistribute(nptr)
        val pnum = canRedistribute(pptr)
        val snum = canRedistribute(splitNode)
        val isLeftOfNext = parentPointerIndex > parentKeyIndex
        val splitNodeIsLeftChild = parentKeyIndex == parentPointerIndex
        val npar = isParent(nptr, parent, parentPointerIndex + 1)
        val ppar = isParent(pptr, parent, parentPointerIndex - 1)

        // check if we can redistribute with next
        if (nnum > 0 && npar) {
            //System.out.println("\t -- Redistributing split node with elements from next");
            if (splitNodeIsLeftChild) {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex)
            } else {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex + 1)
            }
        } else if (pnum > 0 && ppar) {
            //System.out.println("\t -- Redistributing split node with elements from prev");
            if (splitNodeIsLeftChild) {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex - 1)
            } else {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex)
            }
        } else if (snum > 0) {
            if (splitNodeIsLeftChild) {
                redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex)
            } else {
                redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex + 1)
            }
        } else if (npar) {
            //System.out.println("Merging leaf next");
            // it's the case where split node is the left node from parent
            mnode = mergeNodes(
                splitNode, nptr, pptr, parent,
                parentPointerIndex, parentKeyIndex,
                isLeftOfNext,  /*useNextPointer = */true
            )
        } else if (ppar) {
            //System.out.println("Merging leaf prev");
            // it's the case where split node is in the left from parent
            mnode = mergeNodes(
                pptr, splitNode, nptr, parent,
                parentPointerIndex, parentKeyIndex,
                isLeftOfNext,  /*useNextPointer = */false
            )
        } else {
            throw IllegalStateException(
                "Can't have both leaf " +
                        "pointers null and not be root or no " +
                        "common parent"
            )
        }
        return mnode
    }


    /**
     * Extra validation on leaf pointers
     *
     * @param prev previous leaf
     * @param split current leaf (split node)
     * @param next next leaf
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(MiniDBException::class)
    private fun validateNeighbours(prev: TreeLeaf, split: TreeLeaf, next: TreeLeaf) {
        if (split.prevPagePointer != prev.getPageIndex()) {
            // "Split prev pointer not matching prev page index"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        } else if (prev.nextPagePointer != split.getPageIndex()) {
            // "Split page index not matching prev pointer"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }
        if (split.nextPagePointer != next.getPageIndex()) {
            // "Split next pointer not matching next page index"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        } else if (next.prevPagePointer != split.getPageIndex()) {
            // "Split page index not matching next pointer"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }
    }

    /**
     * Handle the internal node redistribution/merging
     *
     * @param mnode node to process
     * @param parent the parent node
     * @param parentPointerIndex parent pointer index
     * @param parentKeyIndex parent key index that mnode is child
     * @return the updated node (if any updates are made)
     * @throws IOException is thrown when an I/O operation fails
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun handleInternalNodeRedistributionOrMerging(
        node: TreeNode,
        parent: TreeInternalNode,
        parentPointerIndex: Int,
        parentKeyIndex: Int
    ): TreeNode? {
        //System.out.println("Internal node merging/redistribution needs to happen");
        var mnode: TreeNode? = node
        val splitNode = mnode as TreeInternalNode?
        val nptr: TreeInternalNode?
        val pptr: TreeInternalNode?

        // load the adjacent nodes
        nptr = readNode(parent.getPointerAt(parentPointerIndex + 1)) as TreeInternalNode?
        pptr = readNode(parent.getPointerAt(parentPointerIndex - 1)) as TreeInternalNode?
        if (nptr == null && pptr == null) {
            // "Can't have both children null"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }
        val nnum = canRedistribute(nptr)
        val pnum = canRedistribute(pptr)
        val snum = canRedistribute(splitNode)
        val isLeftOfNext = parentPointerIndex > parentKeyIndex
        val splitNodeIsLeftChild = parentKeyIndex == parentPointerIndex

        // check if we can redistribute with the next node
        if (nnum > 0) {
            //System.out.println(" -- Internal Redistribution to split with next");
            if (splitNodeIsLeftChild) {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex)
            } else {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex + 1)
            }
        } else if (pnum > 0) {
            //System.out.println(" -- Internal Redistribution to split with prev");
            if (splitNodeIsLeftChild) {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex - 1)
            } else {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex)
            }
        } else if (snum > 0) {
            // check try to send items from next
            if (nptr != null) {
                //System.out.println(" -- Internal Redistribution to next with split");
                if (splitNodeIsLeftChild) {
                    redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex)
                } else {
                    redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex + 1)
                }
            } else {
                //System.out.println(" -- Internal Redistribution to prev with split");
                if (splitNodeIsLeftChild) {
                    redistributeNodes(pptr, splitNode, false, parent, parentKeyIndex - 1)
                } else {
                    redistributeNodes(pptr, splitNode, false, parent, parentKeyIndex)
                }
            }
        } else {
            //System.out.println(" -- Internal merging actually happens");
            // check if we can merge with the right node
            mnode = if (nptr != null && nptr.isTimeToMerge(conf)) {
                mergeNodes(
                    splitNode!!, nptr, pptr!!, parent,
                    parentPointerIndex, parentKeyIndex,
                    isLeftOfNext,  /*useNextPointer = */true
                )
            } else if (pptr != null && pptr.isTimeToMerge(conf)) {
                mergeNodes(
                    pptr, splitNode!!, nptr!!, parent,
                    parentPointerIndex, parentKeyIndex,
                    isLeftOfNext,  /*useNextPointer = */false
                )
            } else {
                // "Can't merge or redistribute, corrupted file?"
                throw MiniDBException(MiniDBException.InvalidBPTreeState)
            }
        }
        return mnode
    }


    /**
     * Map the short value to an actual node type enumeration value.
     * This paradoxically is the opposite of that we do in the similarly named
     * function in each node.
     *
     * @param pval a value read from the file indicating which type of node this is
     * @return nodeType equivalent
     * @throws InvalidPropertiesFormatException is thrown when the node is of an unknown type
     */
    @Throws(InvalidPropertiesFormatException::class)
    private fun getPageType(pval: Short): TreeNode.TreeNodeType {
        when (pval) {
            (1).toShort() -> {
                return (TreeNode.TreeNodeType.TREE_LEAF)
            }

            (2).toShort() -> {
                return (TreeNode.TreeNodeType.TREE_INTERNAL_NODE)
            }

            (3).toShort() -> {
                return (TreeNode.TreeNodeType.TREE_ROOT_INTERNAL)
            }

            (4).toShort() -> {
                return (TreeNode.TreeNodeType.TREE_ROOT_LEAF)
            }

            (5).toShort() -> {
                return (TreeNode.TreeNodeType.TREE_LEAF_OVERFLOW)
            }

            (6).toShort() -> {
                return (TreeNode.TreeNodeType.TREE_FREE_POOL)
            }

            else -> {
                throw InvalidPropertiesFormatException(
                    "Unknown " +
                            "node value read; file possibly corrupt?"
                )
            }
        }
    }

    /**
     * Function that commits the allocation pool to the file;
     * this can be done after each deletion or more unsafely
     * before committing the file changes at the end.
     *
     * @throws IOException is thrown when an I/O operation fails
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun commitFreePool() {
        val freePoolNodes = LinkedList<Long>() // each element reflects a page full of free page indexes
        squeezeFileLength()
        val rest = freeSlots.size
        var poolIndex = 0
        var fpn: TreeFreePoolNode
        val cap = conf.freePoolNodeDegree
        // calculate the number of pages needed
        val pages = Math.ceil(rest / (cap.toDouble())).toInt()
        for (i in 0 until pages) {
            freePoolNodes.add(freeSlots.removeFirst())
        }
        // the ending pointer
        freePoolNodes.add(-1L)

        // get the smallest page available
        firstFreePoolPointer = freePoolNodes.first
        // seek to the position we have to start to write
        treeFile.seek((conf.headerSize - java.lang.Long.SIZE / 8).toLong())
        treeFile.writeLong(firstFreePoolPointer)

        // write all the pages
        for (i in 0 until pages) {
            // create the page
            fpn = TreeFreePoolNode(freePoolNodes[i], freePoolNodes[i + 1])
            // write it.
            var j = 0
            while (j < cap && poolIndex < freeSlots.size) {
                fpn.addToKeyArrayAt(j, ArrayList<Any>(Arrays.asList(freeSlots[poolIndex])))
                fpn.incrementCapacity(conf)
                j++
                poolIndex++
            }
            fpn.writeNode(treeFile, conf)
        }
        // remove the last entry
        freePoolNodes.removeLast()
        if (poolIndex != freeSlots.size) {
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }
    }

    /**
     * This function adjust the file length by purging the high index pages that
     * are used. Low index pages are purged last by design.
     * Unused pages are pruned out.
     * 0 1 2 3 4 5  -- pages
     * 0 1 0 1 0 0  -- used
     * then 4 & 5 are pruned out.
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class)
    private fun squeezeFileLength() {
        freeSlots.sort()
        var lastPos = if (freeSlots.size > 0) freeSlots.last else -1L
        while (lastPos != -1L && lastPos == (totalPages - 1) * conf.pageSize) {
            totalPages--
            freeSlots.removeLast()
            lastPos = if (freeSlots.size > 0) freeSlots.last else -1L
        }
        treeFile.setLength(totalPages * conf.pageSize)
    }

    /**
     * Read each tree node and return it as a generic type
     *
     * @param index index of the page in the file
     * @return a TreeNode object referencing to the loaded page
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class)
    private fun readNode(index: Long): TreeNode? {

        // caution.
        if (index <= 0) {
            return (null)
        }
        treeFile.seek(index)
        val buffer = ByteArray(conf.pageSize)
        treeFile.read(buffer)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)

        // get the page type
        val nt: TreeNode.TreeNodeType = getPageType(bbuffer.short)
        if (isInternalNode(nt)) {
            val tnode = TreeInternalNode(nt, index)
            val curCap = bbuffer.int
            for (i in 0 until curCap) {
                tnode.addPointerAt(i, bbuffer.long)
                tnode.addToKeyArrayAt(i, conf.readKey(bbuffer))
            }
            // add the final pointer
            tnode.addPointerAt(curCap, bbuffer.long)
            // update the capacity
            tnode.setCurrentCapacity(curCap)
            // set deleted flag
            tnode.setBeingDeleted(false)
            return (tnode)
        } else if (isOverflowPage(nt)) {
            val prevptr = bbuffer.long
            val nextptr = bbuffer.long
            val curCap = bbuffer.int
            val tnode = TreeOverflow(nextptr, prevptr, index)
            tnode.addToKeyArrayAt(0, conf.readKey(bbuffer))

            // read entries
            for (i in 0 until curCap) {
                tnode.addToValueList(i, bbuffer.long)
            }
            // update capacity
            tnode.setCurrentCapacity(curCap)
            return (tnode)
        } else if (isLeaf(nt)) {
            val prevptr = bbuffer.long
            val nextptr = bbuffer.long
            val curCap = bbuffer.int
            val tnode = TreeLeaf(nextptr, prevptr, nt, index)

            // read entries
            for (i in 0 until curCap) {
                tnode.addToKeyArrayAt(i, conf.readKey(bbuffer))
                tnode.addToValueList(i, bbuffer.long)
                tnode.addToOverflowList(i, bbuffer.long)
            }
            // update capacity
            tnode.setCurrentCapacity(curCap)

            // set being deleted to false
            tnode.setBeingDeleted(false)
            return (tnode)
        } else {
            val nextptr = bbuffer.long
            val curCap = bbuffer.int
            val lpOvf = TreeFreePoolNode(index, nextptr)

            // now loop through the
            for (i in 0 until curCap) {
                lpOvf.addToKeyArrayAt(i, ArrayList<Any>(Arrays.asList(bbuffer.long)))
            }

            // update capacity
            lpOvf.setCurrentCapacity(curCap)
            return (lpOvf)
        }
    }

    /**
     * Check if the node is an internal node
     *
     * @param nt nodeType of the node we want to check
     * @return return true if it's an Internal Node, false if it's not.
     */
    private fun isInternalNode(nt: TreeNode.TreeNodeType): Boolean {
        return (nt == TreeNode.TreeNodeType.TREE_INTERNAL_NODE ||
                nt == TreeNode.TreeNodeType.TREE_ROOT_INTERNAL)
    }

    /**
     * Check if the node is an overflow page
     *
     * @param nt nodeType of the node we want to check
     * @return return true if it's an overflow page, false if it's not.
     */
    fun isOverflowPage(nt: TreeNode.TreeNodeType): Boolean {
        return (nt == TreeNode.TreeNodeType.TREE_LEAF_OVERFLOW)
    }

    /**
     * Check if the node is a leaf page
     *
     * @param nt nodeType of the node we want to check
     * @return return true it's a leaf page, false if it's not
     */
    fun isLeaf(nt: TreeNode.TreeNodeType): Boolean {
        return ((nt == TreeNode.TreeNodeType.TREE_LEAF) || (
                nt == TreeNode.TreeNodeType.TREE_LEAF_OVERFLOW) || (
                nt == TreeNode.TreeNodeType.TREE_ROOT_LEAF))
    }

    /**
     * Reads an existing file and generates a B+ configuration based on the stored values
     *
     * @param r file to read from
     * @throws IOException is thrown when an I/O operation fails
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun readFileHeader(r: RandomAccessFile) {
        r.seek(0L)

        // read the page size
        val pageSize = r.readInt()
        if (pageSize < 0 || pageSize != conf.pageSize) {
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }

        // key size
        val keySize = r.readInt()
        if (keySize != conf.keySize) {
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }

        // read the entry size
        val entrySize = r.readInt()
        if (entrySize <= 0 || entrySize != conf.valueSize) {
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }

        // read the number of used pages
        usedPages = r.readLong()
        if (usedPages <= 0) {
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }

        // read the root index
        val rootIndex = r.readLong()
        if (rootIndex < 0) {
            // "Root can't have index < 0"
            throw MiniDBException(MiniDBException.InvalidBPTreeState)
        }

        // read the next lookup page pointer
        firstFreePoolPointer = r.readLong()

        // read the root.
        root = readNode(rootIndex)
    }

    /**
     * Writes the file header containing all the juicy details
     *
     * @param conf valid configuration
     * @throws IOException is thrown when an I/O operation fails
     */
    @Throws(IOException::class)
    private fun writeFileHeader(conf: BPlusConfiguration) {
        treeFile.seek(0L)
        treeFile.writeInt(conf.pageSize)
        treeFile.writeInt(conf.keySize)
        treeFile.writeInt(conf.valueSize)
        treeFile.writeLong(usedPages)
        treeFile.writeLong(root!!.getPageIndex())
        treeFile.writeLong(firstFreePoolPointer)
    }

    /**
     * Just commit the tree by actually closing the FD
     * thus flushing the buffers.
     *
     * @throws IOException is thrown when an I/O operation fails
     * @throws MiniDBException is thrown when there are inconsistencies in the blocks.
     */
    @Throws(IOException::class, MiniDBException::class)
    fun commitTree() {
        commitFreePool()
        writeFileHeader(conf)
        treeFile.close()
    }

    /**
     * Generate the first available index for a page. If no unused pages, allocate new pages.
     */
    @Throws(IOException::class)
    private fun generateFirstAvailablePageIndex(conf: BPlusConfiguration): Long {
        // check if we have unused pages
        if (freeSlots.size == 0) { // file length == conf.pageSize * totalPages, allocate new pages
            val ALLOCATE_NEW_PAGES = 10L
            val tmp = totalPages
            totalPages += ALLOCATE_NEW_PAGES
            treeFile.setLength(conf.pageSize * totalPages)
            for (i in tmp until (tmp + ALLOCATE_NEW_PAGES)) {
                freeSlots.addLast(i * conf.pageSize)
            }
        }
        usedPages++
        return freeSlots.pop()
    }

    /**
     * Delete the page
     *
     * @param pageIndex page index to remove
     * @param sort sort free sort pool?
     */
    @Throws(IOException::class, MiniDBException::class)
    private fun deletePage(pageIndex: Long, sort: Boolean) {
        freeSlots.add(pageIndex)
        usedPages--
        deleteIterations++
        if (sort || isTimeForTriming()) {
            deleteIterations = 0
            commitFreePool()
        }
    }

    /**
     * Check if we have to condition the file
     *
     * @return if it's time for conditioning
     */
    private fun isTimeForTriming(): Boolean {
        return (deleteIterations >= conf.trimFileThreshold)
    }

    private enum class Rank {
        Pred, Succ, PlusOne, Exact
    }

    /**
     * Constructor for unique queries, hence feed it all the above information
     *
     * @param leaf the leaf which our (K, V) might reside
     * @param index index where first key is <= our requested key
     * @param found we found the requested key?
     */
    inner class SearchResult(var leafLoc: TreeLeaf, var index: Int, var found: Boolean)
}
