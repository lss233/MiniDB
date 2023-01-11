package com.lss233.minidb.engine.index.bptree

import java.io.IOException
import java.io.RandomAccessFile
import java.util.*

/**
 *
 * Class that describes all the common properties that
 * each of the node types have.
 *
 * Constructor which takes into the node type as well as the
 * page index
 * @param nodeType the actual node type
 * @param pageIndex the page index in the file
 */
@SuppressWarnings("unused")
abstract class TreeNode(var nodeType: TreeNodeType? = null, var pageIndex: Long = 0) {

    enum class TreeNodeType {
        TREE_LEAF, TREE_INTERNAL_NODE, TREE_ROOT_INTERNAL, TREE_ROOT_LEAF, TREE_LEAF_OVERFLOW, TREE_FREE_POOL
    }

    var keyArray: LinkedList<ArrayList<Any>>? = null // key array


    private var currentCapacity = 0 // current capacity

    private var beingDeleted = false // deleted flag

    init {
        currentCapacity = 0 // current capacity

        keyArray = LinkedList() // instantiate the linked list

        beingDeleted = true
    }


    /**
     * Check if the node is full (and needs splitting)
     * @param conf configuration to deduce which degree to use
     *
     * @return true is the node is full false if it's not.
     */
    open fun isFull(conf: BPlusConfiguration): Boolean {
        return if (isLeaf()) {
            if (isOverflow()) conf.getMaxOverflowNodeCapacity() == currentCapacity else conf.getMaxLeafNodeCapacity() == currentCapacity
        } else {
            // internal
            conf.getMaxInternalNodeCapacity() == currentCapacity
        }
    }

    /**
     * Check if the node is underutilized and needs to be merged
     *
     * @param conf B+ Tree configuration reference
     * @return true is the node needs to be merged or false if it's not
     */
    open fun isTimeToMerge(conf: BPlusConfiguration): Boolean {
        // for roots (internal or leaf) return true only when empty
        return if (isRoot()) {
            getCurrentCapacity() <= 1
        } else if (isLeaf()) {
            // for overflow pages return true only if empty
            if (isOverflow()) {
                isEmpty()
            } else {
                conf.getMinLeafNodeCapacity() >= currentCapacity
            }
        } else  // internal
        {
            conf.getMinInternalNodeCapacity() >= currentCapacity
        }
    }

    /**
     * Returns the current node capacity
     *
     * @return the newCap variable value.
     */
    open fun getCurrentCapacity(): Int {
        return currentCapacity
    }

    /**
     * Set the current capacity
     *
     * @param newCap replace node capacity with this argument.
     */
    open fun setCurrentCapacity(newCap: Int) {
        currentCapacity = newCap
    }

    /**
     * Increment the node capacity by one.
     *
     * @param conf configuration instance for validating the limits.
     */
    open fun incrementCapacity(conf: BPlusConfiguration) {
        currentCapacity++
        validateNodeCapacityLimits(conf)
    }

    /**
     * Decrement the node capacity by one.
     *
     * @param conf configuration instance for validating the limits.
     */
    open fun decrementCapacity(conf: BPlusConfiguration) {
        currentCapacity--
        validateNodeCapacityLimits(conf)
    }

    /**
     * Function that validates the node capacity invariants based on the current configuration instance.
     *
     * @param conf configuration instance for validating the limits.
     */
    open fun validateNodeCapacityLimits(conf: BPlusConfiguration) {
        if (isRoot()) {
            if (currentCapacity < 0) {
                // "Cannot have less than zero elements"
                throw RuntimeException("InvalidBPTreeState")
            } else if (isLeaf() && currentCapacity > conf.getMaxLeafNodeCapacity()) {
                // "Exceeded leaf node allowed capacity at root"
                throw RuntimeException("InvalidBPTreeState")
            } else if (isInternalNode() && currentCapacity > conf.getMaxInternalNodeCapacity()) {
                // "Exceeded internal node allowed capacity at root"
                throw RuntimeException("InvalidBPTreeState")
            }
        } else {
            if (isFreePoolNode()) {
                if (beingDeleted && currentCapacity < 0) {
                    // "Cannot have less than 0 elements in a lookup overflow node when deleting it"
                    throw RuntimeException("InvalidBPTreeState")
                } else if (currentCapacity > conf.freePoolNodeDegree) {
                    // "Exceeded lookup overflow node allowed capacity (node)"
                    throw RuntimeException("InvalidBPTreeState")
                }
            }
            if (isOverflow()) {
                if (beingDeleted && currentCapacity < 0) {
                    // "Cannot have less than 0 elements in a overflow node when deleting it"
                    throw RuntimeException("InvalidBPTreeState")
                } else if (currentCapacity > conf.getMaxOverflowNodeCapacity()) {
                    // "Exceeded overflow node allowed capacity (node)"
                    throw RuntimeException("InvalidBPTreeState")
                }
            } else if (isLeaf()) {
                if (beingDeleted && currentCapacity < 0) {
                    // "Cannot have less than 0 elements in a leaf node when deleting it"
                    throw RuntimeException("InvalidBPTreeState")
                } else if (!beingDeleted && currentCapacity < conf.getMinLeafNodeCapacity()) {
                    // "Cannot have less than " + conf.getMinLeafNodeCapacity() + " elements in a leaf node"
                    throw RuntimeException("InvalidBPTreeState")
                } else if (currentCapacity > conf.getMaxLeafNodeCapacity()) {
                    // "Exceeded leaf node allowed capacity (node)"
                    throw RuntimeException("InvalidBPTreeState")
                }
            } else if (isInternalNode()) {
                if (beingDeleted && currentCapacity < 0) {
                    // "Cannot have less than 0 elements in an internal node"
                    throw RuntimeException("InvalidBPTreeState")
                } else if (!beingDeleted && currentCapacity < conf.getMinInternalNodeCapacity()) {
                    // "Cannot have less than " + conf.getMinInternalNodeCapacity() + " elements in an internal node"
                    throw RuntimeException("InvalidBPTreeState")
                } else if (currentCapacity > conf.getMaxInternalNodeCapacity()) {
                    // "Exceeded internal node allowed capacity (node)"
                    throw RuntimeException("InvalidBPTreeState")
                }
            }
        }
    }

    open fun getBeingDeleted(): Boolean {
        return beingDeleted
    }

    open fun setBeingDeleted(beingDeleted: Boolean) {
        this.beingDeleted = beingDeleted
    }

    /**
     * Check if the node is empty (and *definitely* needs merging)
     *
     * @return true if it is empty false if it's not.
     */
    open fun isEmpty(): Boolean {
        return currentCapacity == 0
    }

    /**
     * Check if the node in question is an overflow page
     *
     * @return true if the node is an overflow page, false if it's not
     */
    open fun isOverflow(): Boolean {
        return nodeType == TreeNodeType.TREE_LEAF_OVERFLOW
    }

    /**
     * Check if the node in question is a leaf (including root)
     *
     * @return true if the node is a leaf, false if it's not.
     */
    open fun isLeaf(): Boolean {
        return nodeType == TreeNodeType.TREE_LEAF || nodeType == TreeNodeType.TREE_LEAF_OVERFLOW || nodeType == TreeNodeType.TREE_ROOT_LEAF
    }

    /**
     * Check if the node in question is a tree root.
     *
     * @return true if it is a tree root, false if it's not.
     */
    open fun isRoot(): Boolean {
        return nodeType == TreeNodeType.TREE_ROOT_INTERNAL ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF
    }

    /**
     * Check if the node in question is an internal node (including root)
     *
     * @return true if the node is an internal node, false if it's not.
     */
    open fun isInternalNode(): Boolean {
        return nodeType == TreeNodeType.TREE_INTERNAL_NODE ||
                nodeType == TreeNodeType.TREE_ROOT_INTERNAL
    }

    /**
     * Check if the node in question is a lookup page overflow node
     *
     * @return true if the node is a lookup page overflow node, false otherwise
     */
    open fun isFreePoolNode(): Boolean {
        return nodeType == TreeNodeType.TREE_FREE_POOL
    }

    /**
     * Return the node type
     *
     * @return the current node type
     */
    open fun getNodeType(): TreeNodeType? {
        return nodeType
    }

    /**
     * Explicitly set the node type
     *
     * @param nodeType set the node type
     */
    open fun setNodeType(nodeType: TreeNodeType?) {
        // check if we presently are a leaf
        if (isLeaf()) {
            this.nodeType = nodeType
            require(!isInternalNode()) { "Cannot convert Leaf to Internal Node" }
        } else {
            this.nodeType = nodeType
            require(!isLeaf()) { "Cannot convert Internal Node to Leaf" }
        }
    }

    /**
     * Get the specific key at position indicated by `index`
     * @param index the position to get the key
     * @return the key at position
     */
    open fun getKeyAt(index: Int): ArrayList<Any> {
        return keyArray!![index]
    }

    /**
     * Return the page index
     *
     * @return current page index
     */
    open fun getPageIndex(): Long {
        return pageIndex
    }

    /**
     * Update the page index
     *
     * @param pageIndex new page index
     */
    open fun setPageIndex(pageIndex: Long) {
        this.pageIndex = pageIndex
    }

    /**
     * Set the key in the array at specific position
     *
     * @param index index to set the key
     * @param key key to set in position
     */
    open fun setKeyArrayAt(index: Int, key: ArrayList<Any>) {
        keyArray!![index] = key
    }

    /**
     * Add key at index while shifting entries
     * pointed by index and after by one.
     *
     * @param index index to shift keys and add
     * @param key key to add in position
     */
    open fun addToKeyArrayAt(index: Int, key: ArrayList<Any>) {
        keyArray!!.add(index, key)
    }

    /**
     * Push a key to head of the array
     *
     * @param key key to push
     */
    open fun pushToKeyArray(key: ArrayList<Any>) {
        keyArray!!.push(key)
    }

    /**
     * Add a key to the last place of the array
     *
     * @param key key to add
     */
    open fun addLastToKeyArray(key: ArrayList<Any>) {
        keyArray!!.addLast(key)
    }

    /**
     * Get last element
     *
     * @return return the last key
     */
    open fun getLastKey(): ArrayList<Any> {
        return keyArray!!.last
    }

    /**
     * Get first key
     *
     * @return return the first key value
     */
    open fun getFirstKey(): ArrayList<Any> {
        return keyArray!!.first
    }

    /**
     * Pop the key at the head of the array
     *
     * @return key that is in the head of the array
     */
    open fun popKey(): ArrayList<Any> {
        return keyArray!!.pop()
    }

    /**
     * Remove and pop the last key of the array
     *
     * @return key that is in the last place of the array
     */
    open fun removeLastKey(): ArrayList<Any> {
        return keyArray!!.removeLast()
    }

    /**
     * Remove and pop the key at specific position
     *
     * @param index index that points where to remvoe the key
     * @return removed key
     */
    open fun removeKeyAt(index: Int): ArrayList<Any> {
        return keyArray!!.removeAt(index)
    }

    /**
     * Get the page type that maps the enumeration to numbers that are
     * easily stored in our file.
     *
     * @return the number representation of the node type
     * @throws InvalidPropertiesFormatException is thrown when the page type is not matched.
     */
    @Throws(InvalidPropertiesFormatException::class)
    open fun getPageType(): Short {
        return when (getNodeType()) {
            TreeNodeType.TREE_LEAF -> {
                1
            }

            TreeNodeType.TREE_INTERNAL_NODE -> {
                2
            }

            TreeNodeType.TREE_ROOT_INTERNAL -> {
                3
            }

            TreeNodeType.TREE_ROOT_LEAF -> {
                4
            }

            TreeNodeType.TREE_LEAF_OVERFLOW -> {
                5
            }

            TreeNodeType.TREE_FREE_POOL -> {
                6
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
     * Abstract method that all classes must implement that writes
     * each node type to a page slot.
     *
     * More details in each implementation.
     *
     * @param r an *already* open pointer which points to our B+ Tree file
     * @param conf B+ Tree configuration
     * @throws IOException is thrown when an I/O operation fails.
     */
    @Throws(IOException::class)
    abstract fun writeNode(r: RandomAccessFile, conf: BPlusConfiguration)

}