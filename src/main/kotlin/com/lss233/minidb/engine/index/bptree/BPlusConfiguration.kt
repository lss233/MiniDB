package com.lss233.minidb.engine.index.bptree

import java.lang.reflect.Type

/**
 * Class that stores all the configuration parameters for our B+ Tree.
 * You can view a description on all the parameters below...
 * @param valueSize entry size (in bytes)
 * @param unique whether one key can have multiple values. This corresponds to unique index.
 * @param trimFileThreshold iterations to trim the file
 */
@SuppressWarnings("WeakerAccess", "unused")
class BPlusConfiguration(
    pageSize: Int,
    var valueSize: Int = 0,
    types: ArrayList<Type>,
    sizes: ArrayList<Int>,
    colIDs: ArrayList<Int>,
    var unique: Boolean = false,
    var trimFileThreshold: Int = 0
): Configuration(pageSize, types, sizes,colIDs) {


    var headerSize = 0 // header size (in bytes)

    var leafHeaderSize = 0 // leaf node header size (in bytes)

    var internalNodeHeaderSize = 0 // internal node header size (in bytes)

    var overflowNodeHeaderSize = 0 // overflow node header size

    var freePoolNodeHeaderSize = 0 // free pool page header size


    var leafNodeDegree = 0 // leaf node degree

    var treeDegree = 0 // tree degree (internal node degree)

    var overflowPageDegree = 0 // overflow page degree

    var freePoolNodeDegree = 0 // lookup overflow page degree


    init {

        headerSize = (Int.SIZE_BITS * 3 + 4 * Long.SIZE_BITS) / 8 // header size in bytes

        leafHeaderSize = (Short.SIZE_BITS + 2 * Long.SIZE_BITS + Int.SIZE_BITS) / 8 // 22 bytes

        internalNodeHeaderSize = (Short.SIZE_BITS + Int.SIZE_BITS) / 8 // 6 bytes

        overflowNodeHeaderSize = (Short.SIZE_BITS + 2 * Long.SIZE_BITS + Int.SIZE_BITS) / 8 + keySize // 22 + keySize bytes

        freePoolNodeHeaderSize = (Short.SIZE_BITS + Long.SIZE_BITS + Int.SIZE_BITS) / 8 // 14 bytes

        // now calculate the degree

        // data: key and a value and an overflow pointer
        leafNodeDegree = calculateDegree(keySize + valueSize + Long.SIZE_BITS / 8, leafHeaderSize)
        // data: key and a pointer
        treeDegree = (pageSize - internalNodeHeaderSize - Long.SIZE_BITS / 8) / (keySize + Long.SIZE_BITS / 8)

        overflowPageDegree = calculateDegree(valueSize, overflowNodeHeaderSize)

        freePoolNodeDegree = calculateDegree(Long.SIZE_BITS / 8, freePoolNodeHeaderSize)

        checkDegreeValidity()
    }

    private fun calculateDegree(elementSize: Int, elementHeaderSize: Int): Int {
        return (pageSize - elementHeaderSize) / elementSize
    }


    /**
     * Little function that checks if we have any degree < 2 (which is not allowed)
     */
    private fun checkDegreeValidity() {
        require(!(treeDegree < 2 || leafNodeDegree < 2 || overflowPageDegree < 2 || freePoolNodeDegree < 2)) { "Can't have a degree < 2" }
    }

    fun getMaxInternalNodeCapacity(): Int {
        return treeDegree
    }

    fun getMinInternalNodeCapacity(): Int {
        return (treeDegree - 1) / 2
    }

    fun getMaxLeafNodeCapacity(): Int {
        return leafNodeDegree
    }

    fun getMinLeafNodeCapacity(): Int {
        return (leafNodeDegree - 1) / 2
    }

    fun getMaxOverflowNodeCapacity(): Int {
        return overflowPageDegree
    }

    fun getFreePoolNodeDegree(): Int {
        return freePoolNodeDegree
    }

    fun getFreePoolNodeOffset(): Long {
        return freePoolNodeHeaderSize.toLong()
    }

    fun getPageCountOffset(): Int {
        return 12
    }

}
