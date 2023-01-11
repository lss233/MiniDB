package com.lss233.minidb.engine.index.bptree

import java.io.RandomAccessFile
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
 * */
@SuppressWarnings("WeakerAccess")
class BPlusTree {

    private val root: TreeNode? = null

    private val aChild: TreeNode? = null

    private val treeFile: RandomAccessFile? = null

    var conf: BPlusConfiguration? = null

    private val freeSlots: LinkedList<Long>? = null // each element reflects a free page

    private val firstFreePoolPointer: Long = 0

    private val usedPages: Long = 0 // number of used pages (>= 1, header page)

    private val totalPages: Long = 0 // number of pages in the file (>= 1, used pages + free pages), can be counted from file length

    private val deleteIterations = 0




}