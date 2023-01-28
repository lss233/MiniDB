package com.lss233.minidb.engine.storage


import com.lss233.minidb.engine.index.bptree.BPlusConfiguration
import com.lss233.minidb.engine.index.bptree.BPlusTree
import com.lss233.minidb.engine.index.bptree.MainDataConfiguration
import com.lss233.minidb.engine.index.bptree.MainDataFile
import com.lss233.minidb.exception.MiniDBException
import com.lss233.minidb.utils.Misc
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors

/**
 * TODO 需要合并到 memory 当中的 Table 与 engine 当中的 Relation
 * 关系表，存储表的文件单体
 * 封装了BPTree索引, 一些基本操作封装在这一实体当中
 * 当进行插入、更新、删除等操作时调用这里的方法，
 * 调用后能够对应的修改索引文件
 */
class RelationTable {

    var meta: RelationMeta? = null

    var directory: String? = null // the directory to store the relation data

    var data: MainDataFile? = null // main tree for the data

    var superKeyTrees: ArrayList<BPlusTree>? = null

    var indexTrees: ArrayList<BPlusTree?>? = null

    var nullTrees: ArrayList<BPlusTree?>? = null

    // create the relation, save the metadata and create data trees
    @Throws(IOException::class, MiniDBException::class)
    fun create() {
        meta!!.nextRowID = 0L
        meta!!.validate()
        meta!!.write(Paths.get(directory!!, "meta").toString())
        createOrResumeData(true)
    }

    @Throws(IOException::class)
    fun drop() {
        close()
        Misc.rmDir(directory!!)
    }

    @Throws(IOException::class, MiniDBException::class)
    fun deleteAllData() {
        drop()
        File(directory!!).mkdir()
        create()
    }

    /**
     * close the relation, save meta data and commit trees.
     */
    @Throws(IOException::class, MiniDBException::class)
    fun close() {
        meta!!.write(Paths.get(directory!!, "meta").toString())
        data!!.close()
        for (each in superKeyTrees!!) {
            each.commitTree()
        }
        for (each in indexTrees!!) {
            each!!.commitTree()
        }
        for (each in nullTrees!!) {
            each!!.commitTree()
        }
    }

    /**
     * resume a relation from disk
     */
    @Throws(IOException::class, ClassNotFoundException::class, MiniDBException::class)
    fun resume() {
        meta = RelationMeta().read(Paths.get(directory!!, "meta").toString())
        createOrResumeData(false)
    }

    @Throws(IOException::class, MiniDBException::class)
    private fun createOrResumeData(create: Boolean) {
        val mode = if (create) "rw+" else "rw"

        // main data
        val colIDs = ArrayList<Int>()
        for (i in 0 until meta!!.ncols) {
            colIDs.add(i)
        }
        data = MainDataFile(
            MainDataConfiguration(meta!!.coltypes!!, meta!!.colsizes!!, colIDs),
            mode,
            Paths.get(directory!!, "data").toString(),
            BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    ArrayList(listOf(java.lang.Long::class.java)),
                    ArrayList(listOf(8)),
                    ArrayList(listOf(0)),
                    true,
                    1000
                ),
                mode,
                Paths.get(directory!!, "rowID2position").toString()
            )
        )

        superKeyTrees = ArrayList(meta!!.superKeys!!.size)
        indexTrees = ArrayList(meta!!.indices!!.size)
        nullTrees = ArrayList(meta!!.nullableColIds!!.size)

        // resume null tree
        for (i in nullTrees!!.indices) {
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    ArrayList(listOf(java.lang.Long::class.java)),
                    ArrayList(listOf(8)),
                    ArrayList(listOf(0)),
                    false,
                    1000
                ),
                mode,
                Paths.get(directory!!, String.format("null.%d.data", meta!!.nullableColIds!![i])).toString()
            )
            nullTrees!![i] = tmp
        }

        // resume indices trees
        for (i in indexTrees!!.indices) {
            val colId = meta!!.indices!![i]
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    colId.stream().map<Any> { x: Int? ->
                        meta!!.coltypes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId.stream().map<Any> { x: Int? ->
                        meta!!.colsizes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId,
                    false,
                    1000
                ),
                mode,
                Paths.get(directory!!, String.format("index.%d.data", i)).toString()
            )
            indexTrees!![i] = tmp
        }

        // resume super key trees
        for (i in superKeyTrees!!.indices) {
            val colId = meta!!.superKeys!![i]
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    colId.stream().map<Any> { x: Int? ->
                        meta!!.coltypes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId.stream().map<Any> { x: Int? ->
                        meta!!.colsizes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId,
                    true,
                    1000
                ),
                mode,
                Paths.get(directory!!, String.format("key.%d.data", i)).toString()
            )
            superKeyTrees!![i] = tmp
        }
    }

    /**
     * insert one row, cols are in the same order as the definition (the order in meta.col names).
     */
    @Throws(MiniDBException::class, IOException::class)
    fun insert(row: ArrayList<Any?>) {
        val rowID = meta!!.nextRowID++
        // check length
        if (row.size != meta!!.ncols) throw MiniDBException(
            String.format("%d values required but %d values are given.", meta!!.ncols, row.size)
        )
        val indeedNullCols = ArrayList<Int>()
        for (i in 0 until meta!!.ncols) {
            if (!meta!!.nullableColIds!!.contains(i) && row[i] == null) {
                // check nullable
                throw MiniDBException(String.format("column (%s) cannot be null!", meta!!.colnames!![i]))
            }
            if (row[i] != null) {
                // check type
                if (meta!!.coltypes!![i] != row[i]!!.javaClass) {
                    throw MiniDBException(
                        String.format(
                            "Non-compatible type. Given: %s, Required: %s!",
                            row[i]!!.javaClass.typeName,
                            meta!!.coltypes!![i].typeName
                        )
                    )
                }

                // check string length
                if (meta!!.coltypes!![i] == java.lang.String::class.java) {
                    val length = (row[i] as String?)!!.toByteArray(StandardCharsets.UTF_8).size
                    if (length > meta!!.colsizes!![i]) {
                        throw MiniDBException(
                            java.lang.String.format(
                                "column (%s) length exceeds! The value is (%s) with a length of %d (in bytes), but the limit is %d.",
                                meta!!.colnames!![i],
                                row[i], length, meta!!.colsizes!![i]
                            )
                        )
                    }
                }
            } else {
                // null columns, set the default value of the corresponding type.
                indeedNullCols.add(i)
                if (meta!!.coltypes!![i] == java.lang.String::class.java) {
                    row[i] = ""
                } else if (meta!!.coltypes!![i] == java.lang.Integer::class.java) {
                    row[i] = 0
                } else if (meta!!.coltypes!![i] == java.lang.Long::class.java) {
                    row[i] = 0L
                } else if (meta!!.coltypes!![i] == java.lang.Double::class.java) {
                    row[i] = 0.0
                } else if (meta!!.coltypes!![i] == java.lang.Float::class.java) {
                    row[i] = 0f
                }
            }
        }

        // check unique constrains
        for (tree in superKeyTrees!!) {
            val thisRow: ArrayList<Any?> = tree.conf.colIDs.stream().map { x -> row[x] }.collect(
                Collectors.toCollection { ArrayList() }
            )
            if (tree.search(thisRow).size > 0) {
                // duplicate keys
                throw MiniDBException(
                    String.format(
                        "Value (%s) already exists!",
                        tree.conf.keyToString(thisRow)
                    )
                )
            }
        }

        // now it is time to insert!
        data!!.insertRow(row, rowID)
        for (tree in superKeyTrees!!) {
            val thisRow: ArrayList<Any?> = tree.conf.colIDs.stream().map { x -> row[x] }.collect(
                Collectors.toCollection { ArrayList() }
            )
            tree.insertPair(thisRow, rowID)
        }
        for (tree in indexTrees!!) {
            val thisRow: ArrayList<Any?> = tree!!.conf.colIDs.stream().map { x -> row[x] }.collect(
                Collectors.toCollection { ArrayList() }
            )
            tree.insertPair(thisRow, rowID)
        }

        // record null columns
        for (i in nullTrees!!.indices) {
            if (indeedNullCols.contains(i)) {
                nullTrees!![i]!!
                    .insertPair(ArrayList(listOf(rowID)), -1L)
            }
        }
    }

    @Throws(IOException::class, MiniDBException::class)
    fun delete(rowID: Long) {
        val row = data!!.readRow(rowID)
        data!!.deleteRow(rowID)
        for (tree in superKeyTrees!!) {
            val thisRow = tree.conf.colIDs.stream().map { x -> row[x] }.collect(
                Collectors.toCollection { ArrayList() }
            )
            tree.deletePair(thisRow, rowID)
        }
        for (tree in indexTrees!!) {
            val thisRow = tree!!.conf.colIDs.stream().map { x -> row[x] }.collect(
                Collectors.toCollection { ArrayList() }
            )
            tree.deletePair(thisRow, rowID)
        }
        for (tree in nullTrees!!) {
            tree!!.deletePair(ArrayList(listOf(rowID)), -1L)
        }
    }

    @Throws(IOException::class, MiniDBException::class)
    fun delete(rowIDs: Collection<Long>) {
        for (rowID in rowIDs) {
            delete(rowID)
        }
    }

    @Throws(IOException::class, MiniDBException::class)
    fun readRows(rowIDs: Collection<Long?>): LinkedList<MainDataFile.SearchResult> {
        val ans = LinkedList<MainDataFile.SearchResult>()
        for (rowID in rowIDs) {
            val result: MainDataFile.SearchResult = MainDataFile.SearchResult(data!!.readRow(rowID!!), rowID)
            for (nullTree in nullTrees!!) {
                if (nullTree!!.search(ArrayList(listOf(result.rowID))).size != 0) {
                    result.key!![nullTree.conf.colIDs[0]] = nullTree
                }
            }
            ans.add(result)
        }
        return ans
    }


    /**
     * linear scan,
     * process each row to restore the null value
     */
    fun searchRows(pred: Function<MainDataFile.SearchResult, Boolean?>): LinkedList<MainDataFile.SearchResult> {
        val predpred =
            Function { result: MainDataFile.SearchResult? ->
                try {
                    for (nullTree in nullTrees!!) {
                        if (nullTree!!.search(ArrayList(listOf(result!!.rowID))).size != 0) {
                            result.key!![nullTree.conf.colIDs[0]] = nullTree
                        }
                    }
                    return@Function result?.let { pred.apply(it) }
                } catch (e: java.lang.Exception) {
                    throw Exception("Error!")
                }
            }
        try {
            return data!!.searchRows(predpred)
        } catch (e: Exception) {
            throw Exception("Error!")
        }
    }

    private class Helper(
        var tables: ArrayList<RelationTable>,
        var func: Consumer<ArrayList<MainDataFile.SearchResult>>
    ) {
        var placeHolder: ArrayList<MainDataFile.SearchResult> = ArrayList()
        var i = 0
        var n: Int = tables.size

        fun run() {
            enumFunc(0)
        }

        private fun enumFunc(i: Int) {
            if (i >= n) {
                func.accept(placeHolder)
                return
            }
            tables[i].searchRows { x: MainDataFile.SearchResult ->
                placeHolder.add(x)
                enumFunc(i + 1)
                placeHolder.removeAt(placeHolder.size - 1)
                false
            }
        }
    }

    fun traverseRelations(
        tables: ArrayList<RelationTable>,
        func: Consumer<ArrayList<MainDataFile.SearchResult>>
    ) {
        Helper(tables, func).run()
    }
}
