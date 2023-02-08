package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.config.MiniDBConfig
import com.lss233.minidb.engine.index.bptree.BPlusConfiguration
import com.lss233.minidb.engine.index.bptree.BPlusTree
import com.lss233.minidb.engine.index.bptree.MainDataConfiguration
import com.lss233.minidb.engine.index.bptree.MainDataFile
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.storage.RelationMeta
import com.lss233.minidb.exception.MiniDBException
import com.lss233.minidb.utils.Misc
import com.lss233.minidb.utils.TypeUtil
import miniDB.parser.ast.expression.Expression
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.*
import java.util.function.Function
import java.util.function.Predicate
import java.util.stream.Collectors

/**
 * Relational table storage structure,
 * @param tableName Indicate the name of the relational table.
 * @param relation Indicates the initialization data in the relational table.
 * @param belongDatabase Indicate the database to which the relational table belongs.
 * @param belongSchema Indicates the schema to which the relational table belongs.
 */
class Table(tableName: String, private var relation: Relation?, belongDatabase :String, belongSchema: String): View() {

    var tableName: String

    private var belongDatabase: String

    private var belongSchema: String

    constructor(tableName: String, columns: MutableList<Column>, tuples: MutableList<NTuple>, belongDatabase :String, belongSchema: String) :
            this(tableName, Relation(columns, tuples.map { it.toArray() }.toMutableList()), belongDatabase, belongSchema)

    /**
     * This construction parameter is used to prefetch Table (Relation) information entries.
     */
    constructor(tableName:String, belongDatabase :String, belongSchema: String) :
            this(tableName, null, belongDatabase, belongSchema) {
        this.tableName = tableName
        relation = null
    }

    var absTableDirectory = Paths.get(MiniDBConfig.DATA_FILE, belongDatabase, belongSchema, tableName).toString()

    private var meta: RelationMeta  // Basic information about the relationship table

    var data: MainDataFile? = null // Main tree for the data

    private var superKeyTrees: ArrayList<BPlusTree>? = null

    private var indexTrees: ArrayList<BPlusTree?>? = null

    private var nullTrees: ArrayList<BPlusTree?>? = null

    init {
        this.tableName = tableName
        this.belongDatabase = belongDatabase
        this.belongSchema = belongSchema
        // TODO need to init the basic table information
        meta = RelationMeta()
        meta.ncols = relation!!.columns.size
        for (column in relation!!.columns) {
            meta.colnames.add(column.name)
            meta.coltypes.add(TypeUtil.getType(column.definition.dataType.typeName))
            // TODO 修改一下变量大小
            meta.colsizes.add(10)
        }
        meta.nullableColIds = ArrayList(mutableListOf(2))
        meta.superKeys = ArrayList()
        meta.superKeys.add(ArrayList(mutableListOf(0)))
        meta.indices = ArrayList()
        meta.indices.add(ArrayList(mutableListOf(1)))
    }

    /**
     * create the relation, save the metadata and create data trees
     */
    fun create() {
        meta.nextRowID = 0L
        meta.validate()
        meta.write(Paths.get(absTableDirectory, "meta").toString())
        createOrResumeData(true)
    }

    fun drop() {
        close()
        Misc.rmDir(absTableDirectory)
    }

    fun deleteAllData() {
        drop()
        File(absTableDirectory).mkdir()
        create()
    }

    /**
     * close the relation, save meta data and commit trees.
     */
    fun close() {
        meta.write(Paths.get(absTableDirectory, "meta").toString())
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
    fun resume() {
        meta = RelationMeta().read(Paths.get(absTableDirectory, "meta").toString())
        createOrResumeData(false)
    }

    // TODO resume the data on disk should be synchronized to memory
    private fun createOrResumeData(create: Boolean) {
        val mode = if (create) "rw+" else "rw"

        // main data
        val colIDs = ArrayList<Int>()
        for (i in 0 until meta.ncols) {
            colIDs.add(i)
        }
        data = MainDataFile(
            MainDataConfiguration(meta.coltypes, meta.colsizes, colIDs),
            mode,
            Paths.get(absTableDirectory, "data").toString(),
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
                Paths.get(absTableDirectory, "rowID2position").toString()
            )
        )

        superKeyTrees = ArrayList(meta.superKeys.size)
        indexTrees = ArrayList(meta.indices.size)
        nullTrees = ArrayList(meta.nullableColIds.size)

        // resume null tree
        for (i in nullTrees!!.indices) {
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    ArrayList(listOf(Long::class.java)),
                    ArrayList(listOf(8)),
                    ArrayList(listOf(0)),
                    false,
                    1000
                ),
                mode,
                Paths.get(absTableDirectory, String.format("null.%d.data", meta.nullableColIds[i])).toString()
            )
            nullTrees!![i] = tmp
        }

        // resume indices trees
        for (i in indexTrees!!.indices) {
            val colId = meta.indices[i]
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    colId.stream().map<Any> { x: Int? ->
                        meta.coltypes[x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId.stream().map<Any> { x: Int? ->
                        meta.colsizes[x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId,
                    false,
                    1000
                ),
                mode,
                Paths.get(absTableDirectory, String.format("index.%d.data", i)).toString()
            )
            indexTrees!![i] = tmp
        }

        // resume super key trees
        for (i in superKeyTrees!!.indices) {
            val colId = meta.superKeys[i]
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    colId.stream().map<Any> { x: Int? ->
                        meta.coltypes[x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId.stream().map<Any> { x: Int? ->
                        meta.colsizes[x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId,
                    true,
                    1000
                ),
                mode,
                Paths.get(absTableDirectory, String.format("key.%d.data", i)).toString()
            )
            superKeyTrees!![i] = tmp
        }
    }

    /**
     * Get a copy of relation
     * @return Relation of this table
     */
    override fun getRelation(): Relation
        = relation!!.clone()

    /**
     * Relational tables insert single-row data operations,
     * insert data, and build BPTree.
     */
    override fun insert(row: Array<Any>) {
        val rowID = meta.nextRowID++
        // check columns validity
        if(row.size != meta.ncols) {
            throw RuntimeException("Unable inserting row for table `$tableName`, incorrect row size and column size.")
        }
        val indeedNullCols = ArrayList<Int>()
        for (i in 0 until meta.ncols) {
            // check type
            if (meta.coltypes[i] != row[i].javaClass) {
                throw MiniDBException(
                    String.format(
                        "Non-compatible type. Given: %s, Required: %s!",
                        row[i].javaClass.typeName,
                        meta.coltypes[i].typeName
                    )
                )
            }

            // check string length
            if (meta.coltypes[i] == java.lang.String::class.java) {
                val length = (row[i] as String?)!!.toByteArray(StandardCharsets.UTF_8).size
                if (length > meta.colsizes[i]) {
                    throw MiniDBException(
                        java.lang.String.format(
                            "column (%s) length exceeds! The value is (%s) with a length of %d (in bytes), but the limit is %d.",
                            meta.colnames[i],
                            row[i], length, meta.colsizes[i]
                        )
                    )
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
        relation!!.rows.add(row)
    }

    override fun insert(row: NTuple) {
        val tuple = NTuple()
        val arr = ArrayList<Any>()

        for(column in relation!!.columns) {
            if(row.columns.contains(column)) {
                tuple.add(row[column])
                arr.add((row[column] as Cell<*>).value!!)
            } else {
                tuple.add(Cell(column, column.defaultValue() ?:
                    throw RuntimeException("Unable inserting row for table `$tableName`, " +
                            "no default value for column `${column.name}`.")))
                arr.add(column.defaultValue()!!)
            }
        }
        relation!!.tuples.add(tuple)
        insert(arr.toArray())
    }

    // TODO 需要修改更新操作与索引相关的更新
    override fun update(cond: Predicate<NTuple>, updated: Array<Cell<Expression>>): Int {
        var affectsCounter = 0
        for (tuple in relation!!.tuples) {
            if(cond.test(tuple)) {
                affectsCounter ++
                for (cell in updated) {
                    tuple[cell.column] = cell.value
                }
            }
        }
        return affectsCounter
    }

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

    fun delete(rowIDs: Collection<Long>) {
        for (rowID in rowIDs) {
            delete(rowID)
        }
    }

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
                } catch (e: Exception) {
                    throw Exception("Error!")
                }
            }
        try {
            return data!!.searchRows(predpred)
        } catch (e: Exception) {
            throw Exception("Error!")
        }
    }
}
