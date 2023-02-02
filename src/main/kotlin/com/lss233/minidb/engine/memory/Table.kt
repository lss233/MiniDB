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
import miniDB.parser.ast.expression.Expression
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.function.Predicate
import java.util.stream.Collectors

class Table(val name: String, private val relation: Relation): View() {

    constructor(name: String, columns: MutableList<Column>, tuples: MutableList<NTuple>) :
            this(name, Relation(columns, tuples.map { it.toArray() }.toMutableList()))

    /**
     * ------------------------------Append code start-------------------------------
     */
    
    // TODO 获取所属 Database 和 Schema
    private var directory = Paths.get(MiniDBConfig.DATA_FILE, name).toString()

    private var meta: RelationMeta  // Basic information about the relationship table

    var data: MainDataFile? = null // Main tree for the data

    private var superKeyTrees: ArrayList<BPlusTree>? = null

    private var indexTrees: ArrayList<BPlusTree?>? = null

    private var nullTrees: ArrayList<BPlusTree?>? = null
    
    init {
        meta = RelationMeta()
        // TODO need to init the basic table information
    }

    /**
     * create the relation, save the metadata and create data trees
     */
    fun create() {
        meta.nextRowID = 0L
        meta.validate()
        meta.write(Paths.get(directory, "meta").toString())
        createOrResumeData(true)
    }

    fun drop() {
        close()
        Misc.rmDir(directory)
    }

    fun deleteAllData() {
        drop()
        File(directory).mkdir()
        create()
    }

    /**
     * close the relation, save meta data and commit trees.
     */
    fun close() {
        meta.write(Paths.get(directory, "meta").toString())
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
        meta = RelationMeta().read(Paths.get(directory, "meta").toString())
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
            MainDataConfiguration(meta.coltypes!!, meta.colsizes!!, colIDs),
            mode,
            Paths.get(directory, "data").toString(),
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
                Paths.get(directory, "rowID2position").toString()
            )
        )

        superKeyTrees = ArrayList(meta.superKeys!!.size)
        indexTrees = ArrayList(meta.indices!!.size)
        nullTrees = ArrayList(meta.nullableColIds!!.size)

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
                Paths.get(directory, String.format("null.%d.data", meta.nullableColIds!![i])).toString()
            )
            nullTrees!![i] = tmp
        }

        // resume indices trees
        for (i in indexTrees!!.indices) {
            val colId = meta.indices!![i]
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    colId.stream().map<Any> { x: Int? ->
                        meta.coltypes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId.stream().map<Any> { x: Int? ->
                        meta.colsizes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId,
                    false,
                    1000
                ),
                mode,
                Paths.get(directory, String.format("index.%d.data", i)).toString()
            )
            indexTrees!![i] = tmp
        }

        // resume super key trees
        for (i in superKeyTrees!!.indices) {
            val colId = meta.superKeys!![i]
            val tmp = BPlusTree(
                BPlusConfiguration(
                    1024,
                    8,
                    colId.stream().map<Any> { x: Int? ->
                        meta.coltypes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId.stream().map<Any> { x: Int? ->
                        meta.colsizes!![x!!]
                    }.collect(Collectors.toCollection { ArrayList() }),
                    colId,
                    true,
                    1000
                ),
                mode,
                Paths.get(directory, String.format("key.%d.data", i)).toString()
            )
            superKeyTrees!![i] = tmp
        }
    }
    /**
     * ------------------------------Append code end-------------------------------
     */

    /**
     * Get a copy of relation
     * @return Relation of this table
     */
    override fun getRelation(): Relation
        = relation.clone()

    /**
     * Relational tables insert single-row data operations,
     * insert data, and build BPTree.
     */
    override fun insert(row: Array<Any>) {
        val rowID = meta.nextRowID++
        // check columns validity
        if(row.size != meta.ncols) {
            throw RuntimeException("Unable inserting row for table `$name`, incorrect row size and column size.")
        }
        val indeedNullCols = ArrayList<Int>()
        for (i in 0 until meta.ncols) {
            // check type
            if (meta.coltypes!![i] != row[i].javaClass) {
                throw MiniDBException(
                    String.format(
                        "Non-compatible type. Given: %s, Required: %s!",
                        row[i].javaClass.typeName,
                        meta.coltypes!![i].typeName
                    )
                )
            }

            // check string length
            if (meta.coltypes!![i] == java.lang.String::class.java) {
                val length = (row[i] as String?)!!.toByteArray(StandardCharsets.UTF_8).size
                if (length > meta.colsizes!![i]) {
                    throw MiniDBException(
                        java.lang.String.format(
                            "column (%s) length exceeds! The value is (%s) with a length of %d (in bytes), but the limit is %d.",
                            meta.colnames!![i],
                            row[i], length, meta.colsizes!![i]
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
        relation.rows.add(row)
    }

    override fun insert(row: NTuple) {
        val tuple = NTuple()
        val arr = ArrayList<Any>()

        for(column in relation.columns) {
            if(row.columns.contains(column)) {
                tuple.add(row[column])
                arr.add((row[column] as Cell<*>).value!!)
            } else {
                tuple.add(Cell(column, column.defaultValue() ?:
                    throw RuntimeException("Unable inserting row for table `$name`, " +
                            "no default value for column `${column.name}`.")))
                arr.add(column.defaultValue()!!)
            }
        }
        relation.tuples.add(tuple)
        insert(arr.toArray())
    }

    // TODO 需要修改更新操作与索引相关的更新
    override fun update(cond: Predicate<NTuple>, updated: Array<Cell<Expression>>): Int {
        var affectsCounter = 0
        for (tuple in relation.tuples) {
            if(cond.test(tuple)) {
                affectsCounter ++
                for (cell in updated) {
                    tuple[cell.column] = cell.value
                }
            }
        }
        return affectsCounter
    }
}