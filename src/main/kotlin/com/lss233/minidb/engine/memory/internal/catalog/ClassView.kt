package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class ClassView(database: Database) : PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
            = mutableListOf(
        Column("oid", DataType.DataTypeName.INT),
        Column("relname", DataType.DataTypeName.CHAR),
        Column("relnamespace", DataType.DataTypeName.CHAR),
        Column("reltype", DataType.DataTypeName.CHAR),
        Column("reloftype", DataType.DataTypeName.CHAR),
        Column("relowner", DataType.DataTypeName.CHAR),
        Column("relam", DataType.DataTypeName.CHAR),
        Column("relfilenode", DataType.DataTypeName.CHAR),
        Column("reltablespace", DataType.DataTypeName.CHAR),
        Column("relkind", DataType.DataTypeName.CHAR)
    )
    // arrayOf(1, table.name, identifier.parent.idText, 0, 0, 10, 0, "mem", "1", "r")
    override fun generateData(): MutableList<Array<Any>>
            = database.flatMap { (schemaName, schema) ->
                schema.map { (name, view) ->
                    arrayOf<Any>(
                        0, // oid
                        name, // relname
                        schemaName, // relnamespace
                        0, // reltype
                        0, // reloftype
                        10, // relowner
                        0, // relam
                        "mem", // relfilenode
                        "1", // reltablespace
                        if (view is Table) "r" else "v" // relkind
                    ) } }
        .mapIndexed { index, anies: Array<Any> -> run {
                anies[0] = index + (anies[0] as Int)
                anies
            }
        }
        .toMutableList()
}