package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class NamespaceView(database: Database) : PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("oid", DataType.DataTypeName.INT), Column("nspname", DataType.DataTypeName.CHAR),
        Column("nspower", DataType.DataTypeName.CHAR), Column("nspacl", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf(
            arrayOf("pg_catalog", "pg_catalog", "10", "{postgres=UC/postgres,=U/postgres}"),
            arrayOf("public", "public", "10", "{postgres=UC/postgres,=U/postgres}"),
        )
}