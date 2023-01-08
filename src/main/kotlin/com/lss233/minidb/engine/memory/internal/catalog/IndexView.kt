package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class IndexView(database: Database): PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("indexrelid", DataType.DataTypeName.INT), Column("indrelid", DataType.DataTypeName.INT),
        Column("indnatts", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()
}