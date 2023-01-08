package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class InheritsView(database: Database): PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("inhrelid", DataType.DataTypeName.INT), Column("inhparent", DataType.DataTypeName.CHAR),
        Column("inhseqno", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()
}