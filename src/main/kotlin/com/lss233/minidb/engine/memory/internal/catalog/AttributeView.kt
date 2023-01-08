package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class AttributeView(database: Database): PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("attrelid", DataType.DataTypeName.INT), Column("attname", DataType.DataTypeName.CHAR),
        Column("atttypid", DataType.DataTypeName.INT), Column("attstattarget", DataType.DataTypeName.CHAR),
        Column("attlen", DataType.DataTypeName.CHAR), Column("attnum", DataType.DataTypeName.CHAR),
        Column("attndims", DataType.DataTypeName.CHAR), Column("attcacheoff", DataType.DataTypeName.CHAR),
        Column("attbyval", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()
}