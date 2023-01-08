package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class TypeView(database: Database): PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
            Column("oid", DataType.DataTypeName.INT),
            Column("typname", DataType.DataTypeName.CHAR),
            Column("typnamespace", DataType.DataTypeName.CHAR),
            Column("typowner", DataType.DataTypeName.CHAR),
            Column("typlen", DataType.DataTypeName.CHAR),
            Column("typbyval", DataType.DataTypeName.CHAR),
            Column("typtype", DataType.DataTypeName.CHAR),
            Column("typcategory", DataType.DataTypeName.CHAR),
            Column("typdelim", DataType.DataTypeName.CHAR),
            Column("typndims", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
            = mutableListOf()
}