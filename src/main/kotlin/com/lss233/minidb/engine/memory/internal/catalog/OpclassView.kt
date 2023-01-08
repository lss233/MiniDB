package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class OpclassView(database: Database) : PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("oid", DataType.DataTypeName.INT),
        Column("opcmethod", DataType.DataTypeName.INT),
        Column("opcname", DataType.DataTypeName.TEXT),
        Column("opcnamespace", DataType.DataTypeName.INT),
        Column("opcowner", DataType.DataTypeName.INT),
        Column("opcfamily", DataType.DataTypeName.INT),
        Column("opcdefault", DataType.DataTypeName.BOOLEAN),
        Column("opckeytype", DataType.DataTypeName.INT)
        )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()

}