package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class RewriteView(database: Database) : PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("oid", DataType.DataTypeName.INT),
        Column("rulename", DataType.DataTypeName.VARCHAR),
        Column("ev_class", DataType.DataTypeName.INT),
        Column("ev_type", DataType.DataTypeName.CHAR),
        Column("ev_enabled", DataType.DataTypeName.CHAR),
        Column("is_instead", DataType.DataTypeName.BOOLEAN),
        Column("ev_equal", DataType.DataTypeName.JSON),
        Column("ev_action", DataType.DataTypeName.JSON)
        )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()

}