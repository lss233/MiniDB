package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class TablespaceView(database: Database): PostgresCatalogView(database) {
    override fun getColumns(): MutableList<Column>
            = mutableListOf(
                Column("oid", DataType.DataTypeName.INT),
                Column("spcname", DataType.DataTypeName.CHAR),
                Column("spcowner", DataType.DataTypeName.CHAR),
                Column("spcacl", DataType.DataTypeName.CHAR),
                Column("spcoptions", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
            = mutableListOf(
                arrayOf("1", "default_tablespace", "1", "", "")
    )
}