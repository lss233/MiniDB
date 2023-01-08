package com.lss233.minidb.engine.memory.internal.information

import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class ColumnsView(): InformationSchemaView() {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("table_catalog", DataType.DataTypeName.CHAR), Column("table_schema", DataType.DataTypeName.CHAR),
        Column("table_name", DataType.DataTypeName.CHAR), Column("column_name", DataType.DataTypeName.CHAR),
        Column("ordinal_position", DataType.DataTypeName.CHAR), Column("udt_catalog", DataType.DataTypeName.CHAR),
        Column("udt_schema", DataType.DataTypeName.CHAR), Column("udt_name", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()
}