package com.lss233.minidb.engine.memory.internal.information

import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class RoutinesView: InformationSchemaView() {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("specific_schema", DataType.DataTypeName.CHAR), Column("specific_name", DataType.DataTypeName.CHAR),
        Column("routine_catalog", DataType.DataTypeName.CHAR), Column("routine_schema", DataType.DataTypeName.CHAR),
        Column("routine_name", DataType.DataTypeName.CHAR), Column("routine_type", DataType.DataTypeName.CHAR),
        Column("module_catalog", DataType.DataTypeName.CHAR), Column("data_type", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()
}