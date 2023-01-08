package com.lss233.minidb.engine.memory.internal.information

import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class ParametersView(): InformationSchemaView() {
    override fun getColumns(): MutableList<Column>
        = mutableListOf(
        Column("specific_schema", DataType.DataTypeName.CHAR), Column("specific_name", DataType.DataTypeName.CHAR),
        Column("routine_catalog", DataType.DataTypeName.CHAR), Column("ordinal_position", DataType.DataTypeName.CHAR),
        Column("parameter_mode", DataType.DataTypeName.CHAR), Column("is_result", DataType.DataTypeName.CHAR),
        Column("as_locator", DataType.DataTypeName.CHAR), Column("parameter_name", DataType.DataTypeName.CHAR),
        Column("data_type", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = mutableListOf()
}