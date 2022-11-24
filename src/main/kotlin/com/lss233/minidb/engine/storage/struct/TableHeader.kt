package com.lss233.minidb.engine.storage.struct

import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

class TableHeader constructor(val tableName: String) {

    // TODO recode user & other basic info

    /**
     * 行数记录
     */
    var recordNumber: Int = 0


    var columns = arrayListOf<Column>()

    fun addColumn(column: Column) {
        this.columns.add(column)
    }

    fun getColumnStorageSize(): Int {
        var size = 0

        for (item in columns) {
            size += DataType.getTypeSize(item.definition.dataType.typeName)
        }
        return size
    }
}
