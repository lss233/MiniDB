package com.lss233.minidb.engine.storage.struct

import miniDB.parser.ast.fragment.ddl.datatype.DataType

class TableHeader {

    // TODO recode user & other basic info

    var tableField = arrayListOf<TableField>()


    fun addTableFile(table:TableField) {
        this.tableField.add(table)
    }

    fun getColumnStorageSize(): Int {
        var size = 0

        for (item in tableField) {
            size += DataType.getType(item.dataTypeName).toInt()
        }
        return size
    }

}
