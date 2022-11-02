package com.lss233.minidb.engine

/**
 * 行式存储结构中的单体数据(单元格数据)
 * @param colName 数据所标记的列
 * @param data 单元格中存储的数据
 */
class Cell constructor(private var colName: String, private var data: Any) {

    fun getColName():String {
        return this.colName
    }

    fun getData():Any {
        return this.data
    }
}