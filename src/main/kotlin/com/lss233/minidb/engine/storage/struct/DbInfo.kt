package com.lss233.minidb.engine.storage.struct

/**
 * 记录数据库的基本信息
 */
class DbInfo {

    private class TableInfo {
        /**
         * 这张表有多少条记录
         */
        var recordNum = 0

        /**
         * 这张表占多少kb
         */
        var tableKBLen = 0
    }

    private val infoMap = HashMap<String, TableInfo>()

    fun add(tableName: String, recordNum: Int, tableKBLen: Int) {
        val info = TableInfo()
        info.recordNum = recordNum
        info.tableKBLen = tableKBLen
        infoMap[tableName] = info
    }
}