package com.lss233.minidb.engine.storage.struct

/**
 * 存储数据库的基本信息
 * 该类用于管理整个db的信息，是整个存储信息的核心，主要由sql解析器进行调用，生成ioTask
 * 初始化流程
 * 逐个读取表文件，TODO：这里需要先验证 check一下有效性
 * 1.生成数据库表信息，key是表名、po存储：表的所有字段以及字段的长度。
 * 2.生成数据库元信息，插入map。
 * 3.将12生成的东西插入dbs。
 */
class DbStruct {

    companion object {

        @JvmStatic
        //key：数据库名  v：库内表结构
        var dbs = HashMap<String, HashMap<String, DbTableStruct>>()

        @JvmStatic
        fun add(dbName: String, tableInfo: HashMap<String, DbTableStruct>) {
            dbs.put(dbName, tableInfo)
        }

        @JvmStatic
        fun getTableStructByName(dbName: String, tableName: String): DbTableStruct? {
            return dbs[dbName]?.get(tableName)
        }

        // TODO 设计问题  以后要改成真正的获取最大id而不是自增
        @JvmStatic
        fun getTableMaxId(dbName: String, tableName: String): Int {
            return dbs[dbName]!![tableName]!!.recordNum + 1
        }
    }

}