package com.lss233.minidb.engine.config

/**
 * 数据库存储的基本配置项
 */
class DBConfig {

    companion object {

        const val DB_ROOT_PATH :String = "E:\\MiniDB_Data\\"

        const val IO_LINKED_BLOCK_QUEUE_SIZE:Int = 500

        const val FILE_SUFFIX:String = ".db"

        const val TABLE_SUFFIX:String = ".tb"
    }
}