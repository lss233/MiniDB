package com.lss233.minidb.engine.config

/**
 * 数据库存储的基本配置项
 */
class DBConfig {

    companion object {

        /**
         * 系统文件的目录
         */
        const val DB_ROOT_PATH :String = "E:\\MiniDB_Data\\"

        /**
         * 系统表位置
         */
        const val SYSTEM_FILE:String = DB_ROOT_PATH + "DB_Files\\"

        /**
         * 数据表位置
         */
        const val DATA_FILE:String = DB_ROOT_PATH + "Data\\"

        /**
         * 每一次IO读取的块数
         */
        const val IO_LINKED_BLOCK_QUEUE_SIZE:Int = 500

        /**
         * 表文件后缀
         */
        const val TABLE_SUFFIX:String = ".tb"
    }
}
