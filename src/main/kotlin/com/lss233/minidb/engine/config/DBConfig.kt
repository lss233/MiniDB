package com.lss233.minidb.engine.config


class DBConfig {

    companion object {

        @JvmField
        val DB_ROOT_PATH :String = "D:\\db\\"

        @JvmField
        val IO_LINKED_BLOCK_QUEUE_SIZE:Int = 500

        val FILE_SUFFIX:String = ".db"
    }
}