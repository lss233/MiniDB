package com.lss233.minidb.engine.config

/**
 * 进行实例存储的设置
 * 这里按顺序写上存储的定义
 */
class DbStorageConfig {

    companion object {
        /**
         * 记录是否删除标志为
         */
        const val DELETE_FLAG_BIT = 4

        /**
         * 插入这条记录的时间戳
         */
        const val INSERT_TIME_STAMP = 4

        /**
         * 操作过这条记录的事务id
         */
        const val TRANSACTION_ID = 4

        /**
         * 事务读标记的时间戳
         */
        const val READED_TIME_STAMP = 4

        /**
         * 事务写标记的时间戳
         */
        const val UPDATE_TIME_STAMP = 4


        fun getTotalByteLen(): Int {
            return DELETE_FLAG_BIT +
                    INSERT_TIME_STAMP +
                    TRANSACTION_ID +
                    READED_TIME_STAMP +
                    UPDATE_TIME_STAMP
        }
    }
}
