package com.lss233.minidb.utils

import com.lss233.minidb.engine.storage.StorageType

/**
 * 类型相关的全局工具类
 */
class TypeUtil {

    companion object {
        @JvmStatic
        fun isVarLenType(type: String?): Boolean {
            var res = false
            when (type) {
                "char" -> res = true
                "varchar" -> res = true
            }
            return res
        }

        inline fun <reified T> getType(any: T): StorageType {
            return when (T::class.java) {
                java.lang.Integer::class.java -> StorageType.Int
                java.lang.Long::class.java -> StorageType.Long
                java.lang.String::class.java -> StorageType.String
                java.lang.Boolean::class.java -> StorageType.Bool
                java.lang.Float::class.java -> StorageType.Float
                java.lang.Double::class.java -> StorageType.Double
                else -> {
                    throw RuntimeException("Unsupported Type")
                }
            }
        }
    }

}
