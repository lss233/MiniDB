package com.lss233.minidb.utils

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
    }

}