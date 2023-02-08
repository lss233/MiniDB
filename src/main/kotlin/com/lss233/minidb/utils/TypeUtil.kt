package com.lss233.minidb.utils

import com.lss233.minidb.engine.storage.StorageType
import miniDB.parser.ast.fragment.ddl.datatype.DataType.DataTypeName

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

        fun getType(dataType: DataTypeName):java.lang.reflect.Type {
            return when(dataType) {
                DataTypeName.CHAR -> java.lang.String::class.java
                DataTypeName.GEOMETRY -> TODO()
                DataTypeName.POINT -> TODO()
                DataTypeName.LINESTRING -> TODO()
                DataTypeName.POLYGON -> TODO()
                DataTypeName.MULTIPOINT -> TODO()
                DataTypeName.MULTILINESTRING -> TODO()
                DataTypeName.GEOMETRYCOLLECTION -> TODO()
                DataTypeName.MULTIPOLYGON -> TODO()
                DataTypeName.BIT -> TODO()
                DataTypeName.TINYINT -> java.lang.Short::class.java
                DataTypeName.SMALLINT -> TODO()
                DataTypeName.MEDIUMINT -> TODO()
                DataTypeName.INT -> java.lang.Integer::class.java
                DataTypeName.BIGINT -> java.lang.Long::class.java
                DataTypeName.REAL -> TODO()
                DataTypeName.DOUBLE -> java.lang.Double::class.java
                DataTypeName.FLOAT -> java.lang.Float::class.java
                DataTypeName.DECIMAL -> TODO()
                DataTypeName.DATE -> TODO()
                DataTypeName.TIME -> TODO()
                DataTypeName.TIMESTAMP -> TODO()
                DataTypeName.DATETIME -> TODO()
                DataTypeName.YEAR -> TODO()
                DataTypeName.VARCHAR -> java.lang.String::class.java
                DataTypeName.BINARY -> TODO()
                DataTypeName.VARBINARY -> TODO()
                DataTypeName.TINYBLOB -> TODO()
                DataTypeName.BLOB -> TODO()
                DataTypeName.MEDIUMBLOB -> TODO()
                DataTypeName.LONGBLOB -> TODO()
                DataTypeName.TINYTEXT -> TODO()
                DataTypeName.TEXT -> java.lang.String::class.java
                DataTypeName.MEDIUMTEXT -> TODO()
                DataTypeName.LONGTEXT -> TODO()
                DataTypeName.ENUM -> TODO()
                DataTypeName.SET -> TODO()
                DataTypeName.BOOL -> TODO()
                DataTypeName.BOOLEAN -> java.lang.Boolean::class.java
                DataTypeName.SERIAL -> TODO()
                DataTypeName.FIXED -> TODO()
                DataTypeName.JSON -> TODO()
            }
        }
    }
}
