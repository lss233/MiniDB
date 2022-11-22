package com.lss233.minidb.engine.storage.struct

import com.lss233.minidb.engine.config.DBConfig
import com.lss233.minidb.engine.storage.type.StoredType

class DbTableStruct {

    /**
     * 表名
     */
    var tableName: String? = null
        get() {
            // TODO 以后改成返回真正的文件名  预计: 库名_表名.tb
            return field
        }

    /**
     * 记录数
     */
    var recordNum: Int = 0

    /**
     * 字段信息
     */
    var fields: ArrayList<DbTableField>? = null

    /**
     * 该记录的byte长度
     */
    var recordLen: Int = 0

    /**
     * 字段数
     */
    var fieldNum: Int = 0


    var maxPK: Int = 0


    var fieldNameList = ArrayList<String>()


    var fieldTypeList = ArrayList<StoredType>()


    var fieldLensList = ArrayList<Int>()

}