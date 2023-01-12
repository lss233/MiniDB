package com.lss233.minidb.engine.storage.struct

import miniDB.parser.ast.fragment.ddl.datatype.DataType.DataTypeName

/**
 * 定义字段的实体
 * @param filedName 字段名
 */
class DbTableField constructor(var filedName: String){

    /**
     * byte长度
     */
    var byteLen: Int = 0

    /**
     * 存储类型
     */
    var type: DataTypeName? = null

    /**
     * 是否为主键
     */
    var isPrimaryKey = false

}