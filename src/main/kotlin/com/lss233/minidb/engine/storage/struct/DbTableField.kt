package com.lss233.minidb.engine.storage.struct

import com.lss233.minidb.engine.storage.type.StoredType

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
    var type: StoredType? = null

    /**
     * 是否为主键
     */
    var isPrimaryKey = false

}