package com.lss233.minidb.engine.schema

import miniDB.parser.ast.expression.primary.Identifier

/**
 * 进行关系运算过滤中的封装列的信息实体
 * 作为参数使用
 */

class Column {
    val identifier : Identifier
    val name : String
//        set(value) {
//            identifier = Identifier(null, value)
//            field = value
//        }

    constructor(id: Identifier){
        this.identifier = id;
        this.name = id.idText
    }
    constructor(name: String) {
        this.identifier = Identifier(null, name)
        this.name = name
    }

    /**
     * 重写equals方法
     * 比较列仅需要名字相同既可以
     */
    override fun equals(other: Any?): Boolean {
        return when (other) {
            !is Column -> false
            else -> (this === other) || (this.name == other.name)
        }
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }

    fun getFullName(): String {
        return identifier.idTextWithParent;
    }

    fun defaultValue(): Any? {
        return null
    }
}
