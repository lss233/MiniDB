package com.lss233.minidb.engine.schema

import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.fragment.ddl.ColumnDefinition

/**
 * 进行关系运算过滤中的封装列的信息实体
 * 作为参数使用
 */

class Column {

    val identifier : Identifier

    /**
     * 列名
     */
    val name : String
    lateinit var definition: ColumnDefinition

    constructor(id: Identifier){
        this.identifier = id;
        this.name = id.idText
    }
    constructor(id: Identifier, definition: ColumnDefinition): this(id) {
        this.definition = definition
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
