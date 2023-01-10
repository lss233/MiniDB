package com.lss233.minidb.engine.schema

import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.fragment.ddl.ColumnDefinition
import miniDB.parser.ast.fragment.ddl.datatype.DataType
import miniDB.parser.ast.fragment.ddl.datatype.DataType.DataTypeName

/**
 * 进行关系运算过滤中的封装列的信息实体
 * 作为参数使用
 */

class Column {

    val identifier : Identifier
    /**
     * 表名（虚拟）
     * 可能会跟随关系查询而改变
     */
    val tableName: String?

    /**
     * 表名（物理）
     * 不会改变
     */
    val physicalTableName: String?

    /**
     * 列名（虚拟）
     * 可能会跟随关系查询而改变
     */
    val name : String

    /**
     * 列名（物理）
     * 不会改变
     */
    val physicalColumnName: String?
    lateinit var definition: ColumnDefinition

    constructor(id: Identifier){
        this.identifier = id;
        this.name = id.idText
        this.tableName = id.parent?.idText

        this.physicalColumnName = id.idText
        this.physicalTableName = id.parent?.idText
    }
    constructor(id: Identifier, definition: ColumnDefinition): this(id) {
        this.definition = definition
    }

    /**
     * test constructor
     */
    constructor(name: String, typeName: DataTypeName) {
        this.identifier = Identifier(null, name)
        this.name = name
        this.definition = ColumnDefinition(
            DataType(typeName,  true, true, true,null,null,null,null,null),
            true,null,true,null,null,null,null,null,true,true,null
        )
        this.tableName = null
        this.physicalColumnName = null
        this.physicalTableName = null
    }

    constructor(name: String) {
        this.identifier = Identifier(null, name)
        this.name = name
        this.tableName = null
        this.physicalColumnName = null
        this.physicalTableName = null
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

    override fun toString(): String {
        return "Column: $identifier"
    }
}
