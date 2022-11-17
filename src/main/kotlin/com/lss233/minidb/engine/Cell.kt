package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.literal.LiteralNumber
import miniDB.parser.ast.expression.primary.literal.LiteralString

/**
 * 行式存储结构中的单体数据(单元格数据)
 * @param column 数据所标记的列
 * @param value 单元格中存储的数据
 */
class Cell<T> constructor(var column: Column, var value: T) {

    override fun equals(other: Any?): Boolean {
        if (other is LiteralString) {
            return value.toString() == other.unescapedString
        }
        if (other is LiteralNumber) {
            return value.toString().toDoubleOrNull() == other.number
        }
        return if (other is Cell<*>)
            value == other.value
        else
            false
    }

    override fun toString(): String {
        return value.toString()
    }

    override fun hashCode(): Int {
        var result = column.hashCode()
        result = 31 * result + (value?.hashCode() ?: 0)
        return result
    }
}