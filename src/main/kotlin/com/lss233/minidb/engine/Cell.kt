package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.literal.LiteralNumber
import miniDB.parser.ast.expression.primary.literal.LiteralString
import java.lang.IllegalArgumentException

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
            return value.toString().toDoubleOrNull() == other.number.toDouble()
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
    operator fun compareTo(other: Any?): Int {
        if(value !is Number) {
            throw IllegalArgumentException("Value cannot be compared.")
        }
        if(other is Cell<*>) {
            if(other.value !is Number) {
                throw IllegalArgumentException("Target value cannot be compared.")
            }
            return (value as Number).toDouble().compareTo((other.value as Number).toDouble())
        }
        if(other is LiteralNumber) {
            return (value as Number).toDouble().compareTo(other.number.toDouble())
        }
        throw IllegalArgumentException("Unsupported type for $other to compare.")
    }
}
