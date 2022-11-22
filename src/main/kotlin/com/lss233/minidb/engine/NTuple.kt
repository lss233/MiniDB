package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.Identifier

// TODO 这里应当修改成可空，由于add element.vale时，value可能为null，但是这里需要改的东西有些多，还不是很确定，暂时放到这里
class NTuple : ArrayList<Any>() {
    var columns = ArrayList<Column>()


    operator fun plus(element: Any?): NTuple {
        val result = NTuple()
        result.addAll(this)
        result.add(element!!)
        return result
    }

    fun add(element: Cell<*>): Boolean {
        return if(columns.size < size) {
            if(columns[size] != element.column) {
                throw RuntimeException("Unable add NTuple[$size], wrong column (columns[$size] = ${columns[size]}, element.column = ${columns[size]})")
            }

            super.add(element)
        } else {
            columns.add(element.column)
            super.add(element)
        }
    }

    /**
     * 根据列读数
     * @param col 列
     * @return 数据
     */
    operator fun get(col: Column): Any {
        return this[columns
            .indexOf(col)
            .takeIf { it >= 0 } ?: throw RuntimeException("No such column named $col")
        ]
    }

    /**
     * 根据列名读数据
     * @param identifier 列名
     * @return 数据
     */
    operator fun get(identifier: Identifier): Any {
        return this[indexOf(identifier)]
    }

    /**
     * 根据列名设置数据
     * @param colName 列名
     * @param value 数据
     */
    operator fun set(identifier: String, `value`: Any){
        this[indexOf(identifier)] = `value`
    }

    private fun indexOf(identifier: Identifier): Int {
        return columns
            .indexOfFirst { i -> if(identifier.parent == null || i.identifier.parent == null) { identifier.idText == i.identifier.idText } else { i.identifier.idTextWithParentUpUnescape == identifier.idTextWithParentUpUnescape} }
            .takeIf { it >= 0 } ?: throw RuntimeException("No such column named $identifier")
    }

    companion object {
        fun from(vararg items: Any): NTuple {
            val tuple = NTuple();
            for(item in items) {
                tuple.add(item)
            }
            return tuple
        }
        fun from(vararg items: Cell<*>): NTuple {
            val tuple = NTuple();
            for (item in items) {
                tuple.add(item)
            }
            return tuple
        }
    }

}