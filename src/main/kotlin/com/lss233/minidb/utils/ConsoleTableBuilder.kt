package com.lss233.minidb.utils

import com.lss233.minidb.engine.NTuple

class ConsoleTableBuilder {
    private val headers = ArrayList<String>();
    private var body = ArrayList<Collection<Any>>();
    private val columnLengths = ArrayList<Int>();

    fun withHeaders(vararg _headers: String) : ConsoleTableBuilder {
        headers.clear()
        for (header in _headers) {
            headers.add(header)
        }
        return this
    }
    fun withBody(vararg tuples: NTuple): ConsoleTableBuilder {
        body.clear()
        for(row in tuples) {
            body.add(row);
        }
        return this;
    }
    fun withBody(vararg _body: Collection<Any>) : ConsoleTableBuilder {
        body.clear()
        for(row in _body) {
            body.add(row)
        }
        return this
    }

    fun build(): String {
        // 1. Generate columnLengths
        headers.forEachIndexed { index, element -> run {
            if(columnLengths.size <= index) {
                columnLengths.add(element.length)
            } else {
                columnLengths[index] = if (columnLengths[index] > element.length) columnLengths[index] else element.length
            }
        }}
        body.forEach { row -> run {
            row.forEachIndexed { index, element -> run {
                if(columnLengths.size <= index) {
                    columnLengths.add(element.toString().length)
                } else {
                    columnLengths[index] = if (columnLengths[index] > element.toString().length) columnLengths[index] else element.toString().length
                }
            }}
        }}

        // 2. Generate format
        val sb = StringBuilder()
        val divider = StringBuilder()
        columnLengths.forEach {
            run {
                sb.append("| %${it}s ")
                divider.append("+-" + "-".repeat(it) + "-")
        } }
        sb.append("|\n")
        divider.append("+\n")


        // 3. Make values
        var result = divider.toString()
        if (headers.isNotEmpty()) {
            result += String.format(sb.toString(), *headers.toArray())
            result += divider.toString()
        }

        body.forEach { row -> run {
            result += String.format(sb.toString(), *row.map { it -> it.toString() }.toTypedArray())
        } }
        return result + divider.toString()

    }
}