package com.lss233.minidb.engine

import miniDB.parser.ast.stmt.SQLStatement
import miniDB.parser.recognizer.SQLParserDelegate

class SQLParser {
    companion object {
        private val REGEX_STMT_SET = "set (.+) to (.+)".toRegex(RegexOption.IGNORE_CASE)
        private val REGEX_ANY_CAPTURE = " (\\w+.?\\w+) = ANY \\('\\{(.+)}'::(.+?)\\)".toRegex(RegexOption.IGNORE_CASE)
        private val REGEX_CAST = "'((?!').)'::\"(.+?)\"".toRegex()

        /**
         * 在提交到语法解析器之前先进行
         */
        fun parse(str: String): SQLStatement {
            // 先把查询语句转化为 MySQL 风格
            var queryStr = if(REGEX_STMT_SET.matches(str)) {
                str.replace(REGEX_STMT_SET, "SET $1=$2")
            } else {
                str
            }

            queryStr = queryStr.replace(REGEX_CAST, "CAST('$1' AS $2)")


            // Polyfill TO col = ANY('{a,b,c}'::type[])
            REGEX_ANY_CAPTURE.find(queryStr)?.let { match -> run {
                    val (identifier, items, type) = match.destructured
                    queryStr = queryStr.substring(0, match.range.first) + items.split(",")
                        .joinToString(prefix = " (", separator = " OR ", postfix = ")") { "$identifier = '$it'" } + queryStr.substring(match.range.last + 1)
                }
            }
            return SQLParserDelegate.parse(queryStr)
        }
    }

}
