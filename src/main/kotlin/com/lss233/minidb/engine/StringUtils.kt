package com.lss233.minidb.engine

import java.util.*

class StringUtils {
    companion object {
        /**
         * Test whether a text satisfy the given match pattern
         * https://stackoverflow.com/a/898461/7463472
         */
        fun like(text: String, matchPattern: String): Boolean {
            var expr = matchPattern.lowercase(Locale.getDefault()) // ignoring locale for now
            expr = expr.replace(".", "\\.") // "\\" is escaped to "\" (thanks, Alan M)
            // ... escape any other potentially problematic characters here
            expr = expr.replace("?", ".")
            expr = expr.replace("%", ".*")
            val str = text.lowercase(Locale.getDefault())
            return str.matches(expr.toRegex())
        }
    }
}