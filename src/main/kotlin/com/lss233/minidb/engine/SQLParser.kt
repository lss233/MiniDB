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
            if(queryStr == "SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename, c.relacl, pg_get_userbyid(c.relowner) AS tableowner, obj_description(c.oid) AS description, c.relkind, ci.relname As cluster, c.relhasoids AS hasoids, c.relhasindex AS hasindexes, c.relhasrules AS hasrules, t.spcname AS tablespace, c.reloptions AS param, c.relhastriggers AS hastriggers, c.relpersistence AS unlogged, ft.ftoptions, fs.srvname, c.reltuples, ((SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid) > 0) AS inhtable, i2.nspname AS inhschemaname, i2.relname AS inhtablename FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace LEFT JOIN (pg_inherits i INNER JOIN pg_class c2 ON i.inhparent = c2.oid LEFT JOIN pg_namespace n2 ON n2.oid = c2.relnamespace) i2 ON i2.inhrelid = c.oid LEFT JOIN pg_index ind ON(ind.indrelid = c.oid) and (ind.indisclustered = 't') LEFT JOIN pg_class ci ON ci.oid = ind.indexrelid LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid LEFT JOIN pg_foreign_server fs ON ft.ftserver = fs.oid WHERE ((c.relkind = 'r'::\"char\") OR (c.relkind = 'f'::\"char\")) AND n.nspname = 'public' ORDER BY schemaname, tablename") {
                queryStr = "SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename, c.relacl, pg_get_userbyid(c.relowner) AS tableowner, obj_description(c.oid) AS description, c.relkind, ci.relname As cluster, c.relhasindex AS hasindexes, c.relhasrules AS hasrules, t.spcname AS tablespace, c.reloptions AS param, c.relhastriggers AS hastriggers, c.relpersistence AS unlogged, ft.ftoptions, fs.srvname, c.reltuples, ((SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid) > 0) AS inhtable FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace LEFT JOIN (pg_inherits i INNER JOIN pg_class c2 ON i.inhparent = c2.oid LEFT JOIN pg_namespace n2 ON n2.oid = c2.relnamespace) ON inhrelid = c.oid LEFT JOIN pg_index ind ON(ind.indrelid = c.oid) and (ind.indisclustered = 't') LEFT JOIN pg_class ci ON ci.oid = ind.indexrelid LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid LEFT JOIN pg_foreign_server fs ON ft.ftserver = fs.oid WHERE ((c.relkind = 'r'::\"char\") OR (c.relkind = 'f'::\"char\")) AND n.nspname = 'public' ORDER BY schemaname, tablename"
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
