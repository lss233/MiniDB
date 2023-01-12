package com.lss233.minidb.engine.visitor

import miniDB.parser.ast.stmt.ddl.DDLDropTableStatement

class DropTableStatementVisitor: SelectStatementVisitor() {
    override fun visit(node: DDLDropTableStatement) {
    }
}