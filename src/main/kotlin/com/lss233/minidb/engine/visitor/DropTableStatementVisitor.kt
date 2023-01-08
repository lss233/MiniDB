package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.memory.Engine
import miniDB.parser.ast.stmt.ddl.DDLDropTableStatement

class DropTableStatementVisitor: SelectStatementVisitor() {
    override fun visit(node: DDLDropTableStatement) {
    }
}