package com.lss233.minidb.engine.visitor

import miniDB.parser.ast.stmt.dml.DMLSelectStatement
import miniDB.parser.visitor.Visitor

class SelectStatementVisitor: Visitor() {
    override fun visit(node: DMLSelectStatement) {
        val tableVisitor = TableVisitor();
        node.tables.accept(tableVisitor)
    }
}