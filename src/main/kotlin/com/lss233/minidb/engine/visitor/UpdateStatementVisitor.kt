package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.stmt.dml.DMLUpdateStatement
import java.util.function.Predicate

class UpdateStatementVisitor: SelectStatementVisitor() {
    var affects = 0
    override fun visit(node: DMLUpdateStatement) {
        node.tableRefs.accept(this)
        val table = stack.pop() as Table
        node.where.accept(this)
        val cond = stack.pop() as Predicate<NTuple>
        val updated = node.values.map { Cell(Column(it.key), it.value) }.toTypedArray()
        affects =  table.update(cond, updated)
    }
}