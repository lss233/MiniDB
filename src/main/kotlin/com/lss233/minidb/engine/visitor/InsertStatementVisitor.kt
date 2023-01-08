package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
import hu.webarticum.treeprinter.SimpleTreeNode
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.expression.primary.literal.LiteralNumber
import miniDB.parser.ast.expression.primary.literal.LiteralString
import miniDB.parser.ast.stmt.dml.DMLInsertStatement
import miniDB.parser.ast.stmt.dml.DMLReplaceStatement
import miniDB.parser.visitor.Visitor
import java.util.*
import kotlin.math.exp

class InsertStatementVisitor: Visitor() {
    private val stack = Stack<Any>()
    var rootNode = SimpleTreeNode("DMLInsertStatement")
    var relation: Table? = null
    var affects = 0

    override fun visit(node: DMLInsertStatement) {
        val table = Engine[node.table];
        val columns = node.columnNameList?.map { identifier -> Column(identifier) } ?: table.getRelation().columns
        node.rowList.forEach { row -> run {
            affects++
            table.insert(NTuple.from(
                *row.rowExprList.mapIndexed { index, expression -> run {
                    expression.accept(this@InsertStatementVisitor)
                    Cell(columns[index], stack.pop())
                } }.toTypedArray()
            ))
        } }

    }
    override fun visit(node: DMLReplaceStatement) {
        val table = Engine[node.table];
        val columns = node.columnNameList.map { identifier -> Column(identifier) }
        node.rowList.forEach { row -> run {
            table.insert(NTuple.from(
                *row.rowExprList.mapIndexed { index, expression -> Cell(columns[index], expression.toString()) }.toTypedArray()
            ))
        } }
    }

    override fun visit(node: LiteralNumber) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("LiteralNumber(${node.number})")
        stack.push(node.number)
        parentNode.addChild(rootNode)
        rootNode = parentNode
    }

    override fun visit(node: LiteralString) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("LiteralString('${node.unescapedString}')")
        stack.push(node.unescapedString)
        parentNode.addChild(rootNode)
        rootNode = parentNode
    }
}