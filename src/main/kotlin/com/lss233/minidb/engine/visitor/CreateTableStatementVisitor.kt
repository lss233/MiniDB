package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
import hu.webarticum.treeprinter.SimpleTreeNode
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.fragment.ddl.ColumnDefinition
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement
import miniDB.parser.visitor.Visitor
import java.util.*

class CreateTableStatementVisitor: Visitor() {
    private val stack = Stack<Any>()
    var rootNode = SimpleTreeNode("DDLCreateTableStatement")
    var relation: Table? = null
    override fun visit(node: DDLCreateTableStatement) {
        node.table.accept(this)
        val tableIdentifier = stack.pop() as Identifier

        val columns = node.colDefs.map {
            run {
                it.key.accept(this)
                val columIdentifier = stack.pop() as Identifier
                Column(columIdentifier, it.value)
        } }.toMutableList()
        relation = Table(tableIdentifier.idText, columns, mutableListOf())

        stack.push(relation)
    }

    /**
     * Polyfill: Unpack quotes from parser.
     */
    override fun visit(node: Identifier) {
        val parentNode = rootNode
        val idText = node.idText.replace("'(.+)'".toRegex(), "$1")
        rootNode = SimpleTreeNode("Identifier($idText)")
        val parent = if(node.parent != null) {
            node.parent.accept(this)
            stack.pop() as Identifier
        } else {
            null
        }
        stack.push(Identifier(parent, idText))

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }
}