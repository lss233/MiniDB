package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.Relation
import miniDB.parser.ast.fragment.tableref.OuterJoin
import miniDB.parser.ast.fragment.tableref.TableRefFactor
import miniDB.parser.ast.fragment.tableref.TableReference
import miniDB.parser.ast.fragment.tableref.TableReferences
import miniDB.parser.visitor.Visitor

class TableVisitor: Visitor() {
    private var stackDeep = 0;
    private var currentTableRef: TableReference? = null
    private var currentRelation: Relation? = null

    override fun visit(node: TableReferences) {
        super.visit(node)
    }
    override fun visit(node: TableRefFactor) {
        currentTableRef = node

        node.table.idText
    }
    override fun visit(node: OuterJoin) {
        stackDeep++;
        node.leftTableRef.accept(this)
        val leftTableRef = currentTableRef
        node.rightTableRef.accept(this)
        val rightTableRef = currentTableRef

    }

}
