package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.memory.Engine
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
        currentRelation = Engine.databases["db"]?.tables?.get(node.table.idText)
    }
    override fun visit(node: OuterJoin) {
        stackDeep++;
        node.leftTableRef.accept(this)
        val leftTable = currentRelation
        node.rightTableRef.accept(this)
        val rightTable = currentRelation
        if(node.isLeftJoin) {
            if (rightTable != null) {
                leftTable?.join(rightTable)
            }
        }


    }

}
