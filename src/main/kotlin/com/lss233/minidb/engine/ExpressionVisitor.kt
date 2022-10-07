package com.lss233.minidb.engine

import miniDB.parser.ast.expression.comparison.ComparisionEqualsExpression
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.visitor.Visitor
import java.util.function.BiPredicate

class ExpressionVisitor: Visitor() {
//    val map: HashMap<String, String>();
    override fun visit(node: ComparisionEqualsExpression) {
        super.visit(node)
    }
    override fun visit(node: Identifier) {
        super.visit(node)

    }
//    fun build(): BiPredicate<Relation, NTuple> {
//
//    }
}