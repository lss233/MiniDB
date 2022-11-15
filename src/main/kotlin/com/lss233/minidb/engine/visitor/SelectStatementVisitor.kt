package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.schema.Column
import hu.webarticum.treeprinter.SimpleTreeNode
import miniDB.parser.ast.expression.comparison.ComparisionEqualsExpression
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.expression.primary.literal.LiteralNumber
import miniDB.parser.ast.fragment.tableref.OuterJoin
import miniDB.parser.ast.fragment.tableref.TableRefFactor
import miniDB.parser.ast.fragment.tableref.TableReferences
import miniDB.parser.ast.stmt.dml.DMLSelectStatement
import miniDB.parser.visitor.Visitor
import java.lang.RuntimeException
import java.util.*
import java.util.function.Predicate


class SelectStatementVisitor: Visitor() {
    private var selectedRelation: HashMap<String, Relation> = HashMap();
    private val stack = Stack<Any>()
    var rootNode = SimpleTreeNode("DMLSelectStatement")
    var relation: Relation? = null

    override fun visit(node: DMLSelectStatement) {
        stack.clear()

        node.tables.accept(this)
        var result = stack.pop() as Relation
        if(node.where != null) {
            node.where.accept(this)
            val cond = stack.pop() as Predicate<NTuple>
            result = result.select(cond)
        }

        var condProjection : Predicate<Column>? = null
        for (expressionStringPair in node.selectExprList) {
            expressionStringPair.key.accept(this)
            val identifier = stack.pop() as String
            val condLocal = Predicate<Column> {col: Column ->
                col.identifier == expressionStringPair.key
            }
            condProjection = if(condProjection == null) {
                condLocal
            } else {
                condProjection.or(condLocal)
            }
        }
        result = if (condProjection != null) result.projection(condProjection) else result.projection { true }
        relation = result

    }
    override fun visit(node: TableReferences) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("* tables")

        for (tableReference in node.tableReferenceList) {
            tableReference.accept(this)
        }

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }
    override fun visit(node: OuterJoin) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("OuterJoin(leftJoin=${node.isLeftJoin})")

        node.leftTableRef.accept(this)
        val leftTable = stack.pop() as Relation
        selectedRelation[leftTable.alias] = leftTable
        node.rightTableRef.accept(this)
        val rightTable = stack.pop() as Relation
        selectedRelation[rightTable.alias] = rightTable

        stack.push(node)  // Pass outerJoin parameters.
        node.onCond.accept(this)
        val cond = stack.pop() as Predicate<NTuple>

        val result = leftTable.outerJoin(rightTable, node.isLeftJoin, cond)
        stack.push(result)

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }

    override fun visit(node: TableRefFactor) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("Table(${node.table.idText}, alias=${node.alias})")

        node.table.accept(this)
        stack.pop()
        val table = Engine[node.table] ?: throw RuntimeException("ERROR 20001: No such table")
        table.alias = if (node.alias != null) node.alias else node.table.idText
        stack.push(table)
        parentNode.addChild(rootNode)
        rootNode = parentNode
    }

    override fun visit(node: ComparisionEqualsExpression) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("Expression(operator='${node.operator}', leftCombine=${node.isLeftCombine})")

        // 为了保证访问一致性，都得 visit
        node.leftOprand.accept(this)
        val leftIdentifier = when(node.leftOprand) {
            is Identifier ->
                stack.pop() as String
            else ->
                node.leftOprand
        }

        node.rightOprand.accept(this)
        val rightIdentifier = when(node.rightOprand) {
            is Identifier ->
                stack.pop() as String
            else ->
                node.rightOprand
        }

        stack.push(Predicate<NTuple> { t: NTuple ->
            val leftValue = if(leftIdentifier is String) {
                t[node.leftOprand as Identifier]
            } else {
                leftIdentifier
            }
            val rightValue = if(rightIdentifier is String) {
                t[node.rightOprand as Identifier]
            } else {
                rightIdentifier
            }
            leftValue == rightValue
        })

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }
    override fun visit(node: Identifier) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("Identifier(${node.idText})")

        var identifier = node.idText
        if(node.parent != null) {
            node.parent.accept(this)
            identifier = (stack.pop() as String) + "." + identifier
        }
        stack.push(identifier)

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }

    override fun visit(node: LiteralNumber) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("LiteralNumber(${node.number})")
        parentNode.addChild(rootNode)
        rootNode = parentNode
    }




}