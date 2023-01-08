package com.lss233.minidb.engine.visitor

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.schema.Column
import hu.webarticum.treeprinter.SimpleTreeNode
import miniDB.parser.ast.expression.comparison.ComparisionEqualsExpression
import miniDB.parser.ast.expression.comparison.ComparisionGreaterThanExpression
import miniDB.parser.ast.expression.comparison.ComparisionIsExpression
import miniDB.parser.ast.expression.logical.LogicalOrExpression
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.expression.primary.literal.LiteralNumber
import miniDB.parser.ast.expression.primary.literal.LiteralString
import miniDB.parser.ast.fragment.tableref.InnerJoin
import miniDB.parser.ast.fragment.tableref.OuterJoin
import miniDB.parser.ast.fragment.tableref.TableRefFactor
import miniDB.parser.ast.fragment.tableref.TableReferences
import miniDB.parser.ast.stmt.dml.DMLSelectStatement
import miniDB.parser.ast.stmt.dml.DMLSelectUnionStatement
import miniDB.parser.visitor.Visitor
import java.util.*
import java.util.function.Predicate
import kotlin.RuntimeException


open class SelectStatementVisitor: Visitor() {
    protected var selectedRelation: HashMap<String, Relation> = HashMap();
    protected val stack = Stack<Any>()
    var rootNode = SimpleTreeNode("DMLSelectStatement")
    var relation: Relation? = null
    val constantRelation = Relation(mutableListOf(Column("version")), mutableListOf(arrayOf("1.0.0-MINIDB")))
    override fun visit(node: DMLSelectUnionStatement) {
        var result: Relation? = null
        for (dmlSelectStatement in node.selectStmtList) {
            dmlSelectStatement.accept(this)
            val relation = stack.pop() as Relation
            if(result == null) {
                result = relation
            } else {
                result = result.union(relation)
            }
        }
        stack.push(result)
        relation = result
    }
    override fun visit(node: DMLSelectStatement) {
        var result = constantRelation.clone()
        node.tables?.let { tables -> run {
            node.tables.accept(this)
            result = stack.pop() as Relation
        } }
        node.where?.let { where -> run {
            where.accept(this)
            val cond = stack.pop() as Predicate<NTuple>
            result = result.select(cond)
        } }

        val parentNode = rootNode
        rootNode = SimpleTreeNode("* select")

        var condProjection : Predicate<Column>? = null
        for (expressionStringPair in node.selectExprList) {
            expressionStringPair.key.accept(this)
//            val identifier = stack.pop() as String
            val condLocal = Predicate<Column> {col: Column ->
                col.identifier == expressionStringPair.key
            }
            condProjection = condProjection?.or(condLocal) ?: condLocal
        }
        parentNode.addChild(rootNode)
        rootNode = parentNode

        result = if (condProjection != null) result.projection(condProjection) else result.projection { true }
        result = Relation(result.columns.map { column ->
            Column(node.selectExprList.firstOrNull { column.identifier == it.key }?.value ?: column.name)
        }.toMutableList(),
        result.rows)
        stack.push(result)
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
    override fun visit(node: InnerJoin) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("InnerJoin")

        node.leftTableRef.accept(this)
        val leftTable = stack.pop() as Relation
        leftTable.alias?.let { run { selectedRelation[it] = leftTable }}

        node.rightTableRef.accept(this)
        val rightTable = stack.pop() as Relation
        rightTable.alias?.let { run { selectedRelation[it] = rightTable }}

//        stack.push(node)  // Pass innerJoin parameters.
        node.onCond.accept(this)
        val cond = stack.pop() as Predicate<NTuple>

        val result = leftTable.innerJoin(rightTable, cond)
        stack.push(result)

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }

    /**
     * 访问 OuterJoin 结点时进行的操作
     * 外连接
     */
    override fun visit(node: OuterJoin) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("OuterJoin(leftJoin=${node.isLeftJoin})")

        // 访问左表
        node.leftTableRef.accept(this)
        // 得到左表对象
        val leftTable = stack.pop() as Relation
        // 保存别名
        leftTable.alias?.let { run { selectedRelation[it] = leftTable }}

        // 访问右表
        node.rightTableRef.accept(this)
        // 得到右表对象
        val rightTable = stack.pop() as Relation
        // 保存别名
        rightTable.alias?.let { run { selectedRelation[it] = rightTable }}

        // 访问条件
        node.onCond.accept(this)
        // 得到条件 Predicate
        val cond = stack.pop() as Predicate<NTuple>
        // 对左表进行外连接操作
        val result = leftTable.outerJoin(rightTable, node.isLeftJoin, cond)
        node.alias?.let {
            run {
            selectedRelation[it.idText] = result
        } }
        // 将结果压入栈中，交由上一级结点处理
        stack.push(result)

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }

    override fun visit(node: TableRefFactor) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("Table(${node.table.idText}, alias=${node.alias})")

        node.table.accept(this)
        stack.pop()
        val table = Engine[node.table].getRelation(node.alias ?: node.table.idText) ?: throw RuntimeException("ERROR 20001: No such table ${node.table}")
//        table.alias = node.alias ?: node.table.idText
        table.alias?.let { run { selectedRelation[it] = table }}
        stack.push(table)
        parentNode.addChild(rootNode)
        rootNode = parentNode
    }

    override fun visit(node: LogicalOrExpression) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("Expression(operator='${node.operator}')")

        var cond: Predicate<NTuple>? = null
        for (i in 0 until node.arity) {
            node.getOperand(i).accept(this)
            val oprand = stack.pop() as Predicate<NTuple>
            cond = if(cond == null) {
                oprand
            } else {
                cond.or(oprand)
            }
        }
        stack.push(cond)
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
    override fun visit(node: ComparisionIsExpression) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("Expression(operator='IS', mode=${node.mode})")
        node.operand.accept(this)
        val operand = when(node.operand) {
            is Identifier ->
                stack.pop() as String
            else ->
                node.operand
        }

        stack.push(Predicate<NTuple> { t: NTuple ->
            val operandVal = if(operand is String) {
                (t[node.operand as Identifier] as Cell<*>).value
            } else {
                operand
            }
            operandVal != null
        })

        parentNode.addChild(rootNode)
        rootNode = parentNode
    }
    override fun visit(node: ComparisionGreaterThanExpression) {
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
                (t[node.leftOprand as Identifier] as Cell<*>).value
            } else {
                leftIdentifier
            }
            val rightValue = if(rightIdentifier is String) {
                (t[node.rightOprand as Identifier] as Cell<*>).value
            } else {
                rightIdentifier
            }

            if(leftValue is LiteralNumber && rightValue is LiteralNumber) {
                leftValue > rightValue
            } else if(leftValue is LiteralNumber && rightValue is Cell<*>) {
                leftValue > rightValue
            } else if(leftValue is Cell<*> && rightValue is LiteralNumber) {
                leftValue > rightValue
            } else if(leftValue is Cell<*> && rightValue is Cell<*>) {
                leftValue > rightValue
            } else {
                throw RuntimeException("Expression cannot be compared.")
            }
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

    override fun visit(node: LiteralString) {
        val parentNode = rootNode
        rootNode = SimpleTreeNode("LiteralString('${node.unescapedString}')")
        parentNode.addChild(rootNode)
        rootNode = parentNode
    }




}