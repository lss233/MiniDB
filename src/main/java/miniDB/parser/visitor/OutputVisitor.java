package miniDB.parser.visitor;

import miniDB.parser.ast.ASTNode;
import miniDB.parser.ast.expression.*;
import miniDB.parser.ast.expression.comparison.*;
import miniDB.parser.ast.expression.logical.LogicalAndExpression;
import miniDB.parser.ast.expression.logical.LogicalOrExpression;
import miniDB.parser.ast.expression.logical.LogicalXORExpression;
import miniDB.parser.ast.expression.misc.InExpressionList;
import miniDB.parser.ast.expression.misc.QueryExpression;
import miniDB.parser.ast.expression.misc.UserExpression;
import miniDB.parser.ast.expression.primary.*;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.ast.expression.primary.function.cast.Cast;
import miniDB.parser.ast.expression.primary.function.cast.Convert;
import miniDB.parser.ast.expression.primary.function.datetime.Extract;
import miniDB.parser.ast.expression.primary.function.datetime.GetFormat;
import miniDB.parser.ast.expression.primary.function.datetime.Timestampadd;
import miniDB.parser.ast.expression.primary.function.datetime.Timestampdiff;
import miniDB.parser.ast.expression.primary.function.groupby.*;
import miniDB.parser.ast.expression.primary.function.string.Char;
import miniDB.parser.ast.expression.primary.function.string.Trim;
import miniDB.parser.ast.expression.primary.literal.*;
import miniDB.parser.ast.expression.string.LikeExpression;
import miniDB.parser.ast.expression.type.CollateExpression;
import miniDB.parser.ast.fragment.*;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition.ColumnFormat;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition.SpecialIndex;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition.Storage;
import miniDB.parser.ast.fragment.ddl.TableOptions;
import miniDB.parser.ast.fragment.ddl.TableOptions.Compression;
import miniDB.parser.ast.fragment.ddl.TableOptions.InsertMethod;
import miniDB.parser.ast.fragment.ddl.TableOptions.PackKeys;
import miniDB.parser.ast.fragment.ddl.TableOptions.RowFormat;
import miniDB.parser.ast.fragment.ddl.datatype.DataType;
import miniDB.parser.ast.fragment.ddl.index.IndexColumnName;
import miniDB.parser.ast.fragment.ddl.index.IndexDefinition;
import miniDB.parser.ast.fragment.ddl.index.IndexDefinition.KeyType;
import miniDB.parser.ast.fragment.ddl.index.IndexOption;
import miniDB.parser.ast.fragment.tableref.*;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.BeginEndStatement;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.ast.stmt.compound.DeclareStatement;
import miniDB.parser.ast.stmt.compound.condition.*;
import miniDB.parser.ast.stmt.compound.condition.Characteristics.Characteristic;
import miniDB.parser.ast.stmt.compound.condition.GetDiagnosticsStatement.StatementInfoItemName;
import miniDB.parser.ast.stmt.compound.condition.SignalStatement.ConditionInfoItemName;
import miniDB.parser.ast.stmt.compound.cursors.CursorCloseStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorDeclareStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorFetchStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorOpenStatement;
import miniDB.parser.ast.stmt.compound.flowcontrol.*;
import miniDB.parser.ast.stmt.dal.*;
import miniDB.parser.ast.stmt.ddl.*;
import miniDB.parser.ast.stmt.ddl.DDLAlterTableStatement.AlterSpecification;
import miniDB.parser.ast.stmt.ddl.DDLCreateProcedureStatement.ProcParameterType;
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement.ForeignKeyDefinition;
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement.ForeignKeyDefinition.REFERENCE_OPTION;
import miniDB.parser.ast.stmt.ddl.DDLCreateTriggerStatement.TriggerOrder;
import miniDB.parser.ast.stmt.dml.*;
import miniDB.parser.ast.stmt.extension.ExtDDLCreatePolicy;
import miniDB.parser.ast.stmt.extension.ExtDDLDropPolicy;
import miniDB.parser.ast.stmt.mts.MTSReleaseStatement;
import miniDB.parser.ast.stmt.mts.MTSRollbackStatement;
import miniDB.parser.ast.stmt.mts.MTSSavepointStatement;
import miniDB.parser.ast.stmt.mts.MTSSetTransactionStatement;
import miniDB.parser.util.Pair;
import miniDB.parser.util.Tuple3;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static miniDB.parser.ast.expression.comparison.ComparisionIsExpression.*;

public class OutputVisitor extends Visitor {
    protected static final Object[] EMPTY_OBJ_ARRAY = new Object[0];
    protected static final int[] EMPTY_INT_ARRAY = new int[0];
    protected final StringBuilder appendable;
    protected final Object[] args;
    protected int[] argsIndex;
    protected Map<PlaceHolder, Object> placeHolderToString;
    protected boolean inUpdateDelete = false;

    public OutputVisitor(StringBuilder appendable) {
        this(appendable, null);
    }

    /**
     * @param args parameters for {@link java.sql.PreparedStatement preparedStmt}
     */
    public OutputVisitor(StringBuilder appendable, Object[] args) {
        this.appendable = appendable;
        this.args = args == null ? EMPTY_OBJ_ARRAY : args;
        this.argsIndex = args == null ? EMPTY_INT_ARRAY : new int[args.length];
    }

    public void setPlaceHolderToString(Map<PlaceHolder, Object> map) {
        this.placeHolderToString = map;
    }

    public String getSql() {
        return appendable.toString();
    }

    /**
     * @return never null. rst[i] ≡ {@link #args}[{@link #argsIndex}[i]]
     */
    public Object[] getArguments() {
        final int argsIndexSize = argsIndex.length;
        if (argsIndexSize <= 0)
            return EMPTY_OBJ_ARRAY;

        boolean noChange = true;
        for (int i = 0; i < argsIndexSize; ++i) {
            if (i != argsIndex[i]) {
                noChange = false;
                break;
            }
        }
        if (noChange)
            return args;

        Object[] rst = new Object[argsIndexSize];
        for (int i = 0; i < argsIndexSize; ++i) {
            rst[i] = args[argsIndex[i]];
        }
        return rst;
    }

    /**
     * @param list never null
     */
    private void printList(List<? extends ASTNode> list) {
        printList(list, ", ");
    }

    /**
     * @param list never null
     */
    private void printList(List<? extends ASTNode> list, String sep) {
        boolean isFst = true;
        for (ASTNode arg : list) {
            if (isFst)
                isFst = false;
            else
                appendable.append(sep);
            arg.accept(this);
        }
    }

    @Override
    public void visit(BetweenAndExpression node) {
        Expression comparee = node.getFirst();
        boolean paren = comparee.getPrecedence() <= node.getPrecedence();
        if (paren)
            appendable.append('(');
        comparee.accept(this);
        if (paren)
            appendable.append(')');

        if (node.isNot())
            appendable.append(" NOT BETWEEN ");
        else
            appendable.append(" BETWEEN ");

        Expression start = node.getSecond();
        paren = start.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        start.accept(this);
        if (paren)
            appendable.append(')');

        appendable.append(" AND ");

        Expression end = node.getThird();
        paren = end.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        end.accept(this);
        if (paren)
            appendable.append(')');
    }

    @Override
    public void visit(ComparisionIsExpression node) {
        Expression comparee = node.getOperand();
        boolean paren = comparee.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        comparee.accept(this);
        if (paren)
            appendable.append(')');
        switch (node.getMode()) {
            case IS_NULL:
                appendable.append(" IS NULL");
                break;
            case IS_TRUE:
                appendable.append(" IS TRUE");
                break;
            case IS_FALSE:
                appendable.append(" IS FALSE");
                break;
            case IS_UNKNOWN:
                appendable.append(" IS UNKNOWN");
                break;
            case IS_NOT_NULL:
                appendable.append(" IS NOT NULL");
                break;
            case IS_NOT_TRUE:
                appendable.append(" IS NOT TRUE");
                break;
            case IS_NOT_FALSE:
                appendable.append(" IS NOT FALSE");
                break;
            case IS_NOT_UNKNOWN:
                appendable.append(" IS NOT UNKNOWN");
                break;
            default:
                throw new IllegalArgumentException(
                        "unknown mode for IS expression: " + node.getMode());
        }
    }

    @Override
    public void visit(InExpressionList node) {
        appendable.append('(');
        printList(node.getList());
        appendable.append(')');
    }

    @Override
    public void visit(LikeExpression node) {
        Expression comparee = node.getFirst();
        boolean paren = comparee.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        comparee.accept(this);
        if (paren)
            appendable.append(')');

        if (node.isNot())
            appendable.append(" NOT LIKE ");
        else
            appendable.append(" LIKE ");

        Expression pattern = node.getSecond();
        paren = pattern.getPrecedence() <= node.getPrecedence();
        if (paren)
            appendable.append('(');
        pattern.accept(this);
        if (paren)
            appendable.append(')');

        Expression escape = node.getThird();
        if (escape != null) {
            appendable.append(" ESCAPE ");
            paren = escape.getPrecedence() <= node.getPrecedence();
            if (paren)
                appendable.append('(');
            escape.accept(this);
            if (paren)
                appendable.append(')');
        }
    }

    @Override
    public void visit(CollateExpression node) {
        Expression string = node.getString();
        boolean paren = string.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        string.accept(this);
        if (paren)
            appendable.append(')');

        appendable.append(" COLLATE ").append(node.getCollateName());
    }

    @Override
    public void visit(UserExpression node) {
        appendable.append(node.getUserAtHost());
    }

    @Override
    public void visit(UnaryOperatorExpression node) {
        appendable.append(node.getOperator()).append(' ');
        boolean paren = node.getOperand().getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        node.getOperand().accept(this);
        if (paren)
            appendable.append(')');
    }

    @Override
    public void visit(BinaryOperatorExpression node) {
        Expression left = node.getLeftOprand();
        boolean paren = node.isLeftCombine() ? left.getPrecedence() < node.getPrecedence()
                : left.getPrecedence() <= node.getPrecedence();
        if (paren)
            appendable.append('(');
        left.accept(this);
        if (paren)
            appendable.append(')');

        appendable.append(' ').append(node.getOperator()).append(' ');

        Expression right = node.getRightOprand();
        paren = node.isLeftCombine() ? right.getPrecedence() <= node.getPrecedence()
                : right.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        right.accept(this);
        if (paren)
            appendable.append(')');
    }

    @Override
    public void visit(PolyadicOperatorExpression node) {
        for (int i = 0, len = node.getArity(); i < len; ++i) {
            if (i > 0)
                appendable.append(' ').append(node.getOperator()).append(' ');
            Expression operand = node.getOperand(i);
            boolean paren = operand.getPrecedence() < node.getPrecedence();
            if (paren)
                appendable.append('(');
            operand.accept(this);
            if (paren)
                appendable.append(')');
        }
    }

    @Override
    public void visit(LogicalAndExpression node) {
        visit((PolyadicOperatorExpression) node);
    }

    @Override
    public void visit(LogicalOrExpression node) {
        visit((PolyadicOperatorExpression) node);
    }

    @Override
    public void visit(LogicalXORExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(ComparisionEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(ComparisionNullSafeEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(InExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(FunctionExpression node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Char node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        printList(node.getArguments());
        String charset = node.getCharset();
        if (charset != null) {
            appendable.append(" USING ").append(charset);
        }
        appendable.append(')');
    }

    @Override
    public void visit(Convert node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        printList(node.getArguments());
        String transcodeName = node.getTranscodeName();
        appendable.append(" USING ").append(transcodeName);
        appendable.append(')');
    }

    @Override
    public void visit(Trim node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        Expression remStr = node.getRemainString();
        switch (node.getDirection()) {
            case DEFAULT:
                if (remStr != null) {
                    remStr.accept(this);
                    appendable.append(" FROM ");
                }
                break;
            case BOTH:
                appendable.append("BOTH ");
                if (remStr != null)
                    remStr.accept(this);
                appendable.append(" FROM ");
                break;
            case LEADING:
                appendable.append("LEADING ");
                if (remStr != null)
                    remStr.accept(this);
                appendable.append(" FROM ");
                break;
            case TRAILING:
                appendable.append("TRAILING ");
                if (remStr != null)
                    remStr.accept(this);
                appendable.append(" FROM ");
                break;
            default:
                throw new IllegalArgumentException(
                        "unknown trim direction: " + node.getDirection());
        }
        Expression str = node.getString();
        str.accept(this);
        appendable.append(')');
    }

    @Override
    public void visit(Cast node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        node.getExpr().accept(this);
        appendable.append(" AS ");
        String typeName = node.getTypeName();
        appendable.append(typeName);
        Expression info1 = node.getTypeInfo1();
        if (info1 != null) {
            appendable.append('(');
            info1.accept(this);
            Expression info2 = node.getTypeInfo2();
            if (info2 != null) {
                appendable.append(", ");
                info2.accept(this);
            }
            appendable.append(')');
        }
        appendable.append(')');
    }

    @Override
    public void visit(Avg node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Max node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Min node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Sum node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Count node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(GroupConcat node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        Expression orderBy = node.getOrderBy();
        if (orderBy != null) {
            appendable.append(" ORDER BY ");
            orderBy.accept(this);
            if (node.isDesc())
                appendable.append(" DESC");
            else
                appendable.append(" ASC");
            List<Expression> list = node.getAppendedColumnNames();
            if (list != null && !list.isEmpty()) {
                appendable.append(", ");
                printList(list);
            }
        }
        String sep = node.getSeparator();
        if (sep != null) {
            appendable.append(" SEPARATOR '").append(sep).append("'");
        }
        appendable.append(')');
    }

    @Override
    public void visit(Extract node) {
        appendable.append("EXTRACT(").append(node.getUnit().name()).append(" FROM ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Timestampdiff node) {
        appendable.append("TIMESTAMPDIFF(").append(node.getUnit().name()).append(", ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Timestampadd node) {
        appendable.append("TIMESTAMPADD(").append(node.getUnit().name()).append(", ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(GetFormat node) {
        appendable.append("GET_FORMAT(");
        GetFormat.FormatType type = node.getFormatType();
        appendable.append(type.name()).append(", ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(PlaceHolder node) {
        if (placeHolderToString == null) {
            appendable.append("${").append(node.getName()).append('}');
            return;
        }
        Object toStringer = placeHolderToString.get(node);
        if (toStringer == null) {
            appendable.append("${").append(node.getName()).append('}');
        } else {
            appendable.append(toStringer.toString());
        }
    }

    @Override
    public void visit(IntervalPrimary node) {
        appendable.append("INTERVAL ");
        Expression quantity = node.getQuantity();
        boolean paren = quantity.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        quantity.accept(this);
        if (paren)
            appendable.append(')');
        IntervalPrimary.Unit unit = node.getUnit();
        appendable.append(' ').append(unit.name());
    }

    @Override
    public void visit(LiteralBitField node) {
        String introducer = node.getIntroducer();
        if (introducer != null)
            appendable.append(introducer).append(' ');
        appendable.append("b'").append(node.getText()).append('\'');
    }

    @Override
    public void visit(LiteralBoolean node) {
        if (node.isTrue()) {
            appendable.append("TRUE");
        } else {
            appendable.append("FALSE");
        }
    }

    @Override
    public void visit(LiteralHexadecimal node) {
        String introducer = node.getIntroducer();
        if (introducer != null)
            appendable.append(introducer).append(' ');
        appendable.append("x'");
        node.appendTo(appendable);
        appendable.append('\'');
    }

    @Override
    public void visit(LiteralNull node) {
        appendable.append("NULL");
    }

    @Override
    public void visit(LiteralNumber node) {
        appendable.append(String.valueOf(node.getNumber()));
    }

    @Override
    public void visit(LiteralString node) {
        String introducer = node.getIntroducer();
        if (introducer != null) {
            appendable.append(introducer);
        } else if (node.isNchars()) {
            appendable.append('N');
        }
        appendable.append('\'').append(new String(node.getBytes())).append('\'');
    }

    @Override
    public void visit(CaseWhenOperatorExpression node) {
        appendable.append("CASE");
        Expression comparee = node.getComparee();
        if (comparee != null) {
            appendable.append(' ');
            comparee.accept(this);
        }
        List<Pair<Expression, Expression>> whenList = node.getWhenList();
        for (Pair<Expression, Expression> whenthen : whenList) {
            appendable.append(" WHEN ");
            Expression when = whenthen.getKey();
            when.accept(this);
            appendable.append(" THEN ");
            Expression then = whenthen.getValue();
            then.accept(this);
        }
        Expression elseRst = node.getElseResult();
        if (elseRst != null) {
            appendable.append(" ELSE ");
            elseRst.accept(this);
        }
        appendable.append(" END");
    }

    @Override
    public void visit(DefaultValue node) {
        appendable.append("DEFAULT");
    }

    @Override
    public void visit(ExistsPrimary node) {
        appendable.append("EXISTS (");
        node.getSubquery().accept(this);
        appendable.append(')');
    }

    @Override
    public void visit(Identifier node) {
        Expression parent = node.getParent();
        if (parent != null) {
            parent.accept(this);
            appendable.append('.');
        }
        appendable.append(node.getIdText());
    }

    private static boolean containsCompIn(Expression pat) {
        if (pat.getPrecedence() > Expression.PRECEDENCE_COMPARISION)
            return false;
        if (pat instanceof BinaryOperatorExpression) {
            if (pat instanceof InExpression) {
                return true;
            }
            BinaryOperatorExpression bp = (BinaryOperatorExpression) pat;
            if (bp.isLeftCombine()) {
                return containsCompIn(bp.getLeftOprand());
            } else {
                return containsCompIn(bp.getLeftOprand());
            }
        } else if (pat instanceof ComparisionIsExpression) {
            ComparisionIsExpression is = (ComparisionIsExpression) pat;
            return containsCompIn(is.getOperand());
        } else if (pat instanceof TernaryOperatorExpression) {
            TernaryOperatorExpression tp = (TernaryOperatorExpression) pat;
            return containsCompIn(tp.getFirst()) || containsCompIn(tp.getSecond())
                    || containsCompIn(tp.getThird());
        } else if (pat instanceof UnaryOperatorExpression) {
            UnaryOperatorExpression up = (UnaryOperatorExpression) pat;
            return containsCompIn(up.getOperand());
        } else {
            return false;
        }
    }

    @Override
    public void visit(MatchExpression node) {
        appendable.append("MATCH (");
        printList(node.getColumns());
        appendable.append(") AGAINST (");
        Expression pattern = node.getPattern();
        boolean inparen = containsCompIn(pattern);
        if (inparen)
            appendable.append('(');
        pattern.accept(this);
        if (inparen)
            appendable.append(')');
        switch (node.getModifier()) {
            case IN_BOOLEAN_MODE:
                appendable.append(" IN BOOLEAN MODE");
                break;
            case IN_NATURAL_LANGUAGE_MODE:
                appendable.append(" IN NATURAL LANGUAGE MODE");
                break;
            case IN_NATURAL_LANGUAGE_MODE_WITH_QUERY_EXPANSION:
                appendable.append(" IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION");
                break;
            case WITH_QUERY_EXPANSION:
                appendable.append(" WITH QUERY EXPANSION");
                break;
            case _DEFAULT:
                break;
            default:
                throw new IllegalArgumentException(
                        "unkown modifier for match expression: " + node.getModifier());
        }
        appendable.append(')');
    }

    private int index = -1;

    private void appendArgsIndex(int value) {
        int i = ++index;
        if (argsIndex.length <= i) {
            int[] a = new int[i + 1];
            if (i > 0)
                System.arraycopy(argsIndex, 0, a, 0, i);
            argsIndex = a;
        }
        argsIndex[i] = value;
    }

    @Override
    public void visit(ParamMarker node) {
        appendable.append('?');
        appendArgsIndex(node.getParamIndex() - 1);
    }

    @Override
    public void visit(RowExpression node) {
        appendable.append("ROW(");
        printList(node.getRowExprList());
        appendable.append(')');
    }

    @Override
    public void visit(SysVarPrimary node) {
        VariableScope scope = node.getScope();
        switch (scope) {
            case GLOBAL:
                appendable.append("@@global.");
                break;
            case SESSION:
                appendable.append("@@");
                break;
            default:
                throw new IllegalArgumentException("unkown scope for sysVar primary: " + scope);
        }
        appendable.append(node.getVarText());
    }

    @Override
    public void visit(UsrDefVarPrimary node) {
        appendable.append(node.getVarText());
    }

    @Override
    public void visit(IndexHint node) {
        IndexHint.IndexAction action = node.getAction();
        switch (action) {
            case FORCE:
                appendable.append("FORCE ");
                break;
            case IGNORE:
                appendable.append("IGNORE ");
                break;
            case USE:
                appendable.append("USE ");
                break;
            default:
                throw new IllegalArgumentException("unkown index action for index hint: " + action);
        }
        IndexHint.IndexType type = node.getType();
        switch (type) {
            case INDEX:
                appendable.append("INDEX ");
                break;
            case KEY:
                appendable.append("KEY ");
                break;
            default:
                throw new IllegalArgumentException("unkown index type for index hint: " + type);
        }
        IndexHint.IndexScope scope = node.getScope();
        switch (scope) {
            case GROUP_BY:
                appendable.append("FOR GROUP BY ");
                break;
            case ORDER_BY:
                appendable.append("FOR ORDER BY ");
                break;
            case JOIN:
                appendable.append("FOR JOIN ");
                break;
            case ALL:
                break;
            default:
                throw new IllegalArgumentException("unkown index scope for index hint: " + scope);
        }
        appendable.append('(');
        List<String> indexList = node.getIndexList();
        boolean isFst = true;
        for (String indexName : indexList) {
            if (isFst)
                isFst = false;
            else
                appendable.append(", ");
            appendable.append(indexName);
        }
        appendable.append(')');
    }

    @Override
    public void visit(TableReferences node) {
        printList(node.getTableReferenceList());
    }

    @Override
    public void visit(InnerJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        left.accept(this);
        if (paren)
            appendable.append(')');

        appendable.append(" INNER JOIN ");
        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren)
            appendable.append('(');
        right.accept(this);
        if (paren)
            appendable.append(')');

        Expression on = node.getOnCond();
        List<String> using = node.getUsing();
        if (on != null) {
            appendable.append(" ON ");
            on.accept(this);
        } else if (using != null) {
            appendable.append(" USING (");
            boolean isFst = true;
            for (String col : using) {
                if (isFst)
                    isFst = false;
                else
                    appendable.append(", ");
                appendable.append(col);
            }
            appendable.append(")");
        }
    }

    @Override
    public void visit(NaturalJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        left.accept(this);
        if (paren)
            appendable.append(')');

        appendable.append(" NATURAL ");
        if (node.isOuter()) {
            if (node.isLeft())
                appendable.append("LEFT ");
            else
                appendable.append("RIGHT ");
        }
        appendable.append("JOIN ");

        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren)
            appendable.append('(');
        right.accept(this);
        if (paren)
            appendable.append(')');
    }

    @Override
    public void visit(StraightJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        left.accept(this);
        if (paren)
            appendable.append(')');

        appendable.append(" STRAIGHT_JOIN ");

        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren)
            appendable.append('(');
        right.accept(this);
        if (paren)
            appendable.append(')');

        Expression on = node.getOnCond();
        if (on != null) {
            appendable.append(" ON ");
            on.accept(this);
        }
    }

    @Override
    public void visit(OuterJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren)
            appendable.append('(');
        left.accept(this);
        if (paren)
            appendable.append(')');

        if (node.isLeftJoin())
            appendable.append(" LEFT JOIN ");
        else
            appendable.append(" RIGHT JOIN ");

        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren)
            appendable.append('(');
        right.accept(this);
        if (paren)
            appendable.append(')');

        Expression on = node.getOnCond();
        List<String> using = node.getUsing();
        if (on != null) {
            appendable.append(" ON ");
            on.accept(this);
        } else if (using != null) {
            appendable.append(" USING (");
            boolean isFst = true;
            for (String col : using) {
                if (isFst)
                    isFst = false;
                else
                    appendable.append(", ");
                appendable.append(col);
            }
            appendable.append(")");
        } else {
            throw new IllegalArgumentException(
                    "either ON or USING must be included for OUTER JOIN");
        }
    }

    @Override
    public void visit(SubqueryFactor node) {
        appendable.append('(');
        QueryExpression query = node.getSubquery();
        query.accept(this);
        appendable.append(") AS ").append(node.getAlias());
    }

    @Override
    public void visit(TableRefFactor node) {
        Identifier table = node.getTable();
        table.accept(this);
        String alias = node.getAlias();
        if (alias != null) {
            appendable.append(" AS ").append(alias);
        }
        List<IndexHint> list = node.getHintList();
        if (list != null && !list.isEmpty()) {
            appendable.append(' ');
            printList(list, " ");
        }
    }

    @Override
    public void visit(Dual dual) {
        appendable.append("DUAL");
    }

    @SuppressWarnings("incomplete-switch")
    @Override
    public void visit(GroupBy node) {
        appendable.append("GROUP BY ");
        boolean isFst = true;
        for (Pair<Expression, SortOrder> p : node.getOrderByList()) {
            if (isFst)
                isFst = false;
            else
                appendable.append(", ");
            Expression col = p.getKey();
            col.accept(this);
            switch (p.getValue()) {
                case DESC:
                    appendable.append(" DESC");
                    break;
            }
        }
        if (node.isWithRollup()) {
            appendable.append(" WITH ROLLUP");
        }
    }

    @SuppressWarnings("incomplete-switch")
    @Override
    public void visit(OrderBy node) {
        appendable.append("ORDER BY ");
        boolean isFst = true;
        for (Pair<Expression, SortOrder> p : node.getOrderByList()) {
            if (isFst)
                isFst = false;
            else
                appendable.append(", ");
            Expression col = p.getKey();
            col.accept(this);
            switch (p.getValue()) {
                case DESC:
                    appendable.append(" DESC");
                    break;
            }
        }
    }

    @Override
    public void visit(Limit node) {
        appendable.append("LIMIT ");
        Object offset = node.getOffset();
        long limitOffset = 0;
        Object size = node.getSize();
        long limitSize = 0;
        if (offset instanceof ParamMarker) {
            ((ParamMarker) offset).accept(this);
        } else {
            limitOffset = Long.valueOf(String.valueOf(offset));
        }

        if (size instanceof ParamMarker) {
            ((ParamMarker) size).accept(this);
        } else {
            limitSize = Long.valueOf(String.valueOf(size));
        }
        if (inUpdateDelete) {
            appendable.append(limitSize + limitOffset);
        } else {
            appendable.append(limitOffset).append(" , ").append(limitSize);
        }
    }

    @Override
    public void visit(ColumnDefinition node) {
        visitChild(node.getDataType());
        if (node.isNotNull()) {
            appendable.append(" NOT NULL");
        }
        Expression defaultVal = node.getDefaultVal();
        if (defaultVal != null) {
            appendable.append(" DEFAULT ");
            if (defaultVal instanceof FunctionExpression) {
                appendable.append(((FunctionExpression) defaultVal).getFunctionName());
            } else {
                visitChild(defaultVal);
            }
        } else if (!node.isNotNull()) {
            appendable.append(" DEFAULT NULL");
        }
        if (node.isAutoIncrement()) {
            appendable.append(" AUTO_INCREMENT");
        }
        SpecialIndex specialIndex = node.getSpecialIndex();
        if (specialIndex != null) {
            appendable.append(specialIndex).append(" KEY ");
        }
        LiteralString comment = node.getComment();
        if (comment != null) {
            appendable.append(" COMMENT ");
            visitChild(comment);
        }
        ColumnFormat colFormat = node.getColumnFormat();
        if (colFormat != null) {
            appendable.append(" COLUMN_FORMAT ").append(colFormat);
        }
        Storage storage = node.getStorage();
        if (storage != null) {
            appendable.append(" STORAGE ").append(storage);
        }
        Expression onUpdate = node.getOnUpdate();
        if (onUpdate != null) {
            appendable.append(" ON UPDATE ");
            if (defaultVal instanceof FunctionExpression) {
                appendable.append(((FunctionExpression) defaultVal).getFunctionName());
            } else {
                visitChild(onUpdate);
            }
        }
    }

    @Override
    public void visit(IndexOption node) {
        if (node.getKeyBlockSize() != null) {
            appendable.append("KEY_BLOCK_SIZE = ");
            node.getKeyBlockSize().accept(this);
        } else if (node.getIndexType() != null) {
            appendable.append("USING ");
            switch (node.getIndexType()) {// USING {BTREE | HASH}
                case BTREE:
                    appendable.append("BTREE");
                    break;
                case HASH:
                    appendable.append("HASH");
                    break;
            }
        } else if (node.getParserName() != null) {
            appendable.append("WITH PARSER ");
            node.getParserName().accept(this);
        } else if (node.getComment() != null) {
            appendable.append("COMMENT ");
            node.getComment().accept(this);
        }
    }

    @Override
    public void visit(AlterSpecification node) {
        throw new UnsupportedOperationException("subclass have not implement visit");
    }

    @Override
    public void visit(DataType node) {
        if (node.getLength() != null) {
            appendable.append(String.valueOf(node.getTypeName()).toLowerCase()).append("(");
            node.getLength().accept(this);
            if (node.getDecimals() != null) {
                appendable.append(",");
                node.getDecimals().accept(this);
            }
            appendable.append(")");
        } else if (node.isBinary()) {
            appendable.append(String.valueOf(node.getTypeName()).toLowerCase()).append(" ");
            appendable.append(" binary ");
        } else if (node.getCollectionVals() != null && !node.getCollectionVals().isEmpty()) {
            appendable.append(String.valueOf(node.getTypeName()).toLowerCase()).append("(");
            Iterator<Expression> itor = node.getCollectionVals().iterator();
            while (itor.hasNext()) {
                itor.next().accept(this);
                if (itor.hasNext()) {
                    appendable.append(",");
                }
            }
            appendable.append(")");
        } else {
            appendable.append(String.valueOf(node.getTypeName()).toLowerCase()).append(" ");
        }
        if (node.isUnsigned()) {
            appendable.append(" unsigned");
        }
        if (node.isZerofill()) {
            appendable.append(" ZEROFILL");
        }
        if (node.getCharSet() != null) {
            appendable.append(" CHARACTER SET ");
            node.getCharSet().accept(this);
        }
        if (node.getCollation() != null) {
            appendable.append(" COLLATE ");
            node.getCollation().accept(this);
        }
    }

    private void printSimpleShowStmt(String attName) {
        appendable.append("SHOW ").append(attName);
    }

    @Override
    public void visit(ShowAuthors node) {
        printSimpleShowStmt("AUTHORS");
    }

    @Override
    public void visit(ShowBinaryLog node) {
        printSimpleShowStmt("BINARY LOGS");
    }

    @Override
    public void visit(ShowBinLogEvent node) {
        appendable.append("SHOW BINLOG EVENTS");
        String logName = node.getLogName();
        if (logName != null)
            appendable.append(" IN ").append(logName);
        Expression pos = node.getPos();
        if (pos != null) {
            appendable.append(" FROM ");
            pos.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    /**
     * ' ' will be prepended
     */
    private void printLikeOrWhere(String like, Expression where) {
        if (like != null) {
            appendable.append(" LIKE ").append(like);
        } else if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
    }

    @Override
    public void visit(ShowCharaterSet node) {
        appendable.append("SHOW CHARACTER SET");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowCollation node) {
        appendable.append("SHOW COLLATION");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowColumns node) {
        appendable.append("SHOW ");
        if (node.isFull())
            appendable.append("FULL ");
        appendable.append("COLUMNS FROM ");
        node.getTable().accept(this);
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowContributors node) {
        printSimpleShowStmt("CONTRIBUTORS");
    }

    @Override
    public void visit(ShowCreate node) {
        appendable.append("SHOW CREATE ").append(node.getType().name()).append(' ');
        node.getId().accept(this);
    }

    @Override
    public void visit(ShowDatabases node) {
        appendable.append("SHOW DATABASES");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowEngine node) {
        appendable.append("SHOW ENGINE ");
        switch (node.getType()) {
            case INNODB_MUTEX:
                appendable.append("INNODB MUTEX");
                break;
            case INNODB_STATUS:
                appendable.append("INNODB STATUS");
                break;
            case PERFORMANCE_SCHEMA_STATUS:
                appendable.append("PERFORMANCE SCHEMA STATUS");
                break;
            default:
                throw new IllegalArgumentException(
                        "unrecognized type for SHOW ENGINE: " + node.getType());
        }
    }

    @Override
    public void visit(ShowEngines node) {
        printSimpleShowStmt("ENGINES");
    }

    @Override
    public void visit(ShowErrors node) {
        appendable.append("SHOW ");
        if (node.isCount()) {
            appendable.append("COUNT(*) ERRORS");
        } else {
            appendable.append("ERRORS");
            Limit limit = node.getLimit();
            if (node.getLimit() != null) {
                appendable.append(' ');
                limit.accept(this);
            }
        }
    }

    @Override
    public void visit(ShowEvents node) {
        appendable.append("SHOW EVENTS");
        Identifier schema = node.getSchema();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowFunctionCode node) {
        appendable.append("SHOW FUNCTION CODE ");
        node.getFunctionName().accept(this);
    }

    @Override
    public void visit(ShowFunctionStatus node) {
        appendable.append("SHOW FUNCTION STATUS");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowGrants node) {
        appendable.append("SHOW GRANTS");
        Expression user = node.getUser();
        if (user != null) {
            appendable.append(" FOR ");
            user.accept(this);
        }
    }

    @Override
    public void visit(ShowIndex node) {
        appendable.append("SHOW ");
        switch (node.getType()) {
            case INDEX:
                appendable.append("INDEX ");
                break;
            case INDEXES:
                appendable.append("INDEXES ");
                break;
            case KEYS:
                appendable.append("KEYS ");
                break;
            default:
                throw new IllegalArgumentException(
                        "unrecognized type for SHOW INDEX: " + node.getType());
        }
        appendable.append("IN ");
        node.getTable().accept(this);
    }

    @Override
    public void visit(ShowMasterStatus node) {
        printSimpleShowStmt("MASTER STATUS");
    }

    @Override
    public void visit(ShowOpenTables node) {
        appendable.append("SHOW OPEN TABLES");
        Identifier db = node.getSchema();
        if (db != null) {
            appendable.append(" FROM ");
            db.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowPlugins node) {
        printSimpleShowStmt("PLUGINS");
    }

    @Override
    public void visit(ShowPrivileges node) {
        printSimpleShowStmt("PRIVILEGES");
    }

    @Override
    public void visit(ShowProcedureCode node) {
        appendable.append("SHOW PROCEDURE CODE ");
        node.getProcedureName().accept(this);
    }

    @Override
    public void visit(ShowProcedureStatus node) {
        appendable.append("SHOW PROCEDURE STATUS");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowProcesslist node) {
        appendable.append("SHOW ");
        if (node.isFull())
            appendable.append("FULL ");
        appendable.append("PROCESSLIST");
    }

    @Override
    public void visit(ShowProfile node) {
        appendable.append("SHOW PROFILE");
        List<ShowProfile.Type> types = node.getTypes();
        boolean isFst = true;
        for (ShowProfile.Type type : types) {
            if (isFst) {
                isFst = false;
                appendable.append(' ');
            } else {
                appendable.append(", ");
            }
            appendable.append(type.name().replace('_', ' '));
        }
        Expression query = node.getForQuery();
        if (query != null) {
            appendable.append(" FOR QUERY ");
            query.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    @Override
    public void visit(ShowProfiles node) {
        printSimpleShowStmt("PROFILES");
    }

    @Override
    public void visit(ShowSlaveHosts node) {
        printSimpleShowStmt("SLAVE HOSTS");
    }

    @Override
    public void visit(ShowSlaveStatus node) {
        printSimpleShowStmt("SLAVE STATUS");
    }

    @Override
    public void visit(ShowStatus node) {
        appendable.append("SHOW ").append(node.getScope().name().replace('_', ' '))
                .append(" STATUS");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowTables node) {
        appendable.append("SHOW");
        if (node.isFull())
            appendable.append(" FULL");
        appendable.append(" TABLES");
        Identifier schema = node.getSchema();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowTableStatus node) {
        appendable.append("SHOW TABLE STATUS");
        Identifier schema = node.getDatabase();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowTriggers node) {
        appendable.append("SHOW TRIGGERS");
        Identifier schema = node.getSchema();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowVariables node) {
        appendable.append("SHOW ").append(node.getScope().name().replace('_', ' '))
                .append(" VARIABLES");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowWarnings node) {
        appendable.append("SHOW ");
        if (node.isCount()) {
            appendable.append("COUNT(*) WARNINGS");
        } else {
            appendable.append("WARNINGS");
            Limit limit = node.getLimit();
            if (limit != null) {
                appendable.append(' ');
                limit.accept(this);
            }
        }
    }

    @Override
    public void visit(DescTableStatement node) {
        appendable.append("DESC ");
        node.getTable().accept(this);
    }

    @Override
    public void visit(DALSetStatement node) {
        appendable.append("SET ");
        boolean isFst = true;
        for (Pair<VariableExpression, Expression> p : node.getAssignmentList()) {
            if (isFst)
                isFst = false;
            else
                appendable.append(", ");
            p.getKey().accept(this);
            appendable.append(" = ");
            p.getValue().accept(this);
        }
    }

    @Override
    public void visit(DALSetNamesStatement node) {
        appendable.append("SET NAMES ");
        if (node.isDefault()) {
            appendable.append("DEFAULT");
        } else {
            appendable.append(node.getCharsetName());
            String collate = node.getCollationName();
            if (collate != null) {
                appendable.append(" COLLATE ");
                appendable.append(collate);
            }
        }
    }

    @Override
    public void visit(DALSetCharacterSetStatement node) {
        appendable.append("SET CHARACTER SET ");
        if (node.isDefault()) {
            appendable.append("DEFAULT");
        } else {
            appendable.append(node.getCharset());
        }
    }

    @Override
    public void visit(MTSSetTransactionStatement node) {
        appendable.append("SET ");
        VariableScope scope = node.getScope();
        if (scope != null) {
            switch (scope) {
                case SESSION:
                    appendable.append("SESSION ");
                    break;
                case GLOBAL:
                    appendable.append("GLOBAL ");
                    break;
                default:
                    throw new IllegalArgumentException(
                            "unknown scope for SET TRANSACTION ISOLATION LEVEL: " + scope);
            }
        }
        appendable.append("TRANSACTION ISOLATION LEVEL ");
        switch (node.getLevel()) {
            case READ_COMMITTED:
                appendable.append("READ COMMITTED");
                break;
            case READ_UNCOMMITTED:
                appendable.append("READ UNCOMMITTED");
                break;
            case REPEATABLE_READ:
                appendable.append("REPEATABLE READ");
                break;
            case SERIALIZABLE:
                appendable.append("SERIALIZABLE");
                break;
            default:
                throw new IllegalArgumentException(
                        "unknown level for SET TRANSACTION ISOLATION LEVEL: " + node.getLevel());
        }
    }

    @Override
    public void visit(MTSSavepointStatement node) {
        appendable.append("SAVEPOINT ");
        node.getSavepoint().accept(this);
    }

    @Override
    public void visit(MTSReleaseStatement node) {
        appendable.append("RELEASE SAVEPOINT ");
        node.getSavepoint().accept(this);
    }

    @Override
    public void visit(MTSRollbackStatement node) {
        appendable.append("ROLLBACK");
        Identifier savepoint = node.getSavepoint();
        if (savepoint == null) {
            MTSRollbackStatement.CompleteType type = node.getCompleteType();
            switch (type) {
                case CHAIN:
                    appendable.append(" AND CHAIN");
                    break;
                case NO_CHAIN:
                    appendable.append(" AND NO CHAIN");
                    break;
                case NO_RELEASE:
                    appendable.append(" NO RELEASE");
                    break;
                case RELEASE:
                    appendable.append(" RELEASE");
                    break;
                case UN_DEF:
                    break;
                default:
                    throw new IllegalArgumentException("unrecgnized complete type: " + type);
            }
        } else {
            appendable.append(" TO SAVEPOINT ");
            savepoint.accept(this);
        }
    }

    @Override
    public void visit(DMLCallStatement node) {
        appendable.append("CALL ");
        node.getProcedure().accept(this);
        appendable.append('(');
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(DMLDeleteStatement node) {
        appendable.append("DELETE ");
        if (node.isLowPriority())
            appendable.append("LOW_PRIORITY ");
        if (node.isQuick())
            appendable.append("QUICK ");
        if (node.isIgnore())
            appendable.append("IGNORE ");
        TableReferences tableRefs = node.getTableRefs();
        if (tableRefs == null) {
            appendable.append("FROM ");
            node.getTableNames().get(0).accept(this);
        } else {
            printList(node.getTableNames());
            appendable.append(" FROM ");
            node.getTableRefs().accept(this);
        }
        Expression where = node.getWhereCondition();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
        OrderBy orderBy = node.getOrderBy();
        if (orderBy != null) {
            appendable.append(' ');
            orderBy.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            inUpdateDelete = true;
            appendable.append(' ');
            limit.accept(this);
            inUpdateDelete = false;
        }
    }

    @Override
    public void visit(DMLInsertStatement node) {
        appendable.append("INSERT ");
        switch (node.getMode()) {
            case DELAY:
                appendable.append("DELAYED ");
                break;
            case HIGH:
                appendable.append("HIGH_PRIORITY ");
                break;
            case LOW:
                appendable.append("LOW_PRIORITY ");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown mode for INSERT: " + node.getMode());
        }
        if (node.isIgnore())
            appendable.append("IGNORE ");
        appendable.append("INTO ");
        node.getTable().accept(this);
        appendable.append(' ');

        List<Identifier> cols = node.getColumnNameList();
        if (cols != null && !cols.isEmpty()) {
            appendable.append('(');
            printList(cols);
            appendable.append(") ");
        }

        QueryExpression select = node.getSelect();
        if (select == null) {
            appendable.append("VALUES ");
            List<RowExpression> rows = node.getRowList();
            if (rows != null && !rows.isEmpty()) {
                boolean isFst = true;
                for (RowExpression row : rows) {
                    if (row == null || row.getRowExprList().isEmpty())
                        continue;
                    if (isFst)
                        isFst = false;
                    else
                        appendable.append(", ");
                    appendable.append('(');
                    printList(row.getRowExprList());
                    appendable.append(')');
                }
            } else {
                throw new IllegalArgumentException("at least one row for INSERT");
            }
        } else {
            select.accept(this);
        }

        List<Pair<Identifier, Expression>> dup = node.getDuplicateUpdate();
        if (dup != null && !dup.isEmpty()) {
            appendable.append(" ON DUPLICATE KEY UPDATE ");
            boolean isFst = true;
            for (Pair<Identifier, Expression> p : dup) {
                if (isFst)
                    isFst = false;
                else
                    appendable.append(", ");
                p.getKey().accept(this);
                appendable.append(" = ");
                p.getValue().accept(this);
            }
        }
    }

    @Override
    public void visit(DMLReplaceStatement node) {
        appendable.append("REPLACE ");
        switch (node.getMode()) {
            case DELAY:
                appendable.append("DELAYED ");
                break;
            case LOW:
                appendable.append("LOW_PRIORITY ");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown mode for INSERT: " + node.getMode());
        }
        appendable.append("INTO ");
        node.getTable().accept(this);
        appendable.append(' ');

        List<Identifier> cols = node.getColumnNameList();
        if (cols != null && !cols.isEmpty()) {
            appendable.append('(');
            printList(cols);
            appendable.append(") ");
        }

        QueryExpression select = node.getSelect();
        if (select == null) {
            appendable.append("VALUES ");
            List<RowExpression> rows = node.getRowList();
            if (rows != null && !rows.isEmpty()) {
                boolean isFst = true;
                for (RowExpression row : rows) {
                    if (row == null || row.getRowExprList().isEmpty())
                        continue;
                    if (isFst)
                        isFst = false;
                    else
                        appendable.append(", ");
                    appendable.append('(');
                    printList(row.getRowExprList());
                    appendable.append(')');
                }
            } else {
                throw new IllegalArgumentException("at least one row for REPLACE");
            }
        } else {
            select.accept(this);
        }
    }

    @Override
    public void visit(DMLSelectStatement node) {
        appendable.append("SELECT ");
        final DMLSelectStatement.SelectOption option = node.getOption();
        switch (option.resultDup) {
            case ALL:
                break;
            case DISTINCT:
                appendable.append("DISTINCT ");
                break;
            case DISTINCTROW:
                appendable.append("DISTINCTROW ");
                break;
            default:
                throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
        if (option.highPriority) {
            appendable.append("HIGH_PRIORITY ");
        }
        if (option.straightJoin) {
            appendable.append("STRAIGHT_JOIN ");
        }
        switch (option.resultSize) {
            case SQL_BIG_RESULT:
                appendable.append("SQL_BIG_RESULT ");
                break;
            case SQL_SMALL_RESULT:
                appendable.append("SQL_SMALL_RESULT ");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
        if (option.sqlBufferResult) {
            appendable.append("SQL_BUFFER_RESULT ");
        }
        switch (option.queryCache) {
            case SQL_CACHE:
                appendable.append("SQL_CACHE ");
                break;
            case SQL_NO_CACHE:
                appendable.append("SQL_NO_CACHE ");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
        if (option.sqlCalcFoundRows) {
            appendable.append("SQL_CALC_FOUND_ROWS ");
        }

        boolean isFst = true;
        List<Pair<Expression, String>> exprList = node.getSelectExprList();
        for (Pair<Expression, String> p : exprList) {
            if (isFst)
                isFst = false;
            else
                appendable.append(", ");
            p.getKey().accept(this);
            String alias = p.getValue();
            if (alias != null) {
                appendable.append(" AS ").append(alias);
            }
        }

        TableReferences from = node.getTables();
        if (from != null) {
            appendable.append(" FROM ");
            from.accept(this);
        }

        Expression where = node.getWhere();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }

        GroupBy group = node.getGroup();
        if (group != null) {
            appendable.append(' ');
            group.accept(this);
        }

        Expression having = node.getHaving();
        if (having != null) {
            appendable.append(" HAVING ");
            having.accept(this);
        }

        OrderBy order = node.getOrder();
        if (order != null) {
            appendable.append(' ');
            order.accept(this);
        }

        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }

        switch (option.lockMode) {
            case FOR_UPDATE:
                appendable.append(" FOR UPDATE");
                break;
            case LOCK_IN_SHARE_MODE:
                appendable.append(" LOCK IN SHARE MODE");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
    }

    @Override
    public void visit(DMLSelectUnionStatement node) {
        List<DMLSelectStatement> list = node.getSelectStmtList();
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("SELECT UNION must have at least one SELECT");
        }
        final int fstDist = node.getFirstDistinctIndex();
        int i = 0;
        for (DMLSelectStatement select : list) {
            if (i > 0) {
                appendable.append(" UNION ");
                if (i > fstDist) {
                    appendable.append("ALL ");
                }
            }
            appendable.append('(');
            select.accept(this);
            appendable.append(')');
            ++i;
        }
        OrderBy order = node.getOrderBy();
        if (order != null) {
            appendable.append(' ');
            order.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    @Override
    public void visit(DMLUpdateStatement node) {
        appendable.append("UPDATE ");
        if (node.isLowPriority()) {
            appendable.append("LOW_PRIORITY ");
        }
        if (node.isIgnore()) {
            appendable.append("IGNORE ");
        }
        node.getTableRefs().accept(this);
        appendable.append(" SET ");
        boolean isFst = true;
        for (Pair<Identifier, Expression> p : node.getValues()) {
            if (isFst)
                isFst = false;
            else
                appendable.append(", ");
            p.getKey().accept(this);
            appendable.append(" = ");
            p.getValue().accept(this);
        }
        Expression where = node.getWhere();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
        OrderBy order = node.getOrderBy();
        if (order != null) {
            appendable.append(' ');
            order.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            inUpdateDelete = true;
            appendable.append(' ');
            limit.accept(this);
            inUpdateDelete = false;
        }
    }

    @Override
    public void visit(DDLTruncateStatement node) {
        appendable.append("TRUNCATE TABLE ");
        node.getTable().accept(this);
    }

    @Override
    public void visit(DDLAlterTableStatement node) {
        throw new UnsupportedOperationException("ALTER TABLE is partially parsed");
    }

    @Override
    public void visit(DDLCreateIndexStatement node) {
        throw new UnsupportedOperationException("CREATE INDEX is partially parsed");
    }

    @Override
    public void visit(DDLRenameTableStatement node) {
        appendable.append("RENAME TABLE ");
        boolean isFst = true;
        for (Pair<Identifier, Identifier> p : node.getList()) {
            if (isFst)
                isFst = false;
            else
                appendable.append(", ");
            p.getKey().accept(this);
            appendable.append(" TO ");
            p.getValue().accept(this);
        }
    }

    @Override
    public void visit(DDLDropIndexStatement node) {
        appendable.append("DROP INDEX ");
        node.getIndexName().accept(this);
        appendable.append(" ON ");
        node.getTable().accept(this);
    }

    @Override
    public void visit(DDLDropTableStatement node) {
        appendable.append("DROP ");
        if (node.isTemp()) {
            appendable.append("TEMPORARY ");
        }
        appendable.append("TABLE ");
        if (node.isIfExists()) {
            appendable.append("IF EXISTS ");
        }
        printList(node.getTableNames());
        switch (node.getMode()) {
            case CASCADE:
                appendable.append(" CASCADE");
                break;
            case RESTRICT:
                appendable.append(" RESTRICT");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException(
                        "unsupported mode for DROP TABLE: " + node.getMode());
        }
    }

    @Override
    public void visit(ExtDDLCreatePolicy node) {
        appendable.append("CREATE POLICY ");
        node.getName().accept(this);
        appendable.append(" (");
        boolean first = true;
        for (Pair<Integer, Expression> p : node.getProportion()) {
            if (first)
                first = false;
            else
                appendable.append(", ");
            appendable.append(p.getKey()).append(' ');
            p.getValue().accept(this);
        }
        appendable.append(')');
    }

    @Override
    public void visit(ExtDDLDropPolicy node) {
        appendable.append("DROP POLICY ");
        node.getPolicyName().accept(this);
    }

    @Override
    public void visit(ComparisionGreaterThanExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(ComparisionGreaterThanOrEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(ComparisionLessThanExpression node) {
        visit((BinaryOperatorExpression) node);

    }

    @Override
    public void visit(ComparisionLessThanOrEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(ShowCreateDatabase node) {
        appendable.append("SHOW CREATE ").append(node.getType().name()).append(' ')
                .append(node.isIfNotExists() ? "IF NOT EXISTS " : "");
        node.getDbName().accept(this);
    }

    @Override
    public void visit(ExplainStatement node) {
        switch (node.getCommand()) {
            case EXPLAIN:
                appendable.append("EXPLAIN ");
                break;
            case DESCRIBE:
                appendable.append("DESCRIBE ");
                break;
            case DESC:
                appendable.append("DESC ");
                break;
            default:
                throw new IllegalArgumentException(
                        "unknown EXPLAIN Syntax for : " + node.getCommand());
        }
        if (node.getTblName() != null) {
            visit(node.getTblName());
            // node.getTblName().accept(this);
            if (node.getColName() != null) {
                appendable.append(" ");
                visit(node.getColName());
                // node.getTblName().accept(this);
            } else if (node.getWild() != null) {
                appendable.append(" ");
                appendable.append(node.getWild());
            }
        } else {
            if (node.getExplainType() != null) {
                switch (node.getExplainType()) {
                    case EXTENDED:
                        appendable.append("EXTENDED");
                        break;
                    case PARTITIONS:
                        appendable.append("PARTITIONS");
                    case FORMAT:
                        if (node.getFormatName() != null) {
                            appendable.append("FORMAT").append("=");
                            switch (node.getFormatName()) {
                                case TRADITIONAL:
                                    appendable.append("TRADITIONAL");
                                    break;
                                case JSON:
                                    appendable.append("JSON");
                                    break;
                                default:
                                    break;
                            }
                        }
                    default:
                        break;
                }
                appendable.append(" ");
            }
            if (node.getConnectionId() != null) {
                appendable.append("FOR CONNECTION ").append(node.getConnectionId().getNumber());
            } else if (node.getExplainableStmt() != null) {
                node.getExplainableStmt().accept(this);
            }
        }
    }

    @Override
    public void visit(DDLCreateTriggerStatement node) {
        appendable.append("CREATE");
        Expression definer = node.getDefiner();
        if (definer != null) {
            appendable.append(" DEFINER = ");
            visitChild(definer);
        }
        appendable.append(" TRIGGER ");
        visitChild(node.getTriggerName());
        appendable.append(" ").append(node.getTriggerTime()).append(" ")
                .append(node.getTriggerEvent()).append(" ON ");
        visitChild(node.getTable());
        appendable.append(" FOR EACH ROW ");
        TriggerOrder order = node.getTriggerOrder();
        if (order != null) {
            appendable.append(order).append(" ");
            visitChild(node.getOtherTriggerName());
            appendable.append(" ");
        }
        appendable.append("\n");
        SQLStatement stmt = node.getStmt();
        if (stmt != null && stmt instanceof CompoundStatement) {
            visitChild(stmt);
        } else {
            visitChild(stmt);
            appendable.append(";\n");
        }
    }

    @Override
    public void visit(BeginEndStatement node) {
        Identifier label = node.getLabel();
        if (label != null) {
            visitChild(label);
            appendable.append(":");
        }
        appendable.append("BEGIN\n");
        List<SQLStatement> list = node.getStatements();
        for (SQLStatement li : list) {
            visitChild(li);
            if (!(li instanceof CompoundStatement)) {
                appendable.append(";\n");
            }
        }
        appendable.append("END");
        if (label != null) {
            appendable.append(" ");
            visitChild(label);
        }
        appendable.append(";\n");
    }

    @Override
    public void visit(IfStatement node) {
        List<Pair<Expression, List<SQLStatement>>> list = node.getIfStatements();
        for (int i = 0, size = list.size(); i < size; i++) {
            Pair<Expression, List<SQLStatement>> li = list.get(i);
            if (i == 0) {
                appendable.append("IF ");
            } else {
                appendable.append("ELSE IF ");
            }
            visitChild(li.getKey());
            appendable.append(" THEN ");
            Iterator<SQLStatement> iter = li.getValue().iterator();
            while (iter.hasNext()) {
                SQLStatement stmt = iter.next();
                if (stmt != null) {
                    visitChild(stmt);
                    if (!(stmt instanceof CompoundStatement)) {
                        appendable.append(";\n");
                    }
                }
            }
        }
        Iterator<SQLStatement> iter = node.getElseStatement().iterator();
        if (iter.hasNext()) {
            appendable.append("ELSE ");
        }
        while (iter.hasNext()) {
            SQLStatement stmt = iter.next();
            if (stmt != null) {
                visitChild(stmt);
                if (!(stmt instanceof CompoundStatement)) {
                    appendable.append(";\n");
                }
            }
        }
        appendable.append("END IF;\n");
    }

    @Override
    public void visit(NewRowPrimary node) {
        appendable.append("NEW.").append(node.getVarText());
    }

    @Override
    public void visit(OldRowPrimary node) {
        appendable.append("OLD.").append(node.getVarText());
    }

    @Override
    public void visit(LoopStatement node) {
        Identifier label = node.getLabel();
        if (label != null) {
            visitChild(label);
            appendable.append(":");
        }
        appendable.append("LOOP\n");
        SQLStatement stmt = node.getStmt();
        visitChild(stmt);
        if (stmt != null && !(stmt instanceof CompoundStatement)) {
            appendable.append(";\n");
        }
        appendable.append("END LOOP");
        if (label != null) {
            appendable.append(" ");
            visitChild(label);
        }
        appendable.append(";\n");
    }

    @Override
    public void visit(IterateStatement node) {
        appendable.append("IETRATE ");
        visitChild(node.getLabel());
        appendable.append(";\n");
    }

    @Override
    public void visit(LeaveStatement node) {
        appendable.append("LEAVE ");
        visitChild(node.getLabel());
        appendable.append(";\n");
    }

    @Override
    public void visit(ReturnStatement node) {
        appendable.append("RETURN ");
        visitChild(node.getLabel());
        appendable.append(";\n");
    }

    @Override
    public void visit(RepeatStatement node) {
        Identifier label = node.getLabel();
        if (label != null) {
            visitChild(label);
            appendable.append(":");
        }
        appendable.append("REPEAT\n");
        SQLStatement stmt = node.getStmt();
        visitChild(stmt);
        if (stmt != null && !(stmt instanceof CompoundStatement)) {
            appendable.append(";\n");
        }
        appendable.append("UNTIL ");
        visitChild(node.getUtilCondition());
        appendable.append(" END REPEAT");
        if (label != null) {
            appendable.append(" ");
            visitChild(label);
        }
        appendable.append(";\n");
    }

    @Override
    public void visit(WhileStatement node) {
        Identifier label = node.getLabel();
        if (label != null) {
            visitChild(label);
            appendable.append(":");
        }
        appendable.append("WHILE ");
        visitChild(node.getWhileCondition());
        appendable.append(" DO\n");
        SQLStatement stmt = node.getStmt();
        visitChild(stmt);
        if (stmt != null && !(stmt instanceof CompoundStatement)) {
            appendable.append(";\n");
        }
        appendable.append("END WHILE");
        if (label != null) {
            appendable.append(" ");
            visitChild(label);
        }
        appendable.append(";\n");
    }

    @Override
    public void visit(CaseStatement node) {
        Expression caseValue = node.getCaseValue();
        if (caseValue != null) {
            appendable.append("CASE ");
            visitChild(caseValue);
            appendable.append("\n");
        } else {
            appendable.append("CASE\n");
        }
        List<Pair<Expression, SQLStatement>> list = node.getWhenList();
        for (Pair<Expression, SQLStatement> li : list) {
            appendable.append("WHEN ");
            visitChild(li.getKey());
            appendable.append(" THEN ");
            SQLStatement stmt = li.getValue();
            if (stmt != null) {
                visitChild(stmt);
                if (!(stmt instanceof CompoundStatement)) {
                    appendable.append(";\n");
                }
            }
        }
        SQLStatement elseStmt = node.getElseStmt();
        if (elseStmt != null) {
            appendable.append("ELSE ");
            visitChild(elseStmt);
            if (!(elseStmt instanceof CompoundStatement)) {
                appendable.append(";\n");
            }
        }
        appendable.append("END CASE;\n");
    }

    @Override
    public void visit(DeclareStatement node) {
        appendable.append("DECLARE ");
        Iterator<Identifier> iter = node.getVarNames().iterator();
        while (iter.hasNext()) {
            visitChild(iter.next());
            if (iter.hasNext()) {
                appendable.append(",");
            }
        }
        appendable.append(" ");
        visitChild(node.getDataType());
        Expression defaultVal = node.getDefaultVal();
        if (defaultVal != null) {
            appendable.append("DEFAULT ");
            visitChild(defaultVal);
        }
        appendable.append(";\n");
    }

    public void visit(DeclareHandlerStatement node) {
        appendable.append("DECLARE ").append(node.getAction()).append(" HANDLER\nFOR ");
        Iterator<ConditionValue> iter = node.getConditionValues().iterator();
        while (iter.hasNext()) {
            visit(iter.next());
            if (iter.hasNext()) {
                appendable.append(" , ");
            }
        }
        appendable.append("\n");
        SQLStatement stmt = node.getStmt();
        if (stmt != null) {
            visitChild(stmt);
            if (!(stmt instanceof CompoundStatement)) {
                appendable.append(";\n");
            }
        }
    }

    public void visit(ConditionValue value) {
        switch (value.getType()) {
            case ErrorCode:
                visitChild(value.getValue());
                break;
            case Exception:
                appendable.append("SQLEXCEPTION");
                break;
            case Name:
                visitChild(value.getValue());
                break;
            case NotFound:
                appendable.append("NOT FOUND");
                break;
            case State:
                appendable.append("SQLSTATE VALUE ");
                visitChild(value.getValue());
                break;
            case Unknown:
                break;
            case Warning:
                appendable.append("SQLWARNING");
                break;
            default:
                break;
        }
    }

    public void visit(DeclareConditionStatement node) {
        appendable.append("DECLARE ");
        visitChild(node.getName());
        appendable.append(" CONDITION FOR ");
        ConditionValue value = node.getValue();
        visit(value);
        appendable.append(";\n");
    }

    public void visit(CursorDeclareStatement node) {
        appendable.append("DECLARE ");
        visitChild(node.getName());
        appendable.append(" CURSOR FOR ");
        SQLStatement stmt = node.getStmt();
        if (stmt != null) {
            visitChild(stmt);
            if (!(stmt instanceof CompoundStatement)) {
                appendable.append(";\n");
            }
        }
    }

    public void visit(CursorCloseStatement node) {
        appendable.append("CLOSE ");
        visitChild(node.getName());
        appendable.append(";\n");
    }

    public void visit(CursorOpenStatement node) {
        appendable.append("OPEN ");
        visitChild(node.getName());
        appendable.append(";\n");
    }

    public void visit(CursorFetchStatement node) {
        appendable.append("FETCH NEXT FROM ");
        visitChild(node.getName());
        appendable.append("INTO ");
        Iterator<Identifier> iter = node.getVarNames().iterator();
        while (iter.hasNext()) {
            visitChild(iter.next());
            if (iter.hasNext()) {
                appendable.append(",");
            }
        }
        appendable.append(";\n");
    }

    public void visit(SignalStatement node) {
        appendable.append("SIGNAL ");
        ConditionValue value = node.getConditionValue();
        switch (value.getType()) {
            case State:
                appendable.append("SQLSTATE VALUE ");
            default:
                appendable.append("\n");
                visitChild(value.getValue());
                break;
        }
        List<Pair<ConditionInfoItemName, Literal>> list = node.getInformationItems();
        if (list != null && !list.isEmpty()) {
            appendable.append("SET ");
            Iterator<Pair<ConditionInfoItemName, Literal>> iter = list.iterator();
            while (iter.hasNext()) {
                Pair<ConditionInfoItemName, Literal> p = iter.next();
                appendable.append(p.getKey().name()).append("=");
                visitChild(p.getValue());
                if (iter.hasNext()) {
                    appendable.append(",");
                } else {
                    appendable.append(";\n");
                }
            }
        }
    }

    public void visit(ResignalStatement node) {
        appendable.append("RESIGNAL ");
        ConditionValue value = node.getConditionValue();
        switch (value.getType()) {
            case State:
                appendable.append("SQLSTATE VALUE ");
            default:
                appendable.append("\n");
                visitChild(value.getValue());
                break;
        }
        List<Pair<ConditionInfoItemName, Literal>> list = node.getInformationItems();
        if (list != null && !list.isEmpty()) {
            appendable.append("SET ");
            Iterator<Pair<ConditionInfoItemName, Literal>> iter = list.iterator();
            while (iter.hasNext()) {
                Pair<ConditionInfoItemName, Literal> p = iter.next();
                appendable.append(p.getKey().name()).append("=");
                visitChild(p.getValue());
                if (iter.hasNext()) {
                    appendable.append(",");
                } else {
                    appendable.append(";\n");
                }
            }
        }
    }

    public void visit(GetDiagnosticsStatement node) {
        appendable.append("GET ");
        switch (node.getType()) {
            case CURRENT:
                appendable.append("CURRENT DIAGNOSTICS ");
                break;
            case NONE:
                appendable.append("DIAGNOSTICS ");
                break;
            case STACKED:
                appendable.append("STACKED DIAGNOSTICS ");
                break;
            default:
                break;

        }
        Expression conditionNumber = node.getConditionNumber();
        if (conditionNumber != null) {
            appendable.append("CONDITION ");
            visitChild(conditionNumber);
            appendable.append("\n");
            Iterator<Pair<Expression, ConditionInfoItemName>> iter =
                    node.getConditionItems().iterator();
            while (iter.hasNext()) {
                Pair<Expression, ConditionInfoItemName> p = iter.next();
                visitChild(p.getKey());
                appendable.append("=").append(p.getValue().name());
                if (iter.hasNext()) {
                    appendable.append(",");
                } else {
                    appendable.append(";\n");
                }
            }
        } else {
            Iterator<Pair<Expression, StatementInfoItemName>> iter =
                    node.getStatementItems().iterator();
            while (iter.hasNext()) {
                Pair<Expression, StatementInfoItemName> p = iter.next();
                visitChild(p.getKey());
                appendable.append("=").append(p.getValue().name());
                if (iter.hasNext()) {
                    appendable.append(",");
                } else {
                    appendable.append(";\n");
                }
            }
        }
    }

    @Override
    public void visit(DDLCreateProcedureStatement node) {
        appendable.append("CREATE");
        Expression definer = node.getDefiner();
        if (definer != null) {
            appendable.append(" DEFINER = ");
            visitChild(definer);
        }
        appendable.append(" PROCEDURE ");
        visitChild(node.getName());
        appendable.append("(");
        Iterator<Tuple3<ProcParameterType, Identifier, DataType>> iter =
                node.getParameters().iterator();
        while (iter.hasNext()) {
            Tuple3<ProcParameterType, Identifier, DataType> tuple = iter.next();
            switch (tuple._1()) {
                case IN:
                    appendable.append("IN ");
                    break;
                case INOUT:
                    appendable.append("INOUT ");
                    break;
                case NONE:
                    break;
                case OUT:
                    appendable.append("OUT ");
                    break;
                default:
                    break;
            }
            visitChild(tuple._2());
            appendable.append(" ");
            visitChild(tuple._3());
            if (iter.hasNext()) {
                appendable.append(",");
            }
        }
        appendable.append(")\n");
        Characteristics charact = node.getCharacteristics();
        if (!charact.isEmpty()) {
            Expression comment = charact.getComment();
            if (comment != null) {
                appendable.append("COMMENT ");
                visitChild(comment);
                appendable.append(" ");
            }
            Characteristic tmp = charact.getLanguageSql();
            if (tmp != null) {
                appendable.append("LANGUAGE SQL ");
            }
            tmp = charact.getDeterministic();
            if (tmp != null) {
                if (tmp == Characteristic.NOT_DETERMINISTIC) {
                    appendable.append("NOT ");
                }
                appendable.append("DETERMINISTIC ");
            }
            tmp = charact.getSqlCharacteristic();
            if (tmp != null) {
                switch (tmp) {
                    case CONTAINS_SQL:
                        appendable.append("CONTAINS SQL ");
                        break;
                    case NO_SQL:
                        appendable.append("NO SQL ");
                        break;
                    case READS_SQL_DATA:
                        appendable.append("READS SQL DATA ");
                        break;
                    case MODIFIES_SQL_DATA:
                        appendable.append("MODIFIES SQL DATA ");
                        break;
                    default:
                        break;
                }
            }
            tmp = charact.getSqlSecurity();
            if (tmp != null) {
                if (tmp == Characteristic.SQL_SECURITY_DEFINER) {
                    appendable.append("SQL SECURITY DEFINER ");
                } else {
                    appendable.append("SQL SECURITY INVOKER ");
                }
            }
            appendable.append("\n");
        }
        SQLStatement stmt = node.getStmt();
        visitChild(stmt);
        if (stmt != null && !(stmt instanceof CompoundStatement)) {
            appendable.append(";\n");
        }
    }

    @Override
    public void visit(DDLCreateFunctionStatement node) {
        appendable.append("CREATE");
        Expression definer = node.getDefiner();
        if (definer != null) {
            appendable.append(" DEFINER = ");
            visitChild(definer);
        }
        appendable.append(" FUNCTION ");
        visitChild(node.getName());
        appendable.append("(");
        Iterator<Pair<Identifier, DataType>> iter = node.getParameters().iterator();
        while (iter.hasNext()) {
            Pair<Identifier, DataType> p = iter.next();
            visitChild(p.getKey());
            appendable.append(" ");
            visitChild(p.getValue());
            if (iter.hasNext()) {
                appendable.append(",");
            }
        }
        appendable.append(")\n");
        appendable.append("RETURNS ");
        visitChild(node.getReturns());
        appendable.append("\n");
        Characteristics charact = node.getCharacteristics();
        if (!charact.isEmpty()) {
            Expression comment = charact.getComment();
            if (comment != null) {
                appendable.append("COMMENT ");
                visitChild(comment);
                appendable.append(" ");
            }
            Characteristic tmp = charact.getLanguageSql();
            if (tmp != null) {
                appendable.append("LANGUAGE SQL ");
            }
            tmp = charact.getDeterministic();
            if (tmp != null) {
                if (tmp == Characteristic.NOT_DETERMINISTIC) {
                    appendable.append("NOT ");
                }
                appendable.append("DETERMINISTIC ");
            }
            tmp = charact.getSqlCharacteristic();
            if (tmp != null) {
                switch (tmp) {
                    case CONTAINS_SQL:
                        appendable.append("CONTAINS SQL ");
                        break;
                    case NO_SQL:
                        appendable.append("NO SQL ");
                        break;
                    case READS_SQL_DATA:
                        appendable.append("READS SQL DATA ");
                        break;
                    case MODIFIES_SQL_DATA:
                        appendable.append("MODIFIES SQL DATA ");
                        break;
                    default:
                        break;
                }
            }
            tmp = charact.getSqlSecurity();
            if (tmp != null) {
                if (tmp == Characteristic.SQL_SECURITY_DEFINER) {
                    appendable.append("SQL SECURITY DEFINER ");
                } else {
                    appendable.append("SQL SECURITY INVOKER ");
                }
            }
            appendable.append("\n");
        }
        SQLStatement stmt = node.getStmt();
        visitChild(stmt);
        if (stmt != null && !(stmt instanceof CompoundStatement)) {
            appendable.append(";\n");
        }
    }

    @Override
    public void visit(DDLCreateLikeStatement node) {
        appendable.append("CREATE ");
        if (node.isTemporary()) {
            appendable.append("TEMPORARY ");
        }
        appendable.append("TABLE ");
        if (node.isIfNotExists()) {
            appendable.append("IF NOT EXISTS ");
        }
        visitChild(node.getTable());
        appendable.append(" LIKE");
        visitChild(node.getLikeTable());
    }

    @Override
    public void visit(DDLCreateTableStatement node) {
        appendable.append("CREATE ");
        if (node.isTemporary()) {
            appendable.append("TEMPORARY ");
        }
        appendable.append("TABLE ");
        if (node.isIfNotExists()) {
            appendable.append("IF NOT EXISTS ");
        }
        visitChild(node.getTable());
        appendable.append("(\n  ");
        List<Pair<Identifier, ColumnDefinition>> columns = node.getColDefs();
        for (int i = 0, size = columns.size(); i < size; i++) {
            if (i != 0) {
                appendable.append(",\n  ");
            }
            Pair<Identifier, ColumnDefinition> column = columns.get(i);
            visitChild(column.getKey());
            appendable.append(" ");
            visitChild(column.getValue());
        }
        visit(node.getPrimaryKey(), KeyType.PRIMARY);
        for (Pair<Identifier, IndexDefinition> key : node.getUniqueKeys()) {
            visit(key.getValue(), KeyType.UNIQUE);
        }
        for (Pair<Identifier, IndexDefinition> key : node.getKeys()) {
            visit(key.getValue(), KeyType.KEY);
        }
        for (Pair<Identifier, IndexDefinition> key : node.getFullTextKeys()) {
            visit(key.getValue(), KeyType.FULLTEXT);
        }
        for (Pair<Identifier, IndexDefinition> key : node.getSpatialKeys()) {
            visit(key.getValue(), KeyType.SPATIAL);
        }
        List<ForeignKeyDefinition> foreignKeyDefs = node.getForeignKeyDefs();
        for (int i = 0, size = foreignKeyDefs.size(); i < size; i++) {
            appendable.append(",\n");
            visitChild(foreignKeyDefs.get(i));
        }
        appendable.append("\n)");
        visitChild(node.getTableOptions());
    }


    @Override
    public void visit(TableOptions node) {
        Expression exp = node.getAutoIncrement();
        if (exp != null) {
            appendable.append(" AUTO_INCREMENT=");
            visitChild(exp);
        }
        exp = node.getAvgRowLength();
        if (exp != null) {
            appendable.append(" AVG_ROW_LENGTH=");
            visitChild(exp);
        }
        Identifier id = node.getEngine();
        if (id != null) {
            appendable.append(" ENGINE=");
            visitChild(id);
        }
        id = node.getCharSet();
        if (id != null) {
            appendable.append(" DEFAULT CHARSET=");
            visitChild(id);
        }
        Boolean checkSum = node.getCheckSum();
        if (checkSum != null) {
            appendable.append(" CHECKSUM=").append(checkSum.booleanValue() ? 1 : 0);
        }
        id = node.getCollation();
        if (id != null) {
            appendable.append(" DEFAULT COLLATE=");
            visitChild(id);
        }
        LiteralString string = node.getComment();
        if (string != null) {
            appendable.append(" COMMENT=");
            visitChild(string);
        }
        Compression compression = node.getCompression();
        if (compression != null) {
            appendable.append(" COMPRESSION='").append(compression).append("'");
        }
        string = node.getConnection();
        if (string != null) {
            appendable.append(" CONNECTION=");
            visitChild(string);
        }
        string = node.getDataDir();
        if (string != null) {
            appendable.append(" DATE DIRECTORY=");
            visitChild(string);
        }
        string = node.getIndexDir();
        if (string != null) {
            appendable.append(" INDEX DIRECTORY=");
            visitChild(string);
        }
        Boolean delayKeyWrite = node.getDelayKeyWrite();
        if (delayKeyWrite != null) {
            appendable.append(" DELAY_KEY_WRITE=").append(delayKeyWrite.booleanValue() ? 1 : 0);
        }
        Boolean encryption = node.getEncryption();
        if (encryption != null) {
            appendable.append(" ENCRYPTION=").append(delayKeyWrite.booleanValue() ? "'Y'" : "'N'");
        }
        InsertMethod insertMethod = node.getInsertMethod();
        if (insertMethod != null) {
            appendable.append(" INSERT_METHOD=").append(insertMethod);
        }
        exp = node.getKeyBlockSize();
        if (exp != null) {
            appendable.append(" KEY_BLOCK_SIZE=");
            visitChild(exp);
        }
        exp = node.getMaxRows();
        if (exp != null) {
            appendable.append(" MAX_ROWS=");
            visitChild(exp);
        }
        exp = node.getMinRows();
        if (exp != null) {
            appendable.append(" MIN_ROWS=");
            visitChild(exp);
        }
        PackKeys packKeys = node.getPackKeys();
        if (packKeys != null) {
            appendable.append(" PACK_KEYS=");
            switch (packKeys) {
                case DEFAULT:
                    appendable.append("DEFAULT");
                    break;
                case FALSE:
                    appendable.append(0);
                    break;
                case TRUE:
                    appendable.append(1);
                    break;
                default:
                    break;
            }
        }
        string = node.getPassword();
        if (string != null) {
            appendable.append(" PASSWORD=");
            visitChild(string);
        }
        RowFormat rowFormat = node.getRowFormat();
        if (rowFormat != null) {
            appendable.append(" ROW_FORMAT=").append(rowFormat);
        }
        /**
         * TODO 
         * | STATS_AUTO_RECALC [=] {DEFAULT|0|1}
         * | STATS_PERSISTENT [=] {DEFAULT|0|1}
         * | STATS_SAMPLE_PAGES [=] value
         * | TABLESPACE tablespace_name [STORAGE {DISK|MEMORY|DEFAULT}]
         */
        List<Identifier> union = node.getUnion();
        if (union != null && !union.isEmpty()) {
            appendable.append(" UNION=(");
            for (int i = 0, size = union.size(); i < size; i++) {
                if (i != 0) {
                    appendable.append(",");
                }
                visitChild(union.get(i));
            }
            appendable.append(")");
        }
    }

    public void visit(IndexDefinition key, KeyType type) {
        if (key == null) {
            return;
        }
        appendable.append(",\n");
        switch (type) {
            case SPATIAL: {
                appendable.append("  SPATIAL KEY ");
                visitChild(key.getIndexName());
                break;
            }
            case FULLTEXT: {
                appendable.append("  FULLTEXT KEY ");
                visitChild(key.getIndexName());
                break;
            }
            case KEY: {
                appendable.append("  KEY ");
                visitChild(key.getIndexName());
                visitChild(key.getIndexType());
                break;
            }
            case PRIMARY: {
                Identifier symbol = key.getSymbol();
                if (symbol != null) {
                    appendable.append("  CONSTRAINT ");
                    visitChild(symbol);
                    appendable.append("PRIMARY KEY ");
                } else {
                    appendable.append("  PRIMARY KEY ");
                }
                visitChild(key.getIndexType());
                break;
            }
            case UNIQUE: {
                Identifier symbol = key.getSymbol();
                if (symbol != null) {
                    appendable.append("  CONSTRAINT ");
                    visitChild(symbol);
                    appendable.append("UNIQUE KEY ");
                } else {
                    appendable.append("  UNIQUE KEY ");
                }
                visitChild(key.getIndexName());
                visitChild(key.getIndexType());
                break;
            }
            default:
                break;
        }
        appendable.append("(");
        List<IndexColumnName> cols = key.getColumns();
        for (int i = 0, size = cols.size(); i < size; i++) {
            if (i != 0) {
                appendable.append(",");
            }
            visitChild(cols.get(i));
        }
        appendable.append(")");
        visitChild(key.getOptions());
    }

    @Override
    public void visit(ForeignKeyDefinition node) {
        Identifier symbol = node.getSymbol();
        if (symbol != null) {
            appendable.append("  CONSTRAINT ");
            visitChild(symbol);
            appendable.append(" FOREIGN KEY ");
        } else {
            appendable.append("  FOREIGN KEY ");
        }
        visitChild(node.getIndexName());
        appendable.append("(");
        List<IndexColumnName> cols = node.getColumns();
        for (int i = 0, size = cols.size(); i < size; i++) {
            if (i != 0) {
                appendable.append(",");
            }
            visitChild(cols.get(i));
        }
        appendable.append(") REFERENCES ");
        visitChild(node.getReferenceTable());
        appendable.append("(");
        cols = node.getReferenceColumns();
        for (int i = 0, size = cols.size(); i < size; i++) {
            if (i != 0) {
                appendable.append(",");
            }
            visitChild(cols.get(i));
        }
        appendable.append(")");
        REFERENCE_OPTION onDelete = node.getOnDelete();
        if (onDelete != null) {
            appendable.append(" ON DELETE ");
            visit(onDelete);
        }
        REFERENCE_OPTION onUpdate = node.getOnUpdate();
        if (onUpdate != null) {
            appendable.append(" ON UPDATE ");
            visit(onDelete);
        }
    }

    private void visit(REFERENCE_OPTION option) {
        switch (option) {
            case CASCADE:
                appendable.append("CASCADE");
                break;
            case NO_ACTION:
                appendable.append("NO ACTION");
                break;
            case RESTRICT:
                appendable.append("RESTRICT");
                break;
            case SET_NULL:
                appendable.append("SET NULL");
                break;
            default:
                break;
        }
    }

    @Override
    public void visit(IndexColumnName node) {
        visitChild(node.getColumnName());
        Expression length = node.getLength();
        if (length != null) {
            appendable.append("(");
            visitChild(length);
            appendable.append(")");
        }
        if (!node.isAsc()) {
            appendable.append(" DESC");
        }
    }
}
