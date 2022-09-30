package miniDB.parser.visitor;

import miniDB.parser.ast.ASTNode;
import miniDB.parser.ast.expression.BinaryOperatorExpression;
import miniDB.parser.ast.expression.PolyadicOperatorExpression;
import miniDB.parser.ast.expression.UnaryOperatorExpression;
import miniDB.parser.ast.expression.comparison.*;
import miniDB.parser.ast.expression.logical.LogicalAndExpression;
import miniDB.parser.ast.expression.logical.LogicalOrExpression;
import miniDB.parser.ast.expression.logical.LogicalXORExpression;
import miniDB.parser.ast.expression.misc.InExpressionList;
import miniDB.parser.ast.expression.misc.UserExpression;
import miniDB.parser.ast.expression.primary.*;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.ast.expression.primary.function.cast.Cast;
import miniDB.parser.ast.expression.primary.function.cast.Convert;
import miniDB.parser.ast.expression.primary.function.datetime.Extract;
import miniDB.parser.ast.expression.primary.function.datetime.GetFormat;
import miniDB.parser.ast.expression.primary.function.datetime.Timestampadd;
import miniDB.parser.ast.expression.primary.function.datetime.Timestampdiff;
import miniDB.parser.ast.expression.primary.function.flowctrl.Ifnull;
import miniDB.parser.ast.expression.primary.function.groupby.*;
import miniDB.parser.ast.expression.primary.function.info.LastInsertId;
import miniDB.parser.ast.expression.primary.function.string.Char;
import miniDB.parser.ast.expression.primary.function.string.Trim;
import miniDB.parser.ast.expression.primary.literal.*;
import miniDB.parser.ast.expression.string.LikeExpression;
import miniDB.parser.ast.expression.type.CollateExpression;
import miniDB.parser.ast.fragment.GroupBy;
import miniDB.parser.ast.fragment.Limit;
import miniDB.parser.ast.fragment.OrderBy;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition;
import miniDB.parser.ast.fragment.ddl.TableOptions;
import miniDB.parser.ast.fragment.ddl.datatype.DataType;
import miniDB.parser.ast.fragment.ddl.index.IndexColumnName;
import miniDB.parser.ast.fragment.ddl.index.IndexDefinition;
import miniDB.parser.ast.fragment.ddl.index.IndexOption;
import miniDB.parser.ast.fragment.tableref.*;
import miniDB.parser.ast.stmt.compound.BeginEndStatement;
import miniDB.parser.ast.stmt.compound.DeclareStatement;
import miniDB.parser.ast.stmt.compound.condition.*;
import miniDB.parser.ast.stmt.compound.cursors.CursorCloseStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorDeclareStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorFetchStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorOpenStatement;
import miniDB.parser.ast.stmt.compound.flowcontrol.*;
import miniDB.parser.ast.stmt.dal.*;
import miniDB.parser.ast.stmt.ddl.*;
import miniDB.parser.ast.stmt.ddl.DDLAlterTableStatement.AlterSpecification;
import miniDB.parser.ast.stmt.ddl.DDLAlterTableStatement.WithValidation;
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement.ForeignKeyDefinition;
import miniDB.parser.ast.stmt.dml.*;
import miniDB.parser.ast.stmt.extension.ExtDDLCreatePolicy;
import miniDB.parser.ast.stmt.extension.ExtDDLDropPolicy;
import miniDB.parser.ast.stmt.mts.*;
import miniDB.parser.util.Pair;

import java.util.Collection;

public abstract class Visitor {

    protected int stackDeep = 0;

    @SuppressWarnings({"rawtypes"})
    protected void visitChild(Object obj) {
        if (obj == null)
            return;
        stackDeep++;
        if (obj instanceof ASTNode) {
            ((ASTNode) obj).accept(this);
        } else if (obj instanceof Collection) {
            for (Object o : (Collection) obj) {
                visitChild(o);
            }
        } else if (obj instanceof Pair) {
            visitChild(((Pair) obj).getKey());
            visitChild(((Pair) obj).getValue());
        }
        stackDeep--;
    }

    public void visit(BetweenAndExpression node) {
        visitChild(node.getFirst());
        visitChild(node.getSecond());
        visitChild(node.getThird());
    }

    public void visit(ComparisionIsExpression node) {
        visitChild(node.getOperand());
    }

    public void visit(InExpressionList node) {
        visitChild(node.getList());
    }

    public void visit(LikeExpression node) {
        visitChild(node.getFirst());
        visitChild(node.getSecond());
        visitChild(node.getThird());
    }

    public void visit(CollateExpression node) {
        visitChild(node.getString());
    }

    public void visit(LogicalXORExpression node) {
        visitChild(node.getLeftOprand());
        visitChild(node.getRightOprand());
    }

    public void visit(UserExpression node) {}

    public void visit(UnaryOperatorExpression node) {
        visitChild(node.getOperand());
    }

    public void visit(BinaryOperatorExpression node) {
        visitChild(node.getLeftOprand());
        visitChild(node.getRightOprand());
    }

    public void visit(PolyadicOperatorExpression node) {
        for (int i = 0, len = node.getArity(); i < len; ++i) {
            visitChild(node.getOperand(i));
        }
    }

    public void visit(LogicalAndExpression node) {
        visit((PolyadicOperatorExpression) node);
    }

    public void visit(LogicalOrExpression node) {
        visit((PolyadicOperatorExpression) node);
    }

    public void visit(ComparisionEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    public void visit(ComparisionNotEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    public void visit(ComparisionLessOrGreaterThanExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    public void visit(ComparisionNullSafeEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    public void visit(ComparisionGreaterThanExpression node) {
        visitChild(node.getLeftOprand());
        visitChild(node.getRightOprand());
    }

    public void visit(ComparisionGreaterThanOrEqualsExpression node) {
        visitChild(node.getLeftOprand());
        visitChild(node.getRightOprand());
    }

    public void visit(ComparisionLessThanExpression node) {
        visitChild(node.getLeftOprand());
        visitChild(node.getRightOprand());
    }

    public void visit(ComparisionLessThanOrEqualsExpression node) {
        visitChild(node.getLeftOprand());
        visitChild(node.getRightOprand());
    }

    public void visit(InExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    public void visit(FunctionExpression node) {
        visitChild(node.getArguments());
    }

    public void visit(LastInsertId node) {
        visitChild(node.getArguments());
    }

    public void visit(Char node) {
        visit((FunctionExpression) node);
    }

    public void visit(Convert node) {
        visit((FunctionExpression) node);
    }

    public void visit(Trim node) {
        visit((FunctionExpression) node);
        visitChild(node.getRemainString());
        visitChild(node.getString());
    }

    public void visit(Cast node) {
        visit((FunctionExpression) node);
        visitChild(node.getExpr());
        visitChild(node.getTypeInfo1());
        visitChild(node.getTypeInfo2());
    }

    public void visit(Avg node) {
        visit((FunctionExpression) node);
    }

    public void visit(Max node) {
        visit((FunctionExpression) node);
    }

    public void visit(Min node) {
        visit((FunctionExpression) node);
    }

    public void visit(Sum node) {
        visit((FunctionExpression) node);
    }

    public void visit(Count node) {
        visit((FunctionExpression) node);
    }

    public void visit(GroupConcat node) {
        visit((FunctionExpression) node);
    }

    public void visit(Timestampdiff node) {
        visit((FunctionExpression) node);
    }

    public void visit(Timestampadd node) {
        visit((FunctionExpression) node);
    }

    public void visit(Extract node) {
        visit((FunctionExpression) node);
    }

    public void visit(GetFormat node) {
        visit((FunctionExpression) node);
    }

    public void visit(Ifnull node) {
        visit((FunctionExpression) node);
    }

    public void visit(IntervalPrimary node) {
        visitChild(node.getQuantity());
    }

    public void visit(LiteralBitField node) {}

    public void visit(LiteralBoolean node) {}

    public void visit(LiteralHexadecimal node) {}

    public void visit(LiteralNull node) {}

    public void visit(LiteralNumber node) {}

    public void visit(LiteralString node) {}

    public void visit(CaseWhenOperatorExpression node) {
        visitChild(node.getComparee());
        visitChild(node.getElseResult());
        visitChild(node.getWhenList());
    }

    public void visit(DefaultValue node) {}

    public void visit(ExistsPrimary node) {
        visitChild(node.getSubquery());
    }

    public void visit(PlaceHolder node) {}

    public void visit(Identifier node) {}

    public void visit(MatchExpression node) {
        visitChild(node.getColumns());
        visitChild(node.getPattern());
    }

    public void visit(ParamMarker node) {}

    public void visit(RowExpression node) {
        visitChild(node.getRowExprList());
    }

    public void visit(SysVarPrimary node) {}

    public void visit(UsrDefVarPrimary node) {}

    public void visit(IndexHint node) {}

    public void visit(InnerJoin node) {
        visitChild(node.getLeftTableRef());
        visitChild(node.getOnCond());
        visitChild(node.getRightTableRef());
    }

    public void visit(NaturalJoin node) {
        visitChild(node.getLeftTableRef());
        visitChild(node.getRightTableRef());
    }

    public void visit(OuterJoin node) {
        visitChild(node.getLeftTableRef());
        visitChild(node.getOnCond());
        visitChild(node.getRightTableRef());
    }

    public void visit(StraightJoin node) {
        visitChild(node.getLeftTableRef());
        visitChild(node.getOnCond());
        visitChild(node.getRightTableRef());
    }

    public void visit(SubqueryFactor node) {
        visitChild(node.getSubquery());
    }

    public void visit(TableReferences node) {
        visitChild(node.getTableReferenceList());
    }

    public void visit(TableRefFactor node) {
        visitChild(node.getHintList());
        visitChild(node.getTable());
        visitChild(node.getParamMarker());
    }

    public void visit(Dual dual) {}

    public void visit(GroupBy node) {
        visitChild(node.getOrderByList());
    }

    public void visit(Limit node) {
        visitChild(node.getOffset());
        visitChild(node.getSize());
    }

    public void visit(OrderBy node) {
        visitChild(node.getOrderByList());
    }

    public void visit(ColumnDefinition columnDefinition) {}

    public void visit(IndexOption indexOption) {}

    public void visit(IndexColumnName indexColumnName) {}

    public void visit(TableOptions node) {}

    public void visit(AlterSpecification node) {}

    public void visit(DataType node) {}

    public void visit(ShowAuthors node) {}

    public void visit(ShowBinaryLog node) {}

    public void visit(ShowBinLogEvent node) {
        visitChild(node.getLimit());
        visitChild(node.getPos());
    }

    public void visit(ShowCharaterSet node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowCharset node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowCollation node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowColumns node) {
        visitChild(node.getTable());
        visitChild(node.getWhere());
        visitChild(node.getPattern());
    }

    public void visit(ShowContributors node) {}

    public void visit(ShowCreate node) {
        visitChild(node.getId());
    }

    public void visit(ShowDatabases node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowEngine node) {}

    public void visit(ShowEngines node) {}

    public void visit(ShowErrors node) {
        visitChild(node.getLimit());
    }

    public void visit(ShowEvents node) {
        visitChild(node.getSchema());
        visitChild(node.getWhere());
    }

    public void visit(ShowFunctionCode node) {
        visitChild(node.getFunctionName());
    }

    public void visit(ShowFunctionStatus node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowGrants node) {
        visitChild(node.getUser());
    }

    public void visit(ShowIndex node) {
        visitChild(node.getTable());
    }

    public void visit(ShowMasterStatus node) {}

    public void visit(ShowOpenTables node) {
        visitChild(node.getSchema());
        visitChild(node.getWhere());
    }

    public void visit(ShowPlugins node) {}

    public void visit(ShowPrivileges node) {}

    public void visit(ShowProcedureCode node) {
        visitChild(node.getProcedureName());
    }

    public void visit(ShowProcedureStatus node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowProcesslist node) {}

    public void visit(ShowProfile node) {
        visitChild(node.getForQuery());
        visitChild(node.getLimit());
    }

    public void visit(ShowProfiles node) {}

    public void visit(ShowSlaveHosts node) {}

    public void visit(ShowSlaveStatus node) {}

    public void visit(ShowStatus node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowTables node) {
        visitChild(node.getSchema());
        visitChild(node.getWhere());
    }

    public void visit(ShowTableStatus node) {
        visitChild(node.getDatabase());
        visitChild(node.getWhere());
    }

    public void visit(ShowTriggers node) {
        visitChild(node.getSchema());
        visitChild(node.getWhere());
    }

    public void visit(ShowVariables node) {
        visitChild(node.getWhere());
    }

    public void visit(ShowWarnings node) {
        visitChild(node.getLimit());
    }

    public void visit(DescTableStatement node) {
        visitChild(node.getTable());
    }

    public void visit(DALSetStatement node) {
        visitChild(node.getAssignmentList());
    }

    public void visit(DALSetNamesStatement node) {}

    public void visit(DALSetCharacterSetStatement node) {}

    public void visit(DMLCallStatement node) {
        visitChild(node.getArguments());
        visitChild(node.getProcedure());
    }

    public void visit(DMLDeleteStatement node) {
        visitChild(node.getLimit());
        visitChild(node.getOrderBy());
        visitChild(node.getTableNames());
        visitChild(node.getTableRefs());
        visitChild(node.getWhereCondition());
    }

    public void visit(DMLInsertStatement node) {
        visitChild(node.getColumnNameList());
        visitChild(node.getDuplicateUpdate());
        visitChild(node.getRowList());
        visitChild(node.getSelect());
        visitChild(node.getTable());
    }

    public void visit(DMLReplaceStatement node) {
        visitChild(node.getColumnNameList());
        visitChild(node.getRowList());
        visitChild(node.getSelect());
        visitChild(node.getTable());
    }

    public void visit(DMLSelectStatement node) {
        visitChild(node.getGroup());
        visitChild(node.getHaving());
        visitChild(node.getLimit());
        visitChild(node.getOrder());
        stackDeep = 0;
        visitChild(node.getSelectExprList());
        visitChild(node.getTables());
        visitChild(node.getWhere());
    }

    public void visit(DMLSelectUnionStatement node) {
        visitChild(node.getLimit());
        visitChild(node.getOrderBy());
        stackDeep = 0;
        visitChild(node.getSelectStmtList());
    }

    public void visit(DMLUpdateStatement node) {
        visitChild(node.getLimit());
        visitChild(node.getOrderBy());
        visitChild(node.getTableRefs());
        visitChild(node.getValues());
        visitChild(node.getWhere());
    }

    public void visit(MTSSetTransactionStatement node) {}

    public void visit(MTSSavepointStatement node) {
        visitChild(node.getSavepoint());
    }

    public void visit(MTSReleaseStatement node) {
        visitChild(node.getSavepoint());
    }

    public void visit(MTSRollbackStatement node) {
        visitChild(node.getSavepoint());
    }

    public void visit(MTSCommitStatement node) {}

    public void visit(DDLTruncateStatement node) {
        visitChild(node.getTable());
    }

    public void visit(DDLAlterTableStatement node) {
        visitChild(node.getTable());
    }

    public void visit(DDLCreateIndexStatement node) {
        visitChild(node.getIndexDefinition());
        visitChild(node.getTable());
    }

    public void visit(DDLCreateTableStatement node) {
        visitChild(node.getTable());
    }

    /**
     *
     * @param node | CREATE TABLE tbl_name { LIKE old_tbl_name | (LIKE old_tbl_name) }
     */
    public void visit(DDLCreateLikeStatement node) {
        visitChild(node.getTable());
    }

    public void visit(DDLRenameTableStatement node) {
        visitChild(node.getList());
    }

    public void visit(DDLDropIndexStatement node) {
        visitChild(node.getIndexName());
        visitChild(node.getTable());
    }

    public void visit(DDLDropTableStatement node) {
        visitChild(node.getTableNames());
    }

    public void visit(ExtDDLCreatePolicy node) {}

    public void visit(ExtDDLDropPolicy node) {}

    public void visit(ShowFields node) {
        visitChild(node.getTable());
        visitChild(node.getPattern());
        visitChild(node.getWhere());
    }

    public void visit(DDLAlterViewStatement node) {}

    public void visit(DDLAlterEventStatement node) {}

    public void visit(DDLCreateEventStatement node) {}

    public void visit(DDLCreateTriggerStatement node) {
        visitChild(node.getDefiner());
        visitChild(node.getTriggerName());
        visitChild(node.getTriggerTime());
        visitChild(node.getTriggerEvent());
        visitChild(node.getTable());
        visitChild(node.getTriggerOrder());
        visitChild(node.getOtherTriggerName());
        visitChild(node.getStmt());
    }

    public void visit(DDLCreateViewStatement node) {}

    public void visit(MTSStartTransactionStatement mtsStartTransactionStatement) {};

    public void visit(ShowCreateDatabase node) {
        visitChild(node.getDbName());
    }

    public void visit(ExplainStatement node) {
        visitChild(node.getTblName());
        visitChild(node.getExplainableStmt());
    }

    public void visit(ForeignKeyDefinition foreignKeyDefinition) {}

    public void visit(WithValidation withValidation) {}

    public void visit(IfStatement node) {
        visitChild(node.getIfStatements());
        visitChild(node.getElseStatement());
    }

    public void visit(BeginEndStatement node) {
        visitChild(node.getLabel());
        visitChild(node.getStatements());
    }

    public void visit(NewRowPrimary node) {}

    public void visit(OldRowPrimary node) {}

    public void visit(LoopStatement node) {
        visitChild(node.getLabel());
        visitChild(node.getStmt());
    }

    public void visit(IterateStatement node) {
        visitChild(node.getLabel());
    }

    public void visit(LeaveStatement node) {
        visitChild(node.getLabel());
    }

    public void visit(ReturnStatement node) {
        visitChild(node.getLabel());
    }

    public void visit(RepeatStatement node) {
        visitChild(node.getLabel());
        visitChild(node.getStmt());
        visitChild(node.getUtilCondition());
    }

    public void visit(WhileStatement node) {
        visitChild(node.getLabel());
        visitChild(node.getStmt());
        visitChild(node.getWhileCondition());
    }

    public void visit(CaseStatement node) {
        visitChild(node.getCaseValue());
        visitChild(node.getWhenList());
        visitChild(node.getElseStmt());
    }

    public void visit(DeclareStatement node) {
        visitChild(node.getVarNames());
        visitChild(node.getDataType());
    }

    public void visit(DeclareHandlerStatement node) {
        visitChild(node.getStmt());
    }

    public void visit(DeclareConditionStatement node) {
        visitChild(node.getName());
    }

    public void visit(CursorDeclareStatement node) {
        visitChild(node.getName());
        visitChild(node.getStmt());
    }

    public void visit(CursorCloseStatement node) {
        visitChild(node.getName());
    }

    public void visit(CursorOpenStatement node) {
        visitChild(node.getName());
    }

    public void visit(CursorFetchStatement node) {
        visitChild(node.getName());
        visitChild(node.getVarNames());
    }

    public void visit(SignalStatement node) {
        visitChild(node.getInformationItems());
    }

    public void visit(ResignalStatement node) {
        visitChild(node.getInformationItems());
    }

    public void visit(GetDiagnosticsStatement node) {
        visitChild(node.getStatementItems());
        visitChild(node.getConditionItems());
    }

    public void visit(DDLCreateProcedureStatement node) {
        visitChild(node.getDefiner());
        visitChild(node.getName());
        visitChild(node.getParameters());
        visitChild(node.getCharacteristics());
        visitChild(node.getStmt());
    }

    public void visit(DDLCreateFunctionStatement node) {
        visitChild(node.getDefiner());
        visitChild(node.getName());
        visitChild(node.getParameters());
        visitChild(node.getReturns());
        visitChild(node.getCharacteristics());
        visitChild(node.getStmt());
    }

    public void visit(DDLDropTriggerStatement node) {
        visitChild(node.getName());
    }

    public void visit(IndexDefinition node) {}

}
