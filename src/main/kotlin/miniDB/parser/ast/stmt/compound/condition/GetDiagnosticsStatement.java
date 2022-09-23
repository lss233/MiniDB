package miniDB.parser.ast.stmt.compound.condition;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.ast.stmt.compound.condition.SignalStatement.ConditionInfoItemName;
import miniDB.parser.util.Pair;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * TODO https://dev.mysql.com/doc/refman/5.7/en/get-diagnostics.html
 * @author liuhuanting
 * @date 2017年11月1日 下午4:25:49
 * 
 */
public class GetDiagnosticsStatement implements CompoundStatement {
    public enum StatementInfoItemName {
        NUMBER, ROW_COUNT
    }
    public enum DiagnosticType {
        NONE, CURRENT, STACKED
    }

    private final DiagnosticType type;
    private final List<Pair<Expression, StatementInfoItemName>> statementItems;
    private final Expression conditionNumber;
    private final List<Pair<Expression, ConditionInfoItemName>> conditionItems;

    public GetDiagnosticsStatement(DiagnosticType type,
            List<Pair<Expression, StatementInfoItemName>> statementItems,
            Expression conditionNumber,
            List<Pair<Expression, ConditionInfoItemName>> conditionItems) {
        this.type = type;
        this.statementItems = statementItems;
        this.conditionNumber = conditionNumber;
        this.conditionItems = conditionItems;
    }

    public DiagnosticType getType() {
        return type;
    }

    public List<Pair<Expression, StatementInfoItemName>> getStatementItems() {
        return statementItems;
    }

    public Expression getConditionNumber() {
        return conditionNumber;
    }

    public List<Pair<Expression, ConditionInfoItemName>> getConditionItems() {
        return conditionItems;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }


}
