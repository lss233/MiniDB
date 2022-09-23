package miniDB.parser.ast.stmt.compound.flowcontrol;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.util.Pair;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * <pre>
 * CASE case_value
 *     WHEN when_value THEN statement_list
 *     [WHEN when_value THEN statement_list] ...
 *     [ELSE statement_list]
 * END CASE
 * 
 * OR
 * 
 * CASE
 *     WHEN search_condition THEN statement_list
 *     [WHEN search_condition THEN statement_list] ...
 *     [ELSE statement_list]
 * END CASE
 * </pre>
 * @author liuhuanting
 * @date 2017年11月1日 下午4:19:37
 * 
 */
public class CaseStatement implements CompoundStatement {
    private final Expression caseValue;
    private final List<Pair<Expression, SQLStatement>> whenList;
    private final SQLStatement elseStmt;

    public CaseStatement(Expression caseValue, List<Pair<Expression, SQLStatement>> whenList,
            SQLStatement elseStmt) {
        this.caseValue = caseValue;
        this.whenList = whenList;
        this.elseStmt = elseStmt;
    }

    public Expression getCaseValue() {
        return caseValue;
    }

    public List<Pair<Expression, SQLStatement>> getWhenList() {
        return whenList;
    }

    public SQLStatement getElseStmt() {
        return elseStmt;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
