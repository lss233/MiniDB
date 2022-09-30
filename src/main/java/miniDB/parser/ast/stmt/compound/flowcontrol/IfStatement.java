package miniDB.parser.ast.stmt.compound.flowcontrol;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.util.Pair;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * <pre>
 * IF search_condition THEN statement_list
 *    [ELSEIF search_condition THEN statement_list] ...
 *    [ELSE statement_list]
 * END IF
 * </pre>
 * @author liuhuanting
 * @date 2017年11月1日 下午4:15:43
 * 
 */
public class IfStatement implements CompoundStatement {
    private final List<Pair<Expression, List<SQLStatement>>> ifStatements;
    private final List<SQLStatement> elseStatement;

    public IfStatement(List<Pair<Expression, List<SQLStatement>>> ifStatements,
            List<SQLStatement> elseStatements) {
        this.ifStatements = ifStatements;
        this.elseStatement = elseStatements;
    }

    public List<Pair<Expression, List<SQLStatement>>> getIfStatements() {
        return ifStatements;
    }

    public List<SQLStatement> getElseStatement() {
        return elseStatement;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
