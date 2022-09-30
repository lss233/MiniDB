package miniDB.parser.ast.stmt.compound.flowcontrol;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.visitor.Visitor;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:21:00
 * 
 */
public class RepeatStatement implements CompoundStatement {
    private final Identifier label;
    private final SQLStatement stmt;
    private final Expression utilCondition;

    public Identifier getLabel() {
        return label;
    }

    public SQLStatement getStmt() {
        return stmt;
    }

    public Expression getUtilCondition() {
        return utilCondition;
    }

    public RepeatStatement(Identifier label, SQLStatement stmt, Expression utilCondition) {
        this.label = label;
        this.stmt = stmt;
        this.utilCondition = utilCondition;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
