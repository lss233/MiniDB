package miniDB.parser.ast.stmt.compound.flowcontrol;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.visitor.Visitor;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:20:24
 * 
 */
public class LoopStatement implements CompoundStatement {

    private final Identifier label;
    private final SQLStatement stmt;

    public Identifier getLabel() {
        return label;
    }

    public SQLStatement getStmt() {
        return stmt;
    }

    /**
     * <p>Description: </p>
     * @param label
     * @param stmt : 
     */
    public LoopStatement(Identifier label, SQLStatement stmt) {
        this.label = label;
        this.stmt = stmt;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
