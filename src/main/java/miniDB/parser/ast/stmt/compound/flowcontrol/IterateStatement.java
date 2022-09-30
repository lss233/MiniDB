package miniDB.parser.ast.stmt.compound.flowcontrol;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.visitor.Visitor;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:20:05
 * 
 */
public class IterateStatement implements CompoundStatement {
    private final Identifier label;

    public IterateStatement(Identifier label) {
        this.label = label;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public Identifier getLabel() {
        return this.label;
    }
}
