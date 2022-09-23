package miniDB.parser.ast.stmt.compound.cursors;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.visitor.Visitor;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:24:06
 * 
 */
public class CursorCloseStatement implements CompoundStatement {
    private final Identifier name;

    public CursorCloseStatement(Identifier name) {
        this.name = name;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public Identifier getName() {
        return this.name;
    }

}
