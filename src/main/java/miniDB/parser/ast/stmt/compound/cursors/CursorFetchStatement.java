package miniDB.parser.ast.stmt.compound.cursors;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:24:35
 * 
 */
public class CursorFetchStatement implements CompoundStatement {
    private final Identifier name;
    private final List<Identifier> varNames;

    public CursorFetchStatement(Identifier name, List<Identifier> varNames) {
        this.name = name;
        this.varNames = varNames;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public Identifier getName() {
        return this.name;
    }

    public List<Identifier> getVarNames() {
        return varNames;
    }

}
