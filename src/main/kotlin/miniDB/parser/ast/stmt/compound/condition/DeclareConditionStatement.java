package miniDB.parser.ast.stmt.compound.condition;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.visitor.Visitor;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:25:22
 * 
 */
public class DeclareConditionStatement implements CompoundStatement {
    private final Identifier name;
    private final ConditionValue value;

    public DeclareConditionStatement(Identifier name, ConditionValue value) {
        this.name = name;
        this.value = value;
    }

    public Identifier getName() {
        return name;
    }

    public ConditionValue getValue() {
        return value;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
