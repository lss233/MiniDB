package miniDB.parser.ast.expression.primary;

import miniDB.parser.visitor.Visitor;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午5:19:52
 * 
 */
public class NewRowPrimary extends VariableExpression {
    /** include starting '@', e.g. "@'mary''s'" */
    private final String varText;

    public NewRowPrimary(String varText) {
        this.varText = varText;
    }

    public String getVarText() {
        return varText;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
