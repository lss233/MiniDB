package miniDB.parser.ast.expression.primary;

import miniDB.parser.visitor.Visitor;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月22日 上午11:06:08
 * 
 */
public class OldRowPrimary extends VariableExpression {
    /** include starting '@', e.g. "@'mary''s'" */
    private final String varText;

    public OldRowPrimary(String varText) {
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
