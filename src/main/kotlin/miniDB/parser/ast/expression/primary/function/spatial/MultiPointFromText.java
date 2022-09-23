package miniDB.parser.ast.expression.primary.function.spatial;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class MultiPointFromText extends FunctionExpression {

    public MultiPointFromText(Expression expr) {
        super("MULTIPOINTFROMTEXT", wrapList(expr));
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        throw new UnsupportedOperationException("function of char has special arguments");
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
