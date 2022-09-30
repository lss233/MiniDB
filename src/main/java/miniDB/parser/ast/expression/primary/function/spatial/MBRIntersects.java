package miniDB.parser.ast.expression.primary.function.spatial;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class MBRIntersects extends FunctionExpression {

    public MBRIntersects(List<Expression> arguments) {
        super("MBRINTERSECTS", arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new MBRIntersects(arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
