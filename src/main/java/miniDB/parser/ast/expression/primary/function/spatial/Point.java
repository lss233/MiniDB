package miniDB.parser.ast.expression.primary.function.spatial;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class Point extends FunctionExpression {

    public Point(List<Expression> arguments) {
        super("POINT", arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new Point(arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
