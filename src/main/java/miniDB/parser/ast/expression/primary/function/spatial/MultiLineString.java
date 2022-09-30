package miniDB.parser.ast.expression.primary.function.spatial;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class MultiLineString extends FunctionExpression {

    public MultiLineString(List<Expression> arguments) {
        super("MULTILINESTRING", arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new MultiLineString(arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
