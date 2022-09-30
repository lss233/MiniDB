package miniDB.parser.ast.expression.primary.function.spatial;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class ST_Within extends FunctionExpression {

    public ST_Within(List<Expression> arguments) {
        super("ST_WITHIN", arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new ST_Within(arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
