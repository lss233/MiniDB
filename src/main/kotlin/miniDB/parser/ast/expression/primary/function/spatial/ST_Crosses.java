package miniDB.parser.ast.expression.primary.function.spatial;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class ST_Crosses extends FunctionExpression {

    public ST_Crosses(List<Expression> arguments) {
        super("ST_CROSSES", arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new ST_Crosses(arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
