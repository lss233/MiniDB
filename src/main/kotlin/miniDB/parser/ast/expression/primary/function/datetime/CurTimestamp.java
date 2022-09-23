package miniDB.parser.ast.expression.primary.function.datetime;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;

import java.util.List;

public class CurTimestamp extends FunctionExpression {

    public CurTimestamp() {
        super("CURTIMTSTAMP", null);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new CurTimestamp();
    }

}
