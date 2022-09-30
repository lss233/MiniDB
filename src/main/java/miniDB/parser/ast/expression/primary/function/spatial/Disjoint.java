package miniDB.parser.ast.expression.primary.function.spatial;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class Disjoint extends FunctionExpression {

    public Disjoint(List<Expression> arguments) {
        super("DISJOINT", arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new Disjoint(arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
