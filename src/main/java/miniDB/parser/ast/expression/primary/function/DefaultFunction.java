package miniDB.parser.ast.expression.primary.function;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

public class DefaultFunction extends FunctionExpression {
    private String functionName;

    public DefaultFunction(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
        this.functionName = functionName;
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new DefaultFunction(functionName, arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
