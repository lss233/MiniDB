package miniDB.parser.ast.stmt.extension;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.visitor.Visitor;

import java.util.ArrayList;

public class ExecutePrepareStatement implements SQLStatement {

    private final String name;
    private final ArrayList<Expression> vars;

    public ExecutePrepareStatement(String name, ArrayList<Expression> vars) {
        this.name = name;
        this.vars = vars;
    }

    @Override
    public void accept(Visitor visitor) {

    }

    public ArrayList<Expression> getVars() {
        return vars;
    }

    public String getName() {
        return name;
    }

}
