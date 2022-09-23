package miniDB.parser.ast.stmt.extension;

import miniDB.parser.ast.stmt.ddl.DDLStatement;
import miniDB.parser.visitor.Visitor;

public class DropPrepareStatement implements DDLStatement {
    private final String preparedName;

    public DropPrepareStatement(String preparedName) {
        this.preparedName = preparedName;
    }

    @Override
    public void accept(Visitor visitor) {}

    public String getPreparedName() {
        return preparedName;
    }

}
