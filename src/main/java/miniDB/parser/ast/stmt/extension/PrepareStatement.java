package miniDB.parser.ast.stmt.extension;

import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.visitor.Visitor;

public class PrepareStatement implements SQLStatement {
    private final String name;
    private final String stmt;
    private String[] sqls;

    public PrepareStatement(String stmt_name, String preparable_stmt) {
        this.name = stmt_name;
        this.stmt = preparable_stmt;
        this.sqls = stmt.split("\\?");
    }

    @Override
    public void accept(Visitor visitor) {}

    public String getStmt() {
        return stmt;
    }

    public String getName() {
        return name;
    }

    public String[] getSqls() {
        return sqls;
    }

}
