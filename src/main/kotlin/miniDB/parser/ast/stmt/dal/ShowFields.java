package miniDB.parser.ast.stmt.dal;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.visitor.Visitor;

/**
 * @author liuhuanting
 */
public class ShowFields extends DALShowStatement {

    private final boolean full;
    private final Identifier table;
    private final String pattern;
    private final Expression where;

    public ShowFields(boolean full, Identifier table, Identifier database, Expression where) {
        this.full = full;
        this.table = table;
        if (database != null) {
            this.table.setParent(database);
        }
        this.pattern = null;
        this.where = where;
    }

    public ShowFields(boolean full, Identifier table, Identifier database, String pattern) {
        this.full = full;
        this.table = table;
        if (database != null) {
            this.table.setParent(database);
        }
        this.pattern = pattern;
        this.where = null;
    }

    public ShowFields(boolean full, Identifier table, Identifier database) {
        this.full = full;
        this.table = table;
        if (database != null) {
            this.table.setParent(database);
        }
        this.pattern = null;
        this.where = null;
    }

    public boolean isFull() {
        return full;
    }

    public Identifier getTable() {
        return table;
    }

    public String getPattern() {
        return pattern;
    }

    public Expression getWhere() {
        return where;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
