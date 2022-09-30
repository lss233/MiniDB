package miniDB.parser.ast.stmt.ddl;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.visitor.Visitor;

/**
 * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
 * { LIKE old_tbl_name | (LIKE old_tbl_name) }
 *
 * @author ZC.CUI
 */
public class DDLCreateLikeStatement extends DDLCreateTableStatement implements DDLStatement {
    private final Identifier likeTable;
    private String createTableSql;

    public DDLCreateLikeStatement(boolean temporary, boolean ifNotExists, Identifier table,
            Identifier likeTable) {
        super(temporary, ifNotExists, table);
        this.likeTable = likeTable;
    }

    public Identifier getLikeTable() {
        return likeTable;
    }

    public String getCreateTableSql() {
        return createTableSql;
    }

    public void setCreateTableSql(String createTableSql) {
        this.createTableSql = createTableSql;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
