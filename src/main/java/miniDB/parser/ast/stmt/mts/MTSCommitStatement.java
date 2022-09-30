package miniDB.parser.ast.stmt.mts;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.visitor.Visitor;

public class MTSCommitStatement implements SQLStatement {
    public static enum CompleteType {
        /** not specified, then use default */
        UN_DEF, CHAIN,
        /** MySQL's default */
        NO_CHAIN, RELEASE, NO_RELEASE
    }

    private final CompleteType completeType;

    public MTSCommitStatement(CompleteType completeType) {
        if (completeType == null)
            throw new IllegalArgumentException("complete type is null!");
        this.completeType = completeType;
    }

    public MTSCommitStatement(Identifier savepoint) {
        this.completeType = null;
        if (savepoint == null)
            throw new IllegalArgumentException("savepoint is null!");
    }

    public CompleteType getCompleteType() {
        return completeType;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
