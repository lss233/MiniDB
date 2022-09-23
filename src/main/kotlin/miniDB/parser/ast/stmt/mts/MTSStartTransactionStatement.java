package miniDB.parser.ast.stmt.mts;

import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.visitor.Visitor;

/**
 * @author liuhuanting
 */
public class MTSStartTransactionStatement implements SQLStatement {

    public static enum TransactionCharacteristic {
        WITH_CONSISTENT_SNAPSHOT, READ_WRITE, READ_ONLY
    }

    private final TransactionCharacteristic characteristic;

    public MTSStartTransactionStatement(TransactionCharacteristic characteristic) {
        this.characteristic = characteristic;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public TransactionCharacteristic getCharacteristic() {
        return characteristic;
    }

}
