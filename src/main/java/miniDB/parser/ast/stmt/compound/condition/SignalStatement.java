package miniDB.parser.ast.stmt.compound.condition;

import miniDB.parser.ast.expression.primary.literal.Literal;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.util.Pair;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * TODO https://dev.mysql.com/doc/refman/5.7/en/signal.html
 * @author liuhuanting
 * @date 2017年11月1日 下午4:26:13
 * 
 */
public class SignalStatement implements CompoundStatement {
    public enum ConditionInfoItemName {
        CLASS_ORIGIN, SUBCLASS_ORIGIN, MESSAGE_TEXT, MYSQL_ERRNO, CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, CURSOR_NAME, RETURNED_SQLSTATE
    }

    private final ConditionValue conditionValue;
    private final List<Pair<ConditionInfoItemName, Literal>> informationItems;

    public SignalStatement(ConditionValue conditionValue,
            List<Pair<ConditionInfoItemName, Literal>> informationItems) {
        super();
        this.conditionValue = conditionValue;
        this.informationItems = informationItems;
    }

    public ConditionValue getConditionValue() {
        return conditionValue;
    }

    public List<Pair<ConditionInfoItemName, Literal>> getInformationItems() {
        return informationItems;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
