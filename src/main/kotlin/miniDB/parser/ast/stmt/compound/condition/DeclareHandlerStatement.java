package miniDB.parser.ast.stmt.compound.condition;

import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.CompoundStatement;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * <pre>
 * DECLARE handler_action HANDLER
 *     FOR condition_value [, condition_value] ...
 *     statement
 * 
 * handler_action:
 *     CONTINUE
 *   | EXIT
 *   | UNDO
 * 
 * condition_value:
 *     mysql_error_code
 *   | SQLSTATE [VALUE] sqlstate_value
 *   | condition_name
 *   | SQLWARNING
 *   | NOT FOUND
 *   | SQLEXCEPTION
 * </pre>
 * @author liuhuanting
 * @date 2017年11月1日 下午4:25:35
 * 
 */
public class DeclareHandlerStatement implements CompoundStatement {
    public enum HandlerAction {
        CONTINUE, EXIT, UNDO
    }

    private final HandlerAction action;
    private final List<ConditionValue> conditionValues;
    private final SQLStatement stmt;


    public DeclareHandlerStatement(HandlerAction action, List<ConditionValue> conditionValues,
            SQLStatement stmt) {
        this.action = action;
        this.conditionValues = conditionValues;
        this.stmt = stmt;
    }

    public HandlerAction getAction() {
        return action;
    }

    public List<ConditionValue> getConditionValues() {
        return conditionValues;
    }

    public SQLStatement getStmt() {
        return stmt;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
