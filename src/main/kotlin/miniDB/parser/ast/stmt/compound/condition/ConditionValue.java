package miniDB.parser.ast.stmt.compound.condition;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.expression.primary.literal.LiteralNumber;
import miniDB.parser.ast.expression.primary.literal.LiteralString;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月2日 下午4:09:13
 * 
 */
public class ConditionValue {
    public enum ConditionValueType {
        Unknown, ErrorCode, State, Name, Warning, NotFound, Exception
    }

    private ConditionValueType type = ConditionValueType.Unknown;
    private LiteralNumber mysqlErrorCode;
    private LiteralString sqlState;
    private Identifier conditionName;
    private boolean isWarning;
    private boolean isNotFound;
    private boolean isException;

    /**
     * 
     * <p>Description: 请确保type和value相匹配</p>
     * @param type
     * @param value :
     */
    public ConditionValue(ConditionValueType type, Object value) {
        if (value == null) {
            return;
        }
        this.type = type;
        switch (type) {
            case ErrorCode:
                mysqlErrorCode = (LiteralNumber) value;
                break;
            case Exception:
                isException = (boolean) value;
                break;
            case Name:
                conditionName = (Identifier) value;
                break;
            case NotFound:
                isNotFound = (boolean) value;
                break;
            case State:
                sqlState = (LiteralString) value;
                break;
            case Warning:
                isWarning = (boolean) value;
                break;
            default:
                break;
        }
    }

    public ConditionValueType getType() {
        return type;
    }

    public Object getValue() {
        switch (type) {
            case ErrorCode:
                return mysqlErrorCode;
            case Exception:
                return isException;
            case Name:
                return conditionName;
            case NotFound:
                return isNotFound;
            case State:
                return sqlState;
            case Warning:
                return isWarning;
            default:
                return null;
        }
    }
}
