package miniDB.parser.ast.stmt.compound;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.fragment.ddl.datatype.DataType;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:22:14
 * 
 */
public class DeclareStatement implements CompoundStatement {
    private final List<Identifier> varNames;
    private final DataType dataType;
    private final Expression defaultVal;

    public List<Identifier> getVarNames() {
        return varNames;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Expression getDefaultVal() {
        return defaultVal;
    }

    public DeclareStatement(List<Identifier> varNames, DataType dataType, Expression defaultVal) {
        this.varNames = varNames;
        this.dataType = dataType;
        this.defaultVal = defaultVal;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
