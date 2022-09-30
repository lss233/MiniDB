/*
 * Copyright 1999-2012 Alibaba Group.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
/**
 * (created at 2011-7-4)
 */
package miniDB.parser.ast.stmt.ddl;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.fragment.ddl.datatype.DataType;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.condition.Characteristics;
import miniDB.parser.util.Tuple3;
import miniDB.parser.visitor.OutputVisitor;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * CREATE PROCEDURE Syntax<br>
 * 目前语法对象只解析至PROCEDURE sp_name为止
 * 
 * <pre>
 * CREATE
 *  [DEFINER = { user | CURRENT_USER }]
 *  PROCEDURE sp_name ([proc_parameter[,...]])
 *  [characteristic ...] routine_body
 *
 * proc_parameter:
 *  [ IN | OUT | INOUT ] param_name type
 * type:
 *  Any valid MySQL data type
 *  
 * characteristic:
 *  COMMENT 'string'
 *  | LANGUAGE SQL
 *  | [NOT] DETERMINISTIC
 *  | { CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA }
 *  | SQL SECURITY { DEFINER | INVOKER }

 * routine_body:
 *  Valid SQL routine statement
 * </pre>
 */
public class DDLCreateProcedureStatement implements DDLStatement {

    public enum ProcParameterType {
        IN, OUT, INOUT, NONE
    }

    private final Expression definer;
    private final Identifier name;
    private final List<Tuple3<ProcParameterType, Identifier, DataType>> parameters;
    private final Characteristics characteristics;
    private final SQLStatement stmt;

    public DDLCreateProcedureStatement(Expression definer, Identifier name,
            List<Tuple3<ProcParameterType, Identifier, DataType>> parameters,
            Characteristics characteristics, SQLStatement stmt) {
        this.definer = definer;
        this.name = name;
        this.parameters = parameters;
        this.characteristics = characteristics;
        this.stmt = stmt;
    }

    public Expression getDefiner() {
        return definer;
    }

    public Identifier getName() {
        return name;
    }

    public List<Tuple3<ProcParameterType, Identifier, DataType>> getParameters() {
        return parameters;
    }

    public Characteristics getCharacteristics() {
        return characteristics;
    }

    public SQLStatement getStmt() {
        return stmt;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        accept(new OutputVisitor(sb));
        return sb.toString();
    }
}
