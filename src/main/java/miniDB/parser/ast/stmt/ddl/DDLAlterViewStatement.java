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
import miniDB.parser.recognizer.mysql.syntax.MySQLDDLParser.Algorithm;
import miniDB.parser.recognizer.mysql.syntax.MySQLDDLParser.SqlSecurity;
import miniDB.parser.visitor.Visitor;

/**
 * ALTER VIEW Syntax<br>
 * 目前语法对象只解析至VIEW view_name为止
 * 
 * <pre>
 * ALTER
 *  [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
 *  [DEFINER = { user | CURRENT_USER }]
 *  [SQL SECURITY { DEFINER | INVOKER }]
 *  VIEW view_name [(column_list)]
 *  AS select_statement
 *  [WITH [CASCADED | LOCAL] CHECK OPTION]
 * </pre>
 */
public class DDLAlterViewStatement implements DDLStatement {
    /**
     * [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
     */
    private final Algorithm algorithm;
    private final Expression definer;

    private final SqlSecurity sqlSecurity;

    /**
     * 视图名
     */
    private final Identifier view;

    public DDLAlterViewStatement(Algorithm algorithm, Expression definer, SqlSecurity sqlSecurity,
            Identifier view) {
        super();
        this.algorithm = algorithm;
        this.definer = definer;
        this.sqlSecurity = sqlSecurity;
        this.view = view;
    }



    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }



    public Algorithm getAlgorithm() {
        return algorithm;
    }



    public Expression getDefiner() {
        return definer;
    }



    public SqlSecurity getSqlSecurity() {
        return sqlSecurity;
    }



    public Identifier getView() {
        return view;
    }


}
