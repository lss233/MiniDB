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
import miniDB.parser.visitor.Visitor;

/**
 * ALTER VIEW Syntax<br>
 * 目前语法对象只解析至EVENT event_name为止
 * 
 * <pre>
 * ALTER
 *  [DEFINER = { user | CURRENT_USER }]
 *  EVENT event_name
 *  [ON SCHEDULE schedule]
 *  [ON COMPLETION [NOT] PRESERVE]
 *  [RENAME TO new_event_name]
 *  [ENABLE | DISABLE | DISABLE ON SLAVE]
 *  [COMMENT 'comment']
 *  [DO event_body]
 * </pre>
 */
public class DDLAlterEventStatement implements DDLStatement {

    private final Expression definer;

    private final Identifier eventName;


    public DDLAlterEventStatement(Expression definer, Identifier eventName) {
        super();
        this.definer = definer;
        this.eventName = eventName;
    }



    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }



    public Expression getDefiner() {
        return definer;
    }



    public Identifier getEventName() {
        return eventName;
    }


}
