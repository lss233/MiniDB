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
 * (created at 2011-5-20)
 */
package miniDB.parser.ast.stmt.dal;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.visitor.Visitor;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class ShowTableStatus extends DALShowStatement {
    private Identifier database;
    private final String pattern;
    private final Expression where;

    public ShowTableStatus(Identifier database, Expression where) {
        this.database = database;
        this.pattern = null;
        this.where = where;
    }

    public ShowTableStatus(Identifier database, String pattern) {
        this.database = database;
        this.pattern = pattern;
        this.where = null;
    }

    public ShowTableStatus(Identifier database) {
        this.database = database;
        this.pattern = null;
        this.where = null;
    }

    public void setDatabase(Identifier database) {
        this.database = database;
    }

    public Identifier getDatabase() {
        return database;
    }

    public String getPattern() {
        return pattern;
    }

    public Expression getWhere() {
        return where;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
