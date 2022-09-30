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
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.visitor.OutputVisitor;
import miniDB.parser.visitor.Visitor;

/**
 * CREATE TRIGGER Syntax<br>
 * <pre>
 * CREATE
 *   [DEFINER = { user | CURRENT_USER }]
 *   TRIGGER trigger_name
 *   trigger_time trigger_event
 *   ON tbl_name FOR EACH ROW
 *   [trigger_order]
 *   trigger_body
 * 
 * trigger_time: { BEFORE | AFTER }
 * 
 * trigger_event: { INSERT | UPDATE | DELETE }
 * 
 * trigger_order: { FOLLOWS | PRECEDES } other_trigger_name
 * </pre>
 */
public class DDLCreateTriggerStatement implements DDLStatement {

    public enum TriggerTime {
        BEFORE, AFTER
    }
    public enum TriggerEvent {
        INSERT, UPDATE, DELETE
    }

    public enum TriggerOrder {
        FOLLOWS, PRECEDES
    }

    private Expression definer;
    private final Identifier triggerName;
    private final TriggerTime triggerTime;
    private final TriggerEvent triggerEvent;
    private final Identifier table;
    private final TriggerOrder triggerOrder;
    private final Identifier otherTriggerName;
    private final SQLStatement stmt;

    public DDLCreateTriggerStatement(Expression definer, Identifier triggerName,
            TriggerTime triggerTime, TriggerEvent triggerEvent, Identifier table,
            TriggerOrder triggerOrder, Identifier otherTriggerName, SQLStatement stmt) {
        super();
        this.definer = definer;
        this.triggerName = triggerName;
        this.triggerTime = triggerTime;
        this.triggerEvent = triggerEvent;
        this.table = table;
        this.triggerOrder = triggerOrder;
        this.otherTriggerName = otherTriggerName;
        this.stmt = stmt;
    }

    public Expression getDefiner() {
        return definer;
    }

    public void setDefiner(Expression definer) {
        this.definer = definer;
    }

    public Identifier getTriggerName() {
        return triggerName;
    }

    public TriggerTime getTriggerTime() {
        return triggerTime;
    }

    public TriggerEvent getTriggerEvent() {
        return triggerEvent;
    }

    public Identifier getTable() {
        return table;
    }

    public TriggerOrder getTriggerOrder() {
        return triggerOrder;
    }

    public Identifier getOtherTriggerName() {
        return otherTriggerName;
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
