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
 * (created at 2011-5-19)
 */
package miniDB.parser.ast.stmt.dal;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.VariableExpression;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.util.Pair;
import miniDB.parser.visitor.Visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DALSetStatement implements SQLStatement {
    private final List<Pair<VariableExpression, Expression>> assignmentList;

    public DALSetStatement(List<Pair<VariableExpression, Expression>> assignmentList) {
        if (assignmentList == null || assignmentList.isEmpty()) {
            this.assignmentList = Collections.emptyList();
        } else if (assignmentList instanceof ArrayList) {
            this.assignmentList = assignmentList;
        } else {
            this.assignmentList =
                    new ArrayList<Pair<VariableExpression, Expression>>(assignmentList);
        }
    }

    /**
     * @return never null
     */
    public List<Pair<VariableExpression, Expression>> getAssignmentList() {
        return assignmentList;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
