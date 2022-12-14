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
 * (created at 2011-1-21)
 */
package miniDB.parser.ast.expression.primary;

import miniDB.parser.ast.expression.misc.QueryExpression;
import miniDB.parser.ast.fragment.tableref.TableReference;
import miniDB.parser.visitor.Visitor;

import java.util.Map;

/**
 * <code>'?'</code>
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class ParamMarker extends PrimaryExpression implements TableReference, QueryExpression {
    private final int paramIndex;
    private String alias;

    /**
     * @param paramIndex start from 1
     */
    public ParamMarker(int paramIndex) {
        this.paramIndex = paramIndex;
    }

    public ParamMarker(int paramIndex, String alias) {
        this.paramIndex = paramIndex;
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    /**
     * @return start from 1
     */
    public int getParamIndex() {
        return paramIndex;
    }

    @Override
    public int hashCode() {
        return paramIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj instanceof ParamMarker) {
            ParamMarker that = (ParamMarker) obj;
            return this.paramIndex == that.paramIndex;
        }
        return false;
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return parameters.get(paramIndex);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Object removeLastConditionElement() {
        return null;
    }

    @Override
    public boolean isSingleTable() {
        return false;
    }

}
