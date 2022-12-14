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

import miniDB.parser.ast.fragment.VariableScope;
import miniDB.parser.visitor.Visitor;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class SysVarPrimary extends VariableExpression {
    private final VariableScope scope;
    /**
     * 作用域 字符串 可能为null
     */
    private final String scopeStr;
    /** excluding starting "@@", '`' might be included */
    private final String varText;
    private final String varTextUp;

    public SysVarPrimary(VariableScope scope, String scopeStr, String varText, String varTextUp) {
        this.scope = scope;
        this.scopeStr = scopeStr;
        this.varText = varText;
        this.varTextUp = varTextUp;
    }

    /**
     * @return never null
     */
    public VariableScope getScope() {
        return scope;
    }

    public String getVarTextUp() {
        return varTextUp;
    }

    public String getVarText() {
        return varText;
    }

    public String getScopeStr() {
        return scopeStr;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
