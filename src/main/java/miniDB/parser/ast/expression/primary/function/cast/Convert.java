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
 * (created at 2011-1-23)
 */
package miniDB.parser.ast.expression.primary.function.cast;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.function.FunctionExpression;
import miniDB.parser.visitor.Visitor;

import java.util.List;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class Convert extends FunctionExpression {
    /**
     * Either {@link transcodeName} or {@link typeName} is null
     */
    private final String transcodeName;

    public Convert(Expression arg, String transcodeName) {
        super("CONVERT", wrapList(arg));
        if (null == transcodeName) {
            throw new IllegalArgumentException("transcodeName is null");
        }
        this.transcodeName = transcodeName;
    }

    public String getTranscodeName() {
        return transcodeName;
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        throw new UnsupportedOperationException("function of char has special arguments");
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
