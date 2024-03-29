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
package miniDB.parser.ast.expression.primary.literal;

import miniDB.parser.visitor.Visitor;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * literal date is also possible
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class LiteralNumber extends Literal implements Comparable<Object> {
    private Number number;

    public LiteralNumber(Number number) {
        super();
        if (number == null)
            throw new IllegalArgumentException("number is null!");
        this.number = number;
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return number;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public Number getNumber() {
        return number;
    }

    public void setNumber(Number number) {
        this.number = number;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof LiteralNumber){
            return getNumber().equals(((LiteralNumber) obj).getNumber());
        } else {
            return false;
        }
    }
    @Override
    public int compareTo(@NotNull Object o) {
        if(o instanceof LiteralNumber) {
            return Double.compare(this.number.doubleValue(), ((LiteralNumber) o).number.doubleValue());
        }
        return 0;
    }
}
