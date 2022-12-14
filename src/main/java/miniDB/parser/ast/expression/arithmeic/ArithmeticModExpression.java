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
 * (created at 2011-1-20)
 */
package miniDB.parser.ast.expression.arithmeic;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.visitor.Visitor;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * <code>higherExpr ('MOD'|'%') higherExpr</code>
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class ArithmeticModExpression extends ArithmeticBinaryOperatorExpression {
    public ArithmeticModExpression(Expression leftOprand, Expression rightOprand) {
        super(leftOprand, rightOprand, PRECEDENCE_ARITHMETIC_FACTOR_OP);
    }

    @Override
    public String getOperator() {
        return "%";
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Number calculate(Integer integer1, Integer integer2) {
        if (integer1 == null || integer2 == null)
            return null;
        int i1 = integer1.intValue();
        int i2 = integer2.intValue();
        if (i2 == 0)
            return null;
        return i1 % i2;
    }

    @Override
    public Number calculate(Long long1, Long long2) {
        if (long1 == null || long2 == null)
            return null;
        int i1 = long1.intValue();
        int i2 = long2.intValue();
        if (i2 == 0)
            return null;
        return i1 % i2;
    }

    @Override
    public Number calculate(BigInteger bigint1, BigInteger bigint2) {
        if (bigint1 == null || bigint2 == null)
            return null;
        int comp = bigint2.compareTo(BigInteger.ZERO);
        if (comp == 0) {
            return null;
        } else if (comp < 0) {
            return bigint1.negate().mod(bigint2).negate();
        } else {
            return bigint1.mod(bigint2);
        }
    }

    @Override
    public Number calculate(BigDecimal bigDecimal1, BigDecimal bigDecimal2) {
        throw new UnsupportedOperationException();
    }

}
