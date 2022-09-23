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

import java.util.Map;

/**
 * literal date is also possible
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class LiteralString extends Literal {
    private final String introducer;
    private final byte[] data;
    private final boolean nchars;

    /**
     * @param string content of string, excluded of head and tail "'". e.g. for string token of
     *        "'don\\'t'", argument of string is "don\\'t"
     */
    public LiteralString(String introducer, byte[] data, boolean nchars) {
        super();
        this.introducer = introducer;
        if (data == null)
            throw new IllegalArgumentException("argument string is null!");
        this.data = data;
        this.nchars = nchars;
    }

    public String getIntroducer() {
        return introducer;
    }

    public byte[] getBytes() {
        return data;
    }

    public boolean isNchars() {
        return nchars;
    }

    public String getUnescapedString() {
        return getUnescapedString(data, false);
    }

    public String getUnescapedString(boolean toUppercase) {
        return getUnescapedString(data, toUppercase);
    }

    public static String getUnescapedString(byte[] string) {
        return getUnescapedString(string, false);
    }

    public static String getUnescapedString(byte[] string, boolean toUppercase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < string.length; ++i) {
            byte c = string[i];
            if (c == '\\') {
                switch (c = string[++i]) {
                    case '0':
                        sb.append('\0');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'Z':
                        sb.append((char) 26);
                        break;
                    default:
                        sb.append(c);
                }
            } else if (c == '\'') {
                ++i;
                sb.append('\'');
            } else {
                if (toUppercase && c >= 'a' && c <= 'z')
                    c -= 32;
                sb.append((char) c);
            }
        }
        return sb.toString();
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        if (data == null)
            return null;
        return getUnescapedString();
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
