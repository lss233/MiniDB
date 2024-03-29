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
package miniDB.parser.util;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author shaojin.wensj
 */
public class CharTypes {
    private final static boolean[] hexFlags = new boolean[256];
    static {
        for (char c = 0; c < hexFlags.length; ++c) {
            if (c >= 'A' && c <= 'F') {
                hexFlags[c] = true;
            } else if (c >= 'a' && c <= 'f') {
                hexFlags[c] = true;
            } else if (c >= '0' && c <= '9') {
                hexFlags[c] = true;
            }
        }
    }

    public static boolean isHex(char c) {
        return c < 256 && hexFlags[c];
    }

    public static boolean isHex(byte c) {
        return hexFlags[c];
    }

    public static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    public static boolean isDigit(byte c) {
        return c >= '0' && c <= '9';
    }

    private final static boolean[] identifierFlags = new boolean[256];
    static {
        for (char c = 0; c < identifierFlags.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                identifierFlags[c] = true;
            } else if (c >= 'a' && c <= 'z') {
                identifierFlags[c] = true;
            } else if (c >= '0' && c <= '9') {
                identifierFlags[c] = true;
            }
        }
        identifierFlags['_'] = true;
        identifierFlags['$'] = true;
    }

    public static boolean isIdentifierChar(char c) {
        return c > identifierFlags.length || identifierFlags[c];
    }

    /**
     * 判断是否是ID标识字符
     * @param c 待判断的字符
     * @return true 是 false 不是
     */
    public static boolean isIdentifierChar(byte c) {
        return c < 0 || identifierFlags[c];
    }

    private final static boolean[] whitespaceFlags = new boolean[256];
    static {
        whitespaceFlags[' '] = true;
        whitespaceFlags['\n'] = true;
        whitespaceFlags['\r'] = true;
        whitespaceFlags['\t'] = true;
        whitespaceFlags['\f'] = true;
        whitespaceFlags['\b'] = true;
    }

    /**
     * @return false if
     */
    public static boolean isWhitespace(char c) {
        return c <= whitespaceFlags.length && whitespaceFlags[c];
    }

    public static boolean isWhitespace(byte b) {
        return b > 0 && whitespaceFlags[b];
    }

}
