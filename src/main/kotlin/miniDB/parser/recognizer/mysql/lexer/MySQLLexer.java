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
 * (created at 2011-3-11)
 */
package miniDB.parser.recognizer.mysql.lexer;

import miniDB.parser.recognizer.mysql.MySQLToken;
import miniDB.parser.util.CharTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;

/**
 * support MySQL 5.5 token
 * MySQL词法分析器
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class MySQLLexer {
    private static int C_STYLE_COMMENT_VERSION = 50599;

    /**
     * @return previous value
     */
    public static int setCStyleCommentVersion(int version) {
        int v = C_STYLE_COMMENT_VERSION;
        C_STYLE_COMMENT_VERSION = version;
        return v;
    }

    /**
     * End of input character.
     * Used as a sentinel to denote the character one beyond the last defined character in a source file.
     * 输入字符结束
     * 用作标记，表示源文件中最后一个定义字符之外的字符
     */
    private final static byte EOI = 0x1A;

    /**
     * 待处理的SQL语句
     */
    protected final byte[] sql;

    /**
     * 结束游标
     * always be {@link #sql}.length - 1
     */
    protected final int eofIndex;

    /**
     * 当前游标
     * current index of {@link #sql}
     */
    protected int curIndex = -1;

    /**
     * curIndex位置的字符
     * always be {@link #sql}[{@link #curIndex}]
     */
    protected byte ch;

    // /** current token, set by {@link #nextToken()} */
    // private int tokenPos = 0;

    private MySQLToken token;

    /** keyword only */
    private MySQLToken tokenCache;

    private MySQLToken tokenCache2;

    /**
     * 代表一个参数
     * 1 represents first parameter
     */
    private int paramIndex = 0;

    /**
     * A character buffer for literals.
     * 文字的字符缓冲区
     */
    protected final static ThreadLocal<byte[]> sbufRef = new ThreadLocal<byte[]>();

    protected byte[] sbuf;

    private byte[] stringValue;

    /**
     * make sense only for {@link MySQLToken#IDENTIFIER}
     */
    private byte[] stringValueUppercase;

    /**
     * 语句中指定的版本
     */
    protected int version;

    public int getVersion() {
        return version;
    }

    /**
     * update {@link MySQLLexer#stringValue} and {@link MySQLLexer#stringValueUppercase}. It is
     * possible that {@link #sbuf} be changed
     */
    protected void updateStringValue(final byte[] src, final int srcOffset, final int len) {
        // QS_TODO [performance enhance]: use String constant for special
        // identifier, so that parser can use '==' rather than 'equals'
        stringValue = Arrays.copyOfRange(src, srcOffset, srcOffset + len);
        final int end = srcOffset + len;
        boolean lowerCase = false;
        int srcIndex = srcOffset;
        // int hash = 0;
        for (; srcIndex < end; ++srcIndex) {
            byte c = src[srcIndex];
            if (c >= 'a' && c <= 'z') {
                lowerCase = true;
                if (srcIndex > srcOffset) {
                    System.arraycopy(src, srcOffset, sbuf, 0, srcIndex - srcOffset);
                }
                break;
            }
            // hash = 31 * hash + c;
        }
        if (lowerCase) {
            for (int destIndex = srcIndex - srcOffset; destIndex < len; ++destIndex) {
                byte c = src[srcIndex++];
                // hash = 31 * hash + c;
                if (c >= 'a' && c <= 'z') {
                    sbuf[destIndex] = (byte) (c - 32);
                    // hash -= 32;
                } else {
                    sbuf[destIndex] = c;
                }
            }
            stringValueUppercase = Arrays.copyOfRange(sbuf, 0, len);
        } else {
            stringValueUppercase = Arrays.copyOfRange(src, srcOffset, srcOffset + len);
        }
    }

    /**
     * 根据SQL语句解析成词法（Token）
     * @param sql 待解析词法的SQL语句
     * @throws SQLSyntaxErrorException SQL语句解析异常抛出
     */
    public MySQLLexer(byte[] sql) throws SQLSyntaxErrorException {
        // 当文字的字符缓冲区未分配空间，进行分配空间
        if ((this.sbuf = sbufRef.get()) == null) {
            this.sbuf = new byte[1024];
            sbufRef.set(this.sbuf);
        }
        // 当SQL语句byte最后一位不是空格
        if (CharTypes.isWhitespace(sql[sql.length - 1])) {
            this.sql = sql;
        } else {
            this.sql = new byte[sql.length + 1];
            System.arraycopy(sql, 0, this.sql, 0, sql.length);
        }
        // 标记sql语句结束游标
        this.eofIndex = this.sql.length - 1;
        // 标记sql语句结束标记
        this.sql[this.eofIndex] = MySQLLexer.EOI;
        // 切换下一个SQL语句字符
        scanChar();
        // 下一个词法（Token）
        nextToken();
    }

    /**
     * 格式化SQL语句并传给带参语法解析器初始化
     * @param sql 待分析的SQL语句
     * @throws SQLSyntaxErrorException 语句异常抛出
     */
    public MySQLLexer(String sql) throws SQLSyntaxErrorException {
        this(fromSQL2Bytes(sql));
    }

    /**
     * 格式化SQL语句方法
     * @param sql 待处理的SQL语句
     * @return 处理后的SQL语句
     */
    private static byte[] fromSQL2Bytes(String sql) {
        if (CharTypes.isWhitespace(sql.charAt(sql.length() - 1))) {
            return sql.getBytes();
        }
        byte[] bytes = sql.getBytes();
        byte[] chars = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, chars, 0, bytes.length);
        chars[chars.length - 1] = ' ';
        return chars;
    }

    protected MySQLKeywords keywods = MySQLKeywords.DEFAULT_KEYWORDS;

    /**
     * @param token must be a keyword
     */
    public final void addCacheToke(MySQLToken token) {
        if (tokenCache != null) {
            tokenCache2 = token;
        } else {
            tokenCache = token;
        }
    }

    public final MySQLToken token() {
        if (tokenCache2 != null) {
            return tokenCache2;
        }
        if (tokenCache != null) {
            return tokenCache;
        }
        return token;
    }

    public final int getCurrentIndex() {
        return this.curIndex;
    }

    public final byte[] getSQL() {
        return sql;
    }

    public String getSQL(int start, int end) {
        byte[] c = new byte[end - start];
        System.arraycopy(sql, start, c, 0, c.length);
        return new String(c);
    }

    public int getOffsetCache() {
        return offsetCache;
    }

    public int getSizeCache() {
        return sizeCache;
    }

    /**
     * @return start from 1. When there is no parameter yet, return 0.
     */
    public int paramIndex() {
        return paramIndex;
    }

    /**
     * 获得SQL语句游标位置下一个Byte字符
     * @return 返回游标指向的下一个字符
     */
    protected final byte scanChar() {
        return ch = sql[++curIndex];
    }

    /**
     * @param skip if 1, then equals to {@link #scanChar()}
     */
    protected final byte scanChar(int skip) {
        return ch = sql[curIndex += skip];
    }

    protected final boolean hasChars(int howMany) {
        return curIndex + howMany <= eofIndex;
    }

    /**
     * 判断游标是否超过结束位置
     * @return true 没有超过 false 超过/当前位置处于EOF
     */
    protected final boolean eof() {
        return curIndex >= eofIndex;
    }

    private MySQLToken nextTokenInternal() throws SQLSyntaxErrorException {
        switch (ch) {
            case '0':
                switch (sql[curIndex + 1]) {
                    case 'x':
                        scanChar(2);
                        scanHexaDecimal(false);
                        return token;
                    case 'b':
                        scanChar(2);
                        scanBitField(false);
                        return token;
                }
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                scanNumber();
                return token;
            case '.':
                if (CharTypes.isDigit(sql[curIndex + 1])) {
                    scanNumber();
                } else {
                    scanChar();
                    token = MySQLToken.PUNC_DOT;
                }
                return token;
            case '\'':
            case '"':
                scanString();
                return token;
            case 'n':
            case 'N':
                if (sql[curIndex + 1] == '\'') {
                    scanChar();
                    scanString();
                    token = MySQLToken.LITERAL_NCHARS;
                    return token;
                }
                scanIdentifier();
                return token;
            case 'x':
            case 'X':
                if (sql[curIndex + 1] == '\'') {
                    scanChar(2);
                    scanHexaDecimal(true);
                    return token;
                }
                scanIdentifier();
                return token;
            case 'b':
            case 'B':
                if (sql[curIndex + 1] == '\'') {
                    scanChar(2);
                    scanBitField(true);
                    return token;
                }
                scanIdentifier();
                return token;
            case '@':
                if (sql[curIndex + 1] == '@') {
                    scanSystemVariable();
                    return token;
                }
                scanUserVariable();
                return token;
            case '?':
                scanChar();
                token = MySQLToken.QUESTION_MARK;
                ++paramIndex;
                return token;
            case '(':
                scanChar();
                token = MySQLToken.PUNC_LEFT_PAREN;
                return token;
            case ')':
                scanChar();
                token = MySQLToken.PUNC_RIGHT_PAREN;
                return token;
            case '[':
                scanChar();
                token = MySQLToken.PUNC_LEFT_BRACKET;
                return token;
            case ']':
                scanChar();
                token = MySQLToken.PUNC_RIGHT_BRACKET;
                return token;
            case '{':
                scanChar();
                token = MySQLToken.PUNC_LEFT_BRACE;
                return token;
            case '}':
                scanChar();
                token = MySQLToken.PUNC_RIGHT_BRACE;
                return token;
            case ',':
                scanChar();
                token = MySQLToken.PUNC_COMMA;
                return token;
            case ';':
                scanChar();
                token = MySQLToken.PUNC_SEMICOLON;
                return token;
            case ':':
                if (sql[curIndex + 1] == '=') {
                    scanChar(2);
                    token = MySQLToken.OP_ASSIGN;
                    return token;
                }
                scanChar();
                token = MySQLToken.PUNC_COLON;
                return token;
            case '=':
                scanChar();
                token = MySQLToken.OP_EQUALS;
                return token;
            case '~':
                scanChar();
                token = MySQLToken.OP_TILDE;
                return token;
            case '*':
                if (inCStyleComment && sql[curIndex + 1] == '/') {
                    inCStyleComment = false;
                    inCStyleCommentIgnore = false;
                    scanChar(2);
                    token = MySQLToken.PUNC_C_STYLE_COMMENT_END;
                    return token;
                }
                scanChar();
                token = MySQLToken.OP_ASTERISK;
                return token;
            case '-':
                scanChar();
                token = MySQLToken.OP_MINUS;
                return token;
            case '+':
                scanChar();
                token = MySQLToken.OP_PLUS;
                return token;
            case '^':
                scanChar();
                token = MySQLToken.OP_CARET;
                return token;
            case '/':
                scanChar();
                token = MySQLToken.OP_SLASH;
                return token;
            case '%':
                scanChar();
                token = MySQLToken.OP_PERCENT;
                return token;
            case '&':
                if (sql[curIndex + 1] == '&') {
                    scanChar(2);
                    token = MySQLToken.OP_LOGICAL_AND;
                    return token;
                }
                scanChar();
                token = MySQLToken.OP_AMPERSAND;
                return token;
            case '|':
                if (sql[curIndex + 1] == '|') {
                    scanChar(2);
                    token = MySQLToken.OP_LOGICAL_OR;
                    return token;
                }
                scanChar();
                token = MySQLToken.OP_VERTICAL_BAR;
                return token;
            case '!':
                if (sql[curIndex + 1] == '=') {
                    scanChar(2);
                    token = MySQLToken.OP_NOT_EQUALS;
                    return token;
                }
                scanChar();
                token = MySQLToken.OP_EXCLAMATION;
                return token;
            case '>':
                switch (sql[curIndex + 1]) {
                    case '=':
                        scanChar(2);
                        token = MySQLToken.OP_GREATER_OR_EQUALS;
                        return token;
                    case '>':
                        scanChar(2);
                        token = MySQLToken.OP_RIGHT_SHIFT;
                        return token;
                    default:
                        scanChar();
                        token = MySQLToken.OP_GREATER_THAN;
                        return token;
                }
            case '<':
                switch (sql[curIndex + 1]) {
                    case '=':
                        if (sql[curIndex + 2] == '>') {
                            scanChar(3);
                            token = MySQLToken.OP_NULL_SAFE_EQUALS;
                            return token;
                        }
                        scanChar(2);
                        token = MySQLToken.OP_LESS_OR_EQUALS;
                        return token;
                    case '>':
                        scanChar(2);
                        token = MySQLToken.OP_LESS_OR_GREATER;
                        return token;
                    case '<':
                        scanChar(2);
                        token = MySQLToken.OP_LEFT_SHIFT;
                        return token;
                    default:
                        scanChar();
                        token = MySQLToken.OP_LESS_THAN;
                        return token;
                }
            case '`':
                scanIdentifierWithAccent();
                return token;
            default:
                if (CharTypes.isIdentifierChar(ch)) {
                    scanIdentifier();
                } else if (eof()) {
                    token = MySQLToken.EOF;
                    curIndex = eofIndex;
                    // tokenPos = curIndex;
                } else {
                    throw err("unsupported character: " + ch);
                }
                return token;
        }
    }

    /**
     * 切换下一个Token命令
     * @return
     * @throws SQLSyntaxErrorException SQL语句异常抛出
     */
    public MySQLToken nextToken() throws SQLSyntaxErrorException {
        lastIndex = curIndex;
        if (tokenCache2 != null) {
            tokenCache2 = null;
            return tokenCache;
        }
        if (tokenCache != null) {
            tokenCache = null;
            return token;
        }
        if (token == MySQLToken.EOF) {
            throw new SQLSyntaxErrorException(
                    "eof for sql is already reached, cannot get new token");
        }
        // 创建词法实体
        MySQLToken t;
        do {
            skipSeparator();
            t = nextTokenInternal();
        } while (inCStyleComment && inCStyleCommentIgnore
                || MySQLToken.PUNC_C_STYLE_COMMENT_END == t);
        return t;
    }

    protected int lastIndex;

    protected boolean inCStyleComment;

    protected boolean inCStyleCommentIgnore;

    protected int offsetCache;

    protected int sizeCache;

    /**
     * first <code>@</code> is included
     */
    protected void scanUserVariable() throws SQLSyntaxErrorException {
        if (ch != '@')
            throw err("first char must be @");
        offsetCache = curIndex;
        sizeCache = 1;

        boolean dq = false;
        switch (scanChar()) {
            case '"':
                dq = true;
            case '\'':
                loop1: for (++sizeCache;; ++sizeCache) {
                    switch (scanChar()) {
                        case '\\':
                            ++sizeCache;
                            scanChar();
                            break;
                        case '"':
                            if (dq) {
                                ++sizeCache;
                                if (scanChar() == '"') {
                                    break;
                                }
                                break loop1;
                            }
                            break;
                        case '\'':
                            if (!dq) {
                                ++sizeCache;
                                if (scanChar() == '\'') {
                                    break;
                                }
                                break loop1;
                            }
                            break;
                    }
                }
                break;
            case '`':
                loop1: for (++sizeCache;; ++sizeCache) {
                    switch (scanChar()) {
                        case '`':
                            ++sizeCache;
                            if (scanChar() == '`') {
                                break;
                            }
                            break loop1;
                    }
                }
                break;
            default:
                for (; CharTypes.isIdentifierChar(ch) || ch == '.'; ++sizeCache) {
                    scanChar();
                }
        }

        stringValue = Arrays.copyOfRange(sql, offsetCache, offsetCache + sizeCache);
        token = MySQLToken.USR_VAR;
    }

    /**
     * first <code>@@</code> is included
     */
    protected void scanSystemVariable() throws SQLSyntaxErrorException {
        if (ch != '@' || sql[curIndex + 1] != '@')
            throw err("first char must be @@");
        offsetCache = curIndex + 2;
        sizeCache = 0;
        scanChar(2);
        if (ch == '`') {
            for (++sizeCache;; ++sizeCache) {
                if (scanChar() == '`') {
                    ++sizeCache;
                    if (scanChar() != '`') {
                        break;
                    }
                }
            }
        } else {
            for (; CharTypes.isIdentifierChar(ch); ++sizeCache) {
                scanChar();
            }
        }
        updateStringValue(sql, offsetCache, sizeCache);
        token = MySQLToken.SYS_VAR;
    }

    protected void scanString() throws SQLSyntaxErrorException {
        boolean dq = false;
        if (ch == '\'') {
        } else if (ch == '"') {
            dq = true;
        } else {
            throw err("first char must be \" or '");
        }

        offsetCache = curIndex;
        int size = 1;
        sbuf[0] = '\'';
        if (dq) {
            loop: while (true) {
                switch (scanChar()) {
                    case '\'':
                        pubByte((byte) '\\', size++);
                        pubByte((byte) '\'', size++);
                        break;
                    case '\\':
                        pubByte((byte) '\\', size++);
                        pubByte(scanChar(), size++);
                        continue;
                    case '"':
                        if (sql[curIndex + 1] == '"') {
                            pubByte((byte) '"', size++);
                            scanChar();
                            continue;
                        }
                        pubByte((byte) '\'', size++);
                        scanChar();
                        break loop;
                    default:
                        if (eof()) {
                            throw err("unclosed string");
                        }
                        pubByte(ch, size++);
                        continue;
                }
            }
        } else {
            loop: while (true) {
                switch (scanChar()) {
                    case '\\':
                        pubByte((byte) '\\', size++);
                        pubByte(scanChar(), size++);
                        continue;
                    case '\'':
                        if (sql[curIndex + 1] == '\'') {
                            pubByte((byte) '\\', size++);
                            pubByte(scanChar(), size++);
                            continue;
                        }
                        pubByte((byte) '\'', size++);
                        scanChar();
                        break loop;
                    default:
                        if (eof()) {
                            throw err("unclosed string");
                        }
                        pubByte(ch, size++);
                        continue;
                }
            }
        }

        sizeCache = size;
        stringValue = Arrays.copyOfRange(sbuf, 0, size);
        token = MySQLToken.LITERAL_CHARS;
    }

    /**
     * Append a character to sbuf.
     */
    protected final void pubByte(byte ch, int index) {
        if (index >= sbuf.length) {
            byte[] newsbuf = new byte[sbuf.length * 2];
            System.arraycopy(sbuf, 0, newsbuf, 0, sbuf.length);
            sbuf = newsbuf;
        }
        sbuf[index] = ch;
    }

    /**
     * @param quoteMode if false: first <code>0x</code> has been skipped; if true: first
     *        <code>x'</code> has been skipped
     */
    protected void scanHexaDecimal(boolean quoteMode) throws SQLSyntaxErrorException {
        offsetCache = curIndex;
        for (; CharTypes.isHex(ch); scanChar());

        sizeCache = curIndex - offsetCache;
        // if (sizeCache <= 0) {
        // throw err("expect at least one hexdigit");
        // }
        if (quoteMode) {
            if (ch != '\'') {
                throw err("invalid char for hex: " + ch);
            }
            scanChar();
        } else if (CharTypes.isIdentifierChar(ch)) {
            scanIdentifierFromNumber(offsetCache - 2, sizeCache + 2);
            return;
        }

        token = MySQLToken.LITERAL_HEX;
    }

    /**
     * @param quoteMode if false: first <code>0b</code> has been skipped; if true: first
     *        <code>b'</code> has been skipped
     */
    protected void scanBitField(boolean quoteMode) throws SQLSyntaxErrorException {
        offsetCache = curIndex;
        for (; ch == '0' || ch == '1'; scanChar());
        sizeCache = curIndex - offsetCache;
        // if (sizeCache <= 0) {
        // throw err("expect at least one bit");
        // }
        if (quoteMode) {
            if (ch != '\'') {
                throw err("invalid char for bit: " + ch);
            }
            scanChar();
        } else if (CharTypes.isIdentifierChar(ch)) {
            scanIdentifierFromNumber(offsetCache - 2, sizeCache + 2);
            return;
        }

        token = MySQLToken.LITERAL_BIT;
        stringValue = Arrays.copyOfRange(sql, offsetCache, offsetCache + sizeCache);
    }

    /**
     * if first char is <code>.</code>, token may be {@link MySQLToken#PUNC_DOT} if invalid char is
     * presented after <code>.</code>
     */
    protected void scanNumber() throws SQLSyntaxErrorException {
        offsetCache = curIndex;
        sizeCache = 1;
        final boolean fstDot = ch == '.';
        boolean dot = fstDot;
        boolean sign = false;
        int state = fstDot ? 1 : 0;

        for (; scanChar() != MySQLLexer.EOI; ++sizeCache) {
            switch (state) {
                case 0:
                    if (CharTypes.isDigit(ch)) {
                    } else if (ch == '.') {
                        dot = true;
                        state = 1;
                    } else if (ch == 'e' || ch == 'E') {
                        state = 3;
                    } else if (CharTypes.isIdentifierChar(ch)) {
                        scanIdentifierFromNumber(offsetCache, sizeCache);
                        return;
                    } else {
                        token = MySQLToken.LITERAL_NUM_PURE_DIGIT;
                        return;
                    }
                    break;
                case 1:
                    if (CharTypes.isDigit(ch)) {
                        state = 2;
                    } else if (ch == 'e' || ch == 'E') {
                        state = 3;
                    } else if (CharTypes.isIdentifierChar(ch) && fstDot) {
                        sizeCache = 1;
                        ch = sql[curIndex = offsetCache + 1];
                        token = MySQLToken.PUNC_DOT;
                        return;
                    } else {
                        token = MySQLToken.LITERAL_NUM_MIX_DIGIT;
                        return;
                    }
                    break;
                case 2:
                    if (CharTypes.isDigit(ch)) {
                    } else if (ch == 'e' || ch == 'E') {
                        state = 3;
                    } else if (CharTypes.isIdentifierChar(ch) && fstDot) {
                        sizeCache = 1;
                        ch = sql[curIndex = offsetCache + 1];
                        token = MySQLToken.PUNC_DOT;
                        return;
                    } else {
                        token = MySQLToken.LITERAL_NUM_MIX_DIGIT;
                        return;
                    }
                    break;
                case 3:
                    if (CharTypes.isDigit(ch)) {
                        state = 5;
                    } else if (ch == '+' || ch == '-') {
                        sign = true;
                        state = 4;
                    } else if (fstDot) {
                        sizeCache = 1;
                        ch = sql[curIndex = offsetCache + 1];
                        token = MySQLToken.PUNC_DOT;
                        return;
                    } else if (!dot) {
                        if (CharTypes.isIdentifierChar(ch)) {
                            scanIdentifierFromNumber(offsetCache, sizeCache);
                        } else {
                            updateStringValue(sql, offsetCache, sizeCache);
                            MySQLToken tok = keywods.getKeyword(stringValueUppercase);
                            token = tok == null ? MySQLToken.IDENTIFIER : tok;
                        }
                        return;
                    } else {
                        throw err("invalid char after '.' and 'e' for as part of number: " + ch);
                    }
                    break;
                case 4:
                    if (CharTypes.isDigit(ch)) {
                        state = 5;
                        break;
                    } else if (fstDot) {
                        sizeCache = 1;
                        ch = sql[curIndex = offsetCache + 1];
                        token = MySQLToken.PUNC_DOT;
                    } else if (!dot) {
                        ch = sql[--curIndex];
                        --sizeCache;
                        updateStringValue(sql, offsetCache, sizeCache);
                        MySQLToken tok = keywods.getKeyword(stringValueUppercase);
                        token = tok == null ? MySQLToken.IDENTIFIER : tok;
                    } else {
                        throw err("expect digit char after SIGN for 'e': " + ch);
                    }
                    return;
                case 5:
                    if (CharTypes.isDigit(ch)) {
                        break;
                    } else if (CharTypes.isIdentifierChar(ch)) {
                        if (fstDot) {
                            sizeCache = 1;
                            ch = sql[curIndex = offsetCache + 1];
                            token = MySQLToken.PUNC_DOT;
                        } else if (!dot) {
                            if (sign) {
                                ch = sql[curIndex = offsetCache];
                                scanIdentifierFromNumber(curIndex, 0);
                            } else {
                                scanIdentifierFromNumber(offsetCache, sizeCache);
                            }
                        } else {
                            token = MySQLToken.LITERAL_NUM_MIX_DIGIT;
                        }
                    } else {
                        token = MySQLToken.LITERAL_NUM_MIX_DIGIT;
                    }
                    return;
            }
        }
        switch (state) {
            case 0:
                token = MySQLToken.LITERAL_NUM_PURE_DIGIT;
                return;
            case 1:
                if (fstDot) {
                    token = MySQLToken.PUNC_DOT;
                    return;
                }
            case 2:
            case 5:
                token = MySQLToken.LITERAL_NUM_MIX_DIGIT;
                return;
            case 3:
                if (fstDot) {
                    sizeCache = 1;
                    ch = sql[curIndex = offsetCache + 1];
                    token = MySQLToken.PUNC_DOT;
                } else if (!dot) {
                    updateStringValue(sql, offsetCache, sizeCache);
                    MySQLToken tok = keywods.getKeyword(stringValueUppercase);
                    token = tok == null ? MySQLToken.IDENTIFIER : tok;
                } else {
                    throw err("expect digit char after SIGN for 'e': " + ch);
                }
                return;
            case 4:
                if (fstDot) {
                    sizeCache = 1;
                    ch = sql[curIndex = offsetCache + 1];
                    token = MySQLToken.PUNC_DOT;
                } else if (!dot) {
                    ch = sql[--curIndex];
                    --sizeCache;
                    updateStringValue(sql, offsetCache, sizeCache);
                    MySQLToken tok = keywods.getKeyword(stringValueUppercase);
                    token = tok == null ? MySQLToken.IDENTIFIER : tok;
                } else {
                    throw err("expect digit char after SIGN for 'e': " + ch);
                }
                return;
        }
    }

    /**
     * 从数字扫描标识符（ID）
     * NOTE: {@link MySQLToken#IDENTIFIER id} dosn't include <code>'.'</code> for sake of
     * performance issue (based on <i>shaojin.wensj</i>'s design). However, it is not convenient for
     * MySQL compatibility. e.g. <code>".123f"</code> will be regarded as <code>".123"</code> and
     * <code>"f"</code> in MySQL, but in this {@link MySQLLexer}, it will be <code>"."</code> and
     * <code>"123f"</code> because <code>".123f"</code> may be part of <code>"db1.123f"</code> and
     * <code>"123f"</code> is the table name.
     * 
     * @param initSize how many char has already been consumed
     */
    private void scanIdentifierFromNumber(int initOffset, int initSize)
            throws SQLSyntaxErrorException {
        offsetCache = initOffset;
        sizeCache = initSize;
        // 当前字符如果是标识字符，增加缓存大小并将游标移动至下一个字符
        for (; CharTypes.isIdentifierChar(ch); ++sizeCache) {
            scanChar();
        }
        // 更新字符串的值
        updateStringValue(sql, offsetCache, sizeCache);
        MySQLToken tok = keywods.getKeyword(stringValueUppercase);
        token = tok == null ? MySQLToken.IDENTIFIER : tok;
    }

    /**
     * id is NOT included in <code>`</code>.
     */
    protected void scanIdentifier() throws SQLSyntaxErrorException {
        if (ch == '$') {
            if (scanChar() == '{') {
                scanPlaceHolder();
            } else {
                scanIdentifierFromNumber(curIndex - 1, 1);
            }
        } else {
            scanIdentifierFromNumber(curIndex, 0);
        }
    }

    /**
     * not SQL syntax
     */
    protected void scanPlaceHolder() throws SQLSyntaxErrorException {
        offsetCache = curIndex + 1;
        sizeCache = 0;
        for (scanChar(); ch != '}' && !eof(); ++sizeCache) {
            scanChar();
        }
        if (ch == '}')
            scanChar();
        updateStringValue(sql, offsetCache, sizeCache);
        token = MySQLToken.PLACE_HOLDER;
    }

    /**
     * id is included in <code>`</code>. first <code>`</code> is included
     */
    protected void scanIdentifierWithAccent() throws SQLSyntaxErrorException {
        offsetCache = curIndex;
        for (; scanChar() != MySQLLexer.EOI;) {
            if (ch == '`' && scanChar() != '`') {
                break;
            }
        }
        updateStringValue(sql, offsetCache, sizeCache = curIndex - offsetCache);
        token = MySQLToken.IDENTIFIER;
    }

    /**
     * 跳过空格和命令
     * skip whitespace and comment
     */
    protected void skipSeparator() {
        while (!eof()) {
            for (; CharTypes.isWhitespace(ch); scanChar());

            switch (ch) {
                case '#': // MySQL specified
                    for (; scanChar() != '\n';) {
                        if (eof()) {
                            return;
                        }
                    }
                    scanChar();
                    continue;
                case '/':
                    if (hasChars(2) && '*' == sql[curIndex + 1]) {
                        boolean commentSkip;
                        if ('!' == sql[curIndex + 2]) {
                            scanChar(3);
                            inCStyleComment = true;
                            inCStyleCommentIgnore = false;
                            commentSkip = false;
                            // MySQL use 5 digits to indicate version. 50508 means
                            // MySQL 5.5.8
                            if (hasChars(5) && CharTypes.isDigit(ch)
                                    && CharTypes.isDigit(sql[curIndex + 1])
                                    && CharTypes.isDigit(sql[curIndex + 2])
                                    && CharTypes.isDigit(sql[curIndex + 3])
                                    && CharTypes.isDigit(sql[curIndex + 4])) {
                                int version = ch - '0';
                                version *= 10;
                                version += sql[curIndex + 1] - '0';
                                version *= 10;
                                version += sql[curIndex + 2] - '0';
                                version *= 10;
                                version += sql[curIndex + 3] - '0';
                                version *= 10;
                                version += sql[curIndex + 4] - '0';
                                scanChar(5);
                                this.version = version;
                                if (version > C_STYLE_COMMENT_VERSION) {
                                    inCStyleCommentIgnore = true;
                                }
                            }
                            skipSeparator();
                        } else {
                            scanChar(2);
                            commentSkip = true;
                        }

                        if (commentSkip) {
                            for (int state = 0; !eof(); scanChar()) {
                                if (state == 0) {
                                    if ('*' == ch) {
                                        state = 1;
                                    }
                                } else {
                                    if ('/' == ch) {
                                        scanChar();
                                        break;
                                    } else if ('*' != ch) {
                                        state = 0;
                                    }
                                }
                            }
                            continue;
                        }
                    }
                    return;
                case '-':
                    if (hasChars(3) && '-' == sql[curIndex + 1]
                            && CharTypes.isWhitespace(sql[curIndex + 2])) {
                        scanChar(3);
                        for (; !eof(); scanChar()) {
                            if ('\n' == ch) {
                                scanChar();
                                break;
                            }
                        }
                        continue;
                    }
                default:
                    return;
            }
        }
    }

    /**
     * always throw SQLSyntaxErrorException
     */
    protected SQLSyntaxErrorException err(String msg) throws SQLSyntaxErrorException {
        String errMsg = msg + ". " + toString();
        throw new SQLSyntaxErrorException(errMsg);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append('@').append(hashCode()).append('{');
        String sqlLeft = new String(sql, curIndex, sql.length - curIndex);
        sb.append("curIndex=").append(curIndex).append(", ch=").append(ch).append(", token=")
                .append(token).append(", sqlLeft=").append(sqlLeft).append(", sql=")
                .append(new String(sql));
        sb.append('}');
        return sb.toString();
    }

    /**
     * {@link #token} must be {@link MySQLToken#LITERAL_NUM_PURE_DIGIT}
     */
    public Number integerValue() {
        // 2147483647
        // 9223372036854775807
        if (sizeCache < 10 || sizeCache == 10 && (sql[offsetCache] < '2'
                || sql[offsetCache] == '2' && sql[offsetCache + 1] == '0')) {
            int rst = 0;
            int end = offsetCache + sizeCache;
            for (int i = offsetCache; i < end; ++i) {
                rst = (rst << 3) + (rst << 1);
                rst += sql[i] - '0';
            }
            return rst;
        } else if (sizeCache < 19 || sizeCache == 19 && sql[offsetCache] < '9') {
            long rst = 0;
            int end = offsetCache + sizeCache;
            for (int i = offsetCache; i < end; ++i) {
                rst = (rst << 3) + (rst << 1);
                rst += sql[i] - '0';
            }
            return rst;
        } else {
            return new BigInteger(new String(sql, offsetCache, sizeCache), 10);
        }
    }

    public BigDecimal decimalValue() {
        // QS_TODO [performance enhance]: prevent BigDecimal's parser
        return new BigDecimal(new String(sql, offsetCache, sizeCache));
    }

    /**
     * if {@link #stringValue()} returns "'abc\\'d'", then "abc\\'d" is appended
     */
    public void appendStringContent(List<Byte> bytes) {
        for (int i = 1, size = sizeCache - 1; i < size; i++) {
            bytes.add(sbuf[i]);
        }
    }

    public byte[] getStringContent() {
        return Arrays.copyOfRange(sbuf, 1, sizeCache - 1);
    }

    /**
     * make sense for those types of token:<br/>
     * {@link MySQLToken#USR_VAR}: e.g. "@var1", "@'mary''s'";<br/>
     * {@link MySQLToken#SYS_VAR}: e.g. "var2";<br/>
     * {@link MySQLToken#LITERAL_CHARS}, {@link MySQLToken#LITERAL_NCHARS}: e.g. "'ab\\'c'";<br/>
     * {@link MySQLToken#LITERAL_BIT}: e.g. "0101" <br/>
     * {@link MySQLToken#IDENTIFIER}
     */
    public final String stringValue() {
        return new String(stringValue);
    }

    public final byte[] byteValue() {
        return stringValue;
    }

    /**
     * for {@link MySQLToken#IDENTIFIER}, {@link MySQLToken#SYS_VAR}
     */
    public final String stringValueUppercase() {
        return new String(stringValueUppercase);
    }

    public int getLastIndex() {
        return lastIndex;
    }

}
