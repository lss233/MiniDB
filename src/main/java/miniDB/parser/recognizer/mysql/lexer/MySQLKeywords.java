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
 * Project: fastjson
 * 
 * File Created at 2010-12-2
 * 
 * Copyright 1999-2100 Alibaba.com Corporation Limited. All rights reserved.
 *
 * This software is the confidential and proprietary information of Alibaba Company.
 * ("Confidential Information"). You shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement you entered into with Alibaba.com.
 */
package miniDB.parser.recognizer.mysql.lexer;

import miniDB.parser.recognizer.mysql.MySQLToken;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
class MySQLKeywords {
    public static final MySQLKeywords DEFAULT_KEYWORDS = new MySQLKeywords();

    private final Map<String, MySQLToken> keywords = new HashMap<String, MySQLToken>(230);

    private MySQLKeywords() {
        for (MySQLToken type : MySQLToken.class.getEnumConstants()) {
            String name = type.name();
            if (name.startsWith("KW_")) {
                String kw = name.substring("KW_".length());
                keywords.put(kw, type);
            }
        }
        keywords.put("NULL", MySQLToken.LITERAL_NULL);
        keywords.put("FALSE", MySQLToken.LITERAL_BOOL_FALSE);
        keywords.put("TRUE", MySQLToken.LITERAL_BOOL_TRUE);
    }

    /**
     * @param keyUpperCase must be uppercase
     * @return <code>KeyWord</code> or {@link MySQLToken#LITERAL_NULL NULL} or
     *         {@link MySQLToken#LITERAL_BOOL_FALSE FALSE} or {@link MySQLToken#LITERAL_BOOL_TRUE
     *         TRUE}
     */
    public MySQLToken getKeyword(String keyUpperCase) {
        return keywords.get(keyUpperCase);
    }

    public MySQLToken getKeyword(byte[] keyUpperCase) {
        if (keyUpperCase == null || keyUpperCase.length < 2) {
            return null;
        }
        switch (keyUpperCase[0]) {
            case 'A': {
                if (keyUpperCase.length < 2 || keyUpperCase.length > 10) {
                    return null;
                }
                if (keyUpperCase.length == 10 && keyUpperCase[1] == 'C' && keyUpperCase[2] == 'C'
                        && keyUpperCase[3] == 'E' && keyUpperCase[4] == 'S'
                        && keyUpperCase[5] == 'S' && keyUpperCase[6] == 'I'
                        && keyUpperCase[7] == 'B' && keyUpperCase[8] == 'L'
                        && keyUpperCase[9] == 'E') {
                    return MySQLToken.KW_ACCESSIBLE;
                }
                if (keyUpperCase.length == 3 && keyUpperCase[1] == 'D' && keyUpperCase[2] == 'D') {
                    return MySQLToken.KW_ADD;
                }
                if (keyUpperCase[1] == 'L') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 5) {
                        return null;
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'L') {
                        return MySQLToken.KW_ALL;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'T' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'R') {
                        return MySQLToken.KW_ALTER;
                    }
                }
                if (keyUpperCase[1] == 'N') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'A' && keyUpperCase[3] == 'L'
                            && keyUpperCase[4] == 'Y' && keyUpperCase[5] == 'Z'
                            && keyUpperCase[6] == 'E') {
                        return MySQLToken.KW_ANALYZE;
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'D') {
                        return MySQLToken.KW_AND;
                    }
                }
                if (keyUpperCase[1] == 'S') {
                    if (keyUpperCase.length < 2 || keyUpperCase.length > 10) {
                        return null;
                    }
                    if (keyUpperCase.length == 2) {
                        return MySQLToken.KW_AS;
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'C') {
                        return MySQLToken.KW_ASC;
                    }
                    if (keyUpperCase.length == 10 && keyUpperCase[2] == 'E'
                            && keyUpperCase[3] == 'N' && keyUpperCase[4] == 'S'
                            && keyUpperCase[5] == 'I' && keyUpperCase[6] == 'T'
                            && keyUpperCase[7] == 'I' && keyUpperCase[8] == 'V'
                            && keyUpperCase[9] == 'E') {
                        return MySQLToken.KW_ASENSITIVE;
                    }
                }
                return null;
            }
            case 'B': {
                if (keyUpperCase.length < 2 || keyUpperCase.length > 7) {
                    return null;
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'F' && keyUpperCase[3] == 'O'
                            && keyUpperCase[4] == 'R' && keyUpperCase[5] == 'E') {
                        return MySQLToken.KW_BEFORE;
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'T' && keyUpperCase[3] == 'W'
                            && keyUpperCase[4] == 'E' && keyUpperCase[5] == 'E'
                            && keyUpperCase[6] == 'N') {
                        return MySQLToken.KW_BETWEEN;
                    }
                }
                if (keyUpperCase[1] == 'I') {
                    if (keyUpperCase.length != 6) {
                        return null;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'G' && keyUpperCase[3] == 'I'
                            && keyUpperCase[4] == 'N' && keyUpperCase[5] == 'T') {
                        return MySQLToken.KW_BIGINT;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'N' && keyUpperCase[3] == 'A'
                            && keyUpperCase[4] == 'R' && keyUpperCase[5] == 'Y') {
                        return MySQLToken.KW_BINARY;
                    }
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'L' && keyUpperCase[2] == 'O'
                        && keyUpperCase[3] == 'B') {
                    return MySQLToken.KW_BLOB;
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'O' && keyUpperCase[2] == 'T'
                        && keyUpperCase[3] == 'H') {
                    return MySQLToken.KW_BOTH;
                }
                if (keyUpperCase.length == 2 && keyUpperCase[1] == 'Y') {
                    return MySQLToken.KW_BY;
                }
                return null;
            }
            case 'C': {
                if (keyUpperCase.length < 4 || keyUpperCase.length > 17) {
                    return null;
                }
                if (keyUpperCase[1] == 'A') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase.length == 4 && keyUpperCase[2] == 'L'
                            && keyUpperCase[3] == 'L') {
                        return MySQLToken.KW_CALL;
                    }
                    if (keyUpperCase[2] == 'S') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'C'
                                && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'D'
                                && keyUpperCase[6] == 'E') {
                            return MySQLToken.KW_CASCADE;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == 'E') {
                            return MySQLToken.KW_CASE;
                        }
                    }
                }
                if (keyUpperCase[1] == 'H') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 9) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'A') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 9) {
                            return null;
                        }
                        if (keyUpperCase.length == 6 && keyUpperCase[3] == 'N'
                                && keyUpperCase[4] == 'G' && keyUpperCase[5] == 'E') {
                            return MySQLToken.KW_CHANGE;
                        }
                        if (keyUpperCase[3] == 'R') {
                            if (keyUpperCase.length < 4 || keyUpperCase.length > 9) {
                                return null;
                            }
                            if (keyUpperCase.length == 4) {
                                return MySQLToken.KW_CHAR;
                            }
                            if (keyUpperCase.length == 9 && keyUpperCase[4] == 'A'
                                    && keyUpperCase[5] == 'C' && keyUpperCase[6] == 'T'
                                    && keyUpperCase[7] == 'E' && keyUpperCase[8] == 'R') {
                                return MySQLToken.KW_CHARACTER;
                            }
                        }
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'E' && keyUpperCase[3] == 'C'
                            && keyUpperCase[4] == 'K') {
                        return MySQLToken.KW_CHECK;
                    }
                }
                if (keyUpperCase[1] == 'O') {
                    if (keyUpperCase.length < 6 || keyUpperCase.length > 10) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'L') {
                        if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'L'
                                && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'T'
                                && keyUpperCase[6] == 'E') {
                            return MySQLToken.KW_COLLATE;
                        }
                        if (keyUpperCase.length == 6 && keyUpperCase[3] == 'U'
                                && keyUpperCase[4] == 'M' && keyUpperCase[5] == 'N') {
                            return MySQLToken.KW_COLUMN;
                        }
                    }
                    if (keyUpperCase[2] == 'N') {
                        if (keyUpperCase.length < 7 || keyUpperCase.length > 10) {
                            return null;
                        }
                        if (keyUpperCase.length == 9 && keyUpperCase[3] == 'D'
                                && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'T'
                                && keyUpperCase[6] == 'I' && keyUpperCase[7] == 'O'
                                && keyUpperCase[8] == 'N') {
                            return MySQLToken.KW_CONDITION;
                        }
                        if (keyUpperCase.length == 10 && keyUpperCase[3] == 'S'
                                && keyUpperCase[4] == 'T' && keyUpperCase[5] == 'R'
                                && keyUpperCase[6] == 'A' && keyUpperCase[7] == 'I'
                                && keyUpperCase[8] == 'N' && keyUpperCase[9] == 'T') {
                            return MySQLToken.KW_CONSTRAINT;
                        }
                        if (keyUpperCase.length == 8 && keyUpperCase[3] == 'T'
                                && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'N'
                                && keyUpperCase[6] == 'U' && keyUpperCase[7] == 'E') {
                            return MySQLToken.KW_CONTINUE;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'V'
                                && keyUpperCase[4] == 'E' && keyUpperCase[5] == 'R'
                                && keyUpperCase[6] == 'T') {
                            return MySQLToken.KW_CONVERT;
                        }
                    }
                }
                if (keyUpperCase[1] == 'R') {
                    if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                        return null;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'E' && keyUpperCase[3] == 'A'
                            && keyUpperCase[4] == 'T' && keyUpperCase[5] == 'E') {
                        return MySQLToken.KW_CREATE;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'O' && keyUpperCase[3] == 'S'
                            && keyUpperCase[4] == 'S') {
                        return MySQLToken.KW_CROSS;
                    }
                }
                if (keyUpperCase[1] == 'U') {
                    if (keyUpperCase.length < 6 || keyUpperCase.length > 17) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'R') {
                        if (keyUpperCase.length < 6 || keyUpperCase.length > 17) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'R') {
                            if (keyUpperCase.length < 12 || keyUpperCase.length > 17) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'E') {
                                if (keyUpperCase.length < 12 || keyUpperCase.length > 17) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'N') {
                                    if (keyUpperCase.length < 12 || keyUpperCase.length > 17) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'T') {
                                        if (keyUpperCase.length < 12 || keyUpperCase.length > 17) {
                                            return null;
                                        }
                                        if (keyUpperCase[7] == '_') {
                                            if (keyUpperCase.length < 12
                                                    || keyUpperCase.length > 17) {
                                                return null;
                                            }
                                            if (keyUpperCase.length == 12 && keyUpperCase[8] == 'D'
                                                    && keyUpperCase[9] == 'A'
                                                    && keyUpperCase[10] == 'T'
                                                    && keyUpperCase[11] == 'E') {
                                                return MySQLToken.KW_CURRENT_DATE;
                                            }
                                            if (keyUpperCase[8] == 'T') {
                                                if (keyUpperCase.length < 12
                                                        || keyUpperCase.length > 17) {
                                                    return null;
                                                }
                                                if (keyUpperCase[9] == 'I') {
                                                    if (keyUpperCase.length < 12
                                                            || keyUpperCase.length > 17) {
                                                        return null;
                                                    }
                                                    if (keyUpperCase[10] == 'M') {
                                                        if (keyUpperCase.length < 12
                                                                || keyUpperCase.length > 17) {
                                                            return null;
                                                        }
                                                        if (keyUpperCase[11] == 'E') {
                                                            if (keyUpperCase.length < 12
                                                                    || keyUpperCase.length > 17) {
                                                                return null;
                                                            }
                                                            if (keyUpperCase.length == 12) {
                                                                return MySQLToken.KW_CURRENT_TIME;
                                                            }
                                                            if (keyUpperCase.length == 17
                                                                    && keyUpperCase[12] == 'S'
                                                                    && keyUpperCase[13] == 'T'
                                                                    && keyUpperCase[14] == 'A'
                                                                    && keyUpperCase[15] == 'M'
                                                                    && keyUpperCase[16] == 'P') {
                                                                return MySQLToken.KW_CURRENT_TIMESTAMP;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if (keyUpperCase.length == 12 && keyUpperCase[8] == 'U'
                                                    && keyUpperCase[9] == 'S'
                                                    && keyUpperCase[10] == 'E'
                                                    && keyUpperCase[11] == 'R') {
                                                return MySQLToken.KW_CURRENT_USER;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (keyUpperCase.length == 6 && keyUpperCase[3] == 'S'
                                && keyUpperCase[4] == 'O' && keyUpperCase[5] == 'R') {
                            return MySQLToken.KW_CURSOR;
                        }
                    }
                }
                return null;
            }
            case 'D': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 15) {
                    return null;
                }
                if (keyUpperCase[1] == 'A') {
                    if (keyUpperCase.length < 8 || keyUpperCase.length > 15) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'T') {
                        if (keyUpperCase.length < 8 || keyUpperCase.length > 9) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'A') {
                            if (keyUpperCase.length < 8 || keyUpperCase.length > 9) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'B') {
                                if (keyUpperCase.length < 8 || keyUpperCase.length > 9) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'A') {
                                    if (keyUpperCase.length < 8 || keyUpperCase.length > 9) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'S') {
                                        if (keyUpperCase.length < 8 || keyUpperCase.length > 9) {
                                            return null;
                                        }
                                        if (keyUpperCase[7] == 'E') {
                                            if (keyUpperCase.length < 8
                                                    || keyUpperCase.length > 9) {
                                                return null;
                                            }
                                            if (keyUpperCase.length == 8) {
                                                return MySQLToken.KW_DATABASE;
                                            }
                                            if (keyUpperCase.length == 9
                                                    && keyUpperCase[8] == 'S') {
                                                return MySQLToken.KW_DATABASES;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (keyUpperCase[2] == 'Y') {
                        if (keyUpperCase.length < 8 || keyUpperCase.length > 15) {
                            return null;
                        }
                        if (keyUpperCase[3] == '_') {
                            if (keyUpperCase.length < 8 || keyUpperCase.length > 15) {
                                return null;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'H'
                                    && keyUpperCase[5] == 'O' && keyUpperCase[6] == 'U'
                                    && keyUpperCase[7] == 'R') {
                                return MySQLToken.KW_DAY_HOUR;
                            }
                            if (keyUpperCase[4] == 'M') {
                                if (keyUpperCase.length < 10 || keyUpperCase.length > 15) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'I') {
                                    if (keyUpperCase.length < 10 || keyUpperCase.length > 15) {
                                        return null;
                                    }
                                    if (keyUpperCase.length == 15 && keyUpperCase[6] == 'C'
                                            && keyUpperCase[7] == 'R' && keyUpperCase[8] == 'O'
                                            && keyUpperCase[9] == 'S' && keyUpperCase[10] == 'E'
                                            && keyUpperCase[11] == 'C' && keyUpperCase[12] == 'O'
                                            && keyUpperCase[13] == 'N' && keyUpperCase[14] == 'D') {
                                        return MySQLToken.KW_DAY_MICROSECOND;
                                    }
                                    if (keyUpperCase.length == 10 && keyUpperCase[6] == 'N'
                                            && keyUpperCase[7] == 'U' && keyUpperCase[8] == 'T'
                                            && keyUpperCase[9] == 'E') {
                                        return MySQLToken.KW_DAY_MINUTE;
                                    }
                                }
                            }
                            if (keyUpperCase.length == 10 && keyUpperCase[4] == 'S'
                                    && keyUpperCase[5] == 'E' && keyUpperCase[6] == 'C'
                                    && keyUpperCase[7] == 'O' && keyUpperCase[8] == 'N'
                                    && keyUpperCase[9] == 'D') {
                                return MySQLToken.KW_DAY_SECOND;
                            }
                        }
                    }
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 13) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'C') {
                        if (keyUpperCase.length < 3 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 3) {
                            return MySQLToken.KW_DEC;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'I'
                                && keyUpperCase[4] == 'M' && keyUpperCase[5] == 'A'
                                && keyUpperCase[6] == 'L') {
                            return MySQLToken.KW_DECIMAL;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'L'
                                && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'R'
                                && keyUpperCase[6] == 'E') {
                            return MySQLToken.KW_DECLARE;
                        }
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'F' && keyUpperCase[3] == 'A'
                            && keyUpperCase[4] == 'U' && keyUpperCase[5] == 'L'
                            && keyUpperCase[6] == 'T') {
                        return MySQLToken.KW_DEFAULT;
                    }
                    if (keyUpperCase[2] == 'L') {
                        if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'A'
                                && keyUpperCase[4] == 'Y' && keyUpperCase[5] == 'E'
                                && keyUpperCase[6] == 'D') {
                            return MySQLToken.KW_DELAYED;
                        }
                        if (keyUpperCase.length == 6 && keyUpperCase[3] == 'E'
                                && keyUpperCase[4] == 'T' && keyUpperCase[5] == 'E') {
                            return MySQLToken.KW_DELETE;
                        }
                    }
                    if (keyUpperCase[2] == 'S') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 8) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'C') {
                            if (keyUpperCase.length < 4 || keyUpperCase.length > 8) {
                                return null;
                            }
                            if (keyUpperCase.length == 4) {
                                return MySQLToken.KW_DESC;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'R'
                                    && keyUpperCase[5] == 'I' && keyUpperCase[6] == 'B'
                                    && keyUpperCase[7] == 'E') {
                                return MySQLToken.KW_DESCRIBE;
                            }
                        }
                    }
                    if (keyUpperCase.length == 13 && keyUpperCase[2] == 'T'
                            && keyUpperCase[3] == 'E' && keyUpperCase[4] == 'R'
                            && keyUpperCase[5] == 'M' && keyUpperCase[6] == 'I'
                            && keyUpperCase[7] == 'N' && keyUpperCase[8] == 'I'
                            && keyUpperCase[9] == 'S' && keyUpperCase[10] == 'T'
                            && keyUpperCase[11] == 'I' && keyUpperCase[12] == 'C') {
                        return MySQLToken.KW_DETERMINISTIC;
                    }
                }
                if (keyUpperCase[1] == 'I') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 11) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'S') {
                        if (keyUpperCase.length < 8 || keyUpperCase.length > 11) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'T') {
                            if (keyUpperCase.length < 8 || keyUpperCase.length > 11) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'I') {
                                if (keyUpperCase.length < 8 || keyUpperCase.length > 11) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'N') {
                                    if (keyUpperCase.length < 8 || keyUpperCase.length > 11) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'C') {
                                        if (keyUpperCase.length < 8 || keyUpperCase.length > 11) {
                                            return null;
                                        }
                                        if (keyUpperCase[7] == 'T') {
                                            if (keyUpperCase.length < 8
                                                    || keyUpperCase.length > 11) {
                                                return null;
                                            }
                                            if (keyUpperCase.length == 8) {
                                                return MySQLToken.KW_DISTINCT;
                                            }
                                            if (keyUpperCase.length == 11 && keyUpperCase[8] == 'R'
                                                    && keyUpperCase[9] == 'O'
                                                    && keyUpperCase[10] == 'W') {
                                                return MySQLToken.KW_DISTINCTROW;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'V') {
                        return MySQLToken.KW_DIV;
                    }
                }
                if (keyUpperCase.length == 6 && keyUpperCase[1] == 'O' && keyUpperCase[2] == 'U'
                        && keyUpperCase[3] == 'B' && keyUpperCase[4] == 'L'
                        && keyUpperCase[5] == 'E') {
                    return MySQLToken.KW_DOUBLE;
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'R' && keyUpperCase[2] == 'O'
                        && keyUpperCase[3] == 'P') {
                    return MySQLToken.KW_DROP;
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'U' && keyUpperCase[2] == 'A'
                        && keyUpperCase[3] == 'L') {
                    return MySQLToken.KW_DUAL;
                }
                return null;
            }
            case 'E': {
                if (keyUpperCase.length < 4 || keyUpperCase.length > 8) {
                    return null;
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'A' && keyUpperCase[2] == 'C'
                        && keyUpperCase[3] == 'H') {
                    return MySQLToken.KW_EACH;
                }
                if (keyUpperCase[1] == 'L') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 6) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'S') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 6) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'E') {
                            if (keyUpperCase.length < 4 || keyUpperCase.length > 6) {
                                return null;
                            }
                            if (keyUpperCase.length == 4) {
                                return MySQLToken.KW_ELSE;
                            }
                            if (keyUpperCase.length == 6 && keyUpperCase[4] == 'I'
                                    && keyUpperCase[5] == 'F') {
                                return MySQLToken.KW_ELSEIF;
                            }
                        }
                    }
                }
                if (keyUpperCase.length == 8 && keyUpperCase[1] == 'N' && keyUpperCase[2] == 'C'
                        && keyUpperCase[3] == 'L' && keyUpperCase[4] == 'O'
                        && keyUpperCase[5] == 'S' && keyUpperCase[6] == 'E'
                        && keyUpperCase[7] == 'D') {
                    return MySQLToken.KW_ENCLOSED;
                }
                if (keyUpperCase.length == 7 && keyUpperCase[1] == 'S' && keyUpperCase[2] == 'C'
                        && keyUpperCase[3] == 'A' && keyUpperCase[4] == 'P'
                        && keyUpperCase[5] == 'E' && keyUpperCase[6] == 'D') {
                    return MySQLToken.KW_ESCAPED;
                }
                if (keyUpperCase[1] == 'X') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'I') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 6) {
                            return null;
                        }
                        if (keyUpperCase.length == 6 && keyUpperCase[3] == 'S'
                                && keyUpperCase[4] == 'T' && keyUpperCase[5] == 'S') {
                            return MySQLToken.KW_EXISTS;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == 'T') {
                            return MySQLToken.KW_EXIT;
                        }
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'P' && keyUpperCase[3] == 'L'
                            && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'I'
                            && keyUpperCase[6] == 'N') {
                        return MySQLToken.KW_EXPLAIN;
                    }
                }
                return null;
            }
            case 'F': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 8) {
                    return null;
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'A' && keyUpperCase[2] == 'L'
                        && keyUpperCase[3] == 'S' && keyUpperCase[4] == 'E') {
                    return MySQLToken.LITERAL_BOOL_FALSE;
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'E' && keyUpperCase[2] == 'T'
                        && keyUpperCase[3] == 'C' && keyUpperCase[4] == 'H') {
                    return MySQLToken.KW_FETCH;
                }
                if (keyUpperCase[1] == 'L') {
                    if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'O') {
                        if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'A') {
                            if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'T') {
                                if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                                    return null;
                                }
                                if (keyUpperCase.length == 5) {
                                    return MySQLToken.KW_FLOAT;
                                }
                                if (keyUpperCase.length == 6 && keyUpperCase[5] == '4') {
                                    return MySQLToken.KW_FLOAT4;
                                }
                                if (keyUpperCase.length == 6 && keyUpperCase[5] == '8') {
                                    return MySQLToken.KW_FLOAT8;
                                }
                            }
                        }
                    }
                }
                if (keyUpperCase[1] == 'O') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'R') {
                        if (keyUpperCase.length < 3 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 3) {
                            return MySQLToken.KW_FOR;
                        }
                        if (keyUpperCase.length == 5 && keyUpperCase[3] == 'C'
                                && keyUpperCase[4] == 'E') {
                            return MySQLToken.KW_FORCE;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'E'
                                && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'G'
                                && keyUpperCase[6] == 'N') {
                            return MySQLToken.KW_FOREIGN;
                        }
                    }
                }
                if (keyUpperCase[1] == 'R') {
                    if (keyUpperCase.length != 4) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'O') {
                        if (keyUpperCase.length != 4) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'M') {
                            if (keyUpperCase.length != 4) {
                                return null;
                            }
                            if (keyUpperCase.length == 4) {
                                return MySQLToken.KW_FROM;
                            }
                            if (keyUpperCase.length == 4) {
                                return MySQLToken.KW_FROM;
                            }
                        }
                    }
                }
                if (keyUpperCase.length == 8 && keyUpperCase[1] == 'U' && keyUpperCase[2] == 'L'
                        && keyUpperCase[3] == 'L' && keyUpperCase[4] == 'T'
                        && keyUpperCase[5] == 'E' && keyUpperCase[6] == 'X'
                        && keyUpperCase[7] == 'T') {
                    return MySQLToken.KW_FULLTEXT;
                }
                return null;
            }
            case 'G': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 9) {
                    return null;
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 9) {
                        return null;
                    }
                    if (keyUpperCase.length == 9 && keyUpperCase[2] == 'N' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'R' && keyUpperCase[5] == 'A'
                            && keyUpperCase[6] == 'T' && keyUpperCase[7] == 'E'
                            && keyUpperCase[8] == 'D') {
                        return MySQLToken.KW_GENERATED;
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'T') {
                        return MySQLToken.KW_GET;
                    }
                }
                if (keyUpperCase[1] == 'R') {
                    if (keyUpperCase.length != 5) {
                        return null;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'A' && keyUpperCase[3] == 'N'
                            && keyUpperCase[4] == 'T') {
                        return MySQLToken.KW_GRANT;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'O' && keyUpperCase[3] == 'U'
                            && keyUpperCase[4] == 'P') {
                        return MySQLToken.KW_GROUP;
                    }
                }
                return null;
            }
            case 'H': {
                if (keyUpperCase.length < 6 || keyUpperCase.length > 16) {
                    return null;
                }
                if (keyUpperCase.length == 6 && keyUpperCase[1] == 'A' && keyUpperCase[2] == 'V'
                        && keyUpperCase[3] == 'I' && keyUpperCase[4] == 'N'
                        && keyUpperCase[5] == 'G') {
                    return MySQLToken.KW_HAVING;
                }
                if (keyUpperCase.length == 13 && keyUpperCase[1] == 'I' && keyUpperCase[2] == 'G'
                        && keyUpperCase[3] == 'H' && keyUpperCase[4] == '_'
                        && keyUpperCase[5] == 'P' && keyUpperCase[6] == 'R'
                        && keyUpperCase[7] == 'I' && keyUpperCase[8] == 'O'
                        && keyUpperCase[9] == 'R' && keyUpperCase[10] == 'I'
                        && keyUpperCase[11] == 'T' && keyUpperCase[12] == 'Y') {
                    return MySQLToken.KW_HIGH_PRIORITY;
                }
                if (keyUpperCase[1] == 'O') {
                    if (keyUpperCase.length < 11 || keyUpperCase.length > 16) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'U') {
                        if (keyUpperCase.length < 11 || keyUpperCase.length > 16) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'R') {
                            if (keyUpperCase.length < 11 || keyUpperCase.length > 16) {
                                return null;
                            }
                            if (keyUpperCase[4] == '_') {
                                if (keyUpperCase.length < 11 || keyUpperCase.length > 16) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'M') {
                                    if (keyUpperCase.length < 11 || keyUpperCase.length > 16) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'I') {
                                        if (keyUpperCase.length < 11 || keyUpperCase.length > 16) {
                                            return null;
                                        }
                                        if (keyUpperCase.length == 16 && keyUpperCase[7] == 'C'
                                                && keyUpperCase[8] == 'R' && keyUpperCase[9] == 'O'
                                                && keyUpperCase[10] == 'S'
                                                && keyUpperCase[11] == 'E'
                                                && keyUpperCase[12] == 'C'
                                                && keyUpperCase[13] == 'O'
                                                && keyUpperCase[14] == 'N'
                                                && keyUpperCase[15] == 'D') {
                                            return MySQLToken.KW_HOUR_MICROSECOND;
                                        }
                                        if (keyUpperCase.length == 11 && keyUpperCase[7] == 'N'
                                                && keyUpperCase[8] == 'U' && keyUpperCase[9] == 'T'
                                                && keyUpperCase[10] == 'E') {
                                            return MySQLToken.KW_HOUR_MINUTE;
                                        }
                                    }
                                }
                                if (keyUpperCase.length == 11 && keyUpperCase[5] == 'S'
                                        && keyUpperCase[6] == 'E' && keyUpperCase[7] == 'C'
                                        && keyUpperCase[8] == 'O' && keyUpperCase[9] == 'N'
                                        && keyUpperCase[10] == 'D') {
                                    return MySQLToken.KW_HOUR_SECOND;
                                }
                            }
                        }
                    }
                }
                return null;
            }
            case 'I': {
                if (keyUpperCase.length < 2 || keyUpperCase.length > 15) {
                    return null;
                }
                if (keyUpperCase.length == 2 && keyUpperCase[1] == 'F') {
                    return MySQLToken.KW_IF;
                }
                if (keyUpperCase.length == 6 && keyUpperCase[1] == 'G' && keyUpperCase[2] == 'N'
                        && keyUpperCase[3] == 'O' && keyUpperCase[4] == 'R'
                        && keyUpperCase[5] == 'E') {
                    return MySQLToken.KW_IGNORE;
                }
                if (keyUpperCase[1] == 'N') {
                    if (keyUpperCase.length < 2 || keyUpperCase.length > 11) {
                        return null;
                    }
                    if (keyUpperCase.length == 2) {
                        return MySQLToken.KW_IN;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'D' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'X') {
                        return MySQLToken.KW_INDEX;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'F' && keyUpperCase[3] == 'I'
                            && keyUpperCase[4] == 'L' && keyUpperCase[5] == 'E') {
                        return MySQLToken.KW_INFILE;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'N' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'R') {
                        return MySQLToken.KW_INNER;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'O' && keyUpperCase[3] == 'U'
                            && keyUpperCase[4] == 'T') {
                        return MySQLToken.KW_INOUT;
                    }
                    if (keyUpperCase[2] == 'S') {
                        if (keyUpperCase.length < 6 || keyUpperCase.length > 11) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'E') {
                            if (keyUpperCase.length < 6 || keyUpperCase.length > 11) {
                                return null;
                            }
                            if (keyUpperCase.length == 11 && keyUpperCase[4] == 'N'
                                    && keyUpperCase[5] == 'S' && keyUpperCase[6] == 'I'
                                    && keyUpperCase[7] == 'T' && keyUpperCase[8] == 'I'
                                    && keyUpperCase[9] == 'V' && keyUpperCase[10] == 'E') {
                                return MySQLToken.KW_INSENSITIVE;
                            }
                            if (keyUpperCase.length == 6 && keyUpperCase[4] == 'R'
                                    && keyUpperCase[5] == 'T') {
                                return MySQLToken.KW_INSERT;
                            }
                        }
                    }
                    if (keyUpperCase[2] == 'T') {
                        if (keyUpperCase.length < 3 || keyUpperCase.length > 8) {
                            return null;
                        }
                        if (keyUpperCase.length == 3) {
                            return MySQLToken.KW_INT;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == '1') {
                            return MySQLToken.KW_INT1;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == '2') {
                            return MySQLToken.KW_INT2;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == '3') {
                            return MySQLToken.KW_INT3;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == '4') {
                            return MySQLToken.KW_INT4;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == '8') {
                            return MySQLToken.KW_INT8;
                        }
                        if (keyUpperCase[3] == 'E') {
                            if (keyUpperCase.length < 7 || keyUpperCase.length > 8) {
                                return null;
                            }
                            if (keyUpperCase.length == 7 && keyUpperCase[4] == 'G'
                                    && keyUpperCase[5] == 'E' && keyUpperCase[6] == 'R') {
                                return MySQLToken.KW_INTEGER;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'R'
                                    && keyUpperCase[5] == 'V' && keyUpperCase[6] == 'A'
                                    && keyUpperCase[7] == 'L') {
                                return MySQLToken.KW_INTERVAL;
                            }
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == 'O') {
                            return MySQLToken.KW_INTO;
                        }
                    }
                }
                if (keyUpperCase[1] == 'O') {
                    if (keyUpperCase.length < 14 || keyUpperCase.length > 15) {
                        return null;
                    }
                    if (keyUpperCase[2] == '_') {
                        if (keyUpperCase.length < 14 || keyUpperCase.length > 15) {
                            return null;
                        }
                        if (keyUpperCase.length == 14 && keyUpperCase[3] == 'A'
                                && keyUpperCase[4] == 'F' && keyUpperCase[5] == 'T'
                                && keyUpperCase[6] == 'E' && keyUpperCase[7] == 'R'
                                && keyUpperCase[8] == '_' && keyUpperCase[9] == 'G'
                                && keyUpperCase[10] == 'T' && keyUpperCase[11] == 'I'
                                && keyUpperCase[12] == 'D' && keyUpperCase[13] == 'S') {
                            return MySQLToken.KW_IO_AFTER_GTIDS;
                        }
                        if (keyUpperCase.length == 15 && keyUpperCase[3] == 'B'
                                && keyUpperCase[4] == 'E' && keyUpperCase[5] == 'F'
                                && keyUpperCase[6] == 'O' && keyUpperCase[7] == 'R'
                                && keyUpperCase[8] == 'E' && keyUpperCase[9] == '_'
                                && keyUpperCase[10] == 'G' && keyUpperCase[11] == 'T'
                                && keyUpperCase[12] == 'I' && keyUpperCase[13] == 'D'
                                && keyUpperCase[14] == 'S') {
                            return MySQLToken.KW_IO_BEFORE_GTIDS;
                        }
                    }
                }
                if (keyUpperCase.length == 2 && keyUpperCase[1] == 'S') {
                    return MySQLToken.KW_IS;
                }
                if (keyUpperCase.length == 7 && keyUpperCase[1] == 'T' && keyUpperCase[2] == 'E'
                        && keyUpperCase[3] == 'R' && keyUpperCase[4] == 'A'
                        && keyUpperCase[5] == 'T' && keyUpperCase[6] == 'E') {
                    return MySQLToken.KW_ITERATE;
                }
                return null;
            }
            case 'J': {
                if (keyUpperCase.length != 4) {
                    return null;
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'O' && keyUpperCase[2] == 'I'
                        && keyUpperCase[3] == 'N') {
                    return MySQLToken.KW_JOIN;
                }
                return null;
            }
            case 'K': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 4) {
                    return null;
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 4) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'Y') {
                        if (keyUpperCase.length < 3 || keyUpperCase.length > 4) {
                            return null;
                        }
                        if (keyUpperCase.length == 3) {
                            return MySQLToken.KW_KEY;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == 'S') {
                            return MySQLToken.KW_KEYS;
                        }
                    }
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'I' && keyUpperCase[2] == 'L'
                        && keyUpperCase[3] == 'L') {
                    return MySQLToken.KW_KILL;
                }
                return null;
            }
            case 'L': {
                if (keyUpperCase.length < 4 || keyUpperCase.length > 14) {
                    return null;
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'A') {
                        if (keyUpperCase.length < 5 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'D'
                                && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'N'
                                && keyUpperCase[6] == 'G') {
                            return MySQLToken.KW_LEADING;
                        }
                        if (keyUpperCase.length == 5 && keyUpperCase[3] == 'V'
                                && keyUpperCase[4] == 'E') {
                            return MySQLToken.KW_LEAVE;
                        }
                    }
                    if (keyUpperCase.length == 4 && keyUpperCase[2] == 'F'
                            && keyUpperCase[3] == 'T') {
                        return MySQLToken.KW_LEFT;
                    }
                }
                if (keyUpperCase[1] == 'I') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 6) {
                        return null;
                    }
                    if (keyUpperCase.length == 4 && keyUpperCase[2] == 'K'
                            && keyUpperCase[3] == 'E') {
                        return MySQLToken.KW_LIKE;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'M' && keyUpperCase[3] == 'I'
                            && keyUpperCase[4] == 'T') {
                        return MySQLToken.KW_LIMIT;
                    }
                    if (keyUpperCase[2] == 'N') {
                        if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'E') {
                            if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                                return null;
                            }
                            if (keyUpperCase.length == 6 && keyUpperCase[4] == 'A'
                                    && keyUpperCase[5] == 'R') {
                                return MySQLToken.KW_LINEAR;
                            }
                            if (keyUpperCase.length == 5 && keyUpperCase[4] == 'S') {
                                return MySQLToken.KW_LINES;
                            }
                        }
                    }
                }
                if (keyUpperCase[1] == 'O') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 14) {
                        return null;
                    }
                    if (keyUpperCase.length == 4 && keyUpperCase[2] == 'A'
                            && keyUpperCase[3] == 'D') {
                        return MySQLToken.KW_LOAD;
                    }
                    if (keyUpperCase[2] == 'C') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 14) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'A') {
                            if (keyUpperCase.length < 9 || keyUpperCase.length > 14) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'L') {
                                if (keyUpperCase.length < 9 || keyUpperCase.length > 14) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'T') {
                                    if (keyUpperCase.length < 9 || keyUpperCase.length > 14) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'I') {
                                        if (keyUpperCase.length < 9 || keyUpperCase.length > 14) {
                                            return null;
                                        }
                                        if (keyUpperCase[7] == 'M') {
                                            if (keyUpperCase.length < 9
                                                    || keyUpperCase.length > 14) {
                                                return null;
                                            }
                                            if (keyUpperCase[8] == 'E') {
                                                if (keyUpperCase.length < 9
                                                        || keyUpperCase.length > 14) {
                                                    return null;
                                                }
                                                if (keyUpperCase.length == 9) {
                                                    return MySQLToken.KW_LOCALTIME;
                                                }
                                                if (keyUpperCase.length == 14
                                                        && keyUpperCase[9] == 'S'
                                                        && keyUpperCase[10] == 'T'
                                                        && keyUpperCase[11] == 'A'
                                                        && keyUpperCase[12] == 'M'
                                                        && keyUpperCase[13] == 'P') {
                                                    return MySQLToken.KW_LOCALTIMESTAMP;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == 'K') {
                            return MySQLToken.KW_LOCK;
                        }
                    }
                    if (keyUpperCase[2] == 'N') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 8) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'G') {
                            if (keyUpperCase.length < 4 || keyUpperCase.length > 8) {
                                return null;
                            }
                            if (keyUpperCase.length == 4) {
                                return MySQLToken.KW_LONG;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'B'
                                    && keyUpperCase[5] == 'L' && keyUpperCase[6] == 'O'
                                    && keyUpperCase[7] == 'B') {
                                return MySQLToken.KW_LONGBLOB;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'T'
                                    && keyUpperCase[5] == 'E' && keyUpperCase[6] == 'X'
                                    && keyUpperCase[7] == 'T') {
                                return MySQLToken.KW_LONGTEXT;
                            }
                        }
                    }
                    if (keyUpperCase.length == 4 && keyUpperCase[2] == 'O'
                            && keyUpperCase[3] == 'P') {
                        return MySQLToken.KW_LOOP;
                    }
                    if (keyUpperCase.length == 12 && keyUpperCase[2] == 'W'
                            && keyUpperCase[3] == '_' && keyUpperCase[4] == 'P'
                            && keyUpperCase[5] == 'R' && keyUpperCase[6] == 'I'
                            && keyUpperCase[7] == 'O' && keyUpperCase[8] == 'R'
                            && keyUpperCase[9] == 'I' && keyUpperCase[10] == 'T'
                            && keyUpperCase[11] == 'Y') {
                        return MySQLToken.KW_LOW_PRIORITY;
                    }
                }
                return null;
            }
            case 'M': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 29) {
                    return null;
                }
                if (keyUpperCase[1] == 'A') {
                    if (keyUpperCase.length < 5 || keyUpperCase.length > 29) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'S') {
                        if (keyUpperCase.length < 11 || keyUpperCase.length > 29) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'T') {
                            if (keyUpperCase.length < 11 || keyUpperCase.length > 29) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'E') {
                                if (keyUpperCase.length < 11 || keyUpperCase.length > 29) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'R') {
                                    if (keyUpperCase.length < 11 || keyUpperCase.length > 29) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == '_') {
                                        if (keyUpperCase.length < 11 || keyUpperCase.length > 29) {
                                            return null;
                                        }
                                        if (keyUpperCase.length == 11 && keyUpperCase[7] == 'B'
                                                && keyUpperCase[8] == 'I' && keyUpperCase[9] == 'N'
                                                && keyUpperCase[10] == 'D') {
                                            return MySQLToken.KW_MASTER_BIND;
                                        }
                                        if (keyUpperCase.length == 29 && keyUpperCase[7] == 'S'
                                                && keyUpperCase[8] == 'S' && keyUpperCase[9] == 'L'
                                                && keyUpperCase[10] == '_'
                                                && keyUpperCase[11] == 'V'
                                                && keyUpperCase[12] == 'E'
                                                && keyUpperCase[13] == 'R'
                                                && keyUpperCase[14] == 'I'
                                                && keyUpperCase[15] == 'F'
                                                && keyUpperCase[16] == 'Y'
                                                && keyUpperCase[17] == '_'
                                                && keyUpperCase[18] == 'S'
                                                && keyUpperCase[19] == 'E'
                                                && keyUpperCase[20] == 'R'
                                                && keyUpperCase[21] == 'V'
                                                && keyUpperCase[22] == 'E'
                                                && keyUpperCase[23] == 'R'
                                                && keyUpperCase[24] == '_'
                                                && keyUpperCase[25] == 'C'
                                                && keyUpperCase[26] == 'E'
                                                && keyUpperCase[27] == 'R'
                                                && keyUpperCase[28] == 'T') {
                                            return MySQLToken.KW_MASTER_SSL_VERIFY_SERVER_CERT;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'T' && keyUpperCase[3] == 'C'
                            && keyUpperCase[4] == 'H') {
                        return MySQLToken.KW_MATCH;
                    }
                    if (keyUpperCase.length == 8 && keyUpperCase[2] == 'X' && keyUpperCase[3] == 'V'
                            && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'L'
                            && keyUpperCase[6] == 'U' && keyUpperCase[7] == 'E') {
                        return MySQLToken.KW_MAXVALUE;
                    }
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 9 || keyUpperCase.length > 10) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'D') {
                        if (keyUpperCase.length < 9 || keyUpperCase.length > 10) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'I') {
                            if (keyUpperCase.length < 9 || keyUpperCase.length > 10) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'U') {
                                if (keyUpperCase.length < 9 || keyUpperCase.length > 10) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'M') {
                                    if (keyUpperCase.length < 9 || keyUpperCase.length > 10) {
                                        return null;
                                    }
                                    if (keyUpperCase.length == 10 && keyUpperCase[6] == 'B'
                                            && keyUpperCase[7] == 'L' && keyUpperCase[8] == 'O'
                                            && keyUpperCase[9] == 'B') {
                                        return MySQLToken.KW_MEDIUMBLOB;
                                    }
                                    if (keyUpperCase.length == 9 && keyUpperCase[6] == 'I'
                                            && keyUpperCase[7] == 'N' && keyUpperCase[8] == 'T') {
                                        return MySQLToken.KW_MEDIUMINT;
                                    }
                                    if (keyUpperCase.length == 10 && keyUpperCase[6] == 'T'
                                            && keyUpperCase[7] == 'E' && keyUpperCase[8] == 'X'
                                            && keyUpperCase[9] == 'T') {
                                        return MySQLToken.KW_MEDIUMTEXT;
                                    }
                                }
                            }
                        }
                    }
                }
                if (keyUpperCase[1] == 'I') {
                    if (keyUpperCase.length < 9 || keyUpperCase.length > 18) {
                        return null;
                    }
                    if (keyUpperCase.length == 9 && keyUpperCase[2] == 'D' && keyUpperCase[3] == 'D'
                            && keyUpperCase[4] == 'L' && keyUpperCase[5] == 'E'
                            && keyUpperCase[6] == 'I' && keyUpperCase[7] == 'N'
                            && keyUpperCase[8] == 'T') {
                        return MySQLToken.KW_MIDDLEINT;
                    }
                    if (keyUpperCase[2] == 'N') {
                        if (keyUpperCase.length < 13 || keyUpperCase.length > 18) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'U') {
                            if (keyUpperCase.length < 13 || keyUpperCase.length > 18) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'T') {
                                if (keyUpperCase.length < 13 || keyUpperCase.length > 18) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'E') {
                                    if (keyUpperCase.length < 13 || keyUpperCase.length > 18) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == '_') {
                                        if (keyUpperCase.length < 13 || keyUpperCase.length > 18) {
                                            return null;
                                        }
                                        if (keyUpperCase.length == 18 && keyUpperCase[7] == 'M'
                                                && keyUpperCase[8] == 'I' && keyUpperCase[9] == 'C'
                                                && keyUpperCase[10] == 'R'
                                                && keyUpperCase[11] == 'O'
                                                && keyUpperCase[12] == 'S'
                                                && keyUpperCase[13] == 'E'
                                                && keyUpperCase[14] == 'C'
                                                && keyUpperCase[15] == 'O'
                                                && keyUpperCase[16] == 'N'
                                                && keyUpperCase[17] == 'D') {
                                            return MySQLToken.KW_MINUTE_MICROSECOND;
                                        }
                                        if (keyUpperCase.length == 13 && keyUpperCase[7] == 'S'
                                                && keyUpperCase[8] == 'E' && keyUpperCase[9] == 'C'
                                                && keyUpperCase[10] == 'O'
                                                && keyUpperCase[11] == 'N'
                                                && keyUpperCase[12] == 'D') {
                                            return MySQLToken.KW_MINUTE_SECOND;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (keyUpperCase[1] == 'O') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 8) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'D') {
                        if (keyUpperCase.length < 3 || keyUpperCase.length > 8) {
                            return null;
                        }
                        if (keyUpperCase.length == 3) {
                            return MySQLToken.KW_MOD;
                        }
                        if (keyUpperCase.length == 8 && keyUpperCase[3] == 'I'
                                && keyUpperCase[4] == 'F' && keyUpperCase[5] == 'I'
                                && keyUpperCase[6] == 'E' && keyUpperCase[7] == 'S') {
                            return MySQLToken.KW_MODIFIES;
                        }
                    }
                }
                return null;
            }
            case 'N': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 18) {
                    return null;
                }
                if (keyUpperCase.length == 7 && keyUpperCase[1] == 'A' && keyUpperCase[2] == 'T'
                        && keyUpperCase[3] == 'U' && keyUpperCase[4] == 'R'
                        && keyUpperCase[5] == 'A' && keyUpperCase[6] == 'L') {
                    return MySQLToken.KW_NATURAL;
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'U' && keyUpperCase[2] == 'L'
                        && keyUpperCase[3] == 'L') {
                    return MySQLToken.LITERAL_NULL;
                }
                if (keyUpperCase[1] == 'O') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 18) {
                        return null;
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'T') {
                        return MySQLToken.KW_NOT;
                    }
                    if (keyUpperCase.length == 18 && keyUpperCase[2] == '_'
                            && keyUpperCase[3] == 'W' && keyUpperCase[4] == 'R'
                            && keyUpperCase[5] == 'I' && keyUpperCase[6] == 'T'
                            && keyUpperCase[7] == 'E' && keyUpperCase[8] == '_'
                            && keyUpperCase[9] == 'T' && keyUpperCase[10] == 'O'
                            && keyUpperCase[11] == '_' && keyUpperCase[12] == 'B'
                            && keyUpperCase[13] == 'I' && keyUpperCase[14] == 'N'
                            && keyUpperCase[15] == 'L' && keyUpperCase[16] == 'O'
                            && keyUpperCase[17] == 'G') {
                        return MySQLToken.KW_NO_WRITE_TO_BINLOG;
                    }
                }
                if (keyUpperCase.length == 7 && keyUpperCase[1] == 'U' && keyUpperCase[2] == 'M'
                        && keyUpperCase[3] == 'E' && keyUpperCase[4] == 'R'
                        && keyUpperCase[5] == 'I' && keyUpperCase[6] == 'C') {
                    return MySQLToken.KW_NUMERIC;
                }
                return null;
            }
            case 'O': {
                if (keyUpperCase.length < 2 || keyUpperCase.length > 15) {
                    return null;
                }
                if (keyUpperCase.length == 2 && keyUpperCase[1] == 'N') {
                    return MySQLToken.KW_ON;
                }
                if (keyUpperCase[1] == 'P') {
                    if (keyUpperCase.length < 6 || keyUpperCase.length > 15) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'T') {
                        if (keyUpperCase.length < 6 || keyUpperCase.length > 15) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'I') {
                            if (keyUpperCase.length < 6 || keyUpperCase.length > 15) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'M') {
                                if (keyUpperCase.length < 8 || keyUpperCase.length > 15) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'I') {
                                    if (keyUpperCase.length < 8 || keyUpperCase.length > 15) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'Z') {
                                        if (keyUpperCase.length < 8 || keyUpperCase.length > 15) {
                                            return null;
                                        }
                                        if (keyUpperCase[7] == 'E') {
                                            if (keyUpperCase.length < 8
                                                    || keyUpperCase.length > 15) {
                                                return null;
                                            }
                                            if (keyUpperCase.length == 8) {
                                                return MySQLToken.KW_OPTIMIZE;
                                            }
                                            if (keyUpperCase.length == 15 && keyUpperCase[8] == 'R'
                                                    && keyUpperCase[9] == '_'
                                                    && keyUpperCase[10] == 'C'
                                                    && keyUpperCase[11] == 'O'
                                                    && keyUpperCase[12] == 'S'
                                                    && keyUpperCase[13] == 'T'
                                                    && keyUpperCase[14] == 'S') {
                                                return MySQLToken.KW_OPTIMIZER_COSTS;
                                            }
                                        }
                                    }
                                }
                            }
                            if (keyUpperCase[4] == 'O') {
                                if (keyUpperCase.length < 6 || keyUpperCase.length > 10) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'N') {
                                    if (keyUpperCase.length < 6 || keyUpperCase.length > 10) {
                                        return null;
                                    }
                                    if (keyUpperCase.length == 6) {
                                        return MySQLToken.KW_OPTION;
                                    }
                                    if (keyUpperCase.length == 10 && keyUpperCase[6] == 'A'
                                            && keyUpperCase[7] == 'L' && keyUpperCase[8] == 'L'
                                            && keyUpperCase[9] == 'Y') {
                                        return MySQLToken.KW_OPTIONALLY;
                                    }
                                }
                            }
                        }
                    }
                }
                if (keyUpperCase[1] == 'R') {
                    if (keyUpperCase.length < 2 || keyUpperCase.length > 5) {
                        return null;
                    }
                    if (keyUpperCase.length == 2) {
                        return MySQLToken.KW_OR;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'D' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'R') {
                        return MySQLToken.KW_ORDER;
                    }
                }
                if (keyUpperCase[1] == 'U') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'T') {
                        if (keyUpperCase.length < 3 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 3) {
                            return MySQLToken.KW_OUT;
                        }
                        if (keyUpperCase.length == 5 && keyUpperCase[3] == 'E'
                                && keyUpperCase[4] == 'R') {
                            return MySQLToken.KW_OUTER;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'F'
                                && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'L'
                                && keyUpperCase[6] == 'E') {
                            return MySQLToken.KW_OUTFILE;
                        }
                    }
                }
                return null;
            }
            case 'P': {
                if (keyUpperCase.length < 5 || keyUpperCase.length > 9) {
                    return null;
                }
                if (keyUpperCase.length == 9 && keyUpperCase[1] == 'A' && keyUpperCase[2] == 'R'
                        && keyUpperCase[3] == 'T' && keyUpperCase[4] == 'I'
                        && keyUpperCase[5] == 'T' && keyUpperCase[6] == 'I'
                        && keyUpperCase[7] == 'O' && keyUpperCase[8] == 'N') {
                    return MySQLToken.KW_PARTITION;
                }
                if (keyUpperCase[1] == 'R') {
                    if (keyUpperCase.length < 7 || keyUpperCase.length > 9) {
                        return null;
                    }
                    if (keyUpperCase.length == 9 && keyUpperCase[2] == 'E' && keyUpperCase[3] == 'C'
                            && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'S'
                            && keyUpperCase[6] == 'I' && keyUpperCase[7] == 'O'
                            && keyUpperCase[8] == 'N') {
                        return MySQLToken.KW_PRECISION;
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'I' && keyUpperCase[3] == 'M'
                            && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'R'
                            && keyUpperCase[6] == 'Y') {
                        return MySQLToken.KW_PRIMARY;
                    }
                    if (keyUpperCase.length == 9 && keyUpperCase[2] == 'O' && keyUpperCase[3] == 'C'
                            && keyUpperCase[4] == 'E' && keyUpperCase[5] == 'D'
                            && keyUpperCase[6] == 'U' && keyUpperCase[7] == 'R'
                            && keyUpperCase[8] == 'E') {
                        return MySQLToken.KW_PROCEDURE;
                    }
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'U' && keyUpperCase[2] == 'R'
                        && keyUpperCase[3] == 'G' && keyUpperCase[4] == 'E') {
                    return MySQLToken.KW_PURGE;
                }
                return null;
            }
            case 'R': {
                if (keyUpperCase.length < 4 || keyUpperCase.length > 10) {
                    return null;
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'A' && keyUpperCase[2] == 'N'
                        && keyUpperCase[3] == 'G' && keyUpperCase[4] == 'E') {
                    return MySQLToken.KW_RANGE;
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 10) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'A') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 10) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'D') {
                            if (keyUpperCase.length < 4 || keyUpperCase.length > 10) {
                                return null;
                            }
                            if (keyUpperCase.length == 4) {
                                return MySQLToken.KW_READ;
                            }
                            if (keyUpperCase.length == 5 && keyUpperCase[4] == 'S') {
                                return MySQLToken.KW_READS;
                            }
                            if (keyUpperCase.length == 10 && keyUpperCase[4] == '_'
                                    && keyUpperCase[5] == 'W' && keyUpperCase[6] == 'R'
                                    && keyUpperCase[7] == 'I' && keyUpperCase[8] == 'T'
                                    && keyUpperCase[9] == 'E') {
                                return MySQLToken.KW_READ_WRITE;
                            }
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == 'L') {
                            return MySQLToken.KW_REAL;
                        }
                    }
                    if (keyUpperCase.length == 10 && keyUpperCase[2] == 'F'
                            && keyUpperCase[3] == 'E' && keyUpperCase[4] == 'R'
                            && keyUpperCase[5] == 'E' && keyUpperCase[6] == 'N'
                            && keyUpperCase[7] == 'C' && keyUpperCase[8] == 'E'
                            && keyUpperCase[9] == 'S') {
                        return MySQLToken.KW_REFERENCES;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'G' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'X' && keyUpperCase[5] == 'P') {
                        return MySQLToken.KW_REGEXP;
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'L' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'S'
                            && keyUpperCase[6] == 'E') {
                        return MySQLToken.KW_RELEASE;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'N' && keyUpperCase[3] == 'A'
                            && keyUpperCase[4] == 'M' && keyUpperCase[5] == 'E') {
                        return MySQLToken.KW_RENAME;
                    }
                    if (keyUpperCase[2] == 'P') {
                        if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase.length == 6 && keyUpperCase[3] == 'E'
                                && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'T') {
                            return MySQLToken.KW_REPEAT;
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'L'
                                && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'C'
                                && keyUpperCase[6] == 'E') {
                            return MySQLToken.KW_REPLACE;
                        }
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'Q' && keyUpperCase[3] == 'U'
                            && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'R'
                            && keyUpperCase[6] == 'E') {
                        return MySQLToken.KW_REQUIRE;
                    }
                    if (keyUpperCase[2] == 'S') {
                        if (keyUpperCase.length != 8) {
                            return null;
                        }
                        if (keyUpperCase.length == 8 && keyUpperCase[3] == 'I'
                                && keyUpperCase[4] == 'G' && keyUpperCase[5] == 'N'
                                && keyUpperCase[6] == 'A' && keyUpperCase[7] == 'L') {
                            return MySQLToken.KW_RESIGNAL;
                        }
                        if (keyUpperCase.length == 8 && keyUpperCase[3] == 'T'
                                && keyUpperCase[4] == 'R' && keyUpperCase[5] == 'I'
                                && keyUpperCase[6] == 'C' && keyUpperCase[7] == 'T') {
                            return MySQLToken.KW_RESTRICT;
                        }
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'T' && keyUpperCase[3] == 'U'
                            && keyUpperCase[4] == 'R' && keyUpperCase[5] == 'N') {
                        return MySQLToken.KW_RETURN;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'V' && keyUpperCase[3] == 'O'
                            && keyUpperCase[4] == 'K' && keyUpperCase[5] == 'E') {
                        return MySQLToken.KW_REVOKE;
                    }
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'I' && keyUpperCase[2] == 'G'
                        && keyUpperCase[3] == 'H' && keyUpperCase[4] == 'T') {
                    return MySQLToken.KW_RIGHT;
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'L' && keyUpperCase[2] == 'I'
                        && keyUpperCase[3] == 'K' && keyUpperCase[4] == 'E') {
                    return MySQLToken.KW_RLIKE;
                }
                return null;
            }
            case 'S': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 19) {
                    return null;
                }
                if (keyUpperCase[1] == 'C') {
                    if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'H') {
                        if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'E') {
                            if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'M') {
                                if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'A') {
                                    if (keyUpperCase.length < 6 || keyUpperCase.length > 7) {
                                        return null;
                                    }
                                    if (keyUpperCase.length == 6) {
                                        return MySQLToken.KW_SCHEMA;
                                    }
                                    if (keyUpperCase.length == 7 && keyUpperCase[6] == 'S') {
                                        return MySQLToken.KW_SCHEMAS;
                                    }
                                }
                            }
                        }
                    }
                }
                if (keyUpperCase[1] == 'E') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 18) {
                        return null;
                    }
                    if (keyUpperCase.length == 18 && keyUpperCase[2] == 'C'
                            && keyUpperCase[3] == 'O' && keyUpperCase[4] == 'N'
                            && keyUpperCase[5] == 'D' && keyUpperCase[6] == '_'
                            && keyUpperCase[7] == 'M' && keyUpperCase[8] == 'I'
                            && keyUpperCase[9] == 'C' && keyUpperCase[10] == 'R'
                            && keyUpperCase[11] == 'O' && keyUpperCase[12] == 'S'
                            && keyUpperCase[13] == 'E' && keyUpperCase[14] == 'C'
                            && keyUpperCase[15] == 'O' && keyUpperCase[16] == 'N'
                            && keyUpperCase[17] == 'D') {
                        return MySQLToken.KW_SECOND_MICROSECOND;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'L' && keyUpperCase[3] == 'E'
                            && keyUpperCase[4] == 'C' && keyUpperCase[5] == 'T') {
                        return MySQLToken.KW_SELECT;
                    }
                    if (keyUpperCase.length == 9 && keyUpperCase[2] == 'N' && keyUpperCase[3] == 'S'
                            && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'T'
                            && keyUpperCase[6] == 'I' && keyUpperCase[7] == 'V'
                            && keyUpperCase[8] == 'E') {
                        return MySQLToken.KW_SENSITIVE;
                    }
                    if (keyUpperCase.length == 9 && keyUpperCase[2] == 'P' && keyUpperCase[3] == 'A'
                            && keyUpperCase[4] == 'R' && keyUpperCase[5] == 'A'
                            && keyUpperCase[6] == 'T' && keyUpperCase[7] == 'O'
                            && keyUpperCase[8] == 'R') {
                        return MySQLToken.KW_SEPARATOR;
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'T') {
                        return MySQLToken.KW_SET;
                    }
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'H' && keyUpperCase[2] == 'O'
                        && keyUpperCase[3] == 'W') {
                    return MySQLToken.KW_SHOW;
                }
                if (keyUpperCase.length == 6 && keyUpperCase[1] == 'I' && keyUpperCase[2] == 'G'
                        && keyUpperCase[3] == 'N' && keyUpperCase[4] == 'A'
                        && keyUpperCase[5] == 'L') {
                    return MySQLToken.KW_SIGNAL;
                }
                if (keyUpperCase.length == 8 && keyUpperCase[1] == 'M' && keyUpperCase[2] == 'A'
                        && keyUpperCase[3] == 'L' && keyUpperCase[4] == 'L'
                        && keyUpperCase[5] == 'I' && keyUpperCase[6] == 'N'
                        && keyUpperCase[7] == 'T') {
                    return MySQLToken.KW_SMALLINT;
                }
                if (keyUpperCase[1] == 'P') {
                    if (keyUpperCase.length < 7 || keyUpperCase.length > 8) {
                        return null;
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'A' && keyUpperCase[3] == 'T'
                            && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'A'
                            && keyUpperCase[6] == 'L') {
                        return MySQLToken.KW_SPATIAL;
                    }
                    if (keyUpperCase.length == 8 && keyUpperCase[2] == 'E' && keyUpperCase[3] == 'C'
                            && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'F'
                            && keyUpperCase[6] == 'I' && keyUpperCase[7] == 'C') {
                        return MySQLToken.KW_SPECIFIC;
                    }
                }
                if (keyUpperCase[1] == 'Q') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 19) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'L') {
                        if (keyUpperCase.length < 3 || keyUpperCase.length > 19) {
                            return null;
                        }
                        if (keyUpperCase.length == 3) {
                            return MySQLToken.KW_SQL;
                        }
                        if (keyUpperCase.length == 12 && keyUpperCase[3] == 'E'
                                && keyUpperCase[4] == 'X' && keyUpperCase[5] == 'C'
                                && keyUpperCase[6] == 'E' && keyUpperCase[7] == 'P'
                                && keyUpperCase[8] == 'T' && keyUpperCase[9] == 'I'
                                && keyUpperCase[10] == 'O' && keyUpperCase[11] == 'N') {
                            return MySQLToken.KW_SQLEXCEPTION;
                        }
                        if (keyUpperCase.length == 8 && keyUpperCase[3] == 'S'
                                && keyUpperCase[4] == 'T' && keyUpperCase[5] == 'A'
                                && keyUpperCase[6] == 'T' && keyUpperCase[7] == 'E') {
                            return MySQLToken.KW_SQLSTATE;
                        }
                        if (keyUpperCase.length == 10 && keyUpperCase[3] == 'W'
                                && keyUpperCase[4] == 'A' && keyUpperCase[5] == 'R'
                                && keyUpperCase[6] == 'N' && keyUpperCase[7] == 'I'
                                && keyUpperCase[8] == 'N' && keyUpperCase[9] == 'G') {
                            return MySQLToken.KW_SQLWARNING;
                        }
                        if (keyUpperCase[3] == '_') {
                            if (keyUpperCase.length < 14 || keyUpperCase.length > 19) {
                                return null;
                            }
                            if (keyUpperCase.length == 14 && keyUpperCase[4] == 'B'
                                    && keyUpperCase[5] == 'I' && keyUpperCase[6] == 'G'
                                    && keyUpperCase[7] == '_' && keyUpperCase[8] == 'R'
                                    && keyUpperCase[9] == 'E' && keyUpperCase[10] == 'S'
                                    && keyUpperCase[11] == 'U' && keyUpperCase[12] == 'L'
                                    && keyUpperCase[13] == 'T') {
                                return MySQLToken.KW_SQL_BIG_RESULT;
                            }
                            if (keyUpperCase.length == 19 && keyUpperCase[4] == 'C'
                                    && keyUpperCase[5] == 'A' && keyUpperCase[6] == 'L'
                                    && keyUpperCase[7] == 'C' && keyUpperCase[8] == '_'
                                    && keyUpperCase[9] == 'F' && keyUpperCase[10] == 'O'
                                    && keyUpperCase[11] == 'U' && keyUpperCase[12] == 'N'
                                    && keyUpperCase[13] == 'D' && keyUpperCase[14] == '_'
                                    && keyUpperCase[15] == 'R' && keyUpperCase[16] == 'O'
                                    && keyUpperCase[17] == 'W' && keyUpperCase[18] == 'S') {
                                return MySQLToken.KW_SQL_CALC_FOUND_ROWS;
                            }
                            if (keyUpperCase.length == 16 && keyUpperCase[4] == 'S'
                                    && keyUpperCase[5] == 'M' && keyUpperCase[6] == 'A'
                                    && keyUpperCase[7] == 'L' && keyUpperCase[8] == 'L'
                                    && keyUpperCase[9] == '_' && keyUpperCase[10] == 'R'
                                    && keyUpperCase[11] == 'E' && keyUpperCase[12] == 'S'
                                    && keyUpperCase[13] == 'U' && keyUpperCase[14] == 'L'
                                    && keyUpperCase[15] == 'T') {
                                return MySQLToken.KW_SQL_SMALL_RESULT;
                            }
                        }
                    }
                }
                if (keyUpperCase.length == 3 && keyUpperCase[1] == 'S' && keyUpperCase[2] == 'L') {
                    return MySQLToken.KW_SSL;
                }
                if (keyUpperCase[1] == 'T') {
                    if (keyUpperCase.length < 6 || keyUpperCase.length > 13) {
                        return null;
                    }
                    if (keyUpperCase.length == 8 && keyUpperCase[2] == 'A' && keyUpperCase[3] == 'R'
                            && keyUpperCase[4] == 'T' && keyUpperCase[5] == 'I'
                            && keyUpperCase[6] == 'N' && keyUpperCase[7] == 'G') {
                        return MySQLToken.KW_STARTING;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'O' && keyUpperCase[3] == 'R'
                            && keyUpperCase[4] == 'E' && keyUpperCase[5] == 'D') {
                        return MySQLToken.KW_STORED;
                    }
                    if (keyUpperCase.length == 13 && keyUpperCase[2] == 'R'
                            && keyUpperCase[3] == 'A' && keyUpperCase[4] == 'I'
                            && keyUpperCase[5] == 'G' && keyUpperCase[6] == 'H'
                            && keyUpperCase[7] == 'T' && keyUpperCase[8] == '_'
                            && keyUpperCase[9] == 'J' && keyUpperCase[10] == 'O'
                            && keyUpperCase[11] == 'I' && keyUpperCase[12] == 'N') {
                        return MySQLToken.KW_STRAIGHT_JOIN;
                    }
                }
                return null;
            }
            case 'T': {
                if (keyUpperCase.length < 2 || keyUpperCase.length > 10) {
                    return null;
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'A' && keyUpperCase[2] == 'B'
                        && keyUpperCase[3] == 'L' && keyUpperCase[4] == 'E') {
                    return MySQLToken.KW_TABLE;
                }
                if (keyUpperCase.length == 10 && keyUpperCase[1] == 'E' && keyUpperCase[2] == 'R'
                        && keyUpperCase[3] == 'M' && keyUpperCase[4] == 'I'
                        && keyUpperCase[5] == 'N' && keyUpperCase[6] == 'A'
                        && keyUpperCase[7] == 'T' && keyUpperCase[8] == 'E'
                        && keyUpperCase[9] == 'D') {
                    return MySQLToken.KW_TERMINATED;
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'H' && keyUpperCase[2] == 'E'
                        && keyUpperCase[3] == 'N') {
                    return MySQLToken.KW_THEN;
                }
                if (keyUpperCase[1] == 'I') {
                    if (keyUpperCase.length < 7 || keyUpperCase.length > 8) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'N') {
                        if (keyUpperCase.length < 7 || keyUpperCase.length > 8) {
                            return null;
                        }
                        if (keyUpperCase[3] == 'Y') {
                            if (keyUpperCase.length < 7 || keyUpperCase.length > 8) {
                                return null;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'B'
                                    && keyUpperCase[5] == 'L' && keyUpperCase[6] == 'O'
                                    && keyUpperCase[7] == 'B') {
                                return MySQLToken.KW_TINYBLOB;
                            }
                            if (keyUpperCase.length == 7 && keyUpperCase[4] == 'I'
                                    && keyUpperCase[5] == 'N' && keyUpperCase[6] == 'T') {
                                return MySQLToken.KW_TINYINT;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'T'
                                    && keyUpperCase[5] == 'E' && keyUpperCase[6] == 'X'
                                    && keyUpperCase[7] == 'T') {
                                return MySQLToken.KW_TINYTEXT;
                            }
                        }
                    }
                }
                if (keyUpperCase.length == 2 && keyUpperCase[1] == 'O') {
                    return MySQLToken.KW_TO;
                }
                if (keyUpperCase[1] == 'R') {
                    if (keyUpperCase.length == 4 && keyUpperCase[2] == 'U'
                            && keyUpperCase[3] == 'E') {
                        return MySQLToken.LITERAL_BOOL_TRUE;
                    }
                    if (keyUpperCase.length < 7 || keyUpperCase.length > 8) {
                        return null;
                    }
                    if (keyUpperCase.length == 8 && keyUpperCase[2] == 'A' && keyUpperCase[3] == 'I'
                            && keyUpperCase[4] == 'L' && keyUpperCase[5] == 'I'
                            && keyUpperCase[6] == 'N' && keyUpperCase[7] == 'G') {
                        return MySQLToken.KW_TRAILING;
                    }
                    if (keyUpperCase.length == 7 && keyUpperCase[2] == 'I' && keyUpperCase[3] == 'G'
                            && keyUpperCase[4] == 'G' && keyUpperCase[5] == 'E'
                            && keyUpperCase[6] == 'R') {
                        return MySQLToken.KW_TRIGGER;
                    }
                }
                return null;
            }
            case 'U': {
                if (keyUpperCase.length < 3 || keyUpperCase.length > 13) {
                    return null;
                }
                if (keyUpperCase[1] == 'N') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 8) {
                        return null;
                    }
                    if (keyUpperCase.length == 4 && keyUpperCase[2] == 'D'
                            && keyUpperCase[3] == 'O') {
                        return MySQLToken.KW_UNDO;
                    }
                    if (keyUpperCase[2] == 'I') {
                        if (keyUpperCase.length < 5 || keyUpperCase.length > 6) {
                            return null;
                        }
                        if (keyUpperCase.length == 5 && keyUpperCase[3] == 'O'
                                && keyUpperCase[4] == 'N') {
                            return MySQLToken.KW_UNION;
                        }
                        if (keyUpperCase.length == 6 && keyUpperCase[3] == 'Q'
                                && keyUpperCase[4] == 'U' && keyUpperCase[5] == 'E') {
                            return MySQLToken.KW_UNIQUE;
                        }
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'L' && keyUpperCase[3] == 'O'
                            && keyUpperCase[4] == 'C' && keyUpperCase[5] == 'K') {
                        return MySQLToken.KW_UNLOCK;
                    }
                    if (keyUpperCase.length == 8 && keyUpperCase[2] == 'S' && keyUpperCase[3] == 'I'
                            && keyUpperCase[4] == 'G' && keyUpperCase[5] == 'N'
                            && keyUpperCase[6] == 'E' && keyUpperCase[7] == 'D') {
                        return MySQLToken.KW_UNSIGNED;
                    }
                }
                if (keyUpperCase.length == 6 && keyUpperCase[1] == 'P' && keyUpperCase[2] == 'D'
                        && keyUpperCase[3] == 'A' && keyUpperCase[4] == 'T'
                        && keyUpperCase[5] == 'E') {
                    return MySQLToken.KW_UPDATE;
                }
                if (keyUpperCase[1] == 'S') {
                    if (keyUpperCase.length < 3 || keyUpperCase.length > 5) {
                        return null;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'A' && keyUpperCase[3] == 'G'
                            && keyUpperCase[4] == 'E') {
                        return MySQLToken.KW_USAGE;
                    }
                    if (keyUpperCase.length == 3 && keyUpperCase[2] == 'E') {
                        return MySQLToken.KW_USE;
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'I' && keyUpperCase[3] == 'N'
                            && keyUpperCase[4] == 'G') {
                        return MySQLToken.KW_USING;
                    }
                }
                if (keyUpperCase[1] == 'T') {
                    if (keyUpperCase.length < 8 || keyUpperCase.length > 13) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'C') {
                        if (keyUpperCase.length < 8 || keyUpperCase.length > 13) {
                            return null;
                        }
                        if (keyUpperCase[3] == '_') {
                            if (keyUpperCase.length < 8 || keyUpperCase.length > 13) {
                                return null;
                            }
                            if (keyUpperCase.length == 8 && keyUpperCase[4] == 'D'
                                    && keyUpperCase[5] == 'A' && keyUpperCase[6] == 'T'
                                    && keyUpperCase[7] == 'E') {
                                return MySQLToken.KW_UTC_DATE;
                            }
                            if (keyUpperCase[4] == 'T') {
                                if (keyUpperCase.length < 8 || keyUpperCase.length > 13) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'I') {
                                    if (keyUpperCase.length < 8 || keyUpperCase.length > 13) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'M') {
                                        if (keyUpperCase.length < 8 || keyUpperCase.length > 13) {
                                            return null;
                                        }
                                        if (keyUpperCase[7] == 'E') {
                                            if (keyUpperCase.length < 8
                                                    || keyUpperCase.length > 13) {
                                                return null;
                                            }
                                            if (keyUpperCase.length == 8) {
                                                return MySQLToken.KW_UTC_TIME;
                                            }
                                            if (keyUpperCase.length == 13 && keyUpperCase[8] == 'S'
                                                    && keyUpperCase[9] == 'T'
                                                    && keyUpperCase[10] == 'A'
                                                    && keyUpperCase[11] == 'M'
                                                    && keyUpperCase[12] == 'P') {
                                                return MySQLToken.KW_UTC_TIMESTAMP;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                return null;
            }
            case 'V': {
                if (keyUpperCase.length < 6 || keyUpperCase.length > 12) {
                    return null;
                }
                if (keyUpperCase[1] == 'A') {
                    if (keyUpperCase.length < 6 || keyUpperCase.length > 12) {
                        return null;
                    }
                    if (keyUpperCase.length == 6 && keyUpperCase[2] == 'L' && keyUpperCase[3] == 'U'
                            && keyUpperCase[4] == 'E' && keyUpperCase[5] == 'S') {
                        return MySQLToken.KW_VALUES;
                    }
                    if (keyUpperCase[2] == 'R') {
                        if (keyUpperCase.length < 7 || keyUpperCase.length > 12) {
                            return null;
                        }
                        if (keyUpperCase.length == 9 && keyUpperCase[3] == 'B'
                                && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'N'
                                && keyUpperCase[6] == 'A' && keyUpperCase[7] == 'R'
                                && keyUpperCase[8] == 'Y') {
                            return MySQLToken.KW_VARBINARY;
                        }
                        if (keyUpperCase[3] == 'C') {
                            if (keyUpperCase.length < 7 || keyUpperCase.length > 12) {
                                return null;
                            }
                            if (keyUpperCase[4] == 'H') {
                                if (keyUpperCase.length < 7 || keyUpperCase.length > 12) {
                                    return null;
                                }
                                if (keyUpperCase[5] == 'A') {
                                    if (keyUpperCase.length < 7 || keyUpperCase.length > 12) {
                                        return null;
                                    }
                                    if (keyUpperCase[6] == 'R') {
                                        if (keyUpperCase.length < 7 || keyUpperCase.length > 12) {
                                            return null;
                                        }
                                        if (keyUpperCase.length == 7) {
                                            return MySQLToken.KW_VARCHAR;
                                        }
                                        if (keyUpperCase.length == 12 && keyUpperCase[7] == 'A'
                                                && keyUpperCase[8] == 'C' && keyUpperCase[9] == 'T'
                                                && keyUpperCase[10] == 'E'
                                                && keyUpperCase[11] == 'R') {
                                            return MySQLToken.KW_VARCHARACTER;
                                        }
                                    }
                                }
                            }
                        }
                        if (keyUpperCase.length == 7 && keyUpperCase[3] == 'Y'
                                && keyUpperCase[4] == 'I' && keyUpperCase[5] == 'N'
                                && keyUpperCase[6] == 'G') {
                            return MySQLToken.KW_VARYING;
                        }
                    }
                }
                if (keyUpperCase.length == 7 && keyUpperCase[1] == 'I' && keyUpperCase[2] == 'R'
                        && keyUpperCase[3] == 'T' && keyUpperCase[4] == 'U'
                        && keyUpperCase[5] == 'A' && keyUpperCase[6] == 'L') {
                    return MySQLToken.KW_VIRTUAL;
                }
                return null;
            }
            case 'W': {
                if (keyUpperCase.length < 4 || keyUpperCase.length > 5) {
                    return null;
                }
                if (keyUpperCase[1] == 'H') {
                    if (keyUpperCase.length < 4 || keyUpperCase.length > 5) {
                        return null;
                    }
                    if (keyUpperCase[2] == 'E') {
                        if (keyUpperCase.length < 4 || keyUpperCase.length > 5) {
                            return null;
                        }
                        if (keyUpperCase.length == 4 && keyUpperCase[3] == 'N') {
                            return MySQLToken.KW_WHEN;
                        }
                        if (keyUpperCase.length == 5 && keyUpperCase[3] == 'R'
                                && keyUpperCase[4] == 'E') {
                            return MySQLToken.KW_WHERE;
                        }
                    }
                    if (keyUpperCase.length == 5 && keyUpperCase[2] == 'I' && keyUpperCase[3] == 'L'
                            && keyUpperCase[4] == 'E') {
                        return MySQLToken.KW_WHILE;
                    }
                }
                if (keyUpperCase.length == 4 && keyUpperCase[1] == 'I' && keyUpperCase[2] == 'T'
                        && keyUpperCase[3] == 'H') {
                    return MySQLToken.KW_WITH;
                }
                if (keyUpperCase.length == 5 && keyUpperCase[1] == 'R' && keyUpperCase[2] == 'I'
                        && keyUpperCase[3] == 'T' && keyUpperCase[4] == 'E') {
                    return MySQLToken.KW_WRITE;
                }
                return null;
            }
            case 'X': {
                if (keyUpperCase.length != 3) {
                    return null;
                }
                if (keyUpperCase.length == 3 && keyUpperCase[1] == 'O' && keyUpperCase[2] == 'R') {
                    return MySQLToken.KW_XOR;
                }
                return null;
            }
            case 'Y': {
                if (keyUpperCase.length != 10) {
                    return null;
                }
                if (keyUpperCase.length == 10 && keyUpperCase[1] == 'E' && keyUpperCase[2] == 'A'
                        && keyUpperCase[3] == 'R' && keyUpperCase[4] == '_'
                        && keyUpperCase[5] == 'M' && keyUpperCase[6] == 'O'
                        && keyUpperCase[7] == 'N' && keyUpperCase[8] == 'T'
                        && keyUpperCase[9] == 'H') {
                    return MySQLToken.KW_YEAR_MONTH;
                }
                return null;
            }
            case 'Z': {
                if (keyUpperCase.length != 8) {
                    return null;
                }
                if (keyUpperCase.length == 8 && keyUpperCase[1] == 'E' && keyUpperCase[2] == 'R'
                        && keyUpperCase[3] == 'O' && keyUpperCase[4] == 'F'
                        && keyUpperCase[5] == 'I' && keyUpperCase[6] == 'L'
                        && keyUpperCase[7] == 'L') {
                    return MySQLToken.KW_ZEROFILL;
                }
                return null;
            }
        }
        return null;
    }

}
