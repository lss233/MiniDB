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
 * (created at 2011-9-12)
 */
package miniDB.parser.recognizer.mysql.syntax;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.stmt.mts.*;
import miniDB.parser.ast.stmt.mts.MTSCommitStatement.CompleteType;
import miniDB.parser.ast.stmt.mts.MTSStartTransactionStatement.TransactionCharacteristic;
import miniDB.parser.recognizer.mysql.lexer.MySQLLexer;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import static miniDB.parser.recognizer.mysql.MySQLToken.EOF;
import static miniDB.parser.recognizer.mysql.MySQLToken.KW_RELEASE;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class MySQLMTSParser extends MySQLParser {
    private static enum SpecialIdentifier {
        CHAIN, NO, RELEASE, SAVEPOINT, WORK, TRANSACTION, WITH, CONSISTENT, SNAPSHOT, READ, WRITE, ONLY
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers =
            new HashMap<String, SpecialIdentifier>();

    static {
        specialIdentifiers.put("SAVEPOINT", SpecialIdentifier.SAVEPOINT);
        specialIdentifiers.put("WORK", SpecialIdentifier.WORK);
        specialIdentifiers.put("CHAIN", SpecialIdentifier.CHAIN);
        specialIdentifiers.put("RELEASE", SpecialIdentifier.RELEASE);
        specialIdentifiers.put("NO", SpecialIdentifier.NO);
        specialIdentifiers.put("TRANSACTION", SpecialIdentifier.TRANSACTION);
        specialIdentifiers.put("WITH", SpecialIdentifier.WITH);
        specialIdentifiers.put("CONSISTENT", SpecialIdentifier.CONSISTENT);
        specialIdentifiers.put("SNAPSHOT", SpecialIdentifier.SNAPSHOT);
        specialIdentifiers.put("READ", SpecialIdentifier.READ);
        specialIdentifiers.put("WRITE", SpecialIdentifier.WRITE);
        specialIdentifiers.put("ONLY", SpecialIdentifier.ONLY);
    }

    public MySQLMTSParser(MySQLLexer lexer) {
        super(lexer);
    }

    /**
     * first token <code>SAVEPOINT</code> is scanned but not yet consumed
     */
    public MTSSavepointStatement savepoint() throws SQLSyntaxErrorException {
        // matchIdentifier("SAVEPOINT"); // for performance issue, change to
        // follow:
        lexer.nextToken();
        Identifier id = identifier();
        return new MTSSavepointStatement(id);
    }

    /**
     * first token <code>RELEASE</code> is scanned but not yet consumed
     */
    public MTSReleaseStatement release() throws SQLSyntaxErrorException {
        match(KW_RELEASE);
        matchIdentifier("SAVEPOINT");
        Identifier id = identifier();
        return new MTSReleaseStatement(id);
    }

    /**
     * first token <code>ROLLBACK</code> is scanned but not yet consumed
     * 
     * <pre>
     *         ROLLBACK [WORK] TO [SAVEPOINT] identifier
     *         ROLLBACK [WORK] [AND [NO] CHAIN | [NO] RELEASE]
     * </pre>
     */
    public MTSRollbackStatement rollback() throws SQLSyntaxErrorException {
        // matchIdentifier("ROLLBACK"); // for performance issue, change to
        // follow:
        lexer.nextToken();
        SpecialIdentifier siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
        if (siTemp == SpecialIdentifier.WORK) {
            lexer.nextToken();
        }
        switch (lexer.token()) {
            case EOF:
                return new MTSRollbackStatement(MTSRollbackStatement.CompleteType.UN_DEF);
            case KW_TO:
                lexer.nextToken();
                String str = lexer.stringValueUppercase();
                if (specialIdentifiers.get(str) == SpecialIdentifier.SAVEPOINT) {
                    lexer.nextToken();
                }
                Identifier savepoint = identifier();
                return new MTSRollbackStatement(savepoint);
            case KW_AND:
                lexer.nextToken();
                siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                MTSRollbackStatement.CompleteType type = MTSRollbackStatement.CompleteType.CHAIN;
                if (siTemp == SpecialIdentifier.NO) {
                    lexer.nextToken();
                    type = MTSRollbackStatement.CompleteType.NO_CHAIN;
                }
                matchIdentifier("CHAIN");
                switch (lexer.token()) {
                    case EOF:
                        return new MTSRollbackStatement(type);
                    case IDENTIFIER:
                        if ("NO".equals(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            match(KW_RELEASE);
                            match(EOF);
                            return new MTSRollbackStatement(type);
                        }
                    case KW_RELEASE:
                        if (type == MTSRollbackStatement.CompleteType.NO_CHAIN) {
                            lexer.nextToken();
                            match(EOF);
                            return new MTSRollbackStatement(
                                    MTSRollbackStatement.CompleteType.RELEASE);
                        }
                        break;
                    default:
                }
                throw err("unrecognized complete type: " + lexer.token());
            case KW_RELEASE:
                lexer.nextToken();
                match(EOF);
                return new MTSRollbackStatement(MTSRollbackStatement.CompleteType.RELEASE);
            case IDENTIFIER: {
                if ("NO".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    match(KW_RELEASE);
                    match(EOF);
                    return new MTSRollbackStatement(MTSRollbackStatement.CompleteType.NO_RELEASE);
                }
            }
            default:
                throw err("unrecognized complete type: " + lexer.token());
        }
    }

    /**
     * COMMIT [WORK] [AND [NO] CHAIN] [[NO] RELEASE]
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    public MTSCommitStatement commit() throws SQLSyntaxErrorException {
        lexer.nextToken();
        SpecialIdentifier siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
        if (siTemp == SpecialIdentifier.WORK) {
            lexer.nextToken();
        }
        switch (lexer.token()) {
            case EOF:
                return new MTSCommitStatement(MTSCommitStatement.CompleteType.UN_DEF);
            case KW_AND:
                lexer.nextToken();
                siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                CompleteType type = CompleteType.CHAIN;
                if (siTemp == SpecialIdentifier.NO) {
                    lexer.nextToken();
                    type = CompleteType.NO_CHAIN;
                }
                matchIdentifier("CHAIN");
                switch (lexer.token()) {
                    case EOF:
                        return new MTSCommitStatement(type);
                    case IDENTIFIER:
                        if ("NO".equals(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            match(KW_RELEASE);
                            match(EOF);
                            return new MTSCommitStatement(type);
                        }
                        break;
                    case KW_RELEASE:
                        if (type == CompleteType.NO_CHAIN) {
                            lexer.nextToken();
                            match(EOF);
                            return new MTSCommitStatement(CompleteType.RELEASE);
                        }
                    default:
                }
                throw err("unrecognized complete type: " + lexer.token());
            case KW_RELEASE:
                lexer.nextToken();
                match(EOF);
                return new MTSCommitStatement(MTSCommitStatement.CompleteType.RELEASE);
            case IDENTIFIER: {
                if ("NO".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    match(KW_RELEASE);
                    match(EOF);
                    return new MTSCommitStatement(MTSCommitStatement.CompleteType.NO_RELEASE);
                }
            }
            default:
                throw err("unrecognized complete type: " + lexer.token());
        }
    }

    public MTSStartTransactionStatement start() throws SQLSyntaxErrorException {
        lexer.nextToken();
        SpecialIdentifier siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
        if (siTemp == SpecialIdentifier.TRANSACTION) {
            lexer.nextToken();
            if (lexer.token() == EOF) {
                return new MTSStartTransactionStatement(null);
            }
            siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
            switch (siTemp) {
                case WITH:
                    lexer.nextToken();
                    siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (siTemp == SpecialIdentifier.CONSISTENT) {
                        lexer.nextToken();
                        siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (siTemp == SpecialIdentifier.SNAPSHOT) {
                            lexer.nextToken();
                            match(EOF);
                            return new MTSStartTransactionStatement(
                                    TransactionCharacteristic.WITH_CONSISTENT_SNAPSHOT);
                        }
                    }
                    break;
                case READ:
                    lexer.nextToken();
                    siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (siTemp == SpecialIdentifier.WRITE) {
                        lexer.nextToken();
                        match(EOF);
                        return new MTSStartTransactionStatement(
                                TransactionCharacteristic.READ_WRITE);
                    } else if (siTemp == SpecialIdentifier.ONLY) {
                        lexer.nextToken();
                        match(EOF);
                        return new MTSStartTransactionStatement(
                                TransactionCharacteristic.READ_ONLY);
                    }
                    break;
                default:
                    throw err("unrecognized transaction characteristic: " + lexer.token());
            }
        }
        throw err("unrecognized transaction characteristic: ");
    }

}
