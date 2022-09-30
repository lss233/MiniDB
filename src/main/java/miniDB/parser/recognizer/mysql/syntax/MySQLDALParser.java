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
 * (created at 2011-5-19)
 */
package miniDB.parser.recognizer.mysql.syntax;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.*;
import miniDB.parser.ast.expression.primary.literal.LiteralNumber;
import miniDB.parser.ast.expression.primary.literal.LiteralString;
import miniDB.parser.ast.fragment.Limit;
import miniDB.parser.ast.fragment.VariableScope;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.dal.*;
import miniDB.parser.ast.stmt.ddl.DescTableStatement;
import miniDB.parser.ast.stmt.ddl.ExplainStatement;
import miniDB.parser.ast.stmt.ddl.ExplainStatement.Commands;
import miniDB.parser.ast.stmt.ddl.ExplainStatement.ExplainType;
import miniDB.parser.ast.stmt.ddl.ExplainStatement.FormatName;
import miniDB.parser.ast.stmt.dml.DMLStatement;
import miniDB.parser.ast.stmt.mts.MTSSetTransactionStatement;
import miniDB.parser.recognizer.mysql.MySQLToken;
import miniDB.parser.recognizer.mysql.lexer.MySQLLexer;
import miniDB.parser.util.Pair;

import java.sql.SQLSyntaxErrorException;
import java.util.*;

import static miniDB.parser.recognizer.mysql.MySQLToken.*;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class MySQLDALParser extends MySQLParser {
    protected MySQLExprParser exprParser;

    public MySQLDALParser(MySQLLexer lexer, MySQLExprParser exprParser) {
        super(lexer);
        this.exprParser = exprParser;
    }

    private static enum SpecialIdentifier {
        AUTHORS, BINLOG, BLOCK, CODE, COLLATION, COLUMNS, COMMITTED, CONTEXT, CONTRIBUTORS, COUNT, CPU, ENGINE, ENGINES, ERRORS, EVENT, EVENTS, FIELDS, FULL, FUNCTION, GLOBAL, GRANTS, HOSTS, INDEXES, INNODB, IPC, LOCAL, MASTER, MEMORY, MUTEX, NAMES, OPEN, PAGE, PERFORMANCE_SCHEMA, PLUGINS, PRIVILEGES, PROCESSLIST, PROFILE, PROFILES, REPEATABLE, SERIALIZABLE, SESSION, SLAVE, SOURCE, STATUS, STORAGE, SWAPS, TABLES, TRANSACTION, TRIGGERS, UNCOMMITTED, VARIABLES, VIEW, WARNINGS, USER,
        /**
         * SHOW CHARSET
         */
        CHARSET, FORMAT, CONNECTION, JSON, PARTITIONS, EXTENDED, NEW, OLD,
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers =
            new HashMap<String, SpecialIdentifier>();

    static {
        specialIdentifiers.put("AUTHORS", SpecialIdentifier.AUTHORS);
        specialIdentifiers.put("BINLOG", SpecialIdentifier.BINLOG);
        specialIdentifiers.put("COLLATION", SpecialIdentifier.COLLATION);
        specialIdentifiers.put("COLUMNS", SpecialIdentifier.COLUMNS);
        specialIdentifiers.put("CONTRIBUTORS", SpecialIdentifier.CONTRIBUTORS);
        specialIdentifiers.put("EVENT", SpecialIdentifier.EVENT);
        specialIdentifiers.put("FUNCTION", SpecialIdentifier.FUNCTION);
        specialIdentifiers.put("VIEW", SpecialIdentifier.VIEW);
        specialIdentifiers.put("ENGINE", SpecialIdentifier.ENGINE);
        specialIdentifiers.put("ENGINES", SpecialIdentifier.ENGINES);
        specialIdentifiers.put("ERRORS", SpecialIdentifier.ERRORS);
        specialIdentifiers.put("EVENTS", SpecialIdentifier.EVENTS);
        specialIdentifiers.put("FIELDS", SpecialIdentifier.FIELDS);
        specialIdentifiers.put("FULL", SpecialIdentifier.FULL);
        specialIdentifiers.put("GLOBAL", SpecialIdentifier.GLOBAL);
        specialIdentifiers.put("GRANTS", SpecialIdentifier.GRANTS);
        specialIdentifiers.put("MASTER", SpecialIdentifier.MASTER);
        specialIdentifiers.put("OPEN", SpecialIdentifier.OPEN);
        specialIdentifiers.put("PLUGINS", SpecialIdentifier.PLUGINS);
        specialIdentifiers.put("CODE", SpecialIdentifier.CODE);
        specialIdentifiers.put("STATUS", SpecialIdentifier.STATUS);
        specialIdentifiers.put("PRIVILEGES", SpecialIdentifier.PRIVILEGES);
        specialIdentifiers.put("PROCESSLIST", SpecialIdentifier.PROCESSLIST);
        specialIdentifiers.put("PROFILE", SpecialIdentifier.PROFILE);
        specialIdentifiers.put("PROFILES", SpecialIdentifier.PROFILES);
        specialIdentifiers.put("SESSION", SpecialIdentifier.SESSION);
        specialIdentifiers.put("SLAVE", SpecialIdentifier.SLAVE);
        specialIdentifiers.put("STORAGE", SpecialIdentifier.STORAGE);
        specialIdentifiers.put("TABLES", SpecialIdentifier.TABLES);
        specialIdentifiers.put("TRIGGERS", SpecialIdentifier.TRIGGERS);
        specialIdentifiers.put("VARIABLES", SpecialIdentifier.VARIABLES);
        specialIdentifiers.put("WARNINGS", SpecialIdentifier.WARNINGS);
        specialIdentifiers.put("INNODB", SpecialIdentifier.INNODB);
        specialIdentifiers.put("PERFORMANCE_SCHEMA", SpecialIdentifier.PERFORMANCE_SCHEMA);
        specialIdentifiers.put("MUTEX", SpecialIdentifier.MUTEX);
        specialIdentifiers.put("COUNT", SpecialIdentifier.COUNT);
        specialIdentifiers.put("BLOCK", SpecialIdentifier.BLOCK);
        specialIdentifiers.put("CONTEXT", SpecialIdentifier.CONTEXT);
        specialIdentifiers.put("CPU", SpecialIdentifier.CPU);
        specialIdentifiers.put("MEMORY", SpecialIdentifier.MEMORY);
        specialIdentifiers.put("PAGE", SpecialIdentifier.PAGE);
        specialIdentifiers.put("SOURCE", SpecialIdentifier.SOURCE);
        specialIdentifiers.put("SWAPS", SpecialIdentifier.SWAPS);
        specialIdentifiers.put("IPC", SpecialIdentifier.IPC);
        specialIdentifiers.put("LOCAL", SpecialIdentifier.LOCAL);
        specialIdentifiers.put("HOSTS", SpecialIdentifier.HOSTS);
        specialIdentifiers.put("INDEXES", SpecialIdentifier.INDEXES);
        specialIdentifiers.put("TRANSACTION", SpecialIdentifier.TRANSACTION);
        specialIdentifiers.put("UNCOMMITTED", SpecialIdentifier.UNCOMMITTED);
        specialIdentifiers.put("COMMITTED", SpecialIdentifier.COMMITTED);
        specialIdentifiers.put("REPEATABLE", SpecialIdentifier.REPEATABLE);
        specialIdentifiers.put("SERIALIZABLE", SpecialIdentifier.SERIALIZABLE);
        specialIdentifiers.put("NAMES", SpecialIdentifier.NAMES);
        specialIdentifiers.put("CHARSET", SpecialIdentifier.CHARSET);
        specialIdentifiers.put("USER", SpecialIdentifier.USER);
        specialIdentifiers.put("FORMAT", SpecialIdentifier.FORMAT);
        specialIdentifiers.put("CONNECTION", SpecialIdentifier.CONNECTION);
        specialIdentifiers.put("PARTITIONS", SpecialIdentifier.PARTITIONS);
        specialIdentifiers.put("EXTENDED", SpecialIdentifier.EXTENDED);
        specialIdentifiers.put("NEW", SpecialIdentifier.NEW);
        specialIdentifiers.put("OLD", SpecialIdentifier.OLD);
    }

    /**
     * {EXPLAIN | DESCRIBE | DESC} Syntax 中<br>
     * explainable_stmt 部分的解析
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    private DMLStatement matchDMLStatement() throws SQLSyntaxErrorException {
        switch (lexer.token()) {
            case KW_SELECT:
                return new MySQLDMLSelectParser(lexer, exprParser).selectUnion(false);
            case KW_DELETE:
                return new MySQLDMLDeleteParser(lexer, exprParser).delete();
            case KW_INSERT:
                return new MySQLDMLInsertParser(lexer, exprParser).insert();
            case KW_REPLACE:
                return new MySQLDMLReplaceParser(lexer, exprParser).replace();
            case KW_UPDATE:
                return new MySQLDMLUpdateParser(lexer, exprParser).update();
            default:
                throw err("unexpect token for {EXPLAIN | DESCRIBE | DESC} ... explainable_stmt ");
        }
    }

    /**
     * {EXPLAIN | DESCRIBE | DESC} Syntax 中<br>
     * {explainable_stmt | FOR CONNECTION connection_id} 部分的解析
     * 
     * @param command
     * @param explain_type
     * @param format_name
     * @return
     * @throws SQLSyntaxErrorException
     */
    private ExplainStatement getExplainStatementOf(Commands command, ExplainType explain_type,
            FormatName format_name) throws SQLSyntaxErrorException {
        switch (lexer.token()) {
            case KW_SELECT:
            case KW_DELETE:
            case KW_INSERT:
            case KW_REPLACE:
            case KW_UPDATE: {
                DMLStatement explainable_stmt = matchDMLStatement();
                return new ExplainStatement(command, explain_type, format_name, explainable_stmt);
            }
            case KW_FOR: {
                lexer.nextToken();
                if (lexer.token() == IDENTIFIER
                        && SpecialIdentifier.CONNECTION == specialIdentifiers
                                .get(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    switch (lexer.token()) {
                        case LITERAL_NUM_PURE_DIGIT:
                            LiteralNumber connection_id = new LiteralNumber(lexer.integerValue());
                            lexer.nextToken();
                            return new ExplainStatement(command, explain_type, format_name,
                                    connection_id);
                        default:
                            throw err(
                                    "unexpect token for {EXPLAIN | DESCRIBE | DESC} FOR CONNECTION connection_id");
                    }
                }
            }
            default: {
                throw err("unexpect token for {EXPLAIN | DESCRIBE | DESC} Syntax");
            }
        }
    }

    /**
     * 针对 {EXPLAIN | DESCRIBE | DESC} Syntax 语法的解析
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    public ExplainStatement explain() throws SQLSyntaxErrorException {
        Commands command = null;
        switch (lexer.token()) {
            case KW_DESC:
                command = Commands.DESC;
                break;
            case KW_DESCRIBE:
                command = Commands.DESCRIBE;
                break;
            case KW_EXPLAIN:
                command = Commands.EXPLAIN;
                break;
            default: {
                throw err("unexpect token for {EXPLAIN | DESCRIBE | DESC} Syntax");
            }
        }
        match(KW_DESC, KW_DESCRIBE, MySQLToken.KW_EXPLAIN);
        Identifier tbl_name = null;
        Identifier col_name = null;
        String wild = null;
        ExplainType explain_type = null;
        FormatName format_name = null;
        switch (lexer.token()) {
            case KW_SELECT:
            case KW_DELETE:
            case KW_INSERT:
            case KW_REPLACE:
            case KW_UPDATE:
            case KW_FOR: {
                return getExplainStatementOf(command, explain_type, format_name);
            }
            case IDENTIFIER: {
                SpecialIdentifier tempSi = specialIdentifiers.get(lexer.stringValueUppercase());
                if (tempSi == SpecialIdentifier.FORMAT) {
                    explain_type = ExplainType.FORMAT;
                    lexer.nextToken();
                    match(OP_EQUALS);
                    switch (lexer.token()) {
                        case IDENTIFIER: {
                            if (SpecialIdentifier.JSON == specialIdentifiers
                                    .get(lexer.stringValueUppercase())) {
                                format_name = FormatName.JSON;
                                lexer.nextToken();
                                return getExplainStatementOf(command, explain_type, format_name);
                            } else if ("TRADITIONAL".equals(lexer.stringValueUppercase())) {
                                format_name = FormatName.TRADITIONAL;
                                lexer.nextToken();
                                return getExplainStatementOf(command, explain_type, format_name);
                            }
                        }
                        default: {
                            throw err(
                                    "unexpect token for {EXPLAIN | DESCRIBE | DESC} FORMAT = format_name");
                        }
                    }
                } else if (tempSi == SpecialIdentifier.PARTITIONS) {
                    explain_type = ExplainType.PARTITIONS;
                    lexer.nextToken();
                    return getExplainStatementOf(command, explain_type, format_name);
                } else if (tempSi == SpecialIdentifier.EXTENDED) {
                    explain_type = ExplainType.EXTENDED;
                    lexer.nextToken();
                    return getExplainStatementOf(command, explain_type, format_name);
                }
                tbl_name = identifier();
                switch (lexer.token()) {
                    case LITERAL_CHARS:
                        wild = lexer.stringValue();
                        lexer.nextToken();
                        break;
                    case IDENTIFIER: {
                        col_name = identifier();
                        break;
                    }
                    default:
                }
                return new ExplainStatement(command, tbl_name, col_name, wild);
            }
            default: {
                throw err("unexpect token for {EXPLAIN | DESCRIBE | DESC} Syntax");
            }
        }
    }

    public DescTableStatement desc() throws SQLSyntaxErrorException {
        match(KW_DESC, KW_DESCRIBE);
        Identifier table = identifier();
        return new DescTableStatement(table);
    }

    @SuppressWarnings("incomplete-switch")
    public DALShowStatement show() throws SQLSyntaxErrorException {
        match(KW_SHOW);
        String tempStr;
        String tempStrUp;
        Expression tempExpr;
        Identifier tempId;
        SpecialIdentifier tempSi;
        Limit tempLimit;
        switch (lexer.token()) {
            case KW_BINARY:
                lexer.nextToken();
                matchIdentifier("LOGS");
                return new ShowBinaryLog();
            case KW_CHARACTER:
                lexer.nextToken();
                match(KW_SET);
                switch (lexer.token()) {
                    case KW_LIKE:
                        tempStr = like();
                        return new ShowCharaterSet(tempStr);
                    case KW_WHERE:
                        tempExpr = where();
                        return new ShowCharaterSet(tempExpr);
                    default:
                        return new ShowCharaterSet();
                }
            case KW_CREATE: {
                ShowCreate.Type showCreateType = null;
                ShowCreateDatabase.Type showCreateDatabaseType = null;
                switch1: switch (lexer.nextToken()) {
                    case KW_DATABASE:
                        showCreateDatabaseType = ShowCreateDatabase.Type.DATABASE;
                        break;
                    case KW_PROCEDURE:
                        showCreateType = ShowCreate.Type.PROCEDURE;
                        break;
                    case KW_SCHEMA:
                        showCreateDatabaseType = ShowCreateDatabase.Type.SCHEMA;
                        break;
                    case KW_TABLE:
                        showCreateType = ShowCreate.Type.TABLE;
                        break;
                    case KW_TRIGGER:
                        showCreateType = ShowCreate.Type.TRIGGER;
                        break;
                    case IDENTIFIER:
                        tempSi = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (tempSi != null) {
                            switch (tempSi) {
                                case EVENT:
                                    showCreateType = ShowCreate.Type.EVENT;
                                    break switch1;
                                case FUNCTION:
                                    showCreateType = ShowCreate.Type.FUNCTION;
                                    break switch1;
                                case VIEW:
                                    showCreateType = ShowCreate.Type.VIEW;
                                    break switch1;
                                case USER:
                                    showCreateType = ShowCreate.Type.USER;
                                    break switch1;
                            }
                        }
                    default:
                        throw err("unexpect token for SHOW CREATE");
                }
                lexer.nextToken();
                boolean ifNotExists = false;
                tempId = null;
                if (lexer.token() != null) {
                    switch (lexer.token()) {
                        case KW_IF: {
                            lexer.nextToken();
                            if (lexer.token() == MySQLToken.KW_NOT) {
                                lexer.nextToken();
                                if (lexer.token() == MySQLToken.KW_EXISTS) {
                                    ifNotExists = true;
                                    break;
                                }
                            }
                            throw err("unexpect token for SHOW CREATE");
                        }
                        case IDENTIFIER: {
                            tempId = identifier();
                            break;
                        }
                        default: {
                            throw err("unexpect token for SHOW CREATE");
                        }
                    }
                }
                if (tempId == null) {
                    lexer.nextToken();
                    switch (lexer.token()) {
                        case IDENTIFIER: {
                            tempId = identifier();
                            break;
                        }
                        default: {
                            throw err("unexpect token for SHOW CREATE");
                        }
                    }
                }
                if (showCreateDatabaseType != null) {
                    return new ShowCreateDatabase(showCreateDatabaseType, ifNotExists, tempId);
                }
                return new ShowCreate(showCreateType, tempId);
            }
            case KW_SCHEMAS:
            case KW_DATABASES:
                lexer.nextToken();
                switch (lexer.token()) {
                    case KW_LIKE:
                        tempStr = like();
                        return new ShowDatabases(tempStr);
                    case KW_WHERE:
                        tempExpr = where();
                        return new ShowDatabases(tempExpr);
                }
                return new ShowDatabases();
            case KW_KEYS:
                return showIndex(ShowIndex.Type.KEYS);
            case KW_INDEX:
                return showIndex(ShowIndex.Type.INDEX);
            case KW_PROCEDURE:
                lexer.nextToken();
                tempStrUp = lexer.stringValueUppercase();
                tempSi = specialIdentifiers.get(tempStrUp);
                if (tempSi != null) {
                    switch (tempSi) {
                        case CODE:
                            lexer.nextToken();
                            tempId = identifier();
                            return new ShowProcedureCode(tempId);
                        case STATUS:
                            switch (lexer.nextToken()) {
                                case KW_LIKE:
                                    tempStr = like();
                                    return new ShowProcedureStatus(tempStr);
                                case KW_WHERE:
                                    tempExpr = where();
                                    return new ShowProcedureStatus(tempExpr);
                                default:
                                    return new ShowProcedureStatus();
                            }
                    }
                }
                throw err("unexpect token for SHOW PROCEDURE");
            case KW_TABLE:
                lexer.nextToken();
                matchIdentifier("STATUS");
                tempId = null;
                if (lexer.token() == KW_FROM || lexer.token() == KW_IN) {
                    lexer.nextToken();
                    tempId = identifier();
                }
                switch (lexer.token()) {
                    case KW_LIKE:
                        tempStr = like();
                        return new ShowTableStatus(tempId, tempStr);
                    case KW_WHERE:
                        tempExpr = where();
                        return new ShowTableStatus(tempId, tempExpr);
                }
                return new ShowTableStatus(tempId);
            case IDENTIFIER:
                tempStrUp = lexer.stringValueUppercase();
                tempSi = specialIdentifiers.get(tempStrUp);
                if (tempSi == null) {
                    break;
                }
                switch (tempSi) {
                    case INDEXES:
                        return showIndex(ShowIndex.Type.INDEXES);
                    case GRANTS:
                        if (lexer.nextToken() == KW_FOR) {
                            lexer.nextToken();
                            tempExpr = exprParser.expression();
                            return new ShowGrants(tempExpr);
                        }
                        return new ShowGrants();
                    case AUTHORS:
                        lexer.nextToken();
                        return new ShowAuthors();
                    case BINLOG:
                        lexer.nextToken();
                        matchIdentifier("EVENTS");
                        tempStr = null;
                        tempExpr = null;
                        tempLimit = null;
                        if (lexer.token() == KW_IN) {
                            lexer.nextToken();
                            tempStr = lexer.stringValue();
                            lexer.nextToken();
                        }
                        if (lexer.token() == KW_FROM) {
                            lexer.nextToken();
                            tempExpr = exprParser.expression();
                        }
                        if (lexer.token() == KW_LIMIT) {
                            tempLimit = limit();
                        }
                        return new ShowBinLogEvent(tempStr, tempExpr, tempLimit);
                    case COLLATION:
                        switch (lexer.nextToken()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowCollation(tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowCollation(tempExpr);
                        }
                        return new ShowCollation();
                    case COLUMNS:
                        return showColumns(false);
                    case CONTRIBUTORS:
                        lexer.nextToken();
                        return new ShowContributors();
                    case ENGINE:
                        switch (lexer.nextToken()) {
                            case IDENTIFIER:
                                tempStrUp = lexer.stringValueUppercase();
                                tempSi = specialIdentifiers.get(tempStrUp);
                                if (tempSi != null) {
                                    switch (tempSi) {
                                        case INNODB:
                                            lexer.nextToken();
                                            tempStrUp = lexer.stringValueUppercase();
                                            tempSi = specialIdentifiers.get(tempStrUp);
                                            if (tempSi != null) {
                                                switch (tempSi) {
                                                    case STATUS:
                                                        lexer.nextToken();
                                                        return new ShowEngine(
                                                                ShowEngine.Type.INNODB_STATUS);
                                                    case MUTEX:
                                                        lexer.nextToken();
                                                        return new ShowEngine(
                                                                ShowEngine.Type.INNODB_MUTEX);
                                                }
                                            }
                                        case PERFORMANCE_SCHEMA:
                                            lexer.nextToken();
                                            matchIdentifier("STATUS");
                                            return new ShowEngine(
                                                    ShowEngine.Type.PERFORMANCE_SCHEMA_STATUS);
                                    }
                                }
                            default:
                                throw err("unexpect token for SHOW ENGINE");
                        }
                    case ENGINES:
                        lexer.nextToken();
                        return new ShowEngines();
                    case ERRORS:
                        lexer.nextToken();
                        tempLimit = limit();
                        return new ShowErrors(false, tempLimit);
                    case COUNT:
                        lexer.nextToken();
                        match(PUNC_LEFT_PAREN);
                        match(OP_ASTERISK);
                        match(PUNC_RIGHT_PAREN);
                        switch (matchIdentifier("ERRORS", "WARNINGS")) {
                            case 0:
                                return new ShowErrors(true, null);
                            case 1:
                                return new ShowWarnings(true, null);
                        }
                    case EVENTS:
                        tempId = null;
                        switch (lexer.nextToken()) {
                            case KW_IN:
                            case KW_FROM:
                                lexer.nextToken();
                                tempId = identifier();
                        }
                        switch (lexer.token()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowEvents(tempId, tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowEvents(tempId, tempExpr);
                            default:
                                return new ShowEvents(tempId);
                        }
                    case FIELDS:
                        return showFields(false);
                    // lexer.nextToken();
                    // match(MySQLToken.KW_FROM);
                    // Identifier table = identifier();
                    // return new ShowFields(table);
                    case FULL:
                        lexer.nextToken();
                        tempStrUp = lexer.stringValueUppercase();
                        tempSi = specialIdentifiers.get(tempStrUp);
                        if (tempSi != null) {
                            switch (tempSi) {
                                case COLUMNS:
                                    return showColumns(true);
                                case FIELDS:
                                    return showFields(true);
                                case PROCESSLIST:
                                    lexer.nextToken();
                                    return new ShowProcesslist(true);
                                case TABLES:
                                    tempId = null;
                                    switch (lexer.nextToken()) {
                                        case KW_IN:
                                        case KW_FROM:
                                            lexer.nextToken();
                                            tempId = identifier();
                                    }
                                    switch (lexer.token()) {
                                        case KW_LIKE:
                                            tempStr = like();
                                            return new ShowTables(true, tempId, tempStr);
                                        case KW_WHERE:
                                            tempExpr = where();
                                            return new ShowTables(true, tempId, tempExpr);
                                        default:
                                            return new ShowTables(true, tempId);
                                    }
                            }
                        }
                        throw err("unexpected token for SHOW FULL");
                    case FUNCTION:
                        lexer.nextToken();
                        tempStrUp = lexer.stringValueUppercase();
                        tempSi = specialIdentifiers.get(tempStrUp);
                        if (tempSi != null) {
                            switch (tempSi) {
                                case CODE:
                                    lexer.nextToken();
                                    tempId = identifier();
                                    return new ShowFunctionCode(tempId);
                                case STATUS:
                                    switch (lexer.nextToken()) {
                                        case KW_LIKE:
                                            tempStr = like();
                                            return new ShowFunctionStatus(tempStr);
                                        case KW_WHERE:
                                            tempExpr = where();
                                            return new ShowFunctionStatus(tempExpr);
                                        default:
                                            return new ShowFunctionStatus();
                                    }
                            }
                        }
                        throw err("unexpected token for SHOW FUNCTION");
                    case GLOBAL:
                        lexer.nextToken();
                        tempStrUp = lexer.stringValueUppercase();
                        tempSi = specialIdentifiers.get(tempStrUp);
                        if (tempSi != null) {
                            switch (tempSi) {
                                case STATUS:
                                    switch (lexer.nextToken()) {
                                        case KW_LIKE:
                                            tempStr = like();
                                            return new ShowStatus(VariableScope.GLOBAL, tempStr);
                                        case KW_WHERE:
                                            tempExpr = where();
                                            return new ShowStatus(VariableScope.GLOBAL, tempExpr);
                                        default:
                                            return new ShowStatus(VariableScope.GLOBAL);
                                    }
                                case VARIABLES:
                                    switch (lexer.nextToken()) {
                                        case KW_LIKE:
                                            tempStr = like();
                                            return new ShowVariables(VariableScope.GLOBAL, tempStr);
                                        case KW_WHERE:
                                            tempExpr = where();
                                            return new ShowVariables(VariableScope.GLOBAL,
                                                    tempExpr);
                                        default:
                                            return new ShowVariables(VariableScope.GLOBAL);
                                    }
                            }
                        }
                        throw err("unexpected token for SHOW GLOBAL");
                    case MASTER:
                        lexer.nextToken();
                        tempStrUp = lexer.stringValueUppercase();
                        tempSi = specialIdentifiers.get(tempStrUp);
                        if (tempSi != null && tempSi == SpecialIdentifier.STATUS) {
                            lexer.nextToken();
                            return new ShowMasterStatus();
                        }
                        matchIdentifier("LOGS");
                        return new ShowBinaryLog();
                    case OPEN:
                        lexer.nextToken();
                        matchIdentifier("TABLES");
                        tempId = null;
                        switch (lexer.token()) {
                            case KW_IN:
                            case KW_FROM:
                                lexer.nextToken();
                                tempId = identifier();
                        }
                        switch (lexer.token()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowOpenTables(tempId, tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowOpenTables(tempId, tempExpr);
                            default:
                                return new ShowOpenTables(tempId);
                        }
                    case PLUGINS:
                        lexer.nextToken();
                        return new ShowPlugins();
                    case PRIVILEGES:
                        lexer.nextToken();
                        return new ShowPrivileges();
                    case PROCESSLIST:
                        lexer.nextToken();
                        return new ShowProcesslist(false);
                    case PROFILE:
                        return showProfile();
                    case PROFILES:
                        lexer.nextToken();
                        return new ShowProfiles();
                    case LOCAL:
                    case SESSION:
                        lexer.nextToken();
                        tempStrUp = lexer.stringValueUppercase();
                        tempSi = specialIdentifiers.get(tempStrUp);
                        if (tempSi != null) {
                            switch (tempSi) {
                                case STATUS:
                                    switch (lexer.nextToken()) {
                                        case KW_LIKE:
                                            tempStr = like();
                                            return new ShowStatus(VariableScope.SESSION, tempStr);
                                        case KW_WHERE:
                                            tempExpr = where();
                                            return new ShowStatus(VariableScope.SESSION, tempExpr);
                                        default:
                                            return new ShowStatus(VariableScope.SESSION);
                                    }
                                case VARIABLES:
                                    switch (lexer.nextToken()) {
                                        case KW_LIKE:
                                            tempStr = like();
                                            return new ShowVariables(VariableScope.SESSION,
                                                    tempStr);
                                        case KW_WHERE:
                                            tempExpr = where();
                                            return new ShowVariables(VariableScope.SESSION,
                                                    tempExpr);
                                        default:
                                            return new ShowVariables(VariableScope.SESSION);
                                    }
                            }
                        }
                        throw err("unexpected token for SHOW SESSION");
                    case SLAVE:
                        lexer.nextToken();
                        tempStrUp = lexer.stringValueUppercase();
                        tempSi = specialIdentifiers.get(tempStrUp);
                        if (tempSi != null) {
                            switch (tempSi) {
                                case HOSTS:
                                    lexer.nextToken();
                                    return new ShowSlaveHosts();
                                case STATUS:
                                    lexer.nextToken();
                                    return new ShowSlaveStatus();
                            }
                        }
                        throw err("unexpected token for SHOW SLAVE");
                    case STATUS:
                        switch (lexer.nextToken()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowStatus(VariableScope.SESSION, tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowStatus(VariableScope.SESSION, tempExpr);
                            default:
                                return new ShowStatus(VariableScope.SESSION);
                        }
                    case STORAGE:
                        lexer.nextToken();
                        matchIdentifier("ENGINES");
                        return new ShowEngines();
                    case TABLES:
                        tempId = null;
                        switch (lexer.nextToken()) {
                            case KW_IN:
                            case KW_FROM:
                                lexer.nextToken();
                                tempId = identifier();
                        }
                        switch (lexer.token()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowTables(false, tempId, tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowTables(false, tempId, tempExpr);
                            default:
                                return new ShowTables(false, tempId);
                        }
                    case TRIGGERS:
                        tempId = null;
                        switch (lexer.nextToken()) {
                            case KW_IN:
                            case KW_FROM:
                                lexer.nextToken();
                                tempId = identifier();
                        }
                        switch (lexer.token()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowTriggers(tempId, tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowTriggers(tempId, tempExpr);
                            default:
                                return new ShowTriggers(tempId);
                        }
                    case VARIABLES:
                        switch (lexer.nextToken()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowVariables(VariableScope.SESSION, tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowVariables(VariableScope.SESSION, tempExpr);
                            default:
                                return new ShowVariables(VariableScope.SESSION);
                        }
                    case WARNINGS:
                        lexer.nextToken();
                        tempLimit = limit();
                        return new ShowWarnings(false, tempLimit);
                    case CHARSET:
                        lexer.nextToken();
                        switch (lexer.token()) {
                            case KW_LIKE:
                                tempStr = like();
                                return new ShowCharset(tempStr);
                            case KW_WHERE:
                                tempExpr = where();
                                return new ShowCharset(tempExpr);
                            default:
                                return new ShowCharset();
                        }
                }
                break;
        }
        throw err("unexpect token for SHOW");
    }

    private ShowIndex showIndex(ShowIndex.Type type) throws SQLSyntaxErrorException {
        lexer.nextToken();
        match(KW_FROM, KW_IN);
        Identifier tempId = identifier();
        Identifier tempId2 = null;
        Expression tempExpr = null;
        if (lexer.token() == KW_FROM || lexer.token() == KW_IN) {
            lexer.nextToken();
            tempId2 = identifier();
        }
        switch (lexer.token()) {
            case KW_WHERE:
                tempExpr = where();
            default:
        }
        return new ShowIndex(type, tempId, tempId2, tempExpr);
    }

    private ShowProfile showProfile() throws SQLSyntaxErrorException {
        lexer.nextToken();
        List<ShowProfile.Type> types = new LinkedList<ShowProfile.Type>();
        ShowProfile.Type type = showPrifileType();
        if (type == null) {
            types = Collections.emptyList();
        } else if (lexer.token() == PUNC_COMMA) {
            types = new LinkedList<ShowProfile.Type>();
            types.add(type);
            for (; lexer.token() == PUNC_COMMA;) {
                lexer.nextToken();
                type = showPrifileType();
                types.add(type);
            }
        } else {
            types = new ArrayList<ShowProfile.Type>();
            types.add(type);
        }
        Expression forQuery = null;
        if (lexer.token() == KW_FOR) {
            lexer.nextToken();
            matchIdentifier("QUERY");
            forQuery = exprParser.expression();
        }
        Limit limit = limit();
        return new ShowProfile(types, forQuery, limit);
    }

    /**
     * @return null if not a type
     */
    @SuppressWarnings("incomplete-switch")
    private ShowProfile.Type showPrifileType() throws SQLSyntaxErrorException {
        switch (lexer.token()) {
            case KW_ALL:
                lexer.nextToken();
                return ShowProfile.Type.ALL;
            case IDENTIFIER:
                String strUp = lexer.stringValueUppercase();
                SpecialIdentifier si = specialIdentifiers.get(strUp);
                if (si != null) {
                    switch (si) {
                        case BLOCK:
                            lexer.nextToken();
                            matchIdentifier("IO");
                            return ShowProfile.Type.BLOCK_IO;
                        case CONTEXT:
                            lexer.nextToken();
                            matchIdentifier("SWITCHES");
                            return ShowProfile.Type.CONTEXT_SWITCHES;
                        case CPU:
                            lexer.nextToken();
                            return ShowProfile.Type.CPU;
                        case IPC:
                            lexer.nextToken();
                            return ShowProfile.Type.IPC;
                        case MEMORY:
                            lexer.nextToken();
                            return ShowProfile.Type.MEMORY;
                        case PAGE:
                            lexer.nextToken();
                            matchIdentifier("FAULTS");
                            return ShowProfile.Type.PAGE_FAULTS;
                        case SOURCE:
                            lexer.nextToken();
                            return ShowProfile.Type.SOURCE;
                        case SWAPS:
                            lexer.nextToken();
                            return ShowProfile.Type.SWAPS;
                    }
                }
            default:
                return null;
        }
    }

    /**
     * First token is {@link SpecialIdentifier#COLUMNS}
     * 
     * <pre>
     * SHOW [FULL] <code>COLUMNS {FROM | IN} tbl_name [{FROM | IN} db_name] [LIKE 'pattern' | WHERE expr] </code>
     * </pre>
     */
    @SuppressWarnings("incomplete-switch")
    private ShowColumns showColumns(boolean full) throws SQLSyntaxErrorException {
        lexer.nextToken();
        match(KW_FROM, KW_IN);
        Identifier table = identifier();
        Identifier database = null;
        switch (lexer.token()) {
            case KW_FROM:
            case KW_IN:
                lexer.nextToken();
                database = identifier();
        }
        switch (lexer.token()) {
            case KW_LIKE:
                String like = like();
                return new ShowColumns(full, table, database, like);
            case KW_WHERE:
                Expression where = where();
                return new ShowColumns(full, table, database, where);
        }
        return new ShowColumns(full, table, database);
    }

    /**
     * First token is {@link SpecialIdentifier#COLUMNS}
     * 
     * <pre>
     * SHOW [FULL] <code>FIELDS {FROM | IN} tbl_name [{FROM | IN} db_name] [LIKE 'pattern' | WHERE expr] </code>
     * </pre>
     */
    @SuppressWarnings("incomplete-switch")
    private ShowFields showFields(boolean full) throws SQLSyntaxErrorException {
        lexer.nextToken();
        match(KW_FROM, KW_IN);
        Identifier table = identifier();
        Identifier database = null;
        switch (lexer.token()) {
            case KW_FROM:
            case KW_IN:
                lexer.nextToken();
                database = identifier();
        }
        switch (lexer.token()) {
            case KW_LIKE:
                String like = like();
                return new ShowFields(full, table, database, like);
            case KW_WHERE:
                Expression where = where();
                return new ShowFields(full, table, database, where);
        }
        return new ShowFields(full, table, database);
    }

    private String like() throws SQLSyntaxErrorException {
        match(KW_LIKE);
        String pattern = lexer.stringValue();
        lexer.nextToken();
        return pattern;
    }

    private Expression where() throws SQLSyntaxErrorException {
        match(KW_WHERE);
        Expression where = exprParser.expression();
        return where;
    }

    private String getStringValue() throws SQLSyntaxErrorException {
        String name;
        switch (lexer.token()) {
            case IDENTIFIER:
                name = Identifier.unescapeName(lexer.stringValue());
                lexer.nextToken();
                return name;
            case LITERAL_CHARS:
                name = lexer.stringValue();
                name = LiteralString
                        .getUnescapedString(name.substring(1, name.length() - 1).getBytes());
                lexer.nextToken();
                return name;
            default:
                throw err("unexpected token: " + lexer.token());
        }
    }

    /**
     * @return {@link DALSetStatement} or {@link MTSSetTransactionStatement}
     */
    @SuppressWarnings("unchecked")
    public SQLStatement set() throws SQLSyntaxErrorException {
        match(KW_SET);
        if (lexer.token() == KW_OPTION) {
            lexer.nextToken();
        }
        if (lexer.token() == IDENTIFIER && SpecialIdentifier.NAMES == specialIdentifiers
                .get(lexer.stringValueUppercase())) {
            if (lexer.nextToken() == KW_DEFAULT) {
                lexer.nextToken();
                return new DALSetNamesStatement();
            }
            String charsetName = getStringValue();
            String collationName = null;
            if (lexer.token() == KW_COLLATE) {
                lexer.nextToken();
                collationName = getStringValue();
            }
            return new DALSetNamesStatement(charsetName, collationName);
        } else if (lexer.token() == KW_CHARACTER) {
            lexer.nextToken();
            match(KW_SET);
            if (lexer.token() == KW_DEFAULT) {
                lexer.nextToken();
                return new DALSetCharacterSetStatement();
            }
            String charsetName = getStringValue();
            return new DALSetCharacterSetStatement(charsetName);
        }

        List<Pair<VariableExpression, Expression>> assignmentList;
        Object obj = varAssign();
        if (obj instanceof MTSSetTransactionStatement) {
            return (MTSSetTransactionStatement) obj;
        }
        Pair<VariableExpression, Expression> pair = (Pair<VariableExpression, Expression>) obj;
        if (lexer.token() != PUNC_COMMA) {
            assignmentList = new ArrayList<Pair<VariableExpression, Expression>>(1);
            assignmentList.add(pair);
            return new DALSetStatement(assignmentList);
        }
        assignmentList = new LinkedList<Pair<VariableExpression, Expression>>();
        assignmentList.add(pair);
        for (; lexer.token() == PUNC_COMMA;) {
            lexer.nextToken();
            pair = (Pair<VariableExpression, Expression>) varAssign();
            assignmentList.add(pair);
        }
        return new DALSetStatement(assignmentList);
    }

    /**
     * first token is <code>TRANSACTION</code>
     */
    @SuppressWarnings("incomplete-switch")
    private MTSSetTransactionStatement setMTSSetTransactionStatement(VariableScope scope)
            throws SQLSyntaxErrorException {
        lexer.nextToken();
        matchIdentifier("ISOLATION");
        matchIdentifier("LEVEL");

        SpecialIdentifier si;
        switch (lexer.token()) {
            case KW_READ:
                lexer.nextToken();
                si = specialIdentifiers.get(lexer.stringValueUppercase());
                if (si != null) {
                    switch (si) {
                        case COMMITTED:
                            lexer.nextToken();
                            return new MTSSetTransactionStatement(scope,
                                    MTSSetTransactionStatement.IsolationLevel.READ_COMMITTED);
                        case UNCOMMITTED:
                            lexer.nextToken();
                            return new MTSSetTransactionStatement(scope,
                                    MTSSetTransactionStatement.IsolationLevel.READ_UNCOMMITTED);
                    }
                }
                throw err("unknown isolation read level: " + lexer.stringValue());
            case IDENTIFIER:
                si = specialIdentifiers.get(lexer.stringValueUppercase());
                if (si != null) {
                    switch (si) {
                        case REPEATABLE:
                            lexer.nextToken();
                            match(KW_READ);
                            return new MTSSetTransactionStatement(scope,
                                    MTSSetTransactionStatement.IsolationLevel.REPEATABLE_READ);
                        case SERIALIZABLE:
                            lexer.nextToken();
                            return new MTSSetTransactionStatement(scope,
                                    MTSSetTransactionStatement.IsolationLevel.SERIALIZABLE);
                    }
                }
        }
        throw err("unknown isolation level: " + lexer.stringValue());
    }

    private Object varAssign() throws SQLSyntaxErrorException {
        VariableExpression var = null;
        Expression expr;
        VariableScope scope = VariableScope.SESSION;
        String scopeStr = null;
        switch (lexer.token()) {
            case IDENTIFIER:
                boolean explictScope = false;
                SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                if (si != null) {
                    switch (si) {
                        case TRANSACTION:
                            return setMTSSetTransactionStatement(null);
                        case GLOBAL:
                            scope = VariableScope.GLOBAL;
                        case SESSION:
                        case LOCAL:
                            scopeStr = lexer.stringValue();
                            explictScope = true;
                            lexer.nextToken();
                            break;
                        case NEW:
                            lexer.nextToken();
                            match(MySQLToken.PUNC_DOT);
                            var = new NewRowPrimary(lexer.stringValue());
                            break;
                        case OLD:
                            lexer.nextToken();
                            match(MySQLToken.PUNC_DOT);
                            var = new OldRowPrimary(lexer.stringValue());
                            break;
                        default:
                            break;
                    }
                }
                if (explictScope && specialIdentifiers
                        .get(lexer.stringValueUppercase()) == SpecialIdentifier.TRANSACTION) {
                    return setMTSSetTransactionStatement(scope);
                }
                if (var == null) {
                    var = new SysVarPrimary(scope, scopeStr, lexer.stringValue(),
                            lexer.stringValueUppercase());
                }
                match(IDENTIFIER);
                break;
            case SYS_VAR:
                var = systemVariale();
                break;
            case USR_VAR:
                var = new UsrDefVarPrimary(lexer.stringValue());
                lexer.nextToken();
                break;
            default:
                throw err("unexpected token for SET statement");
        }
        match(OP_EQUALS, OP_ASSIGN);
        expr = exprParser.expression();
        return new Pair<VariableExpression, Expression>(var, expr);
    }
}
