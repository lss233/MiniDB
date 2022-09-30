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
 * (created at 2011-7-4)
 */
package miniDB.parser.recognizer.mysql.syntax;


import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.expression.primary.function.info.CurrentUser;
import miniDB.parser.ast.expression.primary.literal.LiteralString;
import miniDB.parser.ast.fragment.OrderBy;
import miniDB.parser.ast.fragment.SortOrder;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition.SpecialIndex;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition.Storage;
import miniDB.parser.ast.fragment.ddl.TableOptions;
import miniDB.parser.ast.fragment.ddl.TableOptions.Compression;
import miniDB.parser.ast.fragment.ddl.datatype.DataType;
import miniDB.parser.ast.fragment.ddl.index.IndexColumnName;
import miniDB.parser.ast.fragment.ddl.index.IndexDefinition;
import miniDB.parser.ast.fragment.ddl.index.IndexDefinition.IndexType;
import miniDB.parser.ast.fragment.ddl.index.IndexOption;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.condition.Characteristics;
import miniDB.parser.ast.stmt.compound.condition.Characteristics.Characteristic;
import miniDB.parser.ast.stmt.ddl.*;
import miniDB.parser.ast.stmt.ddl.DDLAlterTableStatement.ReNameIndex;
import miniDB.parser.ast.stmt.ddl.DDLAlterTableStatement.WithValidation;
import miniDB.parser.ast.stmt.ddl.DDLCreateProcedureStatement.ProcParameterType;
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement.ForeignKeyDefinition;
import miniDB.parser.ast.stmt.ddl.DDLCreateTriggerStatement.TriggerEvent;
import miniDB.parser.ast.stmt.ddl.DDLCreateTriggerStatement.TriggerOrder;
import miniDB.parser.ast.stmt.ddl.DDLCreateTriggerStatement.TriggerTime;
import miniDB.parser.ast.stmt.dml.DMLQueryStatement;
import miniDB.parser.ast.stmt.extension.DropPrepareStatement;
import miniDB.parser.ast.stmt.extension.ExtDDLCreatePolicy;
import miniDB.parser.ast.stmt.extension.ExtDDLDropPolicy;
import miniDB.parser.recognizer.SQLParserDelegate;
import miniDB.parser.recognizer.mysql.MySQLToken;
import miniDB.parser.recognizer.mysql.lexer.MySQLLexer;
import miniDB.parser.util.Pair;
import miniDB.parser.util.Tuple3;

import java.sql.SQLSyntaxErrorException;
import java.util.*;


/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class MySQLDDLParser extends MySQLParser {
    protected MySQLExprParser exprParser;

    public MySQLDDLParser(MySQLLexer lexer, MySQLExprParser exprParser) {
        super(lexer);
        this.exprParser = exprParser;
    }

    private static enum SpecialIdentifier {
        TRUNCATE, TEMPORARY, DEFINER, KEY_BLOCK_SIZE, COMMENT, DYNAMIC, FIXED, BIT, DATE, TIME, TIMESTAMP, DATETIME, YEAR, TEXT, ENUM, ENGINE, AUTO_INCREMENT, AVG_ROW_LENGTH, CHECKSUM, CONNECTION, DATA, DELAY_KEY_WRITE, INSERT_METHOD, MAX_ROWS, MIN_ROWS, PACK_KEYS, PASSWORD, ROW_FORMAT, COMPRESSED, REDUNDANT, COMPACT, MODIFY, DISABLE, ENABLE, DISCARD, IMPORT,
        /**
         * MySQL 5 . 1 legacy syntax
         */
        CHARSET,
        /** EXTENSION syntax */
        POLICY, BOOL, BOOLEAN, SERIAL, COALESCE, REORGANIZE, EXCHANGE, REBUILD, ANALYZE, CHECK, OPTIMIZE, REPAIR, REMOVE, PARTITIONING, TABLES, WITHOUT, VALIDATION, COMPRESSION, ENCRYPTION, UPGRADE, AGGREGATE, FUNCTION, EVENT, INVOKER,

        TEMPTABLE, UNDEFINED, MERGE, VIEW, NO, ACTION, ALGORITHM, DEALLOCATE, PREPARE, GEOMETRY, POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, GEOMETRYCOLLECTION, MULTIPOLYGON, NONE, SHARED, EXCLUSIVE, INPLACE, COPY, JSON, DISK, MEMORY
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers =
            new HashMap<String, SpecialIdentifier>(1, 1);

    static {
        specialIdentifiers.put("SERIAL", SpecialIdentifier.SERIAL);
        specialIdentifiers.put("BOOL", SpecialIdentifier.BOOL);
        specialIdentifiers.put("BOOLEAN", SpecialIdentifier.BOOLEAN);
        specialIdentifiers.put("TRUNCATE", SpecialIdentifier.TRUNCATE);
        specialIdentifiers.put("TEMPORARY", SpecialIdentifier.TEMPORARY);
        specialIdentifiers.put("DEFINER", SpecialIdentifier.DEFINER);
        specialIdentifiers.put("KEY_BLOCK_SIZE", SpecialIdentifier.KEY_BLOCK_SIZE);
        specialIdentifiers.put("COMMENT", SpecialIdentifier.COMMENT);
        specialIdentifiers.put("DYNAMIC", SpecialIdentifier.DYNAMIC);
        specialIdentifiers.put("FIXED", SpecialIdentifier.FIXED);
        specialIdentifiers.put("BIT", SpecialIdentifier.BIT);
        specialIdentifiers.put("DATE", SpecialIdentifier.DATE);
        specialIdentifiers.put("TIME", SpecialIdentifier.TIME);
        specialIdentifiers.put("TIMESTAMP", SpecialIdentifier.TIMESTAMP);
        specialIdentifiers.put("DATETIME", SpecialIdentifier.DATETIME);
        specialIdentifiers.put("YEAR", SpecialIdentifier.YEAR);
        specialIdentifiers.put("TEXT", SpecialIdentifier.TEXT);
        specialIdentifiers.put("ENUM", SpecialIdentifier.ENUM);
        specialIdentifiers.put("ENGINE", SpecialIdentifier.ENGINE);
        specialIdentifiers.put("AUTO_INCREMENT", SpecialIdentifier.AUTO_INCREMENT);
        specialIdentifiers.put("AVG_ROW_LENGTH", SpecialIdentifier.AVG_ROW_LENGTH);
        specialIdentifiers.put("CHECKSUM", SpecialIdentifier.CHECKSUM);
        specialIdentifiers.put("CONNECTION", SpecialIdentifier.CONNECTION);
        specialIdentifiers.put("DATA", SpecialIdentifier.DATA);
        specialIdentifiers.put("DELAY_KEY_WRITE", SpecialIdentifier.DELAY_KEY_WRITE);
        specialIdentifiers.put("INSERT_METHOD", SpecialIdentifier.INSERT_METHOD);
        specialIdentifiers.put("MAX_ROWS", SpecialIdentifier.MAX_ROWS);
        specialIdentifiers.put("MIN_ROWS", SpecialIdentifier.MIN_ROWS);
        specialIdentifiers.put("PACK_KEYS", SpecialIdentifier.PACK_KEYS);
        specialIdentifiers.put("PASSWORD", SpecialIdentifier.PASSWORD);
        specialIdentifiers.put("ROW_FORMAT", SpecialIdentifier.ROW_FORMAT);
        specialIdentifiers.put("COMPRESSED", SpecialIdentifier.COMPRESSED);
        specialIdentifiers.put("REDUNDANT", SpecialIdentifier.REDUNDANT);
        specialIdentifiers.put("COMPACT", SpecialIdentifier.COMPACT);
        specialIdentifiers.put("MODIFY", SpecialIdentifier.MODIFY);
        specialIdentifiers.put("DISABLE", SpecialIdentifier.DISABLE);
        specialIdentifiers.put("ENABLE", SpecialIdentifier.ENABLE);
        specialIdentifiers.put("DISCARD", SpecialIdentifier.DISCARD);
        specialIdentifiers.put("IMPORT", SpecialIdentifier.IMPORT);
        specialIdentifiers.put("CHARSET", SpecialIdentifier.CHARSET);
        specialIdentifiers.put("POLICY", SpecialIdentifier.POLICY);
        specialIdentifiers.put("COALESCE", SpecialIdentifier.COALESCE);
        specialIdentifiers.put("REORGANIZE", SpecialIdentifier.REORGANIZE);
        specialIdentifiers.put("EXCHANGE", SpecialIdentifier.EXCHANGE);
        specialIdentifiers.put("REBUILD", SpecialIdentifier.REBUILD);
        specialIdentifiers.put("ANALYZE ", SpecialIdentifier.ANALYZE);
        specialIdentifiers.put("CHECK", SpecialIdentifier.CHECK);
        specialIdentifiers.put("OPTIMIZE", SpecialIdentifier.OPTIMIZE);
        specialIdentifiers.put("REPAIR", SpecialIdentifier.REPAIR);
        specialIdentifiers.put("REMOVE", SpecialIdentifier.REMOVE);
        specialIdentifiers.put("PARTITIONING", SpecialIdentifier.PARTITIONING);
        specialIdentifiers.put("TABLES", SpecialIdentifier.TABLES);
        specialIdentifiers.put("WITHOUT", SpecialIdentifier.WITHOUT);
        specialIdentifiers.put("VALIDATION", SpecialIdentifier.VALIDATION);
        specialIdentifiers.put("COMPRESSION", SpecialIdentifier.COMPRESSION);
        specialIdentifiers.put("ENCRYPTION", SpecialIdentifier.ENCRYPTION);
        specialIdentifiers.put("UPGRADE", SpecialIdentifier.UPGRADE);
        specialIdentifiers.put("AGGREGATE", SpecialIdentifier.AGGREGATE);
        specialIdentifiers.put("FUNCTION", SpecialIdentifier.FUNCTION);
        specialIdentifiers.put("EVENT", SpecialIdentifier.EVENT);
        specialIdentifiers.put("INVOKER", SpecialIdentifier.INVOKER);
        specialIdentifiers.put("UNDEFINED", SpecialIdentifier.UNDEFINED);
        specialIdentifiers.put("MERGE", SpecialIdentifier.MERGE);
        specialIdentifiers.put("TEMPTABLE", SpecialIdentifier.TEMPTABLE);
        specialIdentifiers.put("VIEW", SpecialIdentifier.VIEW);
        specialIdentifiers.put("NO", SpecialIdentifier.NO);
        specialIdentifiers.put("ACTION", SpecialIdentifier.ACTION);
        specialIdentifiers.put("ALGORITHM", SpecialIdentifier.ALGORITHM);
        specialIdentifiers.put("DEALLOCATE", SpecialIdentifier.DEALLOCATE);
        specialIdentifiers.put("PREPARE", SpecialIdentifier.PREPARE);
        specialIdentifiers.put("GEOMETRY", SpecialIdentifier.GEOMETRY);
        specialIdentifiers.put("POINT", SpecialIdentifier.POINT);
        specialIdentifiers.put("LINESTRING", SpecialIdentifier.LINESTRING);
        specialIdentifiers.put("POLYGON", SpecialIdentifier.POLYGON);
        specialIdentifiers.put("MULTIPOINT", SpecialIdentifier.MULTIPOINT);
        specialIdentifiers.put("MULTILINESTRING", SpecialIdentifier.MULTILINESTRING);
        specialIdentifiers.put("GEOMETRYCOLLECTION", SpecialIdentifier.GEOMETRYCOLLECTION);
        specialIdentifiers.put("MULTIPOLYGON", SpecialIdentifier.MULTIPOLYGON);
        specialIdentifiers.put("NONE", SpecialIdentifier.NONE);
        specialIdentifiers.put("SHARED", SpecialIdentifier.SHARED);
        specialIdentifiers.put("EXCLUSIVE", SpecialIdentifier.EXCLUSIVE);
        specialIdentifiers.put("INPLACE", SpecialIdentifier.INPLACE);
        specialIdentifiers.put("COPY", SpecialIdentifier.COPY);
        specialIdentifiers.put("JSON", SpecialIdentifier.JSON);
        specialIdentifiers.put("DISK", SpecialIdentifier.DISK);
        specialIdentifiers.put("MEMORY", SpecialIdentifier.MEMORY);

    }

    public DDLTruncateStatement truncate() throws SQLSyntaxErrorException {
        matchIdentifier("TRUNCATE");
        if (lexer.token() == MySQLToken.KW_TABLE) {
            lexer.nextToken();
        }
        Identifier tb = identifier();
        return new DDLTruncateStatement(tb);
    }

    /**
     * 判断获取 [DEFINER = { user | CURRENT_USER }] <br>
     * 执行完方法后将自动lexer.nextToken();
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    private Expression getDefiner() throws SQLSyntaxErrorException {
        if (lexer.token() == MySQLToken.IDENTIFIER
                && SpecialIdentifier.DEFINER == specialIdentifiers
                        .get(lexer.stringValueUppercase())) {
            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                if (lexer.nextToken() == MySQLToken.KW_CURRENT_USER) {
                    lexer.nextToken();
                    if (lexer.token() == MySQLToken.PUNC_LEFT_PAREN) {
                        lexer.nextToken();
                        match(MySQLToken.PUNC_RIGHT_PAREN);
                    }
                    return new CurrentUser().setCacheEvalRst(cacheEvalRst);
                } else {
                    return exprParser.expression(); // 后续可能需判断 用户字符否合法
                }
            } else {
                throw err("expect = ");
            }
        }
        return null;
    }

    /**
     * [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
     */
    public enum Algorithm {
        UNDEFINED, MERGE, TEMPTABLE
    }

    /**
     * 判断SQL中是否包含[ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]<br>
     * 执行完方法后将自动lexer.nextToken();
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    private Algorithm getAlgorithm() throws SQLSyntaxErrorException {
        // 判断获取 [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
        if (lexer.token() == MySQLToken.IDENTIFIER
                && SpecialIdentifier.ALGORITHM == specialIdentifiers
                        .get(lexer.stringValueUppercase())) {
            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                if (lexer.nextToken() == MySQLToken.IDENTIFIER) {
                    SpecialIdentifier tmp = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (tmp != null) {
                        switch (tmp) {
                            case UNDEFINED:
                                lexer.nextToken();
                                return Algorithm.UNDEFINED;
                            case MERGE:
                                lexer.nextToken();
                                return Algorithm.MERGE;
                            case TEMPTABLE:
                                lexer.nextToken();
                                return Algorithm.TEMPTABLE;
                            default:
                                break;
                        }
                    }
                }
                throw err("expect {UNDEFINED | MERGE | TEMPTABLE} ");
            } else {
                throw err("expect = ");
            }
        }
        return null;
    }

    /**
     * [SQL SECURITY { DEFINER | INVOKER }]
     */
    public enum SqlSecurity {
        DEFINER, INVOKER
    }

    /**
     * 判断SQL中是否包含[SQL SECURITY { DEFINER | INVOKER }]<br>
     * 执行完方法后将自动lexer.nextToken();
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    private SqlSecurity getSqlSecurity() throws SQLSyntaxErrorException {
        // 判断获取[SQL SECURITY { DEFINER | INVOKER }]
        if (lexer.token() == MySQLToken.KW_SQL) {
            if (lexer.nextToken() == MySQLToken.IDENTIFIER
                    && "SECURITY".equals(lexer.stringValueUppercase())) {
                switch (lexer.nextToken()) {
                    case IDENTIFIER: {
                        if (SpecialIdentifier.INVOKER == specialIdentifiers
                                .get(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            return SqlSecurity.INVOKER;
                        } else if (SpecialIdentifier.DEFINER == specialIdentifiers
                                .get(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            return SqlSecurity.DEFINER;
                        }
                    }
                    default:
                        throw err("expect { DEFINER | INVOKER } ");
                }

            } else {
                throw err("expect SECURITY ");
            }
        }
        return null;
    }

    /**
     * 判断SQL中是否包含[OR REPLACE]<br>
     * 执行完方法后将自动lexer.nextToken();
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    private boolean IsOrRepalce() throws SQLSyntaxErrorException {
        if (lexer.token() == MySQLToken.KW_OR) {
            if (lexer.nextToken() == MySQLToken.KW_REPLACE) {
                lexer.nextToken();
                return true;
            } else {
                throw err("expect REPLACE ");
            }
        }
        return false;
    }

    /**
     * 判断CREATE [AGGREGATE] FUNCTION<br>
     * 执行完方法后将自动lexer.nextToken();
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    private boolean isAggregate() throws SQLSyntaxErrorException {
        if (lexer.token() == MySQLToken.IDENTIFIER
                && SpecialIdentifier.AGGREGATE == specialIdentifiers
                        .get(lexer.stringValueUppercase())) {
            lexer.nextToken();
            return true;
        }
        return false;
    }

    /**
     * nothing has been pre-consumed
     */
    @SuppressWarnings("incomplete-switch")
    public DDLStatement ddlStmt() throws SQLSyntaxErrorException {
        Identifier idTemp1;
        Identifier idTemp2;
        SpecialIdentifier siTemp;
        Algorithm algorithm = null;
        Expression definer = null;
        SqlSecurity sqlSecurity = null;
        switch (lexer.token()) {
            case KW_ALTER:
                boolean ignore = false;
                if (lexer.nextToken() == MySQLToken.KW_IGNORE) {
                    ignore = true;
                    lexer.nextToken();
                }
                algorithm = getAlgorithm();
                definer = getDefiner();
                sqlSecurity = getSqlSecurity();
                switch (lexer.token()) {
                    case KW_TABLE:
                        lexer.nextToken();
                        idTemp1 = identifier();
                        DDLAlterTableStatement alterTableStatement =
                                new DDLAlterTableStatement(ignore, idTemp1);
                        return alterTable(alterTableStatement);
                    case IDENTIFIER: {
                        if (SpecialIdentifier.EVENT == specialIdentifiers
                                .get(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            idTemp1 = identifier();
                            // 目前暂实现至 EVENT 名为止 ，方便禁用语句，其余待后续实现
                            return new DDLAlterEventStatement(definer, idTemp1);
                        } else if (SpecialIdentifier.VIEW == specialIdentifiers
                                .get(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            idTemp1 = identifier();
                            // 目前暂实现至 view 名为止 ，方便禁用语句，其余待后续实现
                            return new DDLAlterViewStatement(algorithm, definer, sqlSecurity,
                                    idTemp1);
                        }
                    }
                    default:
                        throw err("unsupported DDL for ALTER");
                }
            case KW_CREATE:
                lexer.nextToken();
                boolean isAggregate = isAggregate();
                boolean isOrRepalce = IsOrRepalce();
                algorithm = getAlgorithm();
                definer = getDefiner();
                sqlSecurity = getSqlSecurity();
                switch (lexer.token()) {
                    case KW_PROCEDURE:
                        lexer.nextToken();
                        idTemp1 = identifier();
                        // 目前暂实现至 PROCEDURE 名为止 ，方便禁用语句，其余待后续实现
                        return createProcedure(definer, idTemp1);
                    case KW_TRIGGER:
                        lexer.nextToken();
                        idTemp1 = identifier();
                        // 目前暂实现至 TRIGGER 名为止 ，方便禁用语句，其余待后续实现
                        return createTrigger(definer, idTemp1);
                    case KW_UNIQUE:
                    case KW_FULLTEXT:
                    case KW_SPATIAL:
                        lexer.nextToken();
                    case KW_INDEX:
                        lexer.nextToken();
                        idTemp1 = identifier();
                        //                        for (; lexer.token() !=MySQLToken.KW_ON; lexer.nextToken());
                        //                        lexer.nextToken();
                        //                        idTemp2 = identifier();
                        // DDLCreateIndexStatement createIndexStatement =
                        // new DDLCreateIndexStatement(idTemp1);
                        return createIndex(idTemp1);
                    case KW_TABLE:
                        // CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name LIKE old_tbl_name
                        lexer.nextToken();
                        boolean ifNotExists = ifNotExists();
                        Identifier table = identifier();
                        if (lexer.token() == MySQLToken.KW_LIKE)
                            return createTableByLike(false, ifNotExists, table);
                        else
                            return createTable(false, ifNotExists, table);
                    case IDENTIFIER:
                        siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (siTemp != null) {
                            switch (siTemp) {
                                /*
                                 * case TEMPORARY: lexer.nextToken(); match(MySQLToken.KW_TABLE); // 拦截临时表的创建
                                 * throw err("unsupported DDL for CREATE TEMPORARY TABLE"); //
                                 * return createTable(true);
                                 */
                                case POLICY:
                                    lexer.nextToken();
                                    Identifier policyName = identifier();
                                    match(MySQLToken.PUNC_LEFT_PAREN);
                                    ExtDDLCreatePolicy policy = new ExtDDLCreatePolicy(policyName);
                                    for (int j = 0; lexer
                                            .token() != MySQLToken.PUNC_RIGHT_PAREN; ++j) {
                                        if (j > 0) {
                                            match(MySQLToken.PUNC_COMMA);
                                        }
                                        Integer id = lexer.integerValue().intValue();
                                        match(MySQLToken.LITERAL_NUM_PURE_DIGIT);
                                        Expression val = exprParser.expression();
                                        policy.addProportion(id, val);
                                    }
                                    match(MySQLToken.PUNC_RIGHT_PAREN);
                                    return policy;
                                case FUNCTION: {
                                    lexer.nextToken();
                                    idTemp1 = identifier();
                                    // 目前暂实现至 FUNCTION 名为止 ，方便禁用语句，其余待后续实现
                                    return createFunction(definer, idTemp1);
                                }
                                case EVENT: {
                                    lexer.nextToken();
                                    idTemp1 = identifier();
                                    // 目前暂实现至 EVENT 名为止 ，方便禁用语句，其余待后续实现
                                    return new DDLCreateEventStatement(definer, idTemp1);
                                }
                                case VIEW: {
                                    lexer.nextToken();
                                    idTemp1 = identifier();
                                    // 目前暂实现至 view 名为止 ，方便禁用语句，其余待后续实现
                                    DDLCreateViewStatement ddlCreateViewStatement =
                                            new DDLCreateViewStatement(algorithm, definer,
                                                    sqlSecurity, idTemp1);
                                    ddlCreateViewStatement.setOrRepalce(isOrRepalce);
                                    return ddlCreateViewStatement;
                                }
                                case TEMPORARY: {
                                    // CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
                                    //              (create_definition,...)
                                    //              [table_options]
                                    //              [partition_options]
                                    if (lexer.nextToken() == MySQLToken.KW_TABLE) {
                                        lexer.nextToken();
                                        boolean ifNotExists_T = ifNotExists();
                                        Identifier table_T = identifier();
                                        if (lexer.token() == MySQLToken.KW_LIKE)
                                            return createTableByLike(true, ifNotExists_T, table_T);
                                        else
                                            return createTable(true, ifNotExists_T, table_T);
                                    } else {
                                        throw err("unsupported DDL for CREATE");
                                    }
                                }
                            }
                        }
                    default:
                        throw err("unsupported DDL for CREATE");
                }
            case KW_DROP:
                switch (lexer.nextToken()) {
                    case KW_INDEX:
                        lexer.nextToken();
                        idTemp1 = identifier();
                        match(MySQLToken.KW_ON);
                        idTemp2 = identifier();
                        DDLDropIndexStatement dropIndexStatement =
                                new DDLDropIndexStatement(idTemp1, idTemp2);
                        return dropIndex(dropIndexStatement);
                    case KW_TABLE:
                        lexer.nextToken();
                        return dropTable(false);
                    case KW_TRIGGER:
                        lexer.nextToken();
                        return dropTrigger();
                    case IDENTIFIER:
                        siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (siTemp != null) {
                            switch (siTemp) {
                                case TEMPORARY:
                                    lexer.nextToken();
                                    if (lexer.token() == MySQLToken.KW_TABLE) {
                                        match(MySQLToken.KW_TABLE);
                                    } else if (lexer.token() == MySQLToken.IDENTIFIER) {
                                        matchIdentifier("TABLES");
                                    }
                                    return dropTable(true);
                                case POLICY:
                                    lexer.nextToken();
                                    Identifier policyName = identifier();
                                    return new ExtDDLDropPolicy(policyName);
                                case TABLES:
                                    lexer.nextToken();
                                    return dropTable(false);
                                case PREPARE: {
                                    lexer.nextToken();
                                    idTemp1 = identifier();
                                    return new DropPrepareStatement(idTemp1.getIdTextUpUnescape());
                                }
                            }
                        }
                    default:
                        throw err("unsupported DDL for DROP");
                }
            case KW_RENAME:
                lexer.nextToken();
                match(MySQLToken.KW_TABLE);
                idTemp1 = identifier();
                match(MySQLToken.KW_TO);
                idTemp2 = identifier();
                List<Pair<Identifier, Identifier>> list;
                if (lexer.token() != MySQLToken.PUNC_COMMA) {
                    list = new ArrayList<Pair<Identifier, Identifier>>(1);
                    list.add(new Pair<Identifier, Identifier>(idTemp1, idTemp2));
                    return new DDLRenameTableStatement(list);
                }
                list = new LinkedList<Pair<Identifier, Identifier>>();
                list.add(new Pair<Identifier, Identifier>(idTemp1, idTemp2));
                for (; lexer.token() == MySQLToken.PUNC_COMMA;) {
                    lexer.nextToken();
                    idTemp1 = identifier();
                    match(MySQLToken.KW_TO);
                    idTemp2 = identifier();
                    list.add(new Pair<Identifier, Identifier>(idTemp1, idTemp2));
                }
                return new DDLRenameTableStatement(list);
            case IDENTIFIER:
                SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                if (si != null) {
                    switch (si) {
                        case TRUNCATE:
                            return truncate();
                        case DEALLOCATE: {
                            lexer.nextToken();
                            if (lexer.token() == MySQLToken.IDENTIFIER
                                    && SpecialIdentifier.PREPARE == specialIdentifiers
                                            .get(lexer.stringValueUppercase())) {
                                lexer.nextToken();
                            }
                            idTemp1 = identifier();
                            return new DropPrepareStatement(idTemp1.getIdTextUpUnescape());
                        }
                    }
                }
            default:
                throw err("unsupported DDL");
        }
    }

    /**
     * 
     * @return 
     * @throws SQLSyntaxErrorException 
     */
    private DDLStatement dropTrigger() throws SQLSyntaxErrorException {
        boolean ifExists = false;
        if (lexer.token() == MySQLToken.KW_IF) {
            lexer.nextToken();
            match(MySQLToken.KW_EXISTS);
            ifExists = true;
        }
        Identifier tb = identifier();
        return new DDLDropTriggerStatement(ifExists, tb);
    }

    /**
     * <pre>
     * CREATE
     *     [DEFINER = { user | CURRENT_USER }]
     *     FUNCTION sp_name ([func_parameter[,...]])
     *     RETURNS type
     *     [characteristic ...] routine_body
     * 
     * func_parameter:
     *     param_name type
     * 
     * type:
     *     Any valid MySQL data type
     * 
     * characteristic:
     *     COMMENT 'string'
     *   | LANGUAGE SQL
     *   | [NOT] DETERMINISTIC
     *   | { CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA }
     *   | SQL SECURITY { DEFINER | INVOKER }
     * 
     * routine_body:
     *     Valid SQL routine statement
     *     </pre>
     * @param definer
     * @param idTemp1
     * @return 
     * @throws SQLSyntaxErrorException 
     */
    private DDLStatement createFunction(Expression definer, Identifier name)
            throws SQLSyntaxErrorException {
        match(MySQLToken.PUNC_LEFT_PAREN);
        List<Pair<Identifier, DataType>> parameters = new ArrayList<>();
        if (lexer.token() != MySQLToken.PUNC_RIGHT_PAREN) {
            parameters.add(getFuncParameter());
            while (lexer.token() == MySQLToken.PUNC_COMMA) {
                lexer.nextToken();
                parameters.add(getFuncParameter());
            }
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        if ("RETURNS".equals(lexer.stringValueUppercase())) {
            lexer.nextToken();
            DataType returns = dataType();
            Characteristics characteristics = new Characteristics();
            while (characteristic(characteristics)) {
            }
            SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
            return new DDLCreateFunctionStatement(definer, name, parameters, returns,
                    characteristics, stmt);
        } else {
            throw new SQLSyntaxErrorException("expect RETURNS");
        }
    }

    private Pair<Identifier, DataType> getFuncParameter() throws SQLSyntaxErrorException {
        Identifier param = identifier();
        DataType dataType = dataType();
        return new Pair<Identifier, DataType>(param, dataType);
    }

    /**
     * 
     * <pre>
     * CREATE
     *     [DEFINER = { user | CURRENT_USER }]
     *     PROCEDURE sp_name ([proc_parameter[,...]])
     *     [characteristic ...] routine_body
     * 
     * proc_parameter:
     *     [ IN | OUT | INOUT ] param_name type
     * 
     * type:
     *     Any valid MySQL data type
     * 
     * characteristic:
     *     COMMENT 'string'
     *   | LANGUAGE SQL
     *   | [NOT] DETERMINISTIC
     *   | { CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA }
     *   | SQL SECURITY { DEFINER | INVOKER }
     * 
     * routine_body:
     *     Valid SQL routine statement
     *     </pre>
     * @param definer
     * @param idTemp1
     * @return 
     * @throws SQLSyntaxErrorException 
     */
    private DDLStatement createProcedure(Expression definer, Identifier name)
            throws SQLSyntaxErrorException {
        match(MySQLToken.PUNC_LEFT_PAREN);
        List<Tuple3<ProcParameterType, Identifier, DataType>> parameters = new ArrayList<>();
        if (lexer.token() != MySQLToken.PUNC_RIGHT_PAREN) {
            parameters.add(getProcParameter());
            while (lexer.token() == MySQLToken.PUNC_COMMA) {
                lexer.nextToken();
                parameters.add(getProcParameter());
            }
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        Characteristics characteristics = new Characteristics();
        while (characteristic(characteristics)) {
        }
        SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
        return new DDLCreateProcedureStatement(definer, name, parameters, characteristics, stmt);
    }

    private boolean characteristic(Characteristics characteristics) throws SQLSyntaxErrorException {
        String str = lexer.stringValueUppercase();
        switch (str) {
            case "COMMENT": {
                characteristics.setComment(exprParser.expression());
                lexer.nextToken();
                return true;
            }
            case "LANGUAGE": {
                lexer.nextToken();
                match(MySQLToken.KW_SQL);
                characteristics.setLanguageSql(Characteristic.LANGUAGE_SQL);
                return true;
            }
            case "NOT": {
                lexer.nextToken();
                if ("DETERMINISTIC".equals(lexer.stringValueUppercase())) {
                    characteristics.setDeterministic(Characteristic.NOT_DETERMINISTIC);
                    lexer.nextToken();
                    return true;
                } else {
                    return false;
                }
            }
            case "DETERMINISTIC": {
                lexer.nextToken();
                characteristics.setDeterministic(Characteristic.NOT_DETERMINISTIC);
                return true;
            }
            case "CONTAINS": {
                lexer.nextToken();
                match(MySQLToken.KW_SQL);
                characteristics.setSqlCharacteristic(Characteristic.CONTAINS_SQL);
                return true;
            }
            case "NO": {
                lexer.nextToken();
                match(MySQLToken.KW_SQL);
                characteristics.setSqlCharacteristic(Characteristic.NO_SQL);
                return true;
            }
            case "READS": {
                lexer.nextToken();
                match(MySQLToken.KW_SQL);
                if ("DATA".equals(lexer.stringValueUppercase())) {
                    characteristics.setSqlCharacteristic(Characteristic.READS_SQL_DATA);
                    lexer.nextToken();
                    return true;
                } else {
                    return false;
                }
            }
            case "MODIFIES": {
                lexer.nextToken();
                match(MySQLToken.KW_SQL);
                if ("DATA".equals(lexer.stringValueUppercase())) {
                    characteristics.setSqlCharacteristic(Characteristic.MODIFIES_SQL_DATA);
                    lexer.nextToken();
                    return true;
                } else {
                    return false;
                }
            }
            case "SQL": {
                lexer.nextToken();
                if ("SECURITY".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    if ("DEFINER".equals(lexer.stringValueUppercase())) {
                        characteristics.setSqlSecurity(Characteristic.SQL_SECURITY_DEFINER);
                        lexer.nextToken();
                        return true;
                    } else if ("INVOKER".equals(lexer.stringValueUppercase())) {
                        characteristics.setSqlSecurity(Characteristic.SQL_SECURITY_INVOKER);
                        lexer.nextToken();
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            default:
                return false;
        }
    }

    private Tuple3<ProcParameterType, Identifier, DataType> getProcParameter()
            throws SQLSyntaxErrorException {
        ProcParameterType type = ProcParameterType.NONE;
        if (lexer.token() == MySQLToken.KW_IN) {
            type = ProcParameterType.IN;
        } else if (lexer.token() == MySQLToken.KW_OUT) {
            type = ProcParameterType.OUT;
        } else if (lexer.token() == MySQLToken.KW_INOUT) {
            type = ProcParameterType.INOUT;
        }
        Identifier param = identifier();
        DataType dataType = dataType();
        return new Tuple3<ProcParameterType, Identifier, DataType>(type, param, dataType);
    }

    /**
     * <pre>
     * CREATE
     *     [DEFINER = { user | CURRENT_USER }]
     *     TRIGGER trigger_name
     *     trigger_time trigger_event
     *     ON tbl_name FOR EACH ROW
     *     [trigger_order]
     *     trigger_body
     * 
     * trigger_time: { BEFORE | AFTER }
     * 
     * trigger_event: { INSERT | UPDATE | DELETE }
     * 
     * trigger_order: { FOLLOWS | PRECEDES } other_trigger_name
     * </pre>
     * @param definer 
     * @param name trigger name
     * @return 
     * @throws SQLSyntaxErrorException 
     */
    private DDLStatement createTrigger(Expression definer, Identifier name)
            throws SQLSyntaxErrorException {
        TriggerTime time = null;
        switch (lexer.token()) {
            case KW_BEFORE:
                time = TriggerTime.BEFORE;
                break;
            case IDENTIFIER:
                if ("AFTER".equals(lexer.stringValueUppercase())) {
                    time = TriggerTime.AFTER;
                    break;
                }
            default:
                throw new SQLSyntaxErrorException("unexpected trigger_time");
        }
        lexer.nextToken();
        TriggerEvent event = null;
        switch (lexer.token()) {
            case KW_INSERT:
                event = TriggerEvent.INSERT;
                break;
            case KW_UPDATE:
                event = TriggerEvent.UPDATE;
                break;
            case KW_DELETE:
                event = TriggerEvent.DELETE;
                break;
            default:
                throw new SQLSyntaxErrorException("unexpected trigger_event");
        }
        lexer.nextToken();
        match(MySQLToken.KW_ON);
        Identifier table = identifier();
        match(MySQLToken.KW_FOR);
        match(MySQLToken.KW_EACH);
        TriggerOrder order = null;
        Identifier otherTrigger = null;
        if ("ROW".equals(lexer.stringValueUppercase())) {
            lexer.nextToken();
            if (lexer.token() == MySQLToken.IDENTIFIER) {
                if ("FOLLOWS".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    otherTrigger = identifier();
                    order = TriggerOrder.FOLLOWS;
                } else if ("PRECEDES".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    otherTrigger = identifier();
                    order = TriggerOrder.PRECEDES;
                }
            }
        } else {
            throw new SQLSyntaxErrorException("expect ROW");
        }
        SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
        return new DDLCreateTriggerStatement(definer, name, time, event, table, order, otherTrigger,
                stmt);
    }

    /**
     * DROP INDEX index_name ON tbl_name
     *      [algorithm_option | lock_option] ...
     *
     * algorithm_option:
     *      ALGORITHM [=] {DEFAULT|INPLACE|COPY}
     *
     * lock_option:
     *      LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}
     *
     * @param dropIndexStatement
     * @return
     * @throws SQLSyntaxErrorException
     */
    private DDLStatement dropIndex(DDLDropIndexStatement dropIndexStatement)
            throws SQLSyntaxErrorException {
        while (lexer.token() != MySQLToken.EOF) {
            switch (lexer.token()) {
                case KW_LOCK:
                    lexer.nextToken();
                    match(MySQLToken.OP_EQUALS);
                    DDLDropIndexStatement.Lock lock;
                    switch (lexer.token()) {
                        case KW_DEFAULT:
                            lock = DDLDropIndexStatement.Lock.DEFAULT;
                            dropIndexStatement.setLock(lock);
                            lexer.nextToken();
                            break;
                        case IDENTIFIER: {
                            SpecialIdentifier spi =
                                    specialIdentifiers.get(lexer.stringValueUppercase());
                            if (spi != null) {
                                switch (spi) {
                                    case NONE:
                                        lock = DDLDropIndexStatement.Lock.NONE;
                                        dropIndexStatement.setLock(lock);
                                        lexer.nextToken();
                                        break;
                                    case SHARED:
                                        lock = DDLDropIndexStatement.Lock.SHARED;
                                        dropIndexStatement.setLock(lock);
                                        lexer.nextToken();
                                        break;
                                    case EXCLUSIVE:
                                        lock = DDLDropIndexStatement.Lock.EXCLUSIVE;
                                        dropIndexStatement.setLock(lock);
                                        lexer.nextToken();
                                        break;
                                    default:
                                        throw new SQLSyntaxErrorException("unexpected lock type");
                                }
                                break;
                            }
                        }
                        default:
                            throw new SQLSyntaxErrorException("unexpected lock type");
                    }
                    break;
                case IDENTIFIER: {
                    if (SpecialIdentifier.ALGORITHM == specialIdentifiers
                            .get(lexer.stringValueUppercase())) {
                        lexer.nextToken();
                        match(MySQLToken.OP_EQUALS);
                        DDLDropIndexStatement.Algorithm algorithm;
                        switch (lexer.token()) {
                            case KW_DEFAULT:
                                algorithm = DDLDropIndexStatement.Algorithm.DEFAULT;
                                dropIndexStatement.setAlgorithm(algorithm);
                                lexer.nextToken();
                                break;
                            case IDENTIFIER: {
                                SpecialIdentifier spi =
                                        specialIdentifiers.get(lexer.stringValueUppercase());
                                if (spi != null) {
                                    switch (spi) {
                                        case INPLACE:
                                            algorithm = DDLDropIndexStatement.Algorithm.INPLACE;
                                            dropIndexStatement.setAlgorithm(algorithm);
                                            lexer.nextToken();
                                            break;
                                        case COPY:
                                            algorithm = DDLDropIndexStatement.Algorithm.COPY;
                                            dropIndexStatement.setAlgorithm(algorithm);
                                            lexer.nextToken();
                                            break;
                                        default:
                                            throw new SQLSyntaxErrorException(
                                                    "unexpected algorithm type");
                                    }
                                    break;
                                }
                            }
                            default:
                                throw new SQLSyntaxErrorException("unexpected algorithm type");
                        }
                        break;
                    }
                }
                default:
                    throw new SQLSyntaxErrorException("unsupported index definition");
            }
        }
        return dropIndexStatement;
    }

    /**
     * CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX index_name [index_type] ON tbl_name
     * (index_col_name,...) [index_option] [algorithm_option | lock_option] ...
     *
     * index_col_name: col_name [(length)] [ASC | DESC]
     *
     * index_type: USING {BTREE | HASH}
     *
     * index_option: KEY_BLOCK_SIZE [=] value | index_type | WITH PARSER parser_name | COMMENT
     * 'string'
     *
     * algorithm_option: ALGORITHM [=] {DEFAULT|INPLACE|COPY}
     *
     * lock_option: LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}
     *
     * @param indexName
     * @return
     * @throws SQLSyntaxErrorException
     */
    private DDLCreateIndexStatement createIndex(Identifier indexName)
            throws SQLSyntaxErrorException {
        IndexDefinition.IndexType indexType = null;
        List<IndexColumnName> columns = new ArrayList<IndexColumnName>();
        if (lexer.token() == MySQLToken.KW_USING) {
            lexer.nextToken();
            int tp = matchToken("BTREE", "HASH");
            indexType = tp == 0 ? IndexDefinition.IndexType.BTREE : IndexDefinition.IndexType.HASH;
        }
        match(MySQLToken.KW_ON);
        Identifier table = identifier();
        match(MySQLToken.PUNC_LEFT_PAREN);
        for (int i = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++i) {
            if (i > 0)
                match(MySQLToken.PUNC_COMMA);
            IndexColumnName indexColumnName = indexColumnName();
            columns.add(indexColumnName);
        }
        lexer.nextToken();
        List<IndexOption> options = indexOptions();
        IndexDefinition indexDefinition = new IndexDefinition(indexType, columns, options);
        indexDefinition.setIndexName(indexName);
        DDLCreateIndexStatement.Algorithm algorithm = null;
        if (lexer.token() == MySQLToken.IDENTIFIER
                && SpecialIdentifier.ALGORITHM == specialIdentifiers
                        .get(lexer.stringValueUppercase())) {
            lexer.nextToken();
            if (lexer.token() == MySQLToken.OP_EQUALS) {
                lexer.nextToken();
            }
            switch (lexer.token()) {
                case KW_DEFAULT:
                    algorithm = DDLCreateIndexStatement.Algorithm.DEFAULT;
                    lexer.nextToken();
                    break;
                case IDENTIFIER: {
                    SpecialIdentifier spi = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (spi != null) {
                        switch (spi) {
                            case INPLACE:
                                algorithm = DDLCreateIndexStatement.Algorithm.INPLACE;
                                lexer.nextToken();
                                break;
                            case COPY:
                                algorithm = DDLCreateIndexStatement.Algorithm.COPY;
                                lexer.nextToken();
                                break;
                            default:
                                throw new SQLSyntaxErrorException("unexpected algorithm type");
                        }
                        break;
                    }
                }
                default:
                    throw new SQLSyntaxErrorException("unexpected algorithm type");
            }
        }
        DDLCreateIndexStatement.Lock lock = null;
        if (lexer.token() == MySQLToken.KW_LOCK) {
            lexer.nextToken();
            if (lexer.token() == MySQLToken.OP_EQUALS) {
                lexer.nextToken();
            }
            switch (lexer.token()) {
                case KW_DEFAULT:
                    lock = DDLCreateIndexStatement.Lock.DEFAULT;
                    lexer.nextToken();
                    break;
                case IDENTIFIER: {
                    SpecialIdentifier spi = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (spi != null) {
                        switch (spi) {
                            case NONE:
                                lock = DDLCreateIndexStatement.Lock.NONE;
                                lexer.nextToken();
                                break;
                            case SHARED:
                                lock = DDLCreateIndexStatement.Lock.SHARED;
                                lexer.nextToken();
                                break;
                            case EXCLUSIVE:
                                lock = DDLCreateIndexStatement.Lock.EXCLUSIVE;
                                lexer.nextToken();
                                break;
                            default:
                                throw new SQLSyntaxErrorException("unexpected lock type");
                        }
                        break;
                    }
                }
                default:
                    throw new SQLSyntaxErrorException("unexpected lock type");
            }
        }
        return new DDLCreateIndexStatement(table, indexDefinition, algorithm, lock);
    }

    /**
     * <code>TABLE</code> has been consumed
     */
    @SuppressWarnings("incomplete-switch")
    private DDLDropTableStatement dropTable(boolean temp) throws SQLSyntaxErrorException {
        boolean ifExists = false;
        if (lexer.token() == MySQLToken.KW_IF) {
            lexer.nextToken();
            match(MySQLToken.KW_EXISTS);
            ifExists = true;
        }
        Identifier tb = identifier();
        List<Identifier> list;
        if (lexer.token() != MySQLToken.PUNC_COMMA) {
            list = new ArrayList<Identifier>(1);
            list.add(tb);
        } else {
            list = new LinkedList<Identifier>();
            list.add(tb);
            for (; lexer.token() == MySQLToken.PUNC_COMMA;) {
                lexer.nextToken();
                tb = identifier();
                list.add(tb);
            }
        }
        DDLDropTableStatement.Mode mode = DDLDropTableStatement.Mode.UNDEF;
        switch (lexer.token()) {
            case KW_RESTRICT:
                lexer.nextToken();
                mode = DDLDropTableStatement.Mode.RESTRICT;
                break;
            case KW_CASCADE:
                lexer.nextToken();
                mode = DDLDropTableStatement.Mode.CASCADE;
                break;
        }
        return new DDLDropTableStatement(list, temp, ifExists, mode);
    }

    /**
     * token of table name has been consumed
     *
     * @throws SQLSyntaxErrorException
     */
    @SuppressWarnings("incomplete-switch")
    private DDLAlterTableStatement alterTable(DDLAlterTableStatement stmt)
            throws SQLSyntaxErrorException {
        TableOptions options = new TableOptions();
        stmt.setTableOptions(options);
        Identifier id = null;
        Identifier id2 = null;
        Identifier id3 = null;
        ColumnDefinition colDef = null;
        IndexDefinition indexDef = null;
        Expression expr = null;
        Identifier symbol = null;
        for (int i = 0; lexer.token() != MySQLToken.EOF; ++i) {
            if (i > 0) {
                match(MySQLToken.PUNC_COMMA);
            }
            boolean matchTbOptions = tableOptions(options);
            main_switch: switch (lexer.token()) {
                case KW_CONVERT:
                    // | CONVERT TO CHARACTER SET charset_name [COLLATE
                    // collation_name]
                    lexer.nextToken();
                    match(MySQLToken.KW_TO);
                    match(MySQLToken.KW_CHARACTER);
                    match(MySQLToken.KW_SET);
                    id = identifier();
                    id2 = null;
                    if (lexer.token() == MySQLToken.KW_COLLATE) {
                        lexer.nextToken();
                        id2 = identifier();
                    }
                    stmt.setConvertCharset(new Pair<Identifier, Identifier>(id, id2));
                    break main_switch;
                case KW_RENAME:
                    // | RENAME [TO] new_tbl_name
                    lexer.nextToken();
                    if (lexer.token() == MySQLToken.KW_INDEX
                            || lexer.token() == MySQLToken.KW_KEY) {
                        lexer.nextToken();
                        Identifier renameFromOld = identifier();
                        match(MySQLToken.KW_TO);
                        Identifier renameToNew = identifier();
                        stmt.addAlterSpecification(new ReNameIndex(renameFromOld, renameToNew));
                        break main_switch;
                    } else if (lexer.token() == MySQLToken.KW_TO
                            || lexer.token() == MySQLToken.KW_AS) {
                        lexer.nextToken();
                    }
                    id = identifier();
                    stmt.setRenameTo(id);
                    break main_switch;
                case KW_DROP:
                    drop_switch: switch (lexer.nextToken()) {
                        case KW_INDEX:
                        case KW_KEY:
                            // | DROP {INDEX|KEY} index_name
                            lexer.nextToken();
                            id = identifier();
                            stmt.addAlterSpecification(new DDLAlterTableStatement.DropIndex(id));
                            break drop_switch;
                        case KW_FOREIGN:
                            // | DROP FOREIGN KEY
                            lexer.nextToken();
                            match(MySQLToken.KW_KEY);
                            stmt.addAlterSpecification(new DDLAlterTableStatement.DropForeignKey());
                            lexer.nextToken();
                            break drop_switch;
                        case KW_PRIMARY:
                            // | DROP PRIMARY KEY
                            lexer.nextToken();
                            match(MySQLToken.KW_KEY);
                            stmt.addAlterSpecification(new DDLAlterTableStatement.DropPrimaryKey());
                            break drop_switch;
                        case IDENTIFIER:
                            // | DROP [COLUMN] col_name
                            id = identifier();
                            stmt.addAlterSpecification(new DDLAlterTableStatement.DropColumn(id));
                            break drop_switch;
                        case KW_COLUMN:
                            // | DROP [COLUMN] col_name
                            lexer.nextToken();
                            id = identifier();
                            stmt.addAlterSpecification(new DDLAlterTableStatement.DropColumn(id));
                            break drop_switch;
                        case KW_PARTITION:
                            while (lexer.token() != MySQLToken.EOF) {
                                lexer.nextToken();
                            }
                            return stmt;
                        default:
                            throw new SQLSyntaxErrorException("ALTER TABLE error for DROP");
                    }
                    break main_switch;
                case KW_CHANGE:
                    // | CHANGE [COLUMN] old_col_name new_col_name column_definition
                    // [FIRST|AFTER col_name]
                    if (lexer.nextToken() == MySQLToken.KW_COLUMN) {
                        lexer.nextToken();
                    }
                    id = identifier();
                    id2 = identifier();
                    colDef = columnDefinition();
                    if (lexer.token() == MySQLToken.IDENTIFIER) {
                        if ("FIRST".equals(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.ChangeColumn(id, id2, colDef, null));
                        } else if ("AFTER".equals(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                            id3 = identifier();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.ChangeColumn(id, id2, colDef, id3));
                        } else {
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.ChangeColumn(id, id2, colDef));
                        }
                    } else {
                        stmt.addAlterSpecification(
                                new DDLAlterTableStatement.ChangeColumn(id, id2, colDef));
                    }
                    break main_switch;
                case KW_ALTER:
                    // | ALTER [COLUMN] col_name {SET DEFAULT literal | DROP
                    // DEFAULT}
                    if (lexer.nextToken() == MySQLToken.KW_COLUMN) {
                        lexer.nextToken();
                    }
                    id = identifier();
                    switch (lexer.token()) {
                        case KW_SET:
                            lexer.nextToken();
                            match(MySQLToken.KW_DEFAULT);
                            expr = exprParser.expression();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AlterColumnDefaultVal(id, expr));
                            break;
                        case KW_DROP:
                            lexer.nextToken();
                            match(MySQLToken.KW_DEFAULT);
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AlterColumnDefaultVal(id));
                            break;
                        default:
                            throw new SQLSyntaxErrorException("ALTER TABLE error for ALTER");
                    }
                    break main_switch;
                case KW_ADD:
                    add_switch: switch (lexer.nextToken()) {
                        case IDENTIFIER:
                            // | ADD [COLUMN] col_name column_definition [FIRST | AFTER
                            // col_name ]
                            id = identifier();
                            colDef = columnDefinition();
                            if (lexer.token() == MySQLToken.IDENTIFIER) {
                                if ("FIRST".equals(lexer.stringValueUppercase())) {
                                    lexer.nextToken();
                                    stmt.addAlterSpecification(
                                            new DDLAlterTableStatement.AddColumn(id, colDef, null));
                                } else if ("AFTER".equals(lexer.stringValueUppercase())) {
                                    lexer.nextToken();
                                    id2 = identifier();
                                    stmt.addAlterSpecification(
                                            new DDLAlterTableStatement.AddColumn(id, colDef, id2));
                                } else {
                                    stmt.addAlterSpecification(
                                            new DDLAlterTableStatement.AddColumn(id, colDef));
                                }
                            } else {
                                stmt.addAlterSpecification(
                                        new DDLAlterTableStatement.AddColumn(id, colDef));
                            }
                            break add_switch;
                        case PUNC_LEFT_PAREN:
                            // | ADD [COLUMN] (col_name column_definition,...)
                            lexer.nextToken();
                            for (int j = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++j) {
                                DDLAlterTableStatement.AddColumns addColumns =
                                        new DDLAlterTableStatement.AddColumns();
                                stmt.addAlterSpecification(addColumns);
                                if (j > 0) {
                                    match(MySQLToken.PUNC_COMMA);
                                }
                                id = identifier();
                                colDef = columnDefinition();
                                addColumns.addColumn(id, colDef);
                            }
                            match(MySQLToken.PUNC_RIGHT_PAREN);
                            break add_switch;
                        case KW_COLUMN:
                            if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                                // | ADD [COLUMN] (col_name column_definition,...)
                                lexer.nextToken();
                                for (int j = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++j) {
                                    DDLAlterTableStatement.AddColumns addColumns =
                                            new DDLAlterTableStatement.AddColumns();
                                    stmt.addAlterSpecification(addColumns);
                                    if (j > 0) {
                                        match(MySQLToken.PUNC_COMMA);
                                    }
                                    id = identifier();
                                    colDef = columnDefinition();
                                    addColumns.addColumn(id, colDef);
                                }
                                match(MySQLToken.PUNC_RIGHT_PAREN);
                            } else {
                                // | ADD [COLUMN] col_name column_definition [FIRST |
                                // AFTER col_name ]
                                id = identifier();
                                colDef = columnDefinition();
                                if (lexer.token() == MySQLToken.IDENTIFIER) {
                                    if ("FIRST".equals(lexer.stringValueUppercase())) {
                                        lexer.nextToken();
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.AddColumn(id, colDef,
                                                        null));
                                    } else if ("AFTER".equals(lexer.stringValueUppercase())) {
                                        lexer.nextToken();
                                        id2 = identifier();
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.AddColumn(id, colDef,
                                                        id2));
                                    } else {
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.AddColumn(id, colDef));
                                    }
                                } else {
                                    stmt.addAlterSpecification(
                                            new DDLAlterTableStatement.AddColumn(id, colDef));
                                }
                            }
                            break add_switch;
                        case KW_INDEX:
                        case KW_KEY:
                            // | ADD {INDEX|KEY} [index_name] [index_type]
                            // (index_col_name,...) [index_option] ...
                            id = null;
                            if (lexer.nextToken() == MySQLToken.IDENTIFIER) {
                                id = identifier();
                            }
                            indexDef = indexDefinition();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AddIndex(id, indexDef));
                            break add_switch;
                        case KW_CONSTRAINT:
                            //                            lexer.nextToken();
                            //                            keyName = lexer.stringValue();
                            //                            if (lexer.token() == MySQLToken.IDENTIFIER) {
                            //                                lexer.nextToken();
                            //                            }
                            if (lexer.nextToken() == MySQLToken.IDENTIFIER) {
                                symbol = identifier();
                            }
                            // | ADD [CONSTRAINT [symbol]] PRIMARY KEY
                            // [index_type] (index_col_name,...) [index_option] ...
                            if (lexer.token() == MySQLToken.KW_PRIMARY) {
                                lexer.nextToken();
                                match(MySQLToken.KW_KEY);
                                indexDef = indexDefinition();
                                stmt.addAlterSpecification(
                                        new DDLAlterTableStatement.AddPrimaryKey(symbol, indexDef));
                            }
                            // | ADD [CONSTRAINT [symbol]]
                            // UNIQUE [INDEX|KEY] [index_name]
                            //        [index_type] (index_col_name,...) [index_option] ...
                            else if (lexer.token() == MySQLToken.KW_UNIQUE) {
                                // @ZC.CUI | ADD [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY]
                                // [index_name] [index_type] (index_col_name,...) [index_option] ...
                                switch (lexer.nextToken()) {
                                    case KW_INDEX:
                                    case KW_KEY:
                                        lexer.nextToken();
                                }
                                //                                id = null;
                                //                                if (lexer.token() == MySQLToken.IDENTIFIER) {
                                //                                    id = identifier();
                                //                                }
                                indexDef = indexDefinition();
                                stmt.addAlterSpecification(
                                        new DDLAlterTableStatement.AddUniqueKey(symbol, indexDef));
                            }
                            // | ADD [CONSTRAINT [symbol]]
                            // FOREIGN KEY [index_name] (index_col_name,...)
                            // reference_definition
                            else if (lexer.token() == MySQLToken.KW_FOREIGN) {
                                // @ZC.CUI | ADD CONSTRAINT [symbol] FOREIGN KEY [index_name]
                                // (index_col_name,...) reference_definition
                                lexer.nextToken();
                                match(MySQLToken.KW_KEY);
                                DDLAlterTableStatement.AddForeignKey foreignKeyDef =
                                        foreignKeyDefinition(symbol);
                                stmt.addAlterSpecification(foreignKeyDef);
                                while (lexer.token() != MySQLToken.EOF) {
                                    lexer.nextToken();
                                }
                            }
                            break add_switch;
                        case KW_FOREIGN:
                            // @ZC.CUI | ADD FOREIGN KEY [index_name] (index_col_name,...)
                            // reference_definition
                            lexer.nextToken();
                            match(MySQLToken.KW_KEY);
                            DDLAlterTableStatement.AddForeignKey foreignKeyDef =
                                    foreignKeyDefinition(symbol);
                            stmt.addAlterSpecification(foreignKeyDef);
                            while (lexer.token() != MySQLToken.EOF) {
                                lexer.nextToken();
                            }
                            break add_switch;
                        case KW_PRIMARY:
                            // | ADD [CONSTRAINT [symbol]] PRIMARY KEY [index_type]
                            // (index_col_name,...)
                            lexer.nextToken();
                            match(MySQLToken.KW_KEY);
                            indexDef = indexDefinition();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AddPrimaryKey(symbol, indexDef));
                            break add_switch;
                        case KW_UNIQUE:
                            // | ADD UNIQUE [INDEX|KEY] [index_name] [index_type]
                            // (index_col_name,...) [index_option] ...
                            switch (lexer.nextToken()) {
                                case KW_INDEX:
                                case KW_KEY:
                                    lexer.nextToken();
                            }
                            id = null;
                            if (lexer.token() == MySQLToken.IDENTIFIER) {
                                id = identifier();
                            }
                            indexDef = indexDefinition();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AddUniqueKey(id, indexDef));
                            break add_switch;
                        case KW_FULLTEXT:
                            // | ADD FULLTEXT [INDEX|KEY] [index_name]
                            // (index_col_name,...) [index_option] ...
                            switch (lexer.nextToken()) {
                                case KW_INDEX:
                                case KW_KEY:
                                    lexer.nextToken();
                            }
                            id = null;
                            if (lexer.token() == MySQLToken.IDENTIFIER) {
                                id = identifier();
                            }
                            indexDef = indexDefinition();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AddFullTextIndex(id, indexDef));
                            break add_switch;
                        case KW_SPATIAL:
                            // | ADD SPATIAL [INDEX|KEY] [index_name]
                            // (index_col_name,...) [index_option] ...
                            switch (lexer.nextToken()) {
                                case KW_INDEX:
                                case KW_KEY:
                                    lexer.nextToken();
                            }
                            id = null;
                            if (lexer.token() == MySQLToken.IDENTIFIER) {
                                id = identifier();
                            }
                            indexDef = indexDefinition();
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AddSpatialIndex(id, indexDef));
                            break add_switch;
                        case KW_PARTITION:
                            while (lexer.token() != MySQLToken.EOF) {
                                lexer.nextToken();
                            }
                            return stmt;
                        default:
                            throw new SQLSyntaxErrorException("ALTER TABLE error for ADD");
                    }
                    break main_switch;
                case KW_LOCK:
                    // @ZC.CUI | LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}
                    if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                        MySQLToken token = lexer.nextToken();
                        DDLAlterTableStatement.Lock lock;
                        switch (token) {
                            case KW_DEFAULT:
                                lock = DDLAlterTableStatement.Lock.DEFAULT;
                                stmt.setLock(lock);
                                lexer.nextToken();
                                break main_switch;
                            case IDENTIFIER: {
                                SpecialIdentifier spi =
                                        specialIdentifiers.get(lexer.stringValueUppercase());
                                if (spi != null) {
                                    switch (spi) {
                                        case NONE:
                                            lock = DDLAlterTableStatement.Lock.NONE;
                                            stmt.setLock(lock);
                                            lexer.nextToken();
                                            lexer.nextToken();
                                            break;
                                        case SHARED:
                                            lock = DDLAlterTableStatement.Lock.SHARED;
                                            stmt.setLock(lock);
                                            lexer.nextToken();
                                            break;
                                        case EXCLUSIVE:
                                            lock = DDLAlterTableStatement.Lock.EXCLUSIVE;
                                            stmt.setLock(lock);
                                            lexer.nextToken();
                                            break;
                                        default:
                                            throw new SQLSyntaxErrorException(
                                                    "unexpected lock type");
                                    }
                                    break;
                                }
                            }
                            default:
                                throw new SQLSyntaxErrorException("unexpected lock type");
                        }
                    }
                    break main_switch;
                case KW_ORDER:
                    // @ZC.CUI | ORDER BY col_name [, col_name] ...
                    // if (lexer.nextToken() == MySQLToken.KW_BY) {
                    // if (lexer.nextToken() == MySQLToken.IDENTIFIER) {
                    // DDLAlterTableStatement.OrderByColumns oerderByColumns =
                    // new DDLAlterTableStatement.OrderByColumns();
                    // oerderByColumns.addColumns(identifier());
                    // while (lexer.token() !=MySQLToken.EOF) {
                    // match(MySQLToken.PUNC_COMMA);
                    // oerderByColumns.addColumns(identifier());
                    // }
                    // return stmt;
                    // }
                    // }
                    stmt.setOrderBy(orderBy());
                    break main_switch;
                case KW_CHECK:
                case KW_ANALYZE:
                case KW_OPTIMIZE:
                    MySQLToken type = lexer.token();
                    switch (lexer.nextToken()) {
                        case KW_PARTITION:
                            lexer.nextToken();
                    }
                    List<Identifier> partitions = null;
                    for (int j = 0; lexer.token() != MySQLToken.EOF; ++j) {
                        Identifier partition1 = null;
                        if (lexer.token() == MySQLToken.IDENTIFIER) {
                            partition1 = identifier();
                        } else if (lexer.token() == MySQLToken.KW_ALL) {
                            if (type == MySQLToken.KW_ANALYZE) {
                                stmt.addAlterSpecification(
                                        new DDLAlterTableStatement.AnalyzePartition(true,
                                                partitions));
                            } else if (type == MySQLToken.KW_OPTIMIZE) {
                                stmt.addAlterSpecification(
                                        new DDLAlterTableStatement.OptimizePartition(true,
                                                partitions));
                            } else if (type == MySQLToken.KW_CHECK) {
                                stmt.addAlterSpecification(
                                        new DDLAlterTableStatement.CheckPartition(true,
                                                partitions));
                            }
                            break;
                        }
                        if (partition1 != null) {
                            if (partitions == null) {
                                partitions = new ArrayList<>();
                            }
                            partitions.add(partition1);
                        }
                        if (lexer.token() == MySQLToken.EOF) {
                            break;
                        }
                        if (j > 0) {
                            match(MySQLToken.PUNC_COMMA);
                        }
                        lexer.nextToken();
                    }
                    if (partitions != null) {
                        if (type == MySQLToken.KW_ANALYZE) {
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.AnalyzePartition(false, partitions));
                        } else if (type == MySQLToken.KW_OPTIMIZE) {
                            stmt.addAlterSpecification(new DDLAlterTableStatement.OptimizePartition(
                                    false, partitions));
                        } else if (type == MySQLToken.KW_CHECK) {
                            stmt.addAlterSpecification(
                                    new DDLAlterTableStatement.CheckPartition(true, partitions));
                        }
                    }
                    return stmt;
                case KW_PARTITION:
                    lexer.nextToken();
                    if (lexer.token() == MySQLToken.KW_BY) {
                        stmt.addAlterSpecification(new DDLAlterTableStatement.PartitionFunction());
                        while (lexer.token() != MySQLToken.EOF) {
                            lexer.nextToken();
                        }
                    } else {
                        while (lexer.token() != MySQLToken.EOF) {
                            lexer.nextToken();
                        }
                    }
                    return stmt;
                case KW_WITH:
                    //WITH VALIDATION
                    if (lexer.nextToken() == MySQLToken.IDENTIFIER) {
                        SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (si == SpecialIdentifier.VALIDATION) {
                            stmt.addAlterSpecification(new WithValidation(true));
                        }
                    }
                    lexer.nextToken();
                    return stmt;
                case IDENTIFIER:
                    SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (si != null) {
                        switch (si) {
                            // | COALESCE PARTITION number
                            // | REORGANIZE PARTITION partition_names INTO (partition_definitions)
                            // | EXCHANGE PARTITION partition_name WITH TABLE tbl_name
                            // | ANALYZE PARTITION {partition_names | ALL}
                            // | CHECK PARTITION {partition_names | ALL}
                            // | OPTIMIZE PARTITION {partition_names | ALL}
                            // | REBUILD PARTITION {partition_names | ALL}
                            // | REPAIR PARTITION {partition_names | ALL}
                            // | REMOVE PARTITIONING
                            case TRUNCATE:
                            case COALESCE:
                            case REORGANIZE:
                            case REBUILD:
                                return partition(stmt);
                            case EXCHANGE:
                                switch (lexer.nextToken()) {
                                    case KW_PARTITION:
                                        lexer.nextToken();
                                }
                                Identifier partition = null;
                                if (lexer.token() == MySQLToken.IDENTIFIER) {
                                    partition = identifier();
                                }
                                match(MySQLToken.KW_WITH);
                                match(MySQLToken.KW_TABLE);
                                Identifier dstTable = null;
                                if (lexer.token() == MySQLToken.IDENTIFIER) {
                                    dstTable = identifier();
                                }
                                stmt.addAlterSpecification(
                                        new DDLAlterTableStatement.ExchangePartition(partition,
                                                dstTable));
                                return stmt;
                            case ANALYZE:
                            case CHECK:
                            case OPTIMIZE:
                            case REPAIR:
                                switch (lexer.nextToken()) {
                                    case KW_PARTITION:
                                        lexer.nextToken();
                                }
                                List<Identifier> partitions1 = null;
                                for (int j = 0; lexer.token() != MySQLToken.EOF; ++j) {
                                    Identifier partition1 = null;
                                    if (lexer.token() == MySQLToken.IDENTIFIER) {
                                        partition1 = identifier();
                                    } else if (lexer.token() == MySQLToken.KW_ALL) {
                                        if (si == SpecialIdentifier.ANALYZE) {
                                            stmt.addAlterSpecification(
                                                    new DDLAlterTableStatement.AnalyzePartition(
                                                            true, partitions1));
                                        } else if (si == SpecialIdentifier.OPTIMIZE) {
                                            stmt.addAlterSpecification(
                                                    new DDLAlterTableStatement.OptimizePartition(
                                                            true, partitions1));
                                        } else if (si == SpecialIdentifier.CHECK) {
                                            stmt.addAlterSpecification(
                                                    new DDLAlterTableStatement.CheckPartition(true,
                                                            partitions1));
                                        } else if (si == SpecialIdentifier.REPAIR) {
                                            stmt.addAlterSpecification(
                                                    new DDLAlterTableStatement.RepairPartition(true,
                                                            partitions1));
                                        }
                                        break;
                                    }
                                    if (partition1 != null) {
                                        if (partitions1 == null) {
                                            partitions1 = new ArrayList<>();
                                        }
                                        partitions1.add(partition1);
                                    }
                                    if (lexer.token() == MySQLToken.EOF) {
                                        break;
                                    }
                                    if (j > 0) {
                                        match(MySQLToken.PUNC_COMMA);
                                    }
                                    lexer.nextToken();
                                }
                                if (partitions1 != null) {
                                    if (si == SpecialIdentifier.ANALYZE) {
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.AnalyzePartition(true,
                                                        partitions1));
                                    } else if (si == SpecialIdentifier.OPTIMIZE) {
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.OptimizePartition(true,
                                                        partitions1));
                                    } else if (si == SpecialIdentifier.CHECK) {
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.CheckPartition(true,
                                                        partitions1));
                                    } else if (si == SpecialIdentifier.REPAIR) {
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.RepairPartition(true,
                                                        partitions1));
                                    }
                                }
                                return stmt;
                            case REMOVE:
                                lexer.nextToken();
                                matchIdentifier("PARTITIONING");
                                while (lexer.token() != MySQLToken.EOF) {
                                    lexer.nextToken();
                                }
                                return stmt;
                            case IMPORT:
                                // | IMPORT TABLESPACE
                                lexer.nextToken();
                                matchIdentifier("TABLESPACE");
                                stmt.setImportTableSpace(true);
                                break main_switch;
                            case DISCARD:
                                // | DISCARD TABLESPACE
                                lexer.nextToken();
                                matchIdentifier("TABLESPACE");
                                break main_switch;
                            case UPGRADE:
                                lexer.nextToken();
                                break main_switch;
                            case ENABLE:
                                // | ENABLE KEYS
                                lexer.nextToken();
                                match(MySQLToken.KW_KEYS);
                                stmt.setEnableKeys(true);
                                break main_switch;
                            case DISABLE:
                                // | DISABLE KEYS
                                lexer.nextToken();
                                match(MySQLToken.KW_KEYS);
                                stmt.setDisableKeys(true);
                                break main_switch;
                            case MODIFY:
                                // | MODIFY [COLUMN] col_name column_definition [FIRST |
                                // AFTER col_name]
                                if (lexer.nextToken() == MySQLToken.KW_COLUMN) {
                                    lexer.nextToken();
                                }
                                id = identifier();
                                colDef = columnDefinition();
                                if (lexer.token() == MySQLToken.IDENTIFIER) {
                                    if ("FIRST".equals(lexer.stringValueUppercase())) {
                                        lexer.nextToken();
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.ModifyColumn(id, colDef,
                                                        null));
                                    } else if ("AFTER".equals(lexer.stringValueUppercase())) {
                                        lexer.nextToken();
                                        id2 = identifier();
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.ModifyColumn(id, colDef,
                                                        id2));
                                    } else {
                                        stmt.addAlterSpecification(
                                                new DDLAlterTableStatement.ModifyColumn(id,
                                                        colDef));
                                    }
                                } else {
                                    stmt.addAlterSpecification(
                                            new DDLAlterTableStatement.ModifyColumn(id, colDef));
                                }
                                break main_switch;
                            case WITHOUT:
                                if (lexer.nextToken() == MySQLToken.IDENTIFIER) {
                                    si = specialIdentifiers.get(lexer.stringValueUppercase());
                                    if (si == SpecialIdentifier.VALIDATION) {
                                        stmt.addAlterSpecification(new WithValidation(false));
                                    }
                                }
                                lexer.nextToken();
                                break main_switch;
                            case ALGORITHM: {
                                if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                    MySQLToken token = lexer.nextToken();
                                    DDLAlterTableStatement.Algorithm algorithm;
                                    switch (token) {
                                        case KW_DEFAULT:
                                            algorithm = DDLAlterTableStatement.Algorithm.DEFAULT;
                                            stmt.setAlgorithm(algorithm);
                                            lexer.nextToken();
                                            break main_switch;
                                        case IDENTIFIER: {
                                            SpecialIdentifier spi = specialIdentifiers
                                                    .get(lexer.stringValueUppercase());
                                            if (spi != null) {
                                                switch (spi) {
                                                    case INPLACE:
                                                        algorithm =
                                                                DDLAlterTableStatement.Algorithm.INPLACE;
                                                        stmt.setAlgorithm(algorithm);
                                                        lexer.nextToken();
                                                        break main_switch;
                                                    case COPY:
                                                        algorithm =
                                                                DDLAlterTableStatement.Algorithm.COPY;
                                                        stmt.setAlgorithm(algorithm);
                                                        lexer.nextToken();
                                                        break main_switch;
                                                    default:
                                                        throw new SQLSyntaxErrorException(
                                                                "unexpected algorithm type");
                                                }
                                            } else {
                                                throw new SQLSyntaxErrorException(
                                                        "unexpected algorithm type");
                                            }
                                        }
                                        default:
                                            throw new SQLSyntaxErrorException(
                                                    "unexpected algorithm type");
                                    }
                                }
                                break main_switch;
                            }
                            default:
                                throw new SQLSyntaxErrorException("unknown ALTER specification");
                        }
                    } else {
                        throw new SQLSyntaxErrorException("unknown ALTER specification");
                    }
                case KW_FORCE:
                    stmt.setForce(true);
                    lexer.nextToken();
                    break main_switch;
                default:
                    // 允许只有tableOptions的alter，如ALTER TABLE xx default CHARACTER SET latin1
                    if (matchTbOptions && lexer.token() == MySQLToken.EOF) {
                        return stmt;
                    }
                    throw new SQLSyntaxErrorException("unknown ALTER specification");
            }
        }
        return stmt;
    }

    private DDLAlterTableStatement partition(DDLAlterTableStatement stmt)
            throws SQLSyntaxErrorException {
        lexer.nextToken();
        if (lexer.token() == MySQLToken.KW_PARTITION) {
            while (lexer.token() != MySQLToken.EOF) {
                lexer.nextToken();
            }
        }
        return stmt;
    }

    /**
     * 判断建表语句中是否包含 [IF NOT EXISTS] 关键字
     *
     * @return 是否包含 [IF NOT EXISTS] 关键字
     * @throws SQLSyntaxErrorException
     */
    private Boolean ifNotExists() throws SQLSyntaxErrorException {
        boolean ifNotExists = false;
        if (lexer.token() == MySQLToken.KW_IF) {
            lexer.nextToken();
            match(MySQLToken.KW_NOT);
            match(MySQLToken.KW_EXISTS);
            ifNotExists = true;
        }
        return ifNotExists;
    }

    /**
     *
     * @param ifNotExists 是否包含 [IF NOT EXISTS] 关键字
     * @param table 新建表表名
     * @return LIKE建表语句
     * @throws SQLSyntaxErrorException
     */
    private DDLCreateLikeStatement createTableByLike(boolean temporary, boolean ifNotExists,
            Identifier table) throws SQLSyntaxErrorException {
        lexer.nextToken();
        Identifier likeTable = identifier();
        return new DDLCreateLikeStatement(temporary, ifNotExists, table, likeTable);
    }

    /**
     * <code>TABLE</code> has been consumed
     */
    private DDLStatement createTable(boolean temp, boolean ifNotExists, Identifier table)
            throws SQLSyntaxErrorException {
        DDLCreateTableStatement stmt = new DDLCreateTableStatement(temp, ifNotExists, table);
        DDLStatement return_stmt = createTableDefs(stmt);

        if (return_stmt instanceof DDLCreateLikeStatement) {
            // 如果括号内是LIKE,直接返回 DDLCreateLikeStatement
            stmt = null;
            return return_stmt;
        } else {
            TableOptions options = new TableOptions();
            stmt.setTableOptions(options);
            tableOptions(options);

            DDLCreateTableStatement.SelectOption selectOpt = null;
            switch (lexer.token()) {
                case KW_IGNORE:
                    selectOpt = DDLCreateTableStatement.SelectOption.IGNORED;
                    if (lexer.nextToken() == MySQLToken.KW_AS) {
                        lexer.nextToken();
                    }
                    break;
                case KW_REPLACE:
                    selectOpt = DDLCreateTableStatement.SelectOption.REPLACE;
                    if (lexer.nextToken() == MySQLToken.KW_AS) {
                        lexer.nextToken();
                    }
                    break;
                case KW_AS:
                    lexer.nextToken();
                case KW_SELECT:
                    break;
                case KW_PARTITION:
                    while (lexer.token() != MySQLToken.EOF) {
                        lexer.nextToken();
                    }
                    return stmt;
                case EOF:
                    return stmt;
                default:
                    throw new SQLSyntaxErrorException(
                            "DDL CREATE TABLE statement not end properly");
            }
            DMLQueryStatement select =
                    new MySQLDMLSelectParser(lexer, exprParser).selectUnion(false);
            stmt.setSelect(selectOpt, select);
            match(MySQLToken.EOF);
            return stmt;
        }
    }

    /**
     * 考虑到 | CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name (LIKE old_tbl_name) LIKE关键字在括号内,
     * 因此添加了返回值
     *
     * @param stmt
     * @return DDLCreateLikeStatement 或 DDLCreateTableStatement
     * @throws SQLSyntaxErrorException
     */
    @SuppressWarnings("incomplete-switch")
    private DDLStatement createTableDefs(DDLCreateTableStatement stmt)
            throws SQLSyntaxErrorException {
        match(MySQLToken.PUNC_LEFT_PAREN);

        // | CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name (LIKE old_tbl_name)
        if (lexer.token() == MySQLToken.KW_LIKE) {
            DDLCreateLikeStatement createLikeStatement =
                    createTableByLike(stmt.isTemporary(), stmt.isIfNotExists(), stmt.getTable());
            match(MySQLToken.PUNC_RIGHT_PAREN);
            return createLikeStatement;
        }

        IndexDefinition indexDef = null;
        Identifier id = null;
        Identifier symbol = null;
        boolean needMatchComma = true;
        for (int i = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++i) {
            if (i > 0 && needMatchComma) {
                match(MySQLToken.PUNC_COMMA);
            }
            if (!needMatchComma) {
                needMatchComma = true;
            }
            switch (lexer.token()) {
                case KW_PRIMARY:
                    lexer.nextToken();
                    match(MySQLToken.KW_KEY);
                    indexDef = indexDefinition();
                    indexDef.setSymbol(symbol);
                    stmt.setPrimaryKey(indexDef);
                    break;
                case KW_INDEX:
                case KW_KEY:
                    lexer.nextToken();
                    indexDef = indexDefinition();
                    indexDef.setSymbol(symbol);
                    stmt.addIndex(indexDef.getIndexName(), indexDef);
                    break;
                case KW_UNIQUE:
                    switch (lexer.nextToken()) {
                        case KW_INDEX:
                        case KW_KEY:
                            lexer.nextToken();
                            break;
                    }
                    indexDef = indexDefinition();
                    indexDef.setSymbol(symbol);
                    stmt.addUniqueIndex(indexDef.getIndexName(), indexDef);
                    break;
                case KW_FULLTEXT:
                    switch (lexer.nextToken()) {
                        case KW_INDEX:
                        case KW_KEY:
                            lexer.nextToken();
                            break;
                    }
                    indexDef = indexDefinition();
                    indexDef.setSymbol(symbol);
                    if (indexDef.getIndexType() != null) {
                        throw new SQLSyntaxErrorException(
                                "FULLTEXT INDEX can specify no index_type");
                    }
                    stmt.addFullTextIndex(indexDef.getIndexName(), indexDef);
                    break;
                case KW_SPATIAL:
                    switch (lexer.nextToken()) {
                        case KW_INDEX:
                        case KW_KEY:
                            lexer.nextToken();
                            break;
                    }
                    indexDef = indexDefinition();
                    indexDef.setSymbol(symbol);
                    if (indexDef.getIndexType() != null) {
                        throw new SQLSyntaxErrorException(
                                "SPATIAL INDEX can specify no index_type");
                    }
                    stmt.addSpatialIndex(indexDef.getIndexName(), indexDef);
                    break;
                case KW_CHECK:
                    lexer.nextToken();
                    match(MySQLToken.PUNC_LEFT_PAREN);
                    Expression expr = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                    stmt.addCheck(expr);
                    break;
                case IDENTIFIER:
                    Identifier columnName = identifier();
                    ColumnDefinition columnDef = columnDefinition();
                    stmt.addColumnDefinition(columnName, columnDef);
                    if (columnDef.getSpecialIndex() == SpecialIndex.PRIMARY) {
                        List<IndexColumnName> cols = new ArrayList<>();
                        cols.add(new IndexColumnName(columnName, null, true));
                        stmt.setPrimaryKey(new IndexDefinition(IndexType.BTREE, cols, null));
                    } else if (columnDef.getSpecialIndex() == SpecialIndex.UNIQUE) {
                        stmt.addUniqueIndex(columnName, null);
                    }
                    break;
                case KW_CONSTRAINT:
                    lexer.nextToken();
                    if (lexer.token() == MySQLToken.IDENTIFIER) {
                        symbol = identifier();
                    }
                    needMatchComma = false;
                    break;
                case KW_FOREIGN:
                    if (lexer.nextToken() == MySQLToken.KW_KEY) {
                        lexer.nextToken();
                        if (lexer.token() != MySQLToken.PUNC_LEFT_PAREN) {
                            id = identifier();
                        }
                        ForeignKeyDefinition def = foreignKeyDef(id, false);
                        if (symbol != null) {
                            def.setSymbol(symbol);
                        }
                        stmt.getForeignKeyDefs().add(def);
                    }
                    break;
                default:
                    throw new SQLSyntaxErrorException("unsupportted column definition");
            }
            if (needMatchComma) {
                symbol = null;
            }
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        return stmt;
    }

    public ForeignKeyDefinition foreignKeyDef(Identifier id, boolean idSymbol)
            throws SQLSyntaxErrorException {
        List<IndexColumnName> columns = new ArrayList<>();
        match(MySQLToken.PUNC_LEFT_PAREN);
        for (int n = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++n) {
            if (n > 0)
                match(MySQLToken.PUNC_COMMA);
            IndexColumnName indexColumnName = indexColumnName();
            columns.add(indexColumnName);
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        match(MySQLToken.KW_REFERENCES);
        Identifier referenceTable = identifier();
        match(MySQLToken.PUNC_LEFT_PAREN);
        List<IndexColumnName> referenceColumns = new ArrayList<>();
        for (int n = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++n) {
            if (n > 0)
                match(MySQLToken.PUNC_COMMA);
            IndexColumnName indexColumnName = indexColumnName();
            referenceColumns.add(indexColumnName);
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        if (columns.size() != referenceColumns.size()) {
            String keyName;
            if (id == null) {
                keyName = "foreign key without name";
            } else {
                keyName = id.getIdText();
            }
            throw new SQLSyntaxErrorException("Incorrect foreign key definition for '" + keyName
                    + "': Key reference and table reference don't match");
        }

        ForeignKeyDefinition foreignKey =
                new ForeignKeyDefinition(id, columns, referenceTable, referenceColumns);
        if (idSymbol) {
            foreignKey.setSymbol(id);
        }
        while (lexer.token() != MySQLToken.PUNC_COMMA
                && lexer.token() != MySQLToken.PUNC_RIGHT_PAREN
                && lexer.token() != MySQLToken.EOF) {
            match(MySQLToken.KW_ON);
            switch (lexer.token()) {
                case KW_DELETE:
                    MySQLToken deleteToken = lexer.nextToken();
                    ForeignKeyDefinition.REFERENCE_OPTION delete;
                    switch (deleteToken) {
                        case KW_RESTRICT:
                            delete = ForeignKeyDefinition.REFERENCE_OPTION.RESTRICT;
                            foreignKey.setOnDelete(delete);
                            lexer.nextToken();
                            break;
                        case KW_CASCADE:
                            delete = ForeignKeyDefinition.REFERENCE_OPTION.CASCADE;
                            foreignKey.setOnDelete(delete);
                            lexer.nextToken();
                            break;
                        case KW_SET:
                            if (lexer.nextToken() == MySQLToken.LITERAL_NULL) {
                                delete = ForeignKeyDefinition.REFERENCE_OPTION.SET_NULL;
                                foreignKey.setOnDelete(delete);
                                lexer.nextToken();
                            } else
                                throw new SQLSyntaxErrorException(
                                        "error syntax:SET " + lexer.stringValue());
                            break;
                        case IDENTIFIER: {
                            if (SpecialIdentifier.NO == specialIdentifiers
                                    .get(lexer.stringValueUppercase())) {
                                if (lexer.nextToken() == MySQLToken.IDENTIFIER
                                        && SpecialIdentifier.ACTION == specialIdentifiers
                                                .get(lexer.stringValueUppercase())) {
                                    delete = ForeignKeyDefinition.REFERENCE_OPTION.NO_ACTION;
                                    foreignKey.setOnDelete(delete);
                                    lexer.nextToken();
                                } else {
                                    throw new SQLSyntaxErrorException(
                                            "error syntax:NO " + lexer.stringValue());
                                }
                                break;
                            }
                        }
                        default:
                            throw new SQLSyntaxErrorException("error syntax:ON DELETE");
                    }
                    break;
                case KW_UPDATE:
                    MySQLToken updateToken = lexer.nextToken();
                    // Identifier update;
                    switch (updateToken) {
                        case KW_RESTRICT:
                            delete = ForeignKeyDefinition.REFERENCE_OPTION.RESTRICT;
                            foreignKey.setOnUpdate(delete);
                            lexer.nextToken();
                            break;
                        case KW_CASCADE:
                            delete = ForeignKeyDefinition.REFERENCE_OPTION.CASCADE;
                            foreignKey.setOnUpdate(delete);
                            lexer.nextToken();
                            break;
                        case KW_SET:
                            if (lexer.nextToken() == MySQLToken.LITERAL_NULL) {
                                delete = ForeignKeyDefinition.REFERENCE_OPTION.SET_NULL;
                                foreignKey.setOnUpdate(delete);
                                lexer.nextToken();
                            } else
                                throw new SQLSyntaxErrorException(
                                        "error syntax:SET " + lexer.stringValue());
                            break;
                        case IDENTIFIER: {
                            if (SpecialIdentifier.NO == specialIdentifiers
                                    .get(lexer.stringValueUppercase())) {
                                if (lexer.nextToken() == MySQLToken.IDENTIFIER
                                        && SpecialIdentifier.ACTION == specialIdentifiers
                                                .get(lexer.stringValueUppercase())) {
                                    delete = ForeignKeyDefinition.REFERENCE_OPTION.NO_ACTION;
                                    foreignKey.setOnUpdate(delete);
                                    lexer.nextToken();
                                } else {
                                    throw new SQLSyntaxErrorException(
                                            "error syntax:NO " + lexer.stringValue());
                                }
                                break;
                            }
                        }
                        default:
                            throw new SQLSyntaxErrorException("error syntax:ON UPDATE");
                    }
                    break;
                default:
                    throw new SQLSyntaxErrorException("unexpected reference option");
            }
        }
        return foreignKey;
    }

    // col_name column_definition
    // | [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...)
    // [index_option] ...
    // | {INDEX|KEY} [index_name] [index_type] (index_col_name,...)
    // [index_option] ...
    // | [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY] [index_name] [index_type]
    // (index_col_name,...) [index_option] ...
    // | {FULLTEXT|SPATIAL} [INDEX|KEY] [index_name] (index_col_name,...)
    // [index_option] ...
    // | [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name,...)
    // reference_definition
    // | CHECK (expr)
    private IndexDefinition indexDefinition() throws SQLSyntaxErrorException {
        IndexDefinition.IndexType indexType = null;
        List<IndexColumnName> columns = new ArrayList<IndexColumnName>(1);
        Identifier indexName = null;
        if (lexer.token() == MySQLToken.IDENTIFIER) {
            indexName = identifier();
        }
        if (lexer.token() == MySQLToken.KW_USING) {
            lexer.nextToken();
            int tp = matchToken("BTREE", "HASH");
            indexType = tp == 0 ? IndexDefinition.IndexType.BTREE : IndexDefinition.IndexType.HASH;
        }

        match(MySQLToken.PUNC_LEFT_PAREN);
        for (int i = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++i) {
            if (i > 0)
                match(MySQLToken.PUNC_COMMA);
            IndexColumnName indexColumnName = indexColumnName();
            columns.add(indexColumnName);
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        List<IndexOption> options = indexOptions();
        IndexDefinition indexDefinition = new IndexDefinition(indexType, columns, options);
        if (indexName != null)
            indexDefinition.setIndexName(indexName);
        return indexDefinition;
    }

    @SuppressWarnings("incomplete-switch")
    private List<IndexOption> indexOptions() throws SQLSyntaxErrorException {
        List<IndexOption> list = null;
        for (;;) {
            main_switch: switch (lexer.token()) {
                case KW_USING:
                    lexer.nextToken();
                    IndexOption.IndexType indexType =
                            matchToken("BTREE", "HASH") == 0 ? IndexOption.IndexType.BTREE
                                    : IndexOption.IndexType.HASH;
                    if (list == null) {
                        list = new ArrayList<IndexOption>(1);
                    }
                    list.add(new IndexOption(indexType));
                    break main_switch;
                case KW_WITH:
                    lexer.nextToken();
                    matchIdentifier("PARSER");
                    Identifier id = identifier();
                    if (list == null) {
                        list = new ArrayList<IndexOption>(1);
                    }
                    list.add(new IndexOption(id));
                    break main_switch;
                case IDENTIFIER:
                    SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                    if (si != null) {
                        switch (si) {
                            case KEY_BLOCK_SIZE:
                                lexer.nextToken();
                                if (lexer.token() == MySQLToken.OP_EQUALS) {
                                    lexer.nextToken();
                                }
                                Expression val = exprParser.expression();
                                if (list == null) {
                                    list = new ArrayList<IndexOption>(1);
                                }
                                list.add(new IndexOption(val));
                                break main_switch;
                            case COMMENT:
                                lexer.nextToken();
                                LiteralString string = (LiteralString) exprParser.expression();
                                if (list == null) {
                                    list = new ArrayList<IndexOption>(1);
                                }
                                list.add(new IndexOption(string));
                                break main_switch;
                        }
                    }
                case KW_ON:
                    // [ON DELETE reference_option]
                    // [ON UPDATE reference_option]
                    // reference_option:
                    // RESTRICT | CASCADE | SET NULL | NO ACTION
                    lexer.nextToken();
                    if (lexer.token() == MySQLToken.KW_DELETE
                            || lexer.token() == MySQLToken.KW_UPDATE) {
                        lexer.nextToken();
                        if (lexer.token() == MySQLToken.KW_SET) {
                            lexer.nextToken();
                        }
                        if (lexer.stringValueUppercase().equals("NO")) {
                            lexer.nextToken();
                        }
                        lexer.nextToken();
                    }
                    break main_switch;
                default:
                    return list;
            }
        }
    }

    private IndexColumnName indexColumnName() throws SQLSyntaxErrorException {
        // col_name [(length)] [ASC | DESC]
        Identifier colName = identifier();
        Expression len = null;
        if (lexer.token() == MySQLToken.PUNC_LEFT_PAREN) {
            lexer.nextToken();
            len = exprParser.expression();
            match(MySQLToken.PUNC_RIGHT_PAREN);
        }
        switch (lexer.token()) {
            case KW_ASC:
                lexer.nextToken();
                return new IndexColumnName(colName, len, true);
            case KW_DESC:
                lexer.nextToken();
                return new IndexColumnName(colName, len, false);
            default:
                return new IndexColumnName(colName, len, true);
        }
    }

    // data_type:
    // | DATE
    // | TIME
    // | TIMESTAMP
    // | DATETIME
    // | YEAR

    // | spatial_type
    boolean unsigned = false;
    boolean zerofill = false;
    /** for text only */
    boolean binary = false;

    @SuppressWarnings("incomplete-switch")
    public DataType dataType() throws SQLSyntaxErrorException {
        DataType.DataTypeName typeName = null;
        Expression length = null;
        Expression decimals = null;
        Identifier charSet = null;
        Identifier collation = null;
        List<Expression> collectionVals = null;
        unsigned = false;
        zerofill = false;
        binary = false;
        typeName: switch (lexer.token()) {
            case KW_TINYINT:
                // | TINYINT[(length)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.TINYINT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_SMALLINT:
                // | SMALLINT[(length)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.SMALLINT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_MEDIUMINT:
                // | MEDIUMINT[(length)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.MEDIUMINT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_INT4:
            case KW_INTEGER:
            case KW_INT:
                // | INT[(length)] [UNSIGNED] [ZEROFILL]
                // | INTEGER[(length)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.INT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_BIGINT:
                // | BIGINT[(length)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.BIGINT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_REAL:
                // | REAL[(length,decimals)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.REAL;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_COMMA);
                    decimals = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_DOUBLE:
                // | DOUBLE[(length,decimals)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.DOUBLE;
                lexer.nextToken();
                if (lexer.token() == MySQLToken.KW_PRECISION) {
                    lexer.nextToken();
                }
                if (lexer.token() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_COMMA);
                    decimals = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }

                match(lexer);
                break typeName;
            case KW_FLOAT:
                // | FLOAT[(length,decimals)] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.FLOAT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    if (lexer.token() == MySQLToken.PUNC_COMMA) {
                        lexer.nextToken();
                        decimals = exprParser.expression();
                    }
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_NUMERIC:
            case KW_DECIMAL:
            case KW_DEC:
                // | DECIMAL[(length[,decimals])] [UNSIGNED] [ZEROFILL]
                // | NUMERIC[(length[,decimals])] [UNSIGNED] [ZEROFILL]
                typeName = DataType.DataTypeName.DECIMAL;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    if (lexer.token() == MySQLToken.PUNC_COMMA) {
                        match(MySQLToken.PUNC_COMMA);
                        decimals = exprParser.expression();
                    }
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                match(lexer);
                break typeName;
            case KW_CHAR:
                // | CHAR[(length)] [CHARACTER SET charset_name] [COLLATE
                // collation_name]
                typeName = DataType.DataTypeName.CHAR;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                // if (lexer.token() == MySQLToken.KW_BINARY) {
                // // @ZC.CUI | CHAR[(length)] BINARY
                // typeName = DataType.DataTypeName.BINARY;
                // lexer.nextToken();
                // break typeName;
                // }
                if (lexer.token() == MySQLToken.KW_BINARY) {
                    lexer.nextToken();
                    binary = true;
                }
                if (lexer.token() == MySQLToken.KW_CHARACTER) {
                    lexer.nextToken();
                    match(MySQLToken.KW_SET);
                    charSet = identifier();
                } else if (lexer.token() == MySQLToken.IDENTIFIER
                        && "CHARSET".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    charSet = identifier();
                }
                if (lexer.token() == MySQLToken.KW_COLLATE) {
                    lexer.nextToken();
                    collation = identifier();
                }
                break typeName;
            case KW_VARCHAR:
                // | VARCHAR(length) [CHARACTER SET charset_name] [COLLATE
                // collation_name]
                typeName = DataType.DataTypeName.VARCHAR;
                lexer.nextToken();
                match(MySQLToken.PUNC_LEFT_PAREN);
                length = exprParser.expression();
                match(MySQLToken.PUNC_RIGHT_PAREN);
                // if (lexer.token() == MySQLToken.KW_BINARY) {
                // // @ZC.CUI | VARCHAR(length) BINARY
                // typeName = DataType.DataTypeName.BINARY;
                // lexer.nextToken();
                // break typeName;
                // }
                if (lexer.token() == MySQLToken.KW_BINARY) {
                    lexer.nextToken();
                    binary = true;
                }
                if (lexer.token() == MySQLToken.KW_CHARACTER) {
                    lexer.nextToken();
                    match(MySQLToken.KW_SET);
                    charSet = identifier();
                } else if (lexer.token() == MySQLToken.IDENTIFIER
                        && "CHARSET".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    charSet = identifier();
                }
                if (lexer.token() == MySQLToken.KW_COLLATE) {
                    lexer.nextToken();
                    collation = identifier();
                }
                break typeName;
            case KW_BINARY:
                // | BINARY[(length)]
                typeName = DataType.DataTypeName.BINARY;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                break typeName;
            case KW_VARBINARY:
                // | VARBINARY(length)
                typeName = DataType.DataTypeName.VARBINARY;
                lexer.nextToken();
                match(MySQLToken.PUNC_LEFT_PAREN);
                length = exprParser.expression();
                match(MySQLToken.PUNC_RIGHT_PAREN);
                break typeName;
            case KW_TINYBLOB:
                typeName = DataType.DataTypeName.TINYBLOB;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                break typeName;
            case KW_BLOB:
                typeName = DataType.DataTypeName.BLOB;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                break typeName;
            case KW_MEDIUMBLOB:
                typeName = DataType.DataTypeName.MEDIUMBLOB;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                break typeName;
            case KW_LONGBLOB:
                typeName = DataType.DataTypeName.LONGBLOB;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                break typeName;
            case KW_TINYTEXT:
                // | TINYTEXT [BINARY] [CHARACTER SET charset_name] [COLLATE
                // collation_name]
                typeName = DataType.DataTypeName.TINYTEXT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                if (lexer.token() == MySQLToken.KW_BINARY) {
                    lexer.nextToken();
                    binary = true;
                }
                if (lexer.token() == MySQLToken.KW_CHARACTER) {
                    lexer.nextToken();
                    match(MySQLToken.KW_SET);
                    charSet = identifier();
                } else if (lexer.token() == MySQLToken.IDENTIFIER
                        && "CHARSET".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    charSet = identifier();
                }
                if (lexer.token() == MySQLToken.KW_COLLATE) {
                    lexer.nextToken();
                    collation = identifier();
                }
                break typeName;
            case KW_MEDIUMTEXT:
                // | MEDIUMTEXT [BINARY] [CHARACTER SET charset_name] [COLLATE
                // collation_name]
                typeName = DataType.DataTypeName.MEDIUMTEXT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                if (lexer.token() == MySQLToken.KW_BINARY) {
                    lexer.nextToken();
                    binary = true;
                }
                if (lexer.token() == MySQLToken.KW_CHARACTER) {
                    lexer.nextToken();
                    match(MySQLToken.KW_SET);
                    charSet = identifier();
                } else if (lexer.token() == MySQLToken.IDENTIFIER
                        && "CHARSET".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    charSet = identifier();
                }
                if (lexer.token() == MySQLToken.KW_COLLATE) {
                    lexer.nextToken();
                    collation = identifier();
                }
                break typeName;
            case KW_LONG:
                typeName = DataType.DataTypeName.MEDIUMTEXT;
                if (lexer.nextToken() == MySQLToken.KW_VARCHAR) {
                    lexer.nextToken();
                }
                if (lexer.token() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                if (lexer.token() == MySQLToken.KW_BINARY) {
                    lexer.nextToken();
                    binary = true;
                }
                if (lexer.token() == MySQLToken.KW_CHARACTER) {
                    lexer.nextToken();
                    match(MySQLToken.KW_SET);
                    charSet = identifier();
                } else if (lexer.token() == MySQLToken.IDENTIFIER
                        && "CHARSET".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    charSet = identifier();
                }
                if (lexer.token() == MySQLToken.KW_COLLATE) {
                    lexer.nextToken();
                    collation = identifier();
                }
                break typeName;
            case KW_LONGTEXT:
                // | LONGTEXT [BINARY] [CHARACTER SET charset_name] [COLLATE
                // collation_name]
                typeName = DataType.DataTypeName.LONGTEXT;
                if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                    lexer.nextToken();
                    length = exprParser.expression();
                    match(MySQLToken.PUNC_RIGHT_PAREN);
                }
                if (lexer.token() == MySQLToken.KW_BINARY) {
                    lexer.nextToken();
                    binary = true;
                }
                if (lexer.token() == MySQLToken.KW_CHARACTER) {
                    lexer.nextToken();
                    match(MySQLToken.KW_SET);
                    charSet = identifier();
                } else if (lexer.token() == MySQLToken.IDENTIFIER
                        && "CHARSET".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    charSet = identifier();
                }
                if (lexer.token() == MySQLToken.KW_COLLATE) {
                    lexer.nextToken();
                    collation = identifier();
                }
                break typeName;
            case KW_SET:
                // | SET(value1,value2,value3,...) [CHARACTER SET charset_name]
                // [COLLATE collation_name]
                typeName = DataType.DataTypeName.SET;
                lexer.nextToken();
                match(MySQLToken.PUNC_LEFT_PAREN);
                for (int i = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++i) {
                    if (i > 0)
                        match(MySQLToken.PUNC_COMMA);
                    else
                        collectionVals = new ArrayList<Expression>(2);
                    collectionVals.add(exprParser.expression());
                }
                match(MySQLToken.PUNC_RIGHT_PAREN);
                if (lexer.token() == MySQLToken.KW_CHARACTER) {
                    lexer.nextToken();
                    match(MySQLToken.KW_SET);
                    charSet = identifier();
                } else if (lexer.token() == MySQLToken.IDENTIFIER
                        && "CHARSET".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    charSet = identifier();
                }
                if (lexer.token() == MySQLToken.KW_COLLATE) {
                    lexer.nextToken();
                    collation = identifier();
                }
                break typeName;
            case IDENTIFIER:
                SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                if (si != null) {
                    switch (si) {
                        case SERIAL:
                            typeName = DataType.DataTypeName.SERIAL;
                            lexer.nextToken();
                            break typeName;
                        case BIT:
                            // BIT[(length)]
                            typeName = DataType.DataTypeName.BIT;
                            if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                                lexer.nextToken();
                                length = exprParser.expression();
                                match(MySQLToken.PUNC_RIGHT_PAREN);
                            }
                            break typeName;
                        case BOOL:
                            typeName = DataType.DataTypeName.BOOL;
                            lexer.nextToken();
                            break typeName;
                        case BOOLEAN:
                            typeName = DataType.DataTypeName.BOOLEAN;
                            lexer.nextToken();
                            break typeName;
                        case DATE:
                            typeName = DataType.DataTypeName.DATE;
                            lexer.nextToken();
                            break typeName;
                        case FIXED:
                            typeName = DataType.DataTypeName.FIXED;
                            lexer.nextToken();
                            break typeName;
                        case TIME:
                            typeName = DataType.DataTypeName.TIME;
                            if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                                lexer.nextToken();
                                length = exprParser.expression();
                                match(MySQLToken.PUNC_RIGHT_PAREN);
                            }
                            break typeName;
                        case TIMESTAMP:
                            typeName = DataType.DataTypeName.TIMESTAMP;
                            if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                                lexer.nextToken();
                                length = exprParser.expression();
                                match(MySQLToken.PUNC_RIGHT_PAREN);
                            }
                            break typeName;
                        case DATETIME:
                            typeName = DataType.DataTypeName.DATETIME;
                            if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                                lexer.nextToken();
                                length = exprParser.expression();
                                match(MySQLToken.PUNC_RIGHT_PAREN);
                            }
                            break typeName;
                        case YEAR:
                            typeName = DataType.DataTypeName.YEAR;
                            if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                                lexer.nextToken();
                                length = exprParser.expression();
                                match(MySQLToken.PUNC_RIGHT_PAREN);
                            }
                            break typeName;
                        case TEXT:
                            // | TEXT [BINARY] [CHARACTER SET charset_name] [COLLATE
                            // collation_name]
                            typeName = DataType.DataTypeName.TEXT;
                            if (lexer.nextToken() == MySQLToken.PUNC_LEFT_PAREN) {
                                lexer.nextToken();
                                length = exprParser.expression();
                                match(MySQLToken.PUNC_RIGHT_PAREN);
                            }
                            if (lexer.token() == MySQLToken.KW_BINARY) {
                                lexer.nextToken();
                                binary = true;
                            }
                            if (lexer.token() == MySQLToken.KW_CHARACTER) {
                                lexer.nextToken();
                                match(MySQLToken.KW_SET);
                                charSet = identifier();
                            } else if (lexer.token() == MySQLToken.IDENTIFIER
                                    && "CHARSET".equals(lexer.stringValueUppercase())) {
                                lexer.nextToken();
                                charSet = identifier();
                            }
                            if (lexer.token() == MySQLToken.KW_COLLATE) {
                                lexer.nextToken();
                                collation = identifier();
                            }
                            break typeName;
                        case ENUM:
                            // | ENUM(value1,value2,value3,...) [CHARACTER SET
                            // charset_name] [COLLATE collation_name]
                            typeName = DataType.DataTypeName.ENUM;
                            lexer.nextToken();
                            match(MySQLToken.PUNC_LEFT_PAREN);
                            for (int i = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++i) {
                                if (i > 0)
                                    match(MySQLToken.PUNC_COMMA);
                                else
                                    collectionVals = new ArrayList<Expression>(2);
                                collectionVals.add(exprParser.expression());
                            }
                            match(MySQLToken.PUNC_RIGHT_PAREN);
                            if (lexer.token() == MySQLToken.KW_CHARACTER) {
                                lexer.nextToken();
                                match(MySQLToken.KW_SET);
                                charSet = identifier();
                            } else if (lexer.token() == MySQLToken.IDENTIFIER
                                    && "CHARSET".equals(lexer.stringValueUppercase())) {
                                lexer.nextToken();
                                charSet = identifier();
                            }
                            if (lexer.token() == MySQLToken.KW_COLLATE) {
                                lexer.nextToken();
                                collation = identifier();
                            }
                            break typeName;
                        case GEOMETRY: {
                            typeName = DataType.DataTypeName.GEOMETRY;
                            lexer.nextToken();
                            break typeName;
                        }
                        case POINT: {
                            typeName = DataType.DataTypeName.POINT;
                            lexer.nextToken();
                            break typeName;
                        }
                        case LINESTRING:
                            typeName = DataType.DataTypeName.LINESTRING;
                            lexer.nextToken();
                            break typeName;
                        case POLYGON:
                            typeName = DataType.DataTypeName.POLYGON;
                            lexer.nextToken();
                            break typeName;
                        case MULTIPOINT:
                            typeName = DataType.DataTypeName.MULTIPOINT;
                            lexer.nextToken();
                            break typeName;
                        case MULTILINESTRING:
                            typeName = DataType.DataTypeName.MULTILINESTRING;
                            lexer.nextToken();
                            break typeName;
                        case GEOMETRYCOLLECTION:
                            typeName = DataType.DataTypeName.GEOMETRYCOLLECTION;
                            lexer.nextToken();
                            break typeName;
                        case MULTIPOLYGON:
                            typeName = DataType.DataTypeName.MULTIPOLYGON;
                            lexer.nextToken();
                            break typeName;
                        case JSON:
                            typeName = DataType.DataTypeName.JSON;
                            lexer.nextToken();
                            break typeName;
                    }
                }
            default:
                return null;
        }
        return new DataType(typeName, unsigned, zerofill, binary, length, decimals, charSet,
                collation, collectionVals);
    }

    /**
     * signed,unsigned与zerofill无序,且signed与zerofill可以重复n遍
     */
    private void match(MySQLLexer lexer) throws SQLSyntaxErrorException {
        List<MySQLToken> possibleTokens = new ArrayList<MySQLToken>();
        possibleTokens.add(MySQLToken.KW_UNSIGNED);
        possibleTokens.add(MySQLToken.IDENTIFIER);
        possibleTokens.add(MySQLToken.KW_ZEROFILL);
        while (possibleTokens.contains(lexer.token())) {
            if (lexer.token() == MySQLToken.IDENTIFIER
                    && "SIGNED".equals(lexer.stringValueUppercase())) {
                unsigned = false;
                lexer.nextToken();
            } else if (lexer.token() == MySQLToken.KW_UNSIGNED) {
                unsigned = true;
                lexer.nextToken();
            } else if (lexer.token() == MySQLToken.KW_ZEROFILL) {
                zerofill = true;
                lexer.nextToken();
            } else {
                break;
            }
        }
    }

    // column_definition:
    // data_type [NOT NULL | NULL] [DEFAULT default_value]
    // [AUTO_INCREMENT] [UNIQUE [KEY] | [PRIMARY] KEY]
    // [COMMENT 'string']
    // [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
    // [reference_definition]
    @SuppressWarnings("incomplete-switch")
    private ColumnDefinition columnDefinition() throws SQLSyntaxErrorException {
        DataType dataType = dataType();
        boolean notNull = false;
        Expression defaultVal = null;
        boolean autoIncrement = false;
        ColumnDefinition.SpecialIndex sindex = null;
        ColumnDefinition.ColumnFormat format = null;
        LiteralString comment = null;
        Expression onUpdate = null;
        Expression as = null;
        Storage storage = null;
        Boolean stored = null;
        Boolean virtual = null;
        Map<String, MySQLToken> possibleTokens = new HashMap<>();
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_NOT), MySQLToken.KW_NOT);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.LITERAL_NULL),
                MySQLToken.LITERAL_NULL);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_DEFAULT),
                MySQLToken.KW_DEFAULT);
        possibleTokens.put("AUTO_INCREMENT", MySQLToken.IDENTIFIER);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_ON), MySQLToken.KW_ON);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_UNIQUE), MySQLToken.KW_UNIQUE);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_PRIMARY),
                MySQLToken.KW_PRIMARY);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_KEY), MySQLToken.KW_KEY);
        possibleTokens.put("COMMENT", MySQLToken.IDENTIFIER);
        possibleTokens.put("COLUMN_FORMAT", MySQLToken.IDENTIFIER);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_COLLATE),
                MySQLToken.KW_COLLATE);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_GENERATED),
                MySQLToken.KW_GENERATED);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_AS), MySQLToken.KW_AS);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_VIRTUAL),
                MySQLToken.KW_VIRTUAL);
        possibleTokens.put(MySQLToken.keyWordToString(MySQLToken.KW_STORED), MySQLToken.KW_STORED);
        possibleTokens.put("STORAGE", MySQLToken.IDENTIFIER);

        while (lexer.token() != MySQLToken.PUNC_COMMA) {
            if (possibleTokens.containsValue(lexer.token())) {
                switch (lexer.token()) {
                    case KW_NOT:
                        lexer.nextToken();
                        match(MySQLToken.LITERAL_NULL);
                        notNull = true;
                        break;
                    case LITERAL_NULL:
                        lexer.nextToken();
                        break;
                    case KW_DEFAULT:
                        lexer.nextToken();
                        defaultVal = exprParser.expression();
                        break;
                    case IDENTIFIER:
                        switch (lexer.stringValueUppercase()) {
                            case "AUTO_INCREMENT":
                                lexer.nextToken();
                                autoIncrement = true;
                                break;
                            case "COMMENT":
                                lexer.nextToken();
                                comment = (LiteralString) exprParser.expression();
                                break;
                            case "COLUMN_FORMAT":
                                switch (lexer.nextToken()) {
                                    case KW_DEFAULT:
                                        lexer.nextToken();
                                        format = ColumnDefinition.ColumnFormat.DEFAULT;
                                        break;
                                    case IDENTIFIER:
                                        SpecialIdentifier si = specialIdentifiers
                                                .get(lexer.stringValueUppercase());
                                        if (si != null) {
                                            switch (si) {
                                                case FIXED:
                                                    lexer.nextToken();
                                                    format = ColumnDefinition.ColumnFormat.FIXED;
                                                    break;
                                                case DYNAMIC:
                                                    lexer.nextToken();
                                                    format = ColumnDefinition.ColumnFormat.DYNAMIC;
                                                    break;
                                                default:
                                                    lexer.nextToken();
                                                    break;
                                            }
                                        }
                                }
                                break;
                            case "STORAGE":
                                switch (lexer.nextToken()) {
                                    case KW_DEFAULT:
                                        lexer.nextToken();
                                        storage = ColumnDefinition.Storage.DEFAULT;
                                        break;
                                    case IDENTIFIER:
                                        SpecialIdentifier si = specialIdentifiers
                                                .get(lexer.stringValueUppercase());
                                        if (si != null) {
                                            switch (si) {
                                                case DISK:
                                                    lexer.nextToken();
                                                    storage = ColumnDefinition.Storage.DISK;
                                                    break;
                                                case MEMORY:
                                                    lexer.nextToken();
                                                    storage = ColumnDefinition.Storage.MEMORY;
                                                    break;
                                                default:
                                                    lexer.nextToken();
                                                    break;
                                            }
                                        }
                                }
                                break;
                            default:
                                lexer.nextToken();
                                break;
                        }
                        break;
                    case KW_ON:
                        if (lexer.nextToken() == MySQLToken.KW_UPDATE) {
                            lexer.nextToken();
                            onUpdate = exprParser.expression();
                        }
                        break;
                    case KW_UNIQUE:
                        if (lexer.nextToken() == MySQLToken.KW_KEY) {
                            lexer.nextToken();
                        }
                        sindex = ColumnDefinition.SpecialIndex.UNIQUE;
                        break;
                    case KW_PRIMARY:
                        lexer.nextToken();
                        break;
                    case KW_KEY:
                        match(MySQLToken.KW_KEY);
                        sindex = ColumnDefinition.SpecialIndex.PRIMARY;
                        break;
                    case KW_COLLATE:
                        lexer.nextToken();
                        dataType.setCollation(identifier());
                        break;
                    case KW_VIRTUAL:
                        lexer.nextToken();
                        virtual = true;
                        break;
                    case KW_STORED:
                        lexer.nextToken();
                        stored = true;
                        break;
                    case KW_GENERATED:
                        lexer.nextToken();
                        if ("ALWAYS".equals(lexer.stringValueUppercase())) {
                            lexer.nextToken();
                        }
                        break;
                    case KW_AS:
                        lexer.nextToken();
                        as = this.exprParser.expression();
                        break;
                    default:
                        lexer.nextToken();
                        break;
                }
            } else {
                break;
            }
        }

        return new ColumnDefinition(dataType, notNull, defaultVal, autoIncrement, sindex, comment,
                format, onUpdate, storage, virtual, stored, as);
    }

    private boolean tableOptions(TableOptions options) throws SQLSyntaxErrorException {
        boolean matched = false;
        for (int i = 0;; ++i) {
            boolean comma = false;
            if (i > 0 && lexer.token() == MySQLToken.PUNC_COMMA) {
                lexer.nextToken();
                comma = true;
            }
            if (!tableOption(options)) {
                if (comma) {
                    lexer.addCacheToke(MySQLToken.PUNC_COMMA);
                }
                break;
            } else {
                matched = true;
            }
        }
        return matched;
    }

    @SuppressWarnings("incomplete-switch")
    private boolean tableOption(TableOptions options) throws SQLSyntaxErrorException {
        Identifier id = null;
        Expression expr = null;
        os: switch (lexer.token()) {
            case KW_CHARACTER:
                lexer.nextToken();
                match(MySQLToken.KW_SET);
                if (lexer.token() == MySQLToken.OP_EQUALS) {
                    lexer.nextToken();
                }
                id = identifier();
                options.setCharSet(id);
                break;
            case KW_COLLATE:
                lexer.nextToken();
                if (lexer.token() == MySQLToken.OP_EQUALS) {
                    lexer.nextToken();
                }
                id = identifier();
                options.setCollation(id);
                break;
            case KW_DEFAULT:
                // | [DEFAULT] CHARSET [=] charset_name { MySQL 5.1 legacy}
                // | [DEFAULT] CHARACTER SET [=] charset_name
                // | [DEFAULT] COLLATE [=] collation_name
                switch (lexer.nextToken()) {
                    case KW_CHARACTER:
                        lexer.nextToken();
                        match(MySQLToken.KW_SET);
                        if (lexer.token() == MySQLToken.OP_EQUALS) {
                            lexer.nextToken();
                        }
                        id = identifier();
                        options.setCharSet(id);
                        break os;
                    case KW_COLLATE:
                        lexer.nextToken();
                        if (lexer.token() == MySQLToken.OP_EQUALS) {
                            lexer.nextToken();
                        }
                        id = identifier();
                        options.setCollation(id);
                        break os;
                    case IDENTIFIER:
                        SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                        if (si != null) {
                            switch (si) {
                                case CHARSET:
                                    lexer.nextToken();
                                    if (lexer.token() == MySQLToken.OP_EQUALS) {
                                        lexer.nextToken();
                                    }
                                    id = identifier();
                                    options.setCharSet(id);
                                    break os;
                            }
                        }
                    default:
                        lexer.addCacheToke(MySQLToken.KW_DEFAULT);
                        return false;
                }
            case KW_INDEX:
                // | INDEX DIRECTORY [=] 'absolute path to directory'
                lexer.nextToken();
                if (lexer.token() == MySQLToken.IDENTIFIER
                        && "DIRECTORY".equals(lexer.stringValueUppercase())) {
                    if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                        lexer.nextToken();
                    }
                    options.setIndexDir((LiteralString) exprParser.expression());
                    break;
                }
                // lexer.addCacheToke(MySQLToken.KW_INDEX);
                return true;
            case KW_UNION:
                // | UNION [=] (tbl_name[,tbl_name]...)
                if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                    lexer.nextToken();
                }
                match(MySQLToken.PUNC_LEFT_PAREN);
                List<Identifier> union = new ArrayList<Identifier>(2);
                for (int j = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++j) {
                    if (j > 0)
                        match(MySQLToken.PUNC_COMMA);
                    id = identifier();
                    union.add(id);
                }
                match(MySQLToken.PUNC_RIGHT_PAREN);
                options.setUnion(union);
                break os;

            case IDENTIFIER:
                SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                if (si != null) {
                    switch (si) {
                        case CHARSET:
                            // CHARSET [=] charset_name
                            lexer.nextToken();
                            if (lexer.token() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            id = identifier();
                            options.setCharSet(id);
                            break os;
                        case ENGINE:
                            // ENGINE [=] engine_name
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            id = identifier();
                            options.setEngine(id);
                            break os;
                        case AUTO_INCREMENT:
                            // | AUTO_INCREMENT [=] value
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            expr = exprParser.expression();
                            options.setAutoIncrement(expr);
                            break os;
                        case AVG_ROW_LENGTH:
                            // | AVG_ROW_LENGTH [=] value
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            expr = exprParser.expression();
                            options.setAvgRowLength(expr);
                            break os;
                        case CHECKSUM:
                            // | CHECKSUM [=] {0 | 1}
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            switch (lexer.token()) {
                                case LITERAL_BOOL_FALSE:
                                    lexer.nextToken();
                                    options.setCheckSum(false);
                                case LITERAL_BOOL_TRUE:
                                    lexer.nextToken();
                                    options.setCheckSum(true);
                                    break;
                                case LITERAL_NUM_PURE_DIGIT:
                                    int intVal = lexer.integerValue().intValue();
                                    lexer.nextToken();
                                    if (intVal == 0) {
                                        options.setCheckSum(false);
                                    } else {
                                        options.setCheckSum(true);
                                    }
                                    break;
                                default:
                                    throw new SQLSyntaxErrorException(
                                            "table option of CHECKSUM error");
                            }
                            break os;
                        case DELAY_KEY_WRITE:
                            // | DELAY_KEY_WRITE [=] {0 | 1}
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            switch (lexer.token()) {
                                case LITERAL_BOOL_FALSE:
                                    lexer.nextToken();
                                    options.setDelayKeyWrite(false);
                                case LITERAL_BOOL_TRUE:
                                    lexer.nextToken();
                                    options.setDelayKeyWrite(true);
                                    break;
                                case LITERAL_NUM_PURE_DIGIT:
                                    int intVal = lexer.integerValue().intValue();
                                    lexer.nextToken();
                                    if (intVal == 0) {
                                        options.setDelayKeyWrite(false);
                                    } else {
                                        options.setDelayKeyWrite(true);
                                    }
                                    break;
                                default:
                                    throw new SQLSyntaxErrorException(
                                            "table option of DELAY_KEY_WRITE error");
                            }
                            break os;
                        case COMMENT: {
                            // | COMMENT [=] 'string'
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            Expression comment = null;
                            if (lexer.token() == MySQLToken.LITERAL_NCHARS) {
                                ArrayList<Byte> bytes = new ArrayList<>();
                                do {
                                    lexer.appendStringContent(bytes);
                                } while (lexer.nextToken() == MySQLToken.LITERAL_CHARS);
                                byte[] data = new byte[bytes.size()];
                                for (int i = 0, size = bytes.size(); i < size; i++) {
                                    data[i] = bytes.get(i);
                                }
                                comment = new LiteralString(null, data, true)
                                        .setCacheEvalRst(cacheEvalRst);
                            } else if (lexer.token() == MySQLToken.LITERAL_CHARS) {
                                ArrayList<Byte> bytes = new ArrayList<>();
                                do {
                                    lexer.appendStringContent(bytes);
                                } while (lexer.nextToken() == MySQLToken.LITERAL_CHARS);
                                byte[] data = new byte[bytes.size()];
                                for (int i = 0, size = bytes.size(); i < size; i++) {
                                    data[i] = bytes.get(i);
                                }
                                comment = new LiteralString(null, data, false)
                                        .setCacheEvalRst(cacheEvalRst);
                            } else {
                                match(MySQLToken.LITERAL_NCHARS, MySQLToken.LITERAL_CHARS);
                            }
                            options.setComment((LiteralString) comment);
                            break os;
                        }
                        case CONNECTION:
                            // | CONNECTION [=] 'connect_string'
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            options.setConnection((LiteralString) exprParser.expression());
                            break os;
                        case DATA:
                            // | DATA DIRECTORY [=] 'absolute path to directory'
                            lexer.nextToken();
                            matchIdentifier("DIRECTORY");
                            if (lexer.token() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            options.setDataDir((LiteralString) exprParser.expression());
                            break os;
                        case INSERT_METHOD:
                            // | INSERT_METHOD [=] { NO | FIRST | LAST }
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            if (lexer.token() == MySQLToken.IDENTIFIER
                                    && SpecialIdentifier.NO == specialIdentifiers
                                            .get(lexer.stringValueUppercase())) {
                                options.setInsertMethod(TableOptions.InsertMethod.NO);
                                lexer.nextToken();
                            } else {
                                switch (matchIdentifier("FIRST", "LAST")) {
                                    case 0:
                                        options.setInsertMethod(TableOptions.InsertMethod.FIRST);
                                        break;
                                    case 1:
                                        options.setInsertMethod(TableOptions.InsertMethod.LAST);
                                        break;
                                }
                            }
                            break os;
                        case KEY_BLOCK_SIZE:
                            // | KEY_BLOCK_SIZE [=] value
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            options.setKeyBlockSize(exprParser.expression());
                            break os;
                        case MAX_ROWS:
                            // | MAX_ROWS [=] value
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            options.setMaxRows(exprParser.expression());
                            break os;
                        case MIN_ROWS:
                            // | MIN_ROWS [=] value
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            options.setMinRows(exprParser.expression());
                            break os;
                        case PACK_KEYS:
                            // | PACK_KEYS [=] {0 | 1 | DEFAULT}
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            switch (lexer.token()) {
                                case LITERAL_BOOL_FALSE:
                                    lexer.nextToken();
                                    options.setPackKeys(TableOptions.PackKeys.FALSE);
                                    break;
                                case LITERAL_BOOL_TRUE:
                                    lexer.nextToken();
                                    options.setPackKeys(TableOptions.PackKeys.TRUE);
                                    break;
                                case LITERAL_NUM_PURE_DIGIT:
                                    int intVal = lexer.integerValue().intValue();
                                    lexer.nextToken();
                                    if (intVal == 0) {
                                        options.setPackKeys(TableOptions.PackKeys.FALSE);
                                    } else {
                                        options.setPackKeys(TableOptions.PackKeys.TRUE);
                                    }
                                    break;
                                case KW_DEFAULT:
                                    lexer.nextToken();
                                    options.setPackKeys(TableOptions.PackKeys.DEFAULT);
                                    break;
                                default:
                                    throw new SQLSyntaxErrorException(
                                            "table option of PACK_KEYS error");
                            }
                            break os;
                        case PASSWORD:
                            // | PASSWORD [=] 'string'
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            options.setPassword((LiteralString) exprParser.expression());
                            break os;
                        case COMPRESSION:
                            //| COMPRESSION [=] {'ZLIB'|'LZ4'|'NONE'}
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            LiteralString string = (LiteralString) exprParser.expression();
                            if (string != null) {
                                options.setCompression(
                                        Compression.valueOf(string.getUnescapedString(true)));
                            }
                            break os;
                        case ENCRYPTION:
                            //| ENCRYPTION [=] {'Y' | 'N'}
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            string = (LiteralString) exprParser.expression();
                            if (string != null) {
                                String v = string.getUnescapedString(true);
                                if ("Y".equals(v)) {
                                    options.setEncryption(true);
                                } else if ("N".equals(v)) {
                                    options.setEncryption(false);
                                }
                            }
                            break os;
                        case ROW_FORMAT:
                            // | ROW_FORMAT [=]
                            // {DEFAULT|DYNAMIC|FIXED|COMPRESSED|REDUNDANT|COMPACT}
                            if (lexer.nextToken() == MySQLToken.OP_EQUALS) {
                                lexer.nextToken();
                            }
                            switch (lexer.token()) {
                                case KW_DEFAULT:
                                    lexer.nextToken();
                                    options.setRowFormat(TableOptions.RowFormat.DEFAULT);
                                    break os;
                                case IDENTIFIER:
                                    SpecialIdentifier sid =
                                            specialIdentifiers.get(lexer.stringValueUppercase());
                                    if (sid != null) {
                                        switch (sid) {
                                            case DYNAMIC:
                                                lexer.nextToken();
                                                options.setRowFormat(
                                                        TableOptions.RowFormat.DYNAMIC);
                                                break os;
                                            case FIXED:
                                                lexer.nextToken();
                                                options.setRowFormat(TableOptions.RowFormat.FIXED);
                                                break os;
                                            case COMPRESSED:
                                                lexer.nextToken();
                                                options.setRowFormat(
                                                        TableOptions.RowFormat.COMPRESSED);
                                                break os;
                                            case REDUNDANT:
                                                lexer.nextToken();
                                                options.setRowFormat(
                                                        TableOptions.RowFormat.REDUNDANT);
                                                break os;
                                            case COMPACT:
                                                lexer.nextToken();
                                                options.setRowFormat(
                                                        TableOptions.RowFormat.COMPACT);
                                                break os;
                                        }
                                    }
                                default:
                                    throw new SQLSyntaxErrorException(
                                            "table option of ROW_FORMAT error");
                            }
                    }
                }
            default:
                return false;
        }
        return true;
    }

    /**
     * [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name, ...) REFERENCES tbl_name
     * (index_col_name,...) [ON DELETE reference_option] [ON UPDATE reference_option]
     *
     * reference_option: RESTRICT | CASCADE | SET NULL | NO ACTION
     *
     * 生成外键定义
     *
     * @param symbol symbol已解析
     * @return
     * @throws SQLSyntaxErrorException
     */
    private DDLAlterTableStatement.AddForeignKey foreignKeyDefinition(Identifier symbol)
            throws SQLSyntaxErrorException {
        Identifier indexName = null;
        if (lexer.token() == MySQLToken.IDENTIFIER) {
            indexName = identifier();
        }
        List<IndexColumnName> columns = new ArrayList<>();
        match(MySQLToken.PUNC_LEFT_PAREN);
        for (int n = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++n) {
            if (n > 0)
                match(MySQLToken.PUNC_COMMA);
            IndexColumnName indexColumnName = indexColumnName();
            columns.add(indexColumnName);
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        match(MySQLToken.KW_REFERENCES);
        Identifier referenceTable = identifier();
        match(MySQLToken.PUNC_LEFT_PAREN);
        List<IndexColumnName> referenceColumns = new ArrayList<>();
        for (int n = 0; lexer.token() != MySQLToken.PUNC_RIGHT_PAREN; ++n) {
            if (n > 0)
                match(MySQLToken.PUNC_COMMA);
            IndexColumnName indexColumnName = indexColumnName();
            referenceColumns.add(indexColumnName);
        }
        match(MySQLToken.PUNC_RIGHT_PAREN);
        if (columns.size() != referenceColumns.size()) {
            String keyName;
            if (symbol == null) {
                keyName = "foreign key without name";
            } else {
                keyName = symbol.getIdText();
            }
            throw new SQLSyntaxErrorException("Incorrect foreign key definition for '" + keyName
                    + "': Key reference and table reference don't match");
        }

        DDLAlterTableStatement.AddForeignKey addForeignKey =
                new DDLAlterTableStatement.AddForeignKey(indexName, columns, referenceTable,
                        referenceColumns);
        addForeignKey.setSymbol(symbol);
        while (lexer.token() != MySQLToken.EOF) {
            match(MySQLToken.KW_ON);
            switch (lexer.token()) {
                case KW_DELETE:
                    MySQLToken deleteToken = lexer.nextToken();
                    DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION delete;
                    switch (deleteToken) {
                        case KW_RESTRICT:
                            delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.RESTRICT;
                            addForeignKey.setOnDelete(delete);
                            lexer.nextToken();
                            break;
                        case KW_CASCADE:
                            delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.CASCADE;
                            addForeignKey.setOnDelete(delete);
                            lexer.nextToken();
                            break;
                        case KW_SET:
                            if (lexer.nextToken() == MySQLToken.LITERAL_NULL) {
                                delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.SET_NULL;
                                addForeignKey.setOnDelete(delete);
                                lexer.nextToken();
                            } else
                                throw new SQLSyntaxErrorException(
                                        "error syntax:SET " + lexer.stringValue());
                            break;
                        case IDENTIFIER: {
                            if (lexer.nextToken() == MySQLToken.IDENTIFIER
                                    && SpecialIdentifier.ACTION == specialIdentifiers
                                            .get(lexer.stringValueUppercase())) {
                                delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.NO_ACTION;
                                addForeignKey.setOnDelete(delete);
                                lexer.nextToken();

                            } else {
                                throw new SQLSyntaxErrorException(
                                        "error syntax:NO " + lexer.stringValue());
                            }
                            break;
                        }
                        default:
                            throw new SQLSyntaxErrorException("error syntax:ON DELETE");
                    }
                    break;
                case KW_UPDATE:
                    MySQLToken updateToken = lexer.nextToken();
                    // Identifier update;
                    switch (updateToken) {
                        case KW_RESTRICT:
                            delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.RESTRICT;
                            addForeignKey.setOnUpdate(delete);
                            lexer.nextToken();
                            break;
                        case KW_CASCADE:
                            delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.CASCADE;
                            addForeignKey.setOnUpdate(delete);
                            lexer.nextToken();
                            break;
                        case KW_SET:
                            if (lexer.nextToken() == MySQLToken.LITERAL_NULL) {
                                delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.SET_NULL;
                                addForeignKey.setOnUpdate(delete);
                                lexer.nextToken();
                            } else
                                throw new SQLSyntaxErrorException(
                                        "error syntax:SET " + lexer.stringValue());
                            break;
                        case IDENTIFIER: {
                            if (lexer.nextToken() == MySQLToken.IDENTIFIER
                                    && SpecialIdentifier.ACTION == specialIdentifiers
                                            .get(lexer.stringValueUppercase())) {
                                delete = DDLAlterTableStatement.AddForeignKey.REFERENCE_OPTION.NO_ACTION;
                                addForeignKey.setOnUpdate(delete);
                                lexer.nextToken();
                            } else {
                                throw new SQLSyntaxErrorException(
                                        "error syntax:NO " + lexer.stringValue());
                            }
                            break;
                        }
                        default:
                            throw new SQLSyntaxErrorException("error syntax:ON UPDATE");
                    }
                    break;
                default:
                    throw new SQLSyntaxErrorException("unexpected reference option");
            }
        }

        return addForeignKey;
    }

    private OrderBy orderBy() throws SQLSyntaxErrorException {
        if (lexer.token() != MySQLToken.KW_ORDER) {
            return null;
        }
        lexer.nextToken();
        match(MySQLToken.KW_BY);
        Expression expr = exprParser.expression();
        SortOrder order = SortOrder.ASC;
        OrderBy orderBy;
        switch (lexer.token()) {
            case KW_DESC:
                order = SortOrder.DESC;
            case KW_ASC:
                if (lexer.nextToken() != MySQLToken.PUNC_COMMA) {
                    return new OrderBy(expr, order);
                }
            case PUNC_COMMA:
                orderBy = new OrderBy();
                orderBy.addOrderByItem(expr, order);
                break;
            default:
                return new OrderBy(expr, order);
        }
        for (; lexer.token() == MySQLToken.PUNC_COMMA;) {
            lexer.nextToken();
            order = SortOrder.ASC;
            expr = exprParser.expression();
            switch (lexer.token()) {
                case KW_DESC:
                    order = SortOrder.DESC;
                case KW_ASC:
                    lexer.nextToken();
                default:
                    break;
            }
            orderBy.addOrderByItem(expr, order);
        }
        return orderBy;
    }
}
