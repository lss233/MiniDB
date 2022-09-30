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
 * (created at 2011-8-11)
 */
package miniDB.parser.ast.stmt.ddl;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.expression.primary.literal.LiteralNumber;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.dml.DMLStatement;
import miniDB.parser.visitor.OutputVisitor;
import miniDB.parser.visitor.Visitor;

/**
 * <pre>
 * {EXPLAIN | DESCRIBE | DESC}
 *     tbl_name [col_name | wild]
 * 
 * {EXPLAIN | DESCRIBE | DESC}
 *     [explain_type]
 *     {explainable_stmt | FOR CONNECTION connection_id}
 * 
 * explain_type: {
 *     EXTENDED
 *   | PARTITIONS
 *   | FORMAT = format_name
 * }
 * 
 * format_name: {
 *     TRADITIONAL
 *   | JSON
 * }
 * 
 * explainable_stmt: {
 *     SELECT statement
 *   | DELETE statement
 *   | INSERT statement
 *   | REPLACE statement
 *   | UPDATE statement
 * }
 * </pre>
 */
public class ExplainStatement implements SQLStatement {
    /**
     * {EXPLAIN | DESCRIBE | DESC}
     */
    public enum Commands {
        EXPLAIN, DESCRIBE, DESC;
    }

    /**
     * {EXPLAIN | DESCRIBE | DESC}
     */
    private final Commands command;
    /**
     * tbl_name
     */
    private final Identifier tblName;
    /**
     * col_name
     */
    private final Identifier colName;
    /**
     * col_name wild
     */
    private final String wild;

    /**
     * explain_type: { EXTENDED | PARTITIONS | FORMAT = format_name }
     */
    public enum ExplainType {
        EXTENDED, PARTITIONS, FORMAT;
    }

    /**
     * format_name: { TRADITIONAL | JSON }
     */
    public enum FormatName {
        TRADITIONAL, JSON;
    }

    /**
     * explain_type: { EXTENDED | PARTITIONS | FORMAT = format_name }
     */
    private final ExplainType explainType;
    /**
     * format_name: { TRADITIONAL | JSON }
     */
    private final FormatName formatName;
    /**
     * explainable_stmt: <br>
     * { SELECT statement <br>
     * | DELETE statement <br>
     * | INSERT statement <br>
     * | REPLACE statement <br>
     * | UPDATE statement }
     */
    private final DMLStatement explainableStmt;
    /**
     * FOR CONNECTION connection_id
     */
    private final LiteralNumber connectionId;

    /**
     * {EXPLAIN | DESCRIBE | DESC} tbl_name [col_name | wild] 语句对象
     * 
     * @param tblName
     * @param colName
     * @throws IllegalArgumentException tblName is null
     */
    public ExplainStatement(Commands command, Identifier tblName, Identifier colName, String wild) {
        this.command = command;
        if (tblName == null)
            throw new IllegalArgumentException(
                    "tbl_name is null for {EXPLAIN | DESCRIBE | DESC} tbl_name");
        this.tblName = tblName;
        this.colName = colName;
        this.wild = wild;
        this.explainType = null;
        this.formatName = null;
        this.explainableStmt = null;
        this.connectionId = null;
    }

    /**
     * {EXPLAIN | DESCRIBE | DESC} [explain_type] explainable_stmt
     * 
     * @param explainType
     * @param formatName
     * @param explainableStmt
     * @throws IllegalArgumentException explainableStmt is null
     */
    public ExplainStatement(Commands command, ExplainType explainType, FormatName formatName,
            DMLStatement explainableStmt) {
        this.command = command;
        this.tblName = null;
        this.colName = null;
        this.wild = null;
        this.explainType = explainType;
        if (explainType == ExplainType.FORMAT && null == formatName) {
            throw new IllegalArgumentException(
                    "FORMAT is null for {EXPLAIN | DESCRIBE | DESC} FORMAT");
        }
        this.formatName = formatName;
        if (explainableStmt == null) {
            throw new IllegalArgumentException(
                    "explainable_stmt is null for {EXPLAIN | DESCRIBE | DESC} explainable_stmt");
        }
        this.explainableStmt = explainableStmt;
        this.connectionId = null;
    }

    /**
     * {EXPLAIN | DESCRIBE | DESC} [explain_type] FOR CONNECTION connection_id
     * 
     * @param explainType
     * @param formatName
     * @param connectionId
     * @throws IllegalArgumentException connectionId is null
     */
    public ExplainStatement(Commands command, ExplainType explainType, FormatName formatName,
            LiteralNumber connectionId) {
        this.command = command;
        this.tblName = null;
        this.colName = null;
        this.wild = null;
        this.explainType = explainType;
        if (explainType == ExplainType.FORMAT && null == formatName) {
            throw new IllegalArgumentException(
                    "FORMAT is null for {EXPLAIN | DESCRIBE | DESC} FORMAT");
        }
        this.formatName = formatName;
        this.explainableStmt = null;
        if (connectionId == null) {
            throw new IllegalArgumentException(
                    "connection_id is null for {EXPLAIN | DESCRIBE | DESC} FOR CONNECTION connection_id");
        }
        this.connectionId = connectionId;
    }

    /**
     * @return {@link #tblName}
     */
    public Identifier getTblName() {
        return tblName;
    }

    /**
     * @return {@link #colName}
     */
    public Identifier getColName() {
        return colName;
    }

    /**
     * @return {@link #wild}
     */
    public String getWild() {
        return wild;
    }

    /**
     * @return {@link #explainType}
     */
    public ExplainType getExplainType() {
        return explainType;
    }

    /**
     * @return {@link #formatName}
     */
    public FormatName getFormatName() {
        return formatName;
    }

    /**
     * @return {@link #explainableStmt}
     */
    public DMLStatement getExplainableStmt() {
        return explainableStmt;
    }

    /**
     * @return {@link #connectionId}
     */
    public LiteralNumber getConnectionId() {
        return connectionId;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    /**
     * @return {@link #command}
     */
    public Commands getCommand() {
        return command;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        accept(new OutputVisitor(sb));
        return sb.toString();
    }

}
