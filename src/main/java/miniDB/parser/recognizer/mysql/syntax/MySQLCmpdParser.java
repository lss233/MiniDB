package miniDB.parser.recognizer.mysql.syntax;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.comparison.ComparisionEqualsExpression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.expression.primary.literal.Literal;
import miniDB.parser.ast.fragment.ddl.datatype.DataType;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.compound.BeginEndStatement;
import miniDB.parser.ast.stmt.compound.DeclareStatement;
import miniDB.parser.ast.stmt.compound.condition.*;
import miniDB.parser.ast.stmt.compound.condition.ConditionValue.ConditionValueType;
import miniDB.parser.ast.stmt.compound.condition.DeclareHandlerStatement.HandlerAction;
import miniDB.parser.ast.stmt.compound.condition.GetDiagnosticsStatement.DiagnosticType;
import miniDB.parser.ast.stmt.compound.condition.GetDiagnosticsStatement.StatementInfoItemName;
import miniDB.parser.ast.stmt.compound.condition.SignalStatement.ConditionInfoItemName;
import miniDB.parser.ast.stmt.compound.cursors.CursorCloseStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorDeclareStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorFetchStatement;
import miniDB.parser.ast.stmt.compound.cursors.CursorOpenStatement;
import miniDB.parser.ast.stmt.compound.flowcontrol.*;
import miniDB.parser.recognizer.SQLParserDelegate;
import miniDB.parser.recognizer.mysql.MySQLToken;
import miniDB.parser.recognizer.mysql.lexer.MySQLLexer;
import miniDB.parser.util.Pair;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author liuhuanting
 * @date 2017年11月1日 下午4:53:57
 * 
 */
public class MySQLCmpdParser extends MySQLParser {
    private static enum SpecialIdentifier {
        BEGIN, END, UNTIL, DO, CLASS_ORIGIN, SUBCLASS_ORIGIN, MESSAGE_TEXT, MYSQL_ERRNO, CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, CURSOR_NAME, RETURNED_SQLSTATE
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers =
            new HashMap<String, SpecialIdentifier>();

    static {
        specialIdentifiers.put("BEGIN", SpecialIdentifier.BEGIN);
        specialIdentifiers.put("END", SpecialIdentifier.END);
        specialIdentifiers.put("UNTIL", SpecialIdentifier.UNTIL);
        specialIdentifiers.put("DO", SpecialIdentifier.DO);
        specialIdentifiers.put("CLASS_ORIGIN", SpecialIdentifier.CLASS_ORIGIN);
        specialIdentifiers.put("SUBCLASS_ORIGIN", SpecialIdentifier.SUBCLASS_ORIGIN);
        specialIdentifiers.put("MESSAGE_TEXT", SpecialIdentifier.MESSAGE_TEXT);
        specialIdentifiers.put("MYSQL_ERRNO", SpecialIdentifier.MYSQL_ERRNO);
        specialIdentifiers.put("CONSTRAINT_CATALOG", SpecialIdentifier.CONSTRAINT_CATALOG);
        specialIdentifiers.put("CONSTRAINT_SCHEMA", SpecialIdentifier.CONSTRAINT_SCHEMA);
        specialIdentifiers.put("CONSTRAINT_NAME", SpecialIdentifier.CONSTRAINT_NAME);
        specialIdentifiers.put("CATALOG_NAME", SpecialIdentifier.CATALOG_NAME);
        specialIdentifiers.put("SCHEMA_NAME", SpecialIdentifier.SCHEMA_NAME);
        specialIdentifiers.put("TABLE_NAME", SpecialIdentifier.TABLE_NAME);
        specialIdentifiers.put("COLUMN_NAME", SpecialIdentifier.COLUMN_NAME);
        specialIdentifiers.put("CURSOR_NAME", SpecialIdentifier.CURSOR_NAME);
        specialIdentifiers.put("RETURNED_SQLSTATE", SpecialIdentifier.RETURNED_SQLSTATE);
    }

    private MySQLExprParser exprParser;

    public MySQLCmpdParser(MySQLLexer lexer, MySQLExprParser exprParser) {
        super(lexer);
        this.exprParser = exprParser;
    }

    /**
     * <pre>
     * [begin_label:] BEGIN
     *     [statement_list]
     * END [end_label]
     * </pre>
     */
    public BeginEndStatement beginEnd(Identifier label) throws SQLSyntaxErrorException {
        List<SQLStatement> stmts = new ArrayList<SQLStatement>();
        while (!(lexer.token() == MySQLToken.IDENTIFIER
                && "END".equals(lexer.stringValueUppercase()) && lexer.token() != MySQLToken.EOF)) {
            SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
            stmts.add(stmt);
        }
        lexer.nextToken();
        checkEndLabel(label);
        return new BeginEndStatement(label, stmts);
    }

    /**
    * <pre>
    * IF search_condition THEN statement_list
    *    [ELSEIF search_condition THEN statement_list] ...
    *    [ELSE statement_list]
    * END IF
    * </pre>
    */
    public SQLStatement ifStmt() throws SQLSyntaxErrorException {
        List<Pair<Expression, List<SQLStatement>>> ifStatements = new ArrayList<>();
        match(MySQLToken.KW_IF);
        Expression condition = exprParser.expression();
        match(MySQLToken.KW_THEN);
        List<SQLStatement> list = new ArrayList<>();
        SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
        list.add(stmt);
        while (lexer.token() != MySQLToken.KW_ELSEIF && lexer.token() != MySQLToken.KW_ELSE
                && !("END".equals(lexer.stringValueUppercase()))) {
            stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
            list.add(stmt);
        }
        ifStatements.add(new Pair<Expression, List<SQLStatement>>(condition, list));
        while (lexer.token() == MySQLToken.KW_ELSEIF) {
            lexer.nextToken();
            condition = exprParser.expression();
            list = new ArrayList<>();
            match(MySQLToken.KW_THEN);
            stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
            list.add(stmt);
            while (lexer.token() != MySQLToken.KW_ELSEIF && lexer.token() != MySQLToken.KW_ELSE
                    && !("END".equals(lexer.stringValueUppercase()))) {
                stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
                list.add(stmt);
            }
            ifStatements.add(new Pair<Expression, List<SQLStatement>>(condition, list));
        }
        List<SQLStatement> elseStatements = new ArrayList<>();
        if (lexer.token() == MySQLToken.KW_ELSE) {
            lexer.nextToken();
            elseStatements.add(SQLParserDelegate.parse(lexer, exprParser.getCharset(), true));
            while (!("END".equals(lexer.stringValueUppercase()))) {
                elseStatements.add(SQLParserDelegate.parse(lexer, exprParser.getCharset(), true));
            }
        }
        if (specialIdentifiers.get(lexer.stringValueUppercase()) != SpecialIdentifier.END) {
            throw new SQLSyntaxErrorException("expect END");
        }
        lexer.nextToken();
        match(MySQLToken.KW_IF);
        return new IfStatement(ifStatements, elseStatements);
    }

    /**
     * <pre>
     * [begin_label:] LOOP
     *    statement_list
     * END LOOP [end_label]
     * </pre>
     */
    public SQLStatement loop(Identifier label) throws SQLSyntaxErrorException {
        match(MySQLToken.KW_LOOP);
        SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
        if (specialIdentifiers.get(lexer.stringValueUppercase()) != SpecialIdentifier.END) {
            throw new SQLSyntaxErrorException("expect END");
        }
        lexer.nextToken();
        match(MySQLToken.KW_LOOP);
        checkEndLabel(label);
        return new LoopStatement(label, stmt);
    }

    /**
     * <pre>
     * [begin_label:] REPEAT
     *     statement_list
     * UNTIL search_condition
     * END REPEAT [end_label]
     * </pre>
     */
    public SQLStatement repeat(Identifier label) throws SQLSyntaxErrorException {
        match(MySQLToken.KW_REPEAT);
        SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
        if (specialIdentifiers.get(lexer.stringValueUppercase()) != SpecialIdentifier.UNTIL) {
            throw new SQLSyntaxErrorException("expect UNTIL");
        }
        lexer.nextToken();
        Expression exp = exprParser.expression();
        if (specialIdentifiers.get(lexer.stringValueUppercase()) != SpecialIdentifier.END) {
            throw new SQLSyntaxErrorException("expect END");
        }
        lexer.nextToken();
        match(MySQLToken.KW_REPEAT);
        checkEndLabel(label);
        return new RepeatStatement(label, stmt, exp);
    }


    /**
     * <pre>
     * [begin_label:] WHILE search_condition DO
     *      statement_list
     * END WHILE [end_label]
     * </pre>
     */
    private SQLStatement whileStmt(Identifier label) throws SQLSyntaxErrorException {
        match(MySQLToken.KW_WHILE);
        Expression exp = exprParser.expression();
        if (specialIdentifiers.get(lexer.stringValueUppercase()) != SpecialIdentifier.DO) {
            throw new SQLSyntaxErrorException("expect DO");
        }
        SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
        if (specialIdentifiers.get(lexer.stringValueUppercase()) != SpecialIdentifier.END) {
            throw new SQLSyntaxErrorException("expect END");
        }
        lexer.nextToken();
        match(MySQLToken.KW_WHILE);
        checkEndLabel(label);
        return new WhileStatement(label, stmt, exp);
    }

    /**
     * <pre>
     * CASE case_value
     *     WHEN when_value THEN statement_list
     *     [WHEN when_value THEN statement_list] ...
     *     [ELSE statement_list]
     * END CASE
     * 
     * OR
     * 
     * CASE
     *     WHEN search_condition THEN statement_list
     *     [WHEN search_condition THEN statement_list] ...
     *     [ELSE statement_list]
     * END CASE 
     * </pre>
     */
    public SQLStatement caseStmt() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_CASE);
        Expression caseValue = null;
        if (lexer.token() != MySQLToken.KW_WHEN) {
            caseValue = exprParser.expression();
        }
        List<Pair<Expression, SQLStatement>> whenList = new ArrayList<>();
        while (lexer.token() == MySQLToken.KW_WHEN) {
            lexer.nextToken();
            Expression condition = exprParser.expression();
            match(MySQLToken.KW_THEN);
            SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
            whenList.add(new Pair<Expression, SQLStatement>(condition, stmt));
        }
        SQLStatement elseStatement = null;
        if (lexer.token() == MySQLToken.KW_ELSE) {
            lexer.nextToken();
            elseStatement = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
        }
        if (specialIdentifiers.get(lexer.stringValueUppercase()) != SpecialIdentifier.END) {
            throw new SQLSyntaxErrorException("expect END");
        }
        lexer.nextToken();
        match(MySQLToken.KW_CASE);
        return new CaseStatement(caseValue, whenList, elseStatement);
    }

    public SQLStatement iterate() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_ITERATE);
        Identifier label = identifier();
        return new IterateStatement(label);
    }

    public SQLStatement leave() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_LEAVE);
        Identifier label = identifier();
        return new LeaveStatement(label);
    }

    public SQLStatement returnStmt() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_RETURN);
        Identifier label = identifier();
        return new ReturnStatement(label);
    }

    public SQLStatement open() throws SQLSyntaxErrorException {
        Identifier name = identifier();
        return new CursorOpenStatement(name);
    }

    public SQLStatement close() throws SQLSyntaxErrorException {
        Identifier name = identifier();
        return new CursorCloseStatement(name);
    }

    private void checkEndLabel(Identifier label) throws SQLSyntaxErrorException {
        if (lexer.token() == MySQLToken.IDENTIFIER) {
            Identifier endLabel = identifier();
            if (label != null) {
                if (!label.equals(endLabel)) {
                    throw new SQLSyntaxErrorException(
                            "End-lable " + endLabel.getIdText() + " without match");
                }
            } else {
                throw new SQLSyntaxErrorException(
                        "End-lable " + endLabel.getIdText() + " without match");
            }
        }
    }

    public SQLStatement parseWithIdentifier() throws SQLSyntaxErrorException {
        Identifier label = identifier();
        lexer.nextToken();
        match(MySQLToken.PUNC_COLON);
        switch (lexer.token()) {
            case KW_LOOP:
                return loop(label);
            case KW_REPEAT:
                return repeat(label);
            case KW_WHILE:
                return whileStmt(label);
            case IDENTIFIER:
                SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
                if (si != null) {
                    switch (si) {
                        case BEGIN:
                            return beginEnd(label);
                        default:
                            break;
                    }
                }
            default:
                break;
        }
        return null;
    }

    public SQLStatement declare() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_DECLARE);
        switch (lexer.token()) {
            case KW_CONTINUE:
            case KW_EXIT:
            case KW_UNDO: {
                HandlerAction action =
                        lexer.token() == MySQLToken.KW_CONTINUE ? HandlerAction.CONTINUE
                                : lexer.token() == MySQLToken.KW_EXIT ? HandlerAction.EXIT
                                        : HandlerAction.UNDO;
                lexer.nextToken();
                if (!"HANDLER".equals(lexer.stringValueUppercase())) {
                    throw new SQLSyntaxErrorException("expect HANDLER");
                }
                lexer.nextToken();
                match(MySQLToken.KW_FOR);
                List<ConditionValue> conditions = new ArrayList<>();
                conditions.add(conditionValue());
                while (lexer.token() == MySQLToken.PUNC_COMMA) {
                    conditions.add(conditionValue());
                }
                SQLStatement stmt = SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
                return new DeclareHandlerStatement(action, conditions, stmt);
            }
            case IDENTIFIER: {
                Identifier id = identifier();
                if (lexer.token() == MySQLToken.KW_CONDITION) {
                    lexer.nextToken();
                    match(MySQLToken.KW_FOR);
                    ConditionValue condition = conditionValue();
                    return new DeclareConditionStatement(id, condition);
                } else if (lexer.token() == MySQLToken.KW_CURSOR) {
                    lexer.nextToken();
                    match(MySQLToken.KW_FOR);
                    if (lexer.token() != MySQLToken.KW_SELECT) {
                        throw new SQLSyntaxErrorException("expect SELECT");
                    }
                    SQLStatement stmt =
                            SQLParserDelegate.parse(lexer, exprParser.getCharset(), true);
                    return new CursorDeclareStatement(id, stmt);
                } else {
                    List<Identifier> varNames = new ArrayList<>();
                    varNames.add(id);
                    while (lexer.token() == MySQLToken.PUNC_COMMA) {
                        lexer.nextToken();
                        varNames.add(identifier());
                    }
                    DataType dataType = new MySQLDDLParser(lexer, exprParser).dataType();
                    Expression defaultVal = null;
                    if (lexer.token() == MySQLToken.KW_DEFAULT) {
                        lexer.nextToken();
                        defaultVal = exprParser.expression();
                    }
                    return new DeclareStatement(varNames, dataType, defaultVal);
                }
            }
            default:
                break;
        }
        return null;
    }

    public SQLStatement fetch() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_FETCH);
        if ("NEXT".equals(lexer.stringValueUppercase())) {
            lexer.nextToken();
        }
        if (lexer.token() == MySQLToken.KW_FROM) {
            lexer.nextToken();
        }
        Identifier name = identifier();
        match(MySQLToken.KW_INTO);
        Identifier var = identifier();
        List<Identifier> varNames = new ArrayList<>();
        varNames.add(var);
        while (lexer.token() == MySQLToken.PUNC_COMMA) {
            lexer.nextToken();
            varNames.add(identifier());
        }
        return new CursorFetchStatement(name, varNames);
    }

    private ConditionValue conditionValue() throws SQLSyntaxErrorException {
        switch (lexer.token()) {
            case LITERAL_NUM_PURE_DIGIT: {
                return new ConditionValue(ConditionValueType.ErrorCode, exprParser.expression());
            }
            case KW_SQLSTATE: {
                lexer.nextToken();
                if ("VALUE".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                }
                return new ConditionValue(ConditionValueType.State, exprParser.expression());
            }
            case KW_SQLWARNING: {
                lexer.nextToken();
                return new ConditionValue(ConditionValueType.Warning, true);
            }
            case KW_NOT: {
                lexer.nextToken();
                if ("FOUND".equals(lexer.stringValueUppercase())) {
                    lexer.nextToken();
                    return new ConditionValue(ConditionValueType.NotFound, true);
                } else {
                    throw new SQLSyntaxErrorException("expect FOUND");
                }
            }
            case KW_SQLEXCEPTION: {
                lexer.nextToken();
                return new ConditionValue(ConditionValueType.Exception, true);
            }
            case IDENTIFIER: {
                return new ConditionValue(ConditionValueType.Name, identifier());
            }
            default: {
                throw new SQLSyntaxErrorException("unknown condition_value");
            }
        }
    }

    public SQLStatement signal() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_SIGNAL);
        ConditionValue conditionVal = conditionValue();
        if (lexer.token() == MySQLToken.KW_SET) {
            lexer.nextToken();
            List<Pair<ConditionInfoItemName, Literal>> items = new ArrayList<>();
            if (lexer.token() == MySQLToken.IDENTIFIER) {
                items.add(signalInformationItem());
                while (lexer.token() == MySQLToken.PUNC_COMMA) {
                    items.add(signalInformationItem());
                }
            } else {
                throw new SQLSyntaxErrorException("expect condition_information_item_name");
            }
            return new SignalStatement(conditionVal, items);
        } else {
            return new SignalStatement(conditionVal, null);
        }
    }

    public SQLStatement resignal() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_RESIGNAL);
        ConditionValue conditionVal = conditionValue();
        if (lexer.token() == MySQLToken.KW_SET) {
            lexer.nextToken();
            List<Pair<ConditionInfoItemName, Literal>> items = new ArrayList<>();
            if (lexer.token() == MySQLToken.IDENTIFIER) {
                items.add(signalInformationItem());
                while (lexer.token() == MySQLToken.PUNC_COMMA) {
                    items.add(signalInformationItem());
                }
            } else {
                throw new SQLSyntaxErrorException("expect condition_information_item_name");
            }
            return new ResignalStatement(conditionVal, items);
        } else {
            return new ResignalStatement(conditionVal, null);
        }
    }

    private Pair<ConditionInfoItemName, Literal> signalInformationItem()
            throws SQLSyntaxErrorException {
        SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
        if (si != null) {
            lexer.nextToken();
            match(MySQLToken.OP_EQUALS);
            Expression value = exprParser.expression();
            if (value != null && value instanceof Literal) {
                ConditionInfoItemName item = null;
                switch (si) {
                    case CATALOG_NAME:
                        item = ConditionInfoItemName.CATALOG_NAME;
                        break;
                    case CLASS_ORIGIN:
                        item = ConditionInfoItemName.CLASS_ORIGIN;
                        break;
                    case COLUMN_NAME:
                        item = ConditionInfoItemName.COLUMN_NAME;
                        break;
                    case CONSTRAINT_CATALOG:
                        item = ConditionInfoItemName.CONSTRAINT_CATALOG;
                        break;
                    case CONSTRAINT_NAME:
                        item = ConditionInfoItemName.CONSTRAINT_NAME;
                        break;
                    case CONSTRAINT_SCHEMA:
                        item = ConditionInfoItemName.CONSTRAINT_SCHEMA;
                        break;
                    case CURSOR_NAME:
                        item = ConditionInfoItemName.CURSOR_NAME;
                        break;
                    case MESSAGE_TEXT:
                        item = ConditionInfoItemName.MESSAGE_TEXT;
                        break;
                    case MYSQL_ERRNO:
                        item = ConditionInfoItemName.MYSQL_ERRNO;
                        break;
                    case SCHEMA_NAME:
                        item = ConditionInfoItemName.SCHEMA_NAME;
                        break;
                    case SUBCLASS_ORIGIN:
                        item = ConditionInfoItemName.SUBCLASS_ORIGIN;
                        break;
                    case TABLE_NAME:
                        item = ConditionInfoItemName.TABLE_NAME;
                        break;
                    default:
                        throw new SQLSyntaxErrorException(
                                "unknown condition_information_item_name ");
                }
                return new Pair<ConditionInfoItemName, Literal>(item, (Literal) value);
            } else {
                throw new SQLSyntaxErrorException("expect simple_value_specification");
            }
        } else {
            throw new SQLSyntaxErrorException("expect condition_information_item_name");
        }
    }

    public SQLStatement diagnostics() throws SQLSyntaxErrorException {
        match(MySQLToken.KW_GET);
        String str = lexer.stringValueUppercase();
        DiagnosticType type = null;
        if ("CURRENT".equals(str)) {
            type = DiagnosticType.CURRENT;
            lexer.nextToken();
            if (!"DIAGNOSTICS".equals(lexer.stringValueUppercase())) {
                throw new SQLSyntaxErrorException("unsupported statement");
            } else {
                lexer.nextToken();
            }
        } else if ("STACKED".equals(str)) {
            type = DiagnosticType.STACKED;
            lexer.nextToken();
            if (!"DIAGNOSTICS".equals(lexer.stringValueUppercase())) {
                throw new SQLSyntaxErrorException("unsupported statement");
            } else {
                lexer.nextToken();
            }
        } else if ("DIAGNOSTICS".equals(str)) {
            type = DiagnosticType.NONE;
            lexer.nextToken();
        } else {
            throw new SQLSyntaxErrorException("unsupported statement");
        }
        if (lexer.token() == MySQLToken.KW_CONDITION) {
            lexer.nextToken();
            Expression conditionNumber = exprParser.expression();
            List<Pair<Expression, ConditionInfoItemName>> conditionItems = new ArrayList<>();
            conditionItems.add(conditionItem());
            while (lexer.token() == MySQLToken.PUNC_COMMA) {
                lexer.nextToken();
                conditionItems.add(conditionItem());
            }
            return new GetDiagnosticsStatement(type, null, conditionNumber, conditionItems);
        } else {
            List<Pair<Expression, StatementInfoItemName>> statementItems = new ArrayList<>();
            statementItems.add(statementItem());
            while (lexer.token() == MySQLToken.PUNC_COMMA) {
                statementItems.add(statementItem());
            }
            return new GetDiagnosticsStatement(type, statementItems, null, null);
        }
    }

    private Pair<Expression, StatementInfoItemName> statementItem() throws SQLSyntaxErrorException {
        Expression exp = exprParser.expression();
        if (exp instanceof ComparisionEqualsExpression) {
            Expression left = ((ComparisionEqualsExpression) exp).getLeftOprand();
            Expression right = ((ComparisionEqualsExpression) exp).getRightOprand();
            if (right instanceof Identifier) {
                String str = ((Identifier) right).getIdTextUpUnescape();
                if ("NUMBER".equals(str)) {
                    return new Pair<Expression, StatementInfoItemName>(left,
                            StatementInfoItemName.NUMBER);
                } else if ("ROW_COUNT".equals(str)) {
                    return new Pair<Expression, StatementInfoItemName>(left,
                            StatementInfoItemName.ROW_COUNT);
                }
            }
        }
        throw new SQLSyntaxErrorException("unknown statement_information_item_name ");
    }

    private Pair<Expression, ConditionInfoItemName> conditionItem() throws SQLSyntaxErrorException {
        Expression exp = exprParser.expression();
        if (exp instanceof ComparisionEqualsExpression) {
            Expression left = ((ComparisionEqualsExpression) exp).getLeftOprand();
            Expression right = ((ComparisionEqualsExpression) exp).getRightOprand();
            if (right instanceof Identifier) {
                SpecialIdentifier si =
                        specialIdentifiers.get(((Identifier) right).getIdTextUpUnescape());
                ConditionInfoItemName item = null;
                switch (si) {
                    case CATALOG_NAME:
                        item = ConditionInfoItemName.CATALOG_NAME;
                        break;
                    case CLASS_ORIGIN:
                        item = ConditionInfoItemName.CLASS_ORIGIN;
                        break;
                    case COLUMN_NAME:
                        item = ConditionInfoItemName.COLUMN_NAME;
                        break;
                    case CONSTRAINT_CATALOG:
                        item = ConditionInfoItemName.CONSTRAINT_CATALOG;
                        break;
                    case CONSTRAINT_NAME:
                        item = ConditionInfoItemName.CONSTRAINT_NAME;
                        break;
                    case CONSTRAINT_SCHEMA:
                        item = ConditionInfoItemName.CONSTRAINT_SCHEMA;
                        break;
                    case CURSOR_NAME:
                        item = ConditionInfoItemName.CURSOR_NAME;
                        break;
                    case MESSAGE_TEXT:
                        item = ConditionInfoItemName.MESSAGE_TEXT;
                        break;
                    case MYSQL_ERRNO:
                        item = ConditionInfoItemName.MYSQL_ERRNO;
                        break;
                    case SCHEMA_NAME:
                        item = ConditionInfoItemName.SCHEMA_NAME;
                        break;
                    case SUBCLASS_ORIGIN:
                        item = ConditionInfoItemName.SUBCLASS_ORIGIN;
                        break;
                    case TABLE_NAME:
                        item = ConditionInfoItemName.TABLE_NAME;
                        break;
                    case RETURNED_SQLSTATE:
                        item = ConditionInfoItemName.RETURNED_SQLSTATE;
                        break;
                    default:
                        throw new SQLSyntaxErrorException(
                                "unknown condition_information_item_name ");
                }
                return new Pair<Expression, ConditionInfoItemName>(left, item);
            }
        }
        throw new SQLSyntaxErrorException("unknown condition_information_item_name ");
    }

}
