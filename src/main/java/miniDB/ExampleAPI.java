package miniDB;

import miniDB.parser.ast.expression.primary.RowExpression;
import miniDB.parser.ast.stmt.SQLStatement;
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement;
import miniDB.parser.ast.stmt.dml.DMLInsertStatement;
import miniDB.parser.ast.stmt.dml.DMLSelectStatement;
import miniDB.parser.recognizer.SQLParserDelegate;
import miniDB.parser.recognizer.mysql.lexer.MySQLLexer;
import miniDB.parser.recognizer.mysql.syntax.MySQLDMLSelectParser;
import miniDB.parser.recognizer.mysql.syntax.MySQLExprParser;
import miniDB.parser.visitor.OutputVisitor;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @author <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @version 1.0
 * date 2022/9/30 11:57
 */
public class ExampleAPI {


    /**
     * 创建表操作
     * 数据定义语言（DDL）测试
     * @throws SQLSyntaxErrorException 语法异常
     */
    public void DDLCreateTableTestSQL() throws SQLSyntaxErrorException {
        String sql = "CREATE TABLE `Test` ( Id_P int,LastName varchar(255),FirstName varchar(255),Address varchar(255),City varchar(255))";
        // 通过解析SQL后得到建表的实体类
        DDLCreateTableStatement ast = (DDLCreateTableStatement) SQLParserDelegate.parse(sql);
        System.out.println("解析：创建表:"+ ast.getTable().getIdText());
    }

    /**
     * 插入数据操作
     * 数据操纵语言测试（DML）
     * @throws SQLSyntaxErrorException 语法异常
     */
    public void DMLInsertTestSQL() throws SQLSyntaxErrorException {
        String sql = "INSERT INTO table_name VALUES (value1, value2)";
        SQLStatement ast;
        ast = SQLParserDelegate.parse(sql);
        DMLInsertStatement parsInf = (DMLInsertStatement) (ast);

        System.out.println("插入表：" + parsInf.getTable());
        List<RowExpression> rowList = parsInf.getRowList();
        System.out.println("插入列名：");

        for (RowExpression rowExpression : rowList) {
            System.out.println(rowExpression.getRowExprList());
        }
    }

    public void parseWithoutDelegateClass() throws Exception {
        System.out.println();
        String sql =
                "select t1.name,t2.productid,t3.name from customer t1,orderlist t2,product t3 where t1.id=t2.customerid and t2.productid=t3.id;";
        MySQLLexer lexer = new MySQLLexer(sql);
        MySQLDMLSelectParser parser = new MySQLDMLSelectParser(lexer, new MySQLExprParser(lexer));
        DMLSelectStatement select = parser.select();

        System.out.println("Table is:" + select.getTables().getTableReferenceList());

        StringBuilder sb = new StringBuilder();
        select.accept(new OutputVisitor(sb));
        System.out.println(sb);
    }

    public static void main(String[] args) throws Exception {
        ExampleAPI exampleAPI = new ExampleAPI();
        exampleAPI.parseWithoutDelegateClass();
    }
}
