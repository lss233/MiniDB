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
 * (created at 2011-7-5)
 */
package miniDB.parser.ast.stmt.ddl;

import miniDB.parser.ast.ASTNode;
import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.fragment.ddl.ColumnDefinition;
import miniDB.parser.ast.fragment.ddl.TableOptions;
import miniDB.parser.ast.fragment.ddl.index.IndexColumnName;
import miniDB.parser.ast.fragment.ddl.index.IndexDefinition;
import miniDB.parser.ast.stmt.dml.DMLQueryStatement;
import miniDB.parser.util.Pair;
import miniDB.parser.visitor.Visitor;

import java.util.ArrayList;
import java.util.List;

/**
 * NOT FULL AST: foreign key, ... not supported
 * DDLCreateTableStatement 建表语句对应的实体类
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DDLCreateTableStatement implements DDLStatement {

    /**
     * 查询选项（忽略、替换）
     */
    public enum SelectOption {
        IGNORED, REPLACE
    }

    /**
     * 临时标记
     */
    private final boolean temporary;
    /**
     * 存在标记
     */
    private final boolean ifNotExists;
    /**
     * 表标识
     */
    private final Identifier table;
    /**
     * 列定义
     */
    private final List<Pair<Identifier, ColumnDefinition>> colDefs;
    /**
     * 主键
     */
    private IndexDefinition primaryKey;
    /**
     * 列唯一标记
     */
    private final List<Pair<Identifier, IndexDefinition>> uniqueKeys;
    /**
     * 索引键
     */
    private final List<Pair<Identifier, IndexDefinition>> keys;
    /**
     * 全文索引
     */
    private final List<Pair<Identifier, IndexDefinition>> fullTextKeys;
    /**
     * 空间索引
     */
    private final List<Pair<Identifier, IndexDefinition>> spatialKeys;
    /**
     * 外键
     */
    private final List<Pair<Identifier, IndexDefinition>> foreignKeys;
    /**
     * check约束
     */
    private final List<Expression> checks;
    /**
     * 表选项（引擎、存储空间、字符集……）
     */
    private TableOptions tableOptions;
    /**
     * 查询选项（忽略、替换）
     */
    private Pair<SelectOption, DMLQueryStatement> select;
    /**
     * 连接其他表外键定义
     */
    private final List<ForeignKeyDefinition> foreignKeyDefs;

    public DDLCreateTableStatement(boolean temporary, boolean ifNotExists, Identifier table) {
        this.table = table;
        this.temporary = temporary;
        this.ifNotExists = ifNotExists;
        this.colDefs = new ArrayList<Pair<Identifier, ColumnDefinition>>(4);
        this.uniqueKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
        this.keys = new ArrayList<Pair<Identifier, IndexDefinition>>(2);
        this.fullTextKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
        this.spatialKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
        this.foreignKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
        this.checks = new ArrayList<Expression>(1);
        this.foreignKeyDefs = new ArrayList<>();
    }

    /**
     * 设置表选项
     * @param tableOptions 表选项
     * @return 当前对象
     */
    public DDLCreateTableStatement setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
        return this;
    }

    /**
     * 设置表列定义
     * @param colName 新增的列名
     * @param def 列明对应的定义
     * @return 当前对象
     */
    public DDLCreateTableStatement addColumnDefinition(Identifier colName, ColumnDefinition def) {
        colDefs.add(new Pair<Identifier, ColumnDefinition>(colName, def));
        return this;
    }

    /**
     * 设置主键
     * @param def 主键列
     * @return 当前对象
     */
    public DDLCreateTableStatement setPrimaryKey(IndexDefinition def) {
        primaryKey = def;
        return this;
    }

    /**
     * 添加列唯一定义
     * @param colName 列名
     * @param def 索引定义类型
     * @return 当前对象
     */
    public DDLCreateTableStatement addUniqueIndex(Identifier colName, IndexDefinition def) {
        uniqueKeys.add(new Pair<Identifier, IndexDefinition>(colName, def));
        return this;
    }

    /**
     * 田间索引
     * @param colName 待添加索引的列名
     * @param def 索引类型定义
     * @return 当前对象
     */
    public DDLCreateTableStatement addIndex(Identifier colName, IndexDefinition def) {
        keys.add(new Pair<Identifier, IndexDefinition>(colName, def));
        return this;
    }

    /**
     * 添加全文索引列定义
     * @param colName 列名
     * @param def 索引类型定义
     * @return 当前对象
     */
    public DDLCreateTableStatement addFullTextIndex(Identifier colName, IndexDefinition def) {
        fullTextKeys.add(new Pair<Identifier, IndexDefinition>(colName, def));
        return this;
    }

    /**
     * 添加空间索引列定义
     * @param colName 列名
     * @param def 索引类型定义
     * @return 当前对象
     */
    public DDLCreateTableStatement addSpatialIndex(Identifier colName, IndexDefinition def) {
        spatialKeys.add(new Pair<Identifier, IndexDefinition>(colName, def));
        return this;
    }

    /**
     * 添加外键索引列定义
     * @param colName 列名
     * @param def 索引类型定义
     * @return 当前对象
     */
    // TODO 尚未实现
    public DDLCreateTableStatement addForeignIndex(Identifier colName, IndexDefinition def) {
        foreignKeys.add(new Pair<Identifier, IndexDefinition>(colName, def));
        return this;
    }

    /**
     * 添加约束条件
     * @param check 约束
     * @return 当前对象
     */
    public DDLCreateTableStatement addCheck(Expression check) {
        checks.add(check);
        return this;
    }

    public TableOptions getTableOptions() {
        return tableOptions;
    }

    public Pair<SelectOption, DMLQueryStatement> getSelect() {
        return select;
    }

    public void setSelect(SelectOption option, DMLQueryStatement select) {
        this.select =
                new Pair<DDLCreateTableStatement.SelectOption, DMLQueryStatement>(option, select);
    }

    public boolean isTemporary() {
        return temporary;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Identifier getTable() {
        return table;
    }

    /**
     * @return key := columnName
     */
    public List<Pair<Identifier, ColumnDefinition>> getColDefs() {
        return colDefs;
    }

    public IndexDefinition getPrimaryKey() {
        return primaryKey;
    }

    public List<Pair<Identifier, IndexDefinition>> getUniqueKeys() {
        return uniqueKeys;
    }

    public List<Pair<Identifier, IndexDefinition>> getKeys() {
        return keys;
    }

    public List<Pair<Identifier, IndexDefinition>> getFullTextKeys() {
        return fullTextKeys;
    }

    public List<Pair<Identifier, IndexDefinition>> getSpatialKeys() {
        return spatialKeys;
    }

    public List<Pair<Identifier, IndexDefinition>> getForeignKeys() {
        return foreignKeys;
    }

    public List<Expression> getChecks() {
        return checks;
    }

    public List<ForeignKeyDefinition> getForeignKeyDefs() {
        return foreignKeyDefs;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public static class ForeignKeyDefinition implements ASTNode {
        public enum REFERENCE_OPTION {
            RESTRICT, CASCADE, SET_NULL, NO_ACTION
        }

        private final Identifier indexName;
        private final List<IndexColumnName> columns;
        private final Identifier referenceTable;
        private final List<IndexColumnName> referenceColumns;
        private REFERENCE_OPTION onDelete;
        private REFERENCE_OPTION onUpdate;
        private Identifier symbol;

        public ForeignKeyDefinition(Identifier indexName, List<IndexColumnName> columns,
                Identifier referenceTable, List<IndexColumnName> referenceColumns) {
            this.indexName = indexName;
            this.columns = columns;
            this.referenceTable = referenceTable;
            this.referenceColumns = referenceColumns;
        }

        public REFERENCE_OPTION getOnDelete() {
            return onDelete;
        }

        public void setOnDelete(REFERENCE_OPTION onDelete) {
            this.onDelete = onDelete;
        }

        public REFERENCE_OPTION getOnUpdate() {
            return onUpdate;
        }

        public void setOnUpdate(REFERENCE_OPTION onUpdate) {
            this.onUpdate = onUpdate;
        }

        public Identifier getSymbol() {
            return symbol;
        }

        public void setSymbol(Identifier symbol) {
            this.symbol = symbol;
        }

        public Identifier getIndexName() {
            return indexName;
        }

        public List<IndexColumnName> getColumns() {
            return columns;
        }

        public Identifier getReferenceTable() {
            return referenceTable;
        }

        public List<IndexColumnName> getReferenceColumns() {
            return referenceColumns;
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }
    }
}
