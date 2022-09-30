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
 * (created at 2012-8-13)
 */
package miniDB.parser.ast.fragment.ddl.index;

import miniDB.parser.ast.ASTNode;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.visitor.Visitor;

import java.util.Collections;
import java.util.List;

/**
 * 索引定义类
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class IndexDefinition implements ASTNode {

    /**
     * 索引类型
     */
    public enum IndexType {
        BTREE, HASH
    }

    /**
     * 键类型
     */
    public enum KeyType {
        PRIMARY, UNIQUE, KEY, FULLTEXT, SPATIAL
    }

    /**
     * 形如: | [CONSTRAINT [symbol]] PRIMARY KEY [index_type]
     * 有一个 symbol 一个 name
     */
    private Identifier indexName = null;
    private final IndexType indexType;
    private final List<IndexColumnName> columns;
    private final List<IndexOption> options;
    private Identifier symbol;

    @SuppressWarnings("unchecked")
    public IndexDefinition(IndexType indexType, List<IndexColumnName> columns,
            List<IndexOption> options) {
        this.indexType = indexType;
        if (columns == null || columns.isEmpty())
            throw new IllegalArgumentException("columns is null or empty");
        this.columns = columns;
        this.options =
                (List<IndexOption>) (options == null || options.isEmpty() ? Collections.emptyList()
                        : options);
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public void setIndexName(Identifier indexName) {
        this.indexName = indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * @return never null
     */
    public List<IndexColumnName> getColumns() {
        return columns;
    }

    /**
     * @return never null
     */
    public List<IndexOption> getOptions() {
        return options;
    }

    public Identifier getSymbol() {
        return symbol;
    }

    public void setSymbol(Identifier symbol) {
        this.symbol = symbol;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return "IndexDefinition:" + indexName + indexType;
    }
}
