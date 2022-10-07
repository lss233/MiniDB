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
 * (created at 2011-1-28)
 */
package miniDB.parser.ast.stmt.dml;

import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.fragment.GroupBy;
import miniDB.parser.ast.fragment.Limit;
import miniDB.parser.ast.fragment.OrderBy;
import miniDB.parser.ast.fragment.tableref.TableReferences;
import miniDB.parser.util.Pair;
import miniDB.parser.visitor.Visitor;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DMLSelectStatement extends DMLQueryStatement {
    public static enum SelectDuplicationStrategy {
        /** default */
        ALL, DISTINCT, DISTINCTROW
    }

    public static enum QueryCacheStrategy {
        UNDEF, SQL_CACHE, SQL_NO_CACHE
    }

    public static enum SmallOrBigResult {
        UNDEF, SQL_SMALL_RESULT, SQL_BIG_RESULT
    }

    public static enum LockMode {
        UNDEF, FOR_UPDATE, LOCK_IN_SHARE_MODE
    }

    public static final class SelectOption {
        public int version;// /*!40001 SQL_NO_CACHE */ * 指定最低版本号
        public SelectDuplicationStrategy resultDup = SelectDuplicationStrategy.ALL;
        public boolean highPriority = false;
        public boolean straightJoin = false;
        public SmallOrBigResult resultSize = SmallOrBigResult.UNDEF;
        public boolean sqlBufferResult = false;
        public QueryCacheStrategy queryCache = QueryCacheStrategy.UNDEF;
        public boolean sqlCalcFoundRows = false;
        public LockMode lockMode = LockMode.UNDEF;
        public boolean unknownOption = false;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName()).append('{');
            sb.append("resultDup").append('=').append(resultDup.name());
            sb.append(", ").append("highPriority").append('=').append(highPriority);
            sb.append(", ").append("straightJoin").append('=').append(straightJoin);
            sb.append(", ").append("resultSize").append('=').append(resultSize.name());
            sb.append(", ").append("sqlBufferResult").append('=').append(sqlBufferResult);
            sb.append(", ").append("queryCache").append('=').append(queryCache.name());
            sb.append(", ").append("sqlCalcFoundRows").append('=').append(sqlCalcFoundRows);
            sb.append(", ").append("lockMode").append('=').append(lockMode.name());
            sb.append('}');
            return sb.toString();
        }

        public void merge(SelectOption option) {
            if (version == 0) {
                version = option.version;
            }
            if (resultDup == SelectDuplicationStrategy.ALL) {
                resultDup = option.resultDup;
            }
            highPriority |= option.highPriority;
            sqlBufferResult |= option.sqlBufferResult;
            sqlCalcFoundRows |= option.sqlCalcFoundRows;
            unknownOption |= option.unknownOption;
            if (resultSize == SmallOrBigResult.UNDEF) {
                resultSize = option.resultSize;
            }
            if (queryCache == QueryCacheStrategy.UNDEF) {
                queryCache = option.queryCache;
            }
            if (lockMode == LockMode.UNDEF) {
                lockMode = option.lockMode;
            }
        }
    }

    private final SelectOption option;
    /** string: id | `id` | 'id' */
    private List<Pair<Expression, String>> selectExprList;
    private TableReferences tables;
    private Expression where;
    private GroupBy group;
    private Expression having;
    private OrderBy order;
    private Limit outermostLimit;
    private OrderBy outermostOrderBy;
    private Limit limit;

    @SuppressWarnings("unchecked")
    public DMLSelectStatement(SelectOption option, List<Pair<Expression, String>> selectExprList,
            TableReferences tables, Expression where, GroupBy group, Expression having,
            OrderBy order, Limit limit) {
        if (option == null)
            throw new IllegalArgumentException("argument 'option' is null");
        this.option = option;
        if (selectExprList == null || selectExprList.isEmpty()) {
            this.selectExprList = Collections.emptyList();
        } else {
            this.selectExprList = ensureListType(selectExprList);
        }
        this.tables = tables;
        this.where = where;
        this.group = group;
        this.having = having;
        this.order = order;
        this.limit = limit;
    }

    public SelectOption getOption() {
        return option;
    }

    /**
     * @return never null
     */
    public List<Pair<Expression, String>> getSelectExprList() {
        return selectExprList;
    }

    /**
     * @performance slow
     */
    public List<Expression> getSelectExprListWithoutAlias() {
        if (selectExprList == null || selectExprList.isEmpty())
            return Collections.emptyList();
        List<Expression> list = new ArrayList<Expression>(selectExprList.size());
        for (Pair<Expression, String> p : selectExprList) {
            if (p != null && p.getKey() != null) {
                list.add(p.getKey());
            }
        }
        return list;
    }

    public TableReferences getTables() {
        return tables;
    }

    public void setTables(TableReferences tables) {
        this.tables = tables;
    }

    public Expression getWhere() {
        return where;
    }

    public GroupBy getGroup() {
        return group;
    }

    public Expression getHaving() {
        return having;
    }

    public OrderBy getOrder() {
        return order;
    }

    public void setOrder(OrderBy order) {
        this.order = order;
    }

    public void setGroup(GroupBy group) {
        this.group = group;
    }

    public Limit getLimit() {
        return limit;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public void setSelectExprList(List<Pair<Expression, String>> list) {
        this.selectExprList = list;
    }

    public void setWhere(Expression and) {
        this.where = and;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public Limit getOutermostLimit() {
        return outermostLimit;
    }

    public void setOutermostLimit(Limit outermostLimit) {
        this.outermostLimit = outermostLimit;
    }

    public OrderBy getOutermostOrderBy() {
        return outermostOrderBy;
    }

    public void setOutermostOrderBy(OrderBy outermostOrderBy) {
        this.outermostOrderBy = outermostOrderBy;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

}
