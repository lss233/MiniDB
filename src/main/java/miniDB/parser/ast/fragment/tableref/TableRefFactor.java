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
 * (created at 2011-2-9)
 */
package miniDB.parser.ast.fragment.tableref;

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.ast.expression.primary.ParamMarker;
import miniDB.parser.visitor.Visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class TableRefFactor extends AliasableTableReference {
    /** e.g. <code>"`db2`.`tb1`"</code> is possible */
    private Identifier table;
    private ParamMarker marker;
    private final List<IndexHint> hintList;
    private List<Identifier> partitions;

    public TableRefFactor(Identifier table, String alias, List<IndexHint> hintList) {
        super(alias);
        this.table = table;
        if (hintList == null || hintList.isEmpty()) {
            this.hintList = Collections.emptyList();
        } else if (hintList instanceof ArrayList) {
            this.hintList = hintList;
        } else {
            this.hintList = new ArrayList<IndexHint>(hintList);
        }
        this.partitions = null;
    }

    public TableRefFactor(Identifier table, String alias, List<IndexHint> hintList,
            List<Identifier> partitions) {
        this(table, alias, hintList);
        this.partitions = partitions;
    }

    public TableRefFactor(Identifier table, List<IndexHint> hintList) {
        this(table, null, hintList);
    }

    /**
     * <p>Description: </p>
     * @param createParam
     * @param alias
     * @param hintList2
     * @param partition : 
     */
    public TableRefFactor(ParamMarker paramMarker, String alias, List<IndexHint> hintList,
            List<Identifier> partitions) {
        this(null, alias, hintList);
        this.marker = paramMarker;
        this.partitions = partitions;
    }

    public ParamMarker getParamMarker() {
        return marker;
    }

    public Identifier getTable() {
        return table;
    }

    public List<IndexHint> getHintList() {
        return hintList;
    }

    public List<Identifier> getPartitions() {
        return partitions;
    }

    @Override
    public Object removeLastConditionElement() {
        return null;
    }

    @Override
    public boolean isSingleTable() {
        return true;
    }

    @Override
    public int getPrecedence() {
        return PRECEDENCE_FACTOR;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
