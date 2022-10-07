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
package miniDB.parser.ast.fragment.ddl;

import miniDB.parser.ast.ASTNode;
import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.literal.LiteralString;
import miniDB.parser.ast.fragment.ddl.datatype.DataType;
import miniDB.parser.visitor.Visitor;

/**
 * (created at 2012-8-13)
 * NOT FULL AST
 * 列定义
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */

public class ColumnDefinition implements ASTNode {
    public enum SpecialIndex {
        PRIMARY, UNIQUE,
    }

    public enum ColumnFormat {
        FIXED, DYNAMIC, DEFAULT,
    }

    public enum Storage {
        DISK, MEMORY, DEFAULT
    }

    private final DataType dataType;
    private final boolean notNull;
    private final Expression defaultVal;
    private final boolean autoIncrement;
    private final SpecialIndex specialIndex;
    private final LiteralString comment;
    private final ColumnFormat columnFormat;
    private final Expression onUpdate;
    private final Storage storage;
    private final Boolean virtual;
    private final Boolean stored;
    private final Expression as;

    /**
     * @param dataType
     * @param notNull
     * @param defaultVal might be null
     * @param autoIncrement
     * @param specialIndex might be null
     * @param comment might be null
     * @param columnFormat might be null
     */
    public ColumnDefinition(DataType dataType, boolean notNull, Expression defaultVal,
            boolean autoIncrement, SpecialIndex specialIndex, LiteralString comment,
            ColumnFormat columnFormat, Expression onUpdate, Storage storage, Boolean virtual,
            Boolean stored, Expression as) {
        if (dataType == null)
            throw new IllegalArgumentException("data type is null");
        this.dataType = dataType;
        this.notNull = notNull;
        this.defaultVal = defaultVal;
        this.autoIncrement = autoIncrement;
        this.specialIndex = specialIndex;
        this.comment = comment;
        this.columnFormat = columnFormat;
        this.onUpdate = onUpdate;
        this.storage = storage;
        this.virtual = virtual;
        this.stored = stored;
        this.as = as;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public Expression getDefaultVal() {
        return defaultVal;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public SpecialIndex getSpecialIndex() {
        return specialIndex;
    }

    public LiteralString getComment() {
        return comment;
    }

    public ColumnFormat getColumnFormat() {
        return columnFormat;
    }

    public Expression getOnUpdate() {
        return onUpdate;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    /** 
     * @return storage 
     */
    public Storage getStorage() {
        return storage;
    }

}
