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
package miniDB.parser.ast.fragment.ddl.datatype;

import miniDB.parser.ast.ASTNode;
import miniDB.parser.ast.expression.Expression;
import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.visitor.Visitor;

import java.util.HashMap;
import java.util.List;

/**
 * <code>spatial data type</code> for MyISAM is not supported
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DataType implements ASTNode {
    public static final HashMap<DataTypeName, Byte> mapper = new HashMap<>();

    public enum DataTypeName {
        GEOMETRY, POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, GEOMETRYCOLLECTION, MULTIPOLYGON, BIT, TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, REAL, DOUBLE, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, DATETIME, YEAR, CHAR, VARCHAR, BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, ENUM, SET, BOOL, BOOLEAN, SERIAL, FIXED, JSON
    }

    static {
        mapper.put(DataTypeName.BIGINT, (byte) 3);
        mapper.put(DataTypeName.BINARY, (byte) 254);
        mapper.put(DataTypeName.BIT, (byte) 16);
        mapper.put(DataTypeName.BLOB, (byte) 252);
        mapper.put(DataTypeName.BOOL, (byte) 1);
        mapper.put(DataTypeName.BOOLEAN, (byte) 1);
        mapper.put(DataTypeName.CHAR, (byte) 254);
        mapper.put(DataTypeName.DATE, (byte) 10);
        mapper.put(DataTypeName.DATETIME, (byte) 12);
        mapper.put(DataTypeName.DECIMAL, (byte) 246);
        mapper.put(DataTypeName.DOUBLE, (byte) 4);
        mapper.put(DataTypeName.ENUM, (byte) 247);
        mapper.put(DataTypeName.FIXED, (byte) 246);
        mapper.put(DataTypeName.FLOAT, (byte) 4);
        mapper.put(DataTypeName.GEOMETRY, (byte) 255);
        mapper.put(DataTypeName.GEOMETRYCOLLECTION, (byte) 255);
        mapper.put(DataTypeName.INT, (byte) 3);
        mapper.put(DataTypeName.LINESTRING, (byte) 254);
        mapper.put(DataTypeName.LONGBLOB, (byte) 252);
        mapper.put(DataTypeName.LONGTEXT, (byte) 252);
        mapper.put(DataTypeName.MEDIUMBLOB, (byte) 252);
        mapper.put(DataTypeName.MEDIUMINT, (byte) 3);
        mapper.put(DataTypeName.MEDIUMTEXT, (byte) 252);
        mapper.put(DataTypeName.MULTILINESTRING, (byte) 254);
        mapper.put(DataTypeName.MULTIPOINT, (byte) 255);
        mapper.put(DataTypeName.MULTIPOLYGON, (byte) 255);
        mapper.put(DataTypeName.POINT, (byte) 255);
        mapper.put(DataTypeName.POLYGON, (byte) 255);
        mapper.put(DataTypeName.REAL, (byte) 5);
        mapper.put(DataTypeName.SERIAL, (byte) 3);
        mapper.put(DataTypeName.SET, (byte) 248);
        mapper.put(DataTypeName.SMALLINT, (byte) 2);
        mapper.put(DataTypeName.TEXT, (byte) 252);
        mapper.put(DataTypeName.TIME, (byte) 11);
        mapper.put(DataTypeName.TIMESTAMP, (byte) 7);
        mapper.put(DataTypeName.TINYBLOB, (byte) 252);
        mapper.put(DataTypeName.TINYINT, (byte) 1);
        mapper.put(DataTypeName.TINYTEXT, (byte) 252);
        mapper.put(DataTypeName.VARBINARY, (byte) 253);
        mapper.put(DataTypeName.VARCHAR, (byte) 253);
        mapper.put(DataTypeName.YEAR, (byte) 13);
        mapper.put(DataTypeName.JSON, (byte) 245);
    }

    // BIT[(length)]
    // | TINYINT[(length)] [UNSIGNED] [ZEROFILL]
    // | SMALLINT[(length)] [UNSIGNED] [ZEROFILL]
    // | MEDIUMINT[(length)] [UNSIGNED] [ZEROFILL]
    // | INT[(length)] [UNSIGNED] [ZEROFILL]
    // | INTEGER[(length)] [UNSIGNED] [ZEROFILL]
    // | BIGINT[(length)] [UNSIGNED] [ZEROFILL]
    // | DOUBLE[(length,decimals)] [UNSIGNED] [ZEROFILL]
    // | REAL[(length,decimals)] [UNSIGNED] [ZEROFILL]
    // | FLOAT[(length,decimals)] [UNSIGNED] [ZEROFILL]
    // | DECIMAL[(length[,decimals])] [UNSIGNED] [ZEROFILL]
    // | NUMERIC[(length[,decimals])] [UNSIGNED] [ZEROFILL] 同上
    // | DATE[(length)]
    // | TIME[(length)]
    // | TIMESTAMP[(length)]
    // | DATETIME
    // | YEAR
    // | CHAR[(length)][BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | VARCHAR(length)[BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | BINARY[(length)]
    // | VARBINARY(length)
    // | TINYBLOB[(length)]
    // | BLOB[(length)]
    // | MEDIUMBLOB[(length)]
    // | LONGBLOB[(length)]
    // | TINYTEXT [BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | TEXT [BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | MEDIUMTEXT [BINARY][CHARACTER SET charset_name] [COLLATE
    // collation_name]
    // | LONGTEXT [BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | ENUM(value1,value2,value3,...)[CHARACTER SET charset_name] [COLLATE
    // collation_name]
    // | SET(value1,value2,value3,...)[CHARACTER SET charset_name] [COLLATE
    // collation_name]
    // | spatial_type 不支持

    private final DataTypeName typeName;
    private final boolean unsigned;
    private final boolean zerofill;
    /** for text only */
    private final boolean binary;
    private final Expression length;
    private final Expression decimals;
    private final Identifier charSet;
    private Identifier collation;
    private final List<Expression> collectionVals;

    public DataType(DataTypeName typeName, boolean unsigned, boolean zerofill, boolean binary,
            Expression length, Expression decimals, Identifier charSet, Identifier collation,
            List<Expression> collectionVals) {
        if (typeName == null)
            throw new IllegalArgumentException("typeName is null");
        this.typeName = typeName;
        this.unsigned = unsigned;
        this.zerofill = zerofill;
        this.binary = binary;
        this.length = length;
        this.decimals = decimals;
        this.charSet = charSet;
        this.collation = collation;
        this.collectionVals = collectionVals;
    }

    public DataTypeName getTypeName() {
        return typeName;
    }

    public boolean isUnsigned() {
        return unsigned;
    }

    public boolean isZerofill() {
        return zerofill;
    }

    public boolean isBinary() {
        return binary;
    }

    public Expression getLength() {
        return length;
    }

    public Expression getDecimals() {
        return decimals;
    }

    public Identifier getCharSet() {
        return charSet;
    }

    public Identifier getCollation() {
        return collation;
    }

    public List<Expression> getCollectionVals() {
        return collectionVals;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    public void setCollation(Identifier collation) {
        this.collation = collation;
    }

    public boolean hasCollation() {
        switch (typeName) {
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case ENUM:
            case SET:
                return true;
            default:
                return false;
        }
    }

    public static byte getType(DataTypeName type) {
        if (mapper.containsKey(type)) {
            return mapper.get(type);
        }
        return 6;
    }

    public static boolean isBinary(DataTypeName type) {
        switch (type) {
            case BINARY:
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
            case VARBINARY:
                return true;
            default:
                return false;
        }
    }

    public static boolean isBlobFlag(DataTypeName type) {
        switch (type) {
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                return true;
            default:
                return false;
        }
    }

    /**
     * TODO 补全该方法
     *
     * 为了实现：https://dev.mysql.com/doc/internals/en/com-field-list.html
     * 参考资料：https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html#data-types-storage-reqs-strings
     * 
     */
    public long getMaxColumnSize() {
        switch (typeName) {
            case BIGINT:
                break;
            case BINARY:
                break;
            case BIT:
                break;
            case BLOB:
                break;
            case BOOL:
                break;
            case BOOLEAN:
                break;
            case CHAR:
                break;
            case DATE:
                break;
            case DATETIME:
                break;
            case DECIMAL:
                break;
            case DOUBLE:
                break;
            case ENUM:
                break;
            case FIXED:
                break;
            case FLOAT:
                break;
            case GEOMETRY:
                break;
            case GEOMETRYCOLLECTION:
                break;
            case INT:
                break;
            case JSON:
                break;
            case LINESTRING:
                break;
            case LONGBLOB:
                break;
            case LONGTEXT:
                break;
            case MEDIUMBLOB:
                break;
            case MEDIUMINT:
                break;
            case MEDIUMTEXT:
                break;
            case MULTILINESTRING:
                break;
            case MULTIPOINT:
                break;
            case MULTIPOLYGON:
                break;
            case POINT:
                break;
            case POLYGON:
                break;
            case REAL:
                break;
            case SERIAL:
                break;
            case SET:
                break;
            case SMALLINT:
                break;
            case TEXT:
                break;
            case TIME:
                break;
            case TIMESTAMP:
                break;
            case TINYBLOB:
                break;
            case TINYINT:
                break;
            case TINYTEXT:
                break;
            case VARBINARY:
                break;
            case VARCHAR:
                break;
            case YEAR:
                break;
            default:
                break;
        }
        return 0;
    }
}
