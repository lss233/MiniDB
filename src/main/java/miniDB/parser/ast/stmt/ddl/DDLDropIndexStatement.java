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

import miniDB.parser.ast.expression.primary.Identifier;
import miniDB.parser.visitor.Visitor;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DDLDropIndexStatement implements DDLStatement {

    /** | ALGORITHM [=] {DEFAULT|INPLACE|COPY} @author ZC.CUI */
    public enum Algorithm {
        DEFAULT, INPLACE, COPY
    }

    /** | LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE} @author ZC.CUI*/
    public enum Lock {
        DEFAULT, NONE, SHARED, EXCLUSIVE
    }

    private final Identifier indexName;
    private final Identifier table;
    private Algorithm algorithm;
    private Lock lock;

    public DDLDropIndexStatement(Identifier indexName, Identifier table) {
        this.indexName = indexName;
        this.table = table;
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public Identifier getTable() {
        return table;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
