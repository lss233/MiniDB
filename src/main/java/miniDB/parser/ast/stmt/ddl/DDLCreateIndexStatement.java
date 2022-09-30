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
import miniDB.parser.ast.fragment.ddl.index.IndexDefinition;
import miniDB.parser.visitor.Visitor;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DDLCreateIndexStatement implements DDLStatement {

    /** | ALGORITHM [=] {DEFAULT|INPLACE|COPY} @author ZC.CUI */
    public enum Algorithm {
        DEFAULT, INPLACE, COPY
    }

    /** | LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE} @author ZC.CUI*/
    public enum Lock {
        DEFAULT, NONE, SHARED, EXCLUSIVE
    }

    private final Identifier table;
    private final IndexDefinition indexDefinition;
    private final Algorithm algorithm;
    private final Lock lock;

    public DDLCreateIndexStatement(Identifier table, IndexDefinition indexDefinition,
            Algorithm algorithm, Lock lock) {
        this.table = table;
        this.indexDefinition = indexDefinition;
        this.algorithm = algorithm;
        this.lock = lock;
    }

    public Identifier getTable() {
        return table;
    }

    public IndexDefinition getIndexDefinition() {
        return indexDefinition;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public Lock getLock() {
        return lock;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }


}
