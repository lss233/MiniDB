package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.memory.View
import com.lss233.minidb.engine.schema.Column

/**
 * View in pg_catalog schema
 */
abstract class PostgresCatalogView(val database: Database): View() {

    protected abstract fun getColumns(): MutableList<Column>
    protected abstract fun generateData(): MutableList<Array<Any>>

    override fun getRelation(): Relation
        = Relation(getColumns(), generateData())
}