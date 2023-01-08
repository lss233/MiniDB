package com.lss233.minidb.engine.memory.internal.information

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.memory.View
import com.lss233.minidb.engine.schema.Column

/**
 * View in information_schema schema
 */
abstract class InformationSchemaView: View() {

    protected abstract fun getColumns(): MutableList<Column>
    protected abstract fun generateData(): MutableList<Array<Any>>

    override fun getRelation(): Relation
            = Relation(getColumns(), generateData())
}