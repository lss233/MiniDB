package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column
import kotlin.test.Test


internal class RelationTest {
    private val relation = Relation(
        arrayOf(Column("Id"), Column("Name")),
        arrayOf(
            NTuple.from("1", "Cat"),
            NTuple.from("2", "Fox"),
            NTuple.from("3", "Dog"),
        )
    );
    @Test
    fun select() {
        val a = relation select { row: NTuple, r: Relation ->
            row[0] == "1"
        }
        println(a)

    }

    @Test
    fun projection() {
        val a = relation projection { col: Column, _: Relation ->
            col.name == "Name"
        }
        println(a)
    }

    @Test
    fun join() {
        val joinedRelation = Relation(
            arrayOf(Column("Age"), Column("Sex")),
            arrayOf(
                NTuple.from("22", "Male"),
                NTuple.from("12", "Female"),
                NTuple.from("23", "Male"),
            )
        );
        val a = relation join joinedRelation
        println(a)
    }
    @Test
    fun test_toString() {
        println(relation.toString())
    }
}