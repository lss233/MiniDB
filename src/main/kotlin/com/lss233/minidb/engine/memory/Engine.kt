package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.Identifier
import java.util.*
import kotlin.collections.HashMap

object Engine {
    val databases = HashMap<String, Database>()
    operator fun get(identifier: Identifier) : Table? {
        var identifier_ = identifier
        val stack = Stack<Identifier>();
        stack.push(identifier_)
        while(identifier_.parent != null) {
            identifier_ = identifier_.parent
            stack.push(identifier_)
        }
        val db =  if(databases.containsKey(stack.peek().idText)) {
            databases[stack.pop().idText]
        } else {
            databases["pg_catalog"]
        }
        return db?.tables?.get(stack.pop().idText)
    }
    operator fun get(db: Database): Database {
        if(!databases.containsValue(db)) {
            throw RuntimeException("Database ${db.name} does not exist.")
        }
        return db
    }

    fun createDatabase(db: Database): Database {
        val dbName = db.name
        if(databases.containsKey(dbName)) {
            throw RuntimeException("Database $dbName already exists.")
        }
        databases[dbName] = db

        databases["pg_catalog"]!!.tables["pg_database"]!!.insert(
            NTuple.from(
                Cell(Column("oid"), "3"),
                Cell(Column("datname"), db.name),
                Cell(Column("datdba"), "1"),
                Cell(Column("encoding"), "1"),
                Cell(Column("datlocprovider"), 'c'),
                Cell(Column("datistemplate"), "true"),
                Cell(Column("datallowconn"), "true"),
                Cell(Column("datconnlimit"), -1),
                Cell(Column("dattablespace"), "1"),
                Cell(Column("datcollate"), "1"),
                Cell(Column("datctype"), "1"),
                Cell(Column("datacl"), "[]")
            )
        )
        return db
    }
}