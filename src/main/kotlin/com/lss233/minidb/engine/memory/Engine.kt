package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.networking.Session
import miniDB.parser.ast.expression.primary.Identifier
import java.util.*
import javax.xml.crypto.Data
import kotlin.collections.HashMap

object Engine {
    private val databases = HashMap<String, Database>()
    val session = ThreadLocal<Session>()
    operator fun get(identifier: Identifier) : Table {
        val db = databases[session.get()?.properties?.get("database") ?: "minidb"] ?: throw RuntimeException("Database not exists.")
        val schema = if(identifier.parent == null) {
            db["pg_catalog"]
        } else {
            db[identifier.parent.idText]
        }
        return schema[identifier.idText]
    }

    operator fun get(dbName: String): Database {
        if(!databases.containsKey(dbName)) {
            throw RuntimeException("Database $dbName does not exist.")
        }
        return databases[dbName]!!
    }

    fun createDatabase(name: String, dba: Int = 10, encoding: Int = 1, locProvider: Char = 'c', allowConn: Boolean = true, connLimit: Int = -1): Database {
        if(databases.containsKey(name)) {
            throw RuntimeException("Database $name already exists.")
        }
        val db = Database(name, dba, encoding, locProvider, allowConn, connLimit)

        databases[name] = db
        // Assign system schema
        db.initSchema()

        // Create db schema
        db.createSchema("public")

        registerDatabase(db)
        return db
    }

    private fun registerDatabase(db: Database) {
        for(item in databases.values) {
            item["pg_catalog"]["pg_database"].insert(
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
        }
    }

}