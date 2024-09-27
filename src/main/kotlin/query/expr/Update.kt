package query.expr

import query.Query
import query.exec.Executor
import query.schema.Column
import query.schema.Table
import java.sql.Connection

class Update(private val table: Table) {
    private val nullableColumnsArgsValues = mutableListOf<Pair<Column<*>, Any?>>()
    private var conditionClauses: Where? = null

    fun update(init: (Update) -> Unit): Update {
        return Update(table).apply(init)
    }

    operator fun <T : Any> set(column: Column<*>, value: T?): Update {
        nullableColumnsArgsValues.add(column to value)
        return this
    }

    fun where(init: Where.() -> Unit): Update {
        val where = Where()
        init(where)

        conditionClauses = where

        return this
    }

    fun intoSqlArgs(): Query {
        val sql = StringBuilder()

        sql.append("UPDATE ")
        sql.append(table.tableName)
        sql.append(" SET ")

        var args = mutableListOf<Any?>(nullableColumnsArgsValues.size)
        nullableColumnsArgsValues.forEach { (column, value) ->
            if (value == null) {
                sql.append(column.key())
                sql.append(" = COALESCE(?, ")
                sql.append(column.key())
                sql.append(")")
            } else {
                sql.append(column.key())
                sql.append(" = ?")
            }

            if (nullableColumnsArgsValues.indexOfFirst { it.first == column } <
                nullableColumnsArgsValues.size - 1) {
                sql.append(", ")
            }

            args.add(value)
        }

        conditionClauses?.whereClauses()?.let {
            sql.append(it.first)
            args.addAll(it.second)
        }

        return Query(sql.toString(), args)
    }
}

fun Update.persist(conn: Connection): Result<Unit> = Executor(conn, intoSqlArgs()).exec()
