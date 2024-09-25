package query.expr

import query.Query
import query.schema.Column
import query.schema.Table
import java.sql.Connection

class Insert(val table: Table) {
    private val insertColumns = mutableListOf<Column<*>>()
    private val args = mutableListOf<Any>()

    fun insert(init: (Insert) -> Unit): Insert {
        return Insert(table).apply(init)
    }

    operator fun <T> set(column: Column<*>, value: T?): Insert {
        value?.let { v ->
            insertColumns.add(column)
            args.add(v)
        }

        return this
    }

    fun insert(vararg column: Column<*>): Insert {
        insertColumns.addAll(column)
        return this
    }

    fun values(vararg value: Any?): Insert {
        val nullColumns = mutableListOf<Column<*>>()

        value.forEachIndexed { index, any ->
            if (any != null) args.add(any) else nullColumns.add(insertColumns[index])
        }

        insertColumns.removeAll(nullColumns)

        return this
    }

    fun values(map: Map<String, Any?>): Insert {
        insertColumns.forEach { column ->
            map[column.key()]?.let { value -> value.let { args.add(value) } }
        }

        return this
    }

    fun intoSqlArgs(): Query {
        val sql = StringBuilder()

        sql.append("INSERT INTO ")
        sql.append(table.tableName)
        sql.append(" (")

        insertColumns.forEachIndexed { index, column ->
            sql.append(column.key())
            if (index < insertColumns.size - 1) sql.append(", ")
        }

        sql.append(") VALUES (")
        sql.append(insertColumns.joinToString(", ") { "?" })
        sql.append(")")

        return Query(sql.toString(), args)
    }
}

fun Insert.persist(conn: Connection): Result<Int> = intoSqlArgs().persist(conn)
