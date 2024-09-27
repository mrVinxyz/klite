package query.expr

import query.Query
import query.exec.Executor
import query.schema.Column
import query.schema.Table
import java.sql.Connection

class Insert(val table: Table) {
    private val insertColumns = mutableListOf<Column<*>>()
    private val args = mutableListOf<Any>()
    var filter: Filter? = null

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

    fun filter(block: Filter.(Table) -> Unit): Insert {
        filter = Filter(table)
        block(filter!!, table)
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

fun Insert.persist(conn: Connection): Result<Int> {
    val passedFilter = filter.takeIf { it != null }?.execAll(conn)?.getOrThrow()

    passedFilter.takeIf { it == false }?.let {
        return Result.success(0)
    }

    return Executor(conn, intoSqlArgs()).execReturn<Int>()
}
