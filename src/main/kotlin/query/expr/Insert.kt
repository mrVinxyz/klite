package query.expr

import query.Query
import query.execReturnKey
import query.schema.Column
import query.schema.Table
import java.sql.Connection

class Insert(val table: Table) {
    private val insertColumns = mutableListOf<Column<*>>()
    private val args = mutableListOf<Any?>()
    internal var filter: Filter? = null

    fun insert(block: Insert.() -> Unit): Insert = Insert(table).apply(block)

    fun insert(vararg columns: Column<*>): Insert {
        insertColumns.addAll(columns)
        return this
    }

    infix fun <T : Any> Column<T>.to(value: T?) {
        insertColumns.add(this)
        args.add(value)
    }

    fun values(vararg value: Any?): Insert {
        require(value.size == insertColumns.size) { "Number of values must match number of columns" }
        args.addAll(value.toList())
        return this
    }

    fun values(map: Map<String, Any?>): Insert {
        insertColumns.forEach { column ->
            args.add(map[column.key()])
        }
        return this
    }

    fun filter(block: Filter.() -> Unit): Insert {
        filter = Filter(table)
        block(filter!!)
        return this
    }

    fun getFilter() = filter

    fun sqlArgs(): Query {
        val sql = StringBuilder()

        sql.append("INSERT INTO ")
        sql.append(table.tableName)
        sql.append(" (")

        sql.append(insertColumns.joinToString(", ") { it.key() })

        sql.append(") VALUES (")
        sql.append(insertColumns.joinToString(", ") { "?" })
        sql.append(")")

        return Query(sql.toString(), args)
    }
}

fun Insert.persist(conn: Connection): Result<Int> {
    filter?.let { ft ->
        val result = ft.execute(conn)
        if (!result.ok) {
            return Result.failure(error(result.err))
        }
    }

    return sqlArgs().execReturnKey(conn)
}

fun Insert.persistOrThrow(conn: Connection): Int {
    filter?.let { ft ->
        val result = ft.execute(conn)
        if (!result.ok) error(result.err)
    }

    return sqlArgs().execReturnKey(conn).getOrThrow()
}