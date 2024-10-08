package query.expr

import query.Query
import query.Row
import query.execMapList
import query.execMapOne
import query.schema.Column
import query.schema.Table
import java.sql.Connection

class Select(private val table: Table) {
    private val selectColumns = mutableListOf<Column<*>>()
    private var args = mutableListOf<Any>()
    private var whereClauses: Where? = null
    private var joinClauses: Join? = null
    private var orderBy: OrderBy? = null
    private var limit: Int? = null
    private var offset: Int? = null

    fun select(vararg columns: Column<*>): Select {
        columns.takeIf { it.isNotEmpty() }?.let { selectColumns.addAll(columns) }
            ?: selectColumns.addAll(table.getColumnsList())

        return this
    }

    fun select(block: Select.() -> Unit): Select {
        val select = Select(table)
        block(select)
        return select
    }

    fun selectPrimary(value: Int?, block: Select.() -> Unit): Select {
        val primaryKey = table.primaryKey<Int>()
        val select = where { primaryKey eq value }
        block(select)
        return select
    }

    operator fun <T : Any> Column<T>.unaryPlus() {
        selectColumns.add(this)
    }

    fun where(init: Where.() -> Unit): Select {
        val where = if (whereClauses == null) Where() else whereClauses!!
        init(where)
        whereClauses = where

        return this
    }

    fun join(init: Join.() -> Unit): Select {
        val join = Join(table)
        init(join)
        joinClauses = join

        return this
    }

    fun orderBy(init: OrderBy.() -> Unit): Select {
        val orderBy = OrderBy()
        init(orderBy)
        this.orderBy = orderBy

        return this
    }

    fun limit(value: Int?): Select {
        value?.takeIf { it > 0 }?.let { limit = it }
        return this
    }

    fun offset(value: Int?): Select {
        value?.takeIf { it > 0 }?.let { offset = it }
        return this
    }

    fun pagination(limit: Int?, offset: Int?): Select {
        limit(limit)
        offset(offset)
        return this
    }

    fun sqlArgs(): Query {
        val sql = StringBuilder()

        sql.append("SELECT ")
        selectColumns.forEachIndexed { index, column ->
            sql.append(column.key())
            if (index < selectColumns.size - 1) sql.append(", ")
        }

        sql.append(" FROM ")
        sql.append(table.tableName)

        joinClauses?.joinClauses()?.let { sql.append(it) }

        whereClauses?.whereClauses()?.let {
            sql.append(it.first)
            args.addAll(it.second)
        }

        orderBy?.let {
            sql.append(" ORDER BY ")
            sql.append(it.toString())
        }

        limit?.let {
            sql.append(" LIMIT ?")
            args.add(it)
        }

        offset?.let {
            sql.append(" OFFSET ?")
            args.add(it)
        }

        return Query(sql.toString(), args)
    }
}

inline fun <reified R> Select.get(
    conn: Connection,
    mapper: (Row) -> R
): Result<R> =
    sqlArgs().execMapOne(conn, mapper)

inline fun <reified R> Select.list(
    conn: Connection,
    mapper: (Row) -> R
): Result<List<R>> =
    sqlArgs().execMapList(conn, mapper)