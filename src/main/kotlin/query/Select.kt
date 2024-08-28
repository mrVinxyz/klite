package query

import java.sql.Connection

class Selector(private val table: Table) {
    private val selectColumns = mutableListOf<Column<*>>()
    private var argsValues = mutableListOf<Any>()
    private var whereClauses: WhereArgs? = null
    private val joins = mutableListOf<Join>()
    private var orderBy: OrderBy? = null
    private var limit: Int? = null
    private var offset: Int? = null

    fun select(vararg columns: Column<*>): Selector {
        columns.takeIf { it.isNotEmpty() }?.let { selectColumns.addAll(columns) }
            ?: selectColumns.addAll(table.columns())

        return this
    }

    fun where(init: Where.() -> Unit): Selector {
        val where = Where()
        init(where)

        whereClauses = where.clausesArgs()

        return this
    }

    fun join(
        table: Table,
        leftColumn: Column<*>,
        rightColumn: Column<*>,
        joinType: JoinType = JoinType.INNER
    ): Selector {
        val join = Join(this.table, table, leftColumn, rightColumn, joinType)
        joins.add(join)
        return this
    }

    fun orderBy(init: OrderBy.() -> Unit): Selector {
        val orderBy = OrderBy()
        init(orderBy)

        this.orderBy = orderBy

        return this
    }

    fun limit(value: Int?): Selector {
        value?.takeIf { it > 0 }?.let { limit = it }
        return this
    }

    fun offset(value: Int?): Selector {
        value?.takeIf { it > 0 }?.let { offset = it }
        return this
    }

    fun pagination(limit: Int?, offset: Int?): Selector {
        limit(limit)
        offset(offset)
        return this
    }

    fun sqlArgs(): Pair<String, List<Any>> {
        val sql = StringBuilder()

        sql.append("SELECT ")
        selectColumns.forEachIndexed { index, column ->
            sql.append(column.key())
            if (index < selectColumns.size - 1) sql.append(", ")
        }

        sql.append(" FROM ")
        sql.append(table.name())

        joins.forEach {
            sql.append(" ")
            sql.append(it.toString())
        }

        whereClauses?.let { cond ->
            cond.first
                .takeIf { it.isNotEmpty() }
                ?.let {
                    sql.append(" WHERE ")
                    sql.append(it)
                    argsValues.addAll(cond.second)
                }
        }

        orderBy?.let {
            sql.append(" ORDER BY ")
            sql.append(it.toString())
        }

        limit?.let {
            sql.append(" LIMIT ?")
            argsValues.add(it)
        }

        offset?.let {
            sql.append(" OFFSET ?")
            argsValues.add(it)
        }

        return Pair(sql.toString(), argsValues)
    }
}

fun Table.select(vararg columns: Column<*>): Selector {
    return Selector(this).select(*columns)
}

fun <R> Selector.get(conn: Connection, mapper: (Row) -> R): Result<R> {
    return runCatching {
        val (sql, argsValues) = sqlArgs()
        val stmt = conn.prepareStatement(sql)
        setParameters(stmt, argsValues)

        val rs = stmt.executeQuery()
        if (!rs.next()) {
            Result.failure<Unit>(NoSuchElementException("No rows found"))
        }
        val row = Row(rs)
        mapper(row)
    }
}

fun <R> Selector.list(conn: Connection, mapper: (Row) -> R): Result<List<R>> {
    return runCatching {
        val (sql, argsValues) = sqlArgs()
        val stmt = conn.prepareStatement(sql)
        setParameters(stmt, argsValues)

        val rs = stmt.executeQuery()
        val rows = Rows(rs).iterator()

        val resultList = mutableListOf<R>()
        while (rows.hasNext()) {
            val row = rows.next()
            resultList.add(mapper(row))
        }

        resultList
    }
}
