package query

import java.sql.Connection

class Selector(private val table: Table) {
    private val selectColumns = mutableListOf<Column<*>>()
    private var whereClauses: WhereArgs? = null
    private var args = mutableListOf<Any>()
    private val joins = mutableListOf<Join>()

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
            sql.append(it.joinClauses())
        }

        whereClauses?.first?.let {
            sql.append(" WHERE ")
            sql.append(it)
        }

        whereClauses?.second?.let { args.addAll(it) }

        return Pair(sql.toString(), args)
    }
}

fun <R> Selector.get(conn: Connection, mapper: (Row) -> R): Result<R> {
    return runCatching {
        val (sql, args) = sqlArgs()
        val stmt = conn.prepareStatement(sql)
        setParameters(stmt, args)

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
        val (sql, args) = sqlArgs()
        val stmt = conn.prepareStatement(sql)
        setParameters(stmt, args)

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
