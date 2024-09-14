package query

import java.sql.Connection

/**
 * `Select` construct and executes SQL SELECT statements.
 *
 * @param table The table from which data will be selected.
 */
class Select(private val table: Table) {
    private val selectColumns = mutableListOf<Column<*>>()
    private var argsValues = mutableListOf<Any>()
    private var whereClauses: Where? = null
    private val joins = mutableListOf<Join>()
    private var orderBy: OrderBy? = null
    private var limit: Int? = null
    private var offset: Int? = null

    /**
     * Adds columns to be selected.
     *
     * @param columns Vararg of columns to be selected.
     * @return The current [Select] instance.
     */
    fun select(vararg columns: Column<*>): Select {
        columns.takeIf { it.isNotEmpty() }?.let { selectColumns.addAll(columns) }
            ?: selectColumns.addAll(table.getColumnsList())

        return this
    }

    /**
     * Adds a WHERE clause to the SQL query.
     *
     * @param init A lambda function to configure the WHERE clause.
     * @return The current [Select] instance.
     */
    fun where(init: Where.() -> Unit): Select {
        val where = Where()
        init(where)

        whereClauses = where

        return this
    }

    /**
     * Adds a JOIN clause to the SQL query.
     *
     * @param table The table to join with.
     * @param leftColumn The column from the original table.
     * @param rightColumn The column from the attached table.
     * @param joinType The type of join (default is `LEFT`).
     * @return The current [Select] instance.
     */
    fun join(
        table: Table,
        leftColumn: Column<*>,
        rightColumn: Column<*>,
        joinType: JoinType = JoinType.LEFT,
    ): Select {
        val join = Join(this.table, table, leftColumn, rightColumn, joinType)
        joins.add(join)

        return this
    }

    /**
     * Adds an ORDER BY clause to the SQL query.
     *
     * @param init A lambda function to configure the ORDER BY clause.
     * @return The current [Select] instance.
     */
    fun orderBy(init: OrderBy.() -> Unit): Select {
        val orderBy = OrderBy()
        init(orderBy)
        this.orderBy = orderBy

        return this
    }

    /**
     * Adds a LIMIT clause to the SQL query.
     *
     * @param value The maximum number of rows to return.
     * @return The current Select instance.
     */
    fun limit(value: Int?): Select {
        value?.takeIf { it > 0 }?.let { limit = it }
        return this
    }

    /**
     * Adds an OFFSET clause to the SQL query.
     *
     * @param value The number of rows to skip before starting to return rows.
     * @return The current Select instance.
     */
    fun offset(value: Int?): Select {
        value?.takeIf { it > 0 }?.let { offset = it }
        return this
    }

    /**
     * Adds both LIMIT and OFFSET clauses to the SQL query.
     *
     * @param limit The maximum number of rows to return.
     * @param offset The number of rows to skip before starting to return rows.
     * @return The current Select instance.
     */
    fun pagination(limit: Int?, offset: Int?): Select {
        limit(limit)
        offset(offset)
        return this
    }

    /**
     * Builds the SQL SELECT statement and its arguments list.
     *
     * @return A pair containing the SQL statement and the list of arguments.
     */
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

        whereClauses?.clausesArgs()?.intoSqlArgs(sql, argsValues)

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

/** Represents the result of a SELECT query, returning a single mapped result. */
typealias SelectGetResult<R> = Result<R>

/**
 * Executes the SQL SELECT statement and returns a single result.
 *
 * @param conn The database connection to be used.
 * @param mapper A function to map the result set to the desired type.
 * @return The result of the query, containing the mapped object.
 */
fun <R> Select.get(conn: Connection, mapper: (Row) -> R): Result<R> {
    return runCatching {
        val (sql, args) = sqlArgs()
        val stmt = conn.prepareStatement(sql)
        stmt.setParameters(args)

        val rs = stmt.executeQuery()
        if (!rs.next()) {
            Result.failure<Unit>(NoSuchElementException("No rows found"))
        }
        val row = Row(rs)
        mapper(row)
    }
}

/** Represents the result of a SELECT query, returning a list of mapped results. */
typealias SelectListResult<R> = Result<List<R>>

/**
 * Executes the SQL SELECT statement and returns a list of results.
 *
 * @param conn The database connection to be used.
 * @param mapper A function to map the result set to the desired type.
 * @return The result of the query, containing a list of mapped objects.
 */
fun <R> Select.list(conn: Connection, mapper: (Row) -> R): Result<List<R>> {
    return runCatching {
        val (sql, args) = sqlArgs()
        val stmt = conn.prepareStatement(sql)
        stmt.setParameters(args)

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
