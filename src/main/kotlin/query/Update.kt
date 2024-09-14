package query

import java.sql.Connection

/**
 * The `Update` class provides a fluent API for building SQL UPDATE statements.
 *
 * @property table the table on which the UPDATE statement will be executed
 * @property nullableColumnsArgsValues a list of pairs representing the columns to be updated and their corresponding values
 * @property condition the WHERE clause condition for the UPDATE statement
 */
class Update(private val table: Table) {
    /**
     * Represents a list of pairs containing a column and its nullable value.
     * This list is used in the `Update` class to store the columns to be updated and their corresponding values.
     *
     * The list is initialized as a mutable list of `Pair<Column<*>, Any?>`, where `Column` represents a column in a table,
     * and `Any?` represents the nullable value of the column.
     *
     * Example usage:
     * ```
     * nullableColumnsArgsValues.add(column to value)
     * ```
     */
    private val nullableColumnsArgsValues = mutableListOf<Pair<Column<*>, Any?>>()

    /**
     * Represents the condition for a WHERE clause in a database query.
     * This variable is used to specify the condition for filtering rows in a query based on specific criteria.
     *
     * @property condition the WHERE clause condition expressed as a `Where` instance
     */
    private var condition: Where? = null

    /**
     * Executes an update query on the specified table.
     *
     * @param init The initialization block used to set the columns and values to update.
     * @return An instance of [Update] that allows method chaining.
     */
    fun update(init: (Update) -> Unit): Update {
        return Update(table).apply(init)
    }

    /**
     * Sets the value of a column in the update query.
     *
     * @param column the column to set the value for
     * @param value the value to set for the column
     * @return the updated Update object
     */
    operator fun <T : Any> set(column: Column<*>, value: T?): Update {
        nullableColumnsArgsValues.add(column to value)
        return this
    }

    /**
     * Sets the WHERE clause for the database query by using a DSL-style syntax.
     *
     * @param init a lambda function that takes an instance of the [Where] class and allows you to
     *  construct the WHERE clause by using column comparison operations
     * @return an [Update] object that represents the updated query with the WHERE clause applied
     */
    fun where(init: Where.() -> Unit): Update {
        val where = Where()
        init(where)

        condition = where

        return this
    }

    /**
     * This function generates the SQL string and argument list for an UPDATE query.
     *
     * @return A Pair object containing the SQL string and argument list.
     */
    fun sqlArgs(): Pair<String, List<Any?>> {
        val sql = StringBuilder()

        sql.append("UPDATE ")
        sql.append(table.name())
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

        @Suppress("UNCHECKED_CAST")
        condition?.clausesArgs()?.intoSqlArgs(sql, args as MutableList<Any>)

        return Pair(sql.toString(), args)
    }
}

/**
 * Persists the changes made by the Update object to the database.
 *
 * @param conn The database Connection object.
 * @return A Result object containing Unit if the update operation is successful, or failure with an Exception if it fails.
 */
fun Update.persist(conn: Connection): Result<Unit> {
    return runCatching {
        val (sql, args) = sqlArgs()
        conn.prepareStatement(sql).use { stmt ->
            stmt.setParameters(args)
            stmt.executeUpdate()
        }

        Unit
    }
}

