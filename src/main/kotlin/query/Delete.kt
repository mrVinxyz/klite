package query

import java.sql.Connection
import kotlin.use

/**
 * The `Delete` class is responsible for generating DELETE SQL queries for a given `Table` and
 * executing them.
 *
 * @param table The table for which the DELETE query is generated.
 * @constructor Creates a `Delete` instance for the specified `table`.
 * @property table The table for which the DELETE query is generated.
 * @property argsValues A mutable list to store the values of arguments used in the query.
 * @property condition The WHERE clause of the DELETE query.
 */
class Delete(private val table: Table) {
    /** Mutable list to hold values of type Any for SQL query arguments. */
    private val argsValues = mutableListOf<Any>()

    /**
     * Represents a private nullable property that stores the condition for a SQL query. The
     * condition is used in the `deleteWhere` and `sqlArgs` functions of the `Delete` class.
     *
     * @property condition The condition for the SQL query as a `WhereArgs` object.
     * @see Delete
     * @see WhereArgs
     */
    private var condition: WhereArgs? = null

    /**
     * Deletes rows from a table based on a given WHERE clause.
     *
     * @param init a lambda function that takes a `Where` object and defines the WHERE clause for
     *   the query
     * @return a `Delete` object that can be used to further customize the DELETE query
     */
    fun deleteWhere(init: Where.() -> Unit): Delete {
        val where = Where()
        init(where)

        condition = where.clausesArgs()

        return this
    }

    /**
     * Deletes a row from the database table based on the primary key value.
     *
     * @param value the value of the primary key
     * @return a [Delete] object for chaining additional delete operations
     */
    fun <T : Any> deletePrimary(value: T?): Delete {
        val primaryKey = table.primaryKey<T>()
        return deleteWhere { primaryKey eq value }
    }

    /**
     * Retrieves the SQL string and argument values for a delete operation.
     *
     * @return a pair containing the SQL string and a list of argument values
     */
    fun sqlArgs(): Pair<String, List<Any?>> {
        val sql = StringBuilder()

        sql.append("DELETE FROM ")
        sql.append(table.name())

        condition?.intoSqlArgs(sql, argsValues)

        return Pair(sql.toString(), argsValues)
    }
}

/**
 * Deletes rows from the table based on the specified conditions.
 *
 * @param init a lambda expression to specify the conditions for deletion. It takes an instance of
 *   `Where` as a receiver.
 * @return a `Delete` instance that allows further operations on the delete statement.
 */
fun Table.deleteWhere(init: Where.() -> Unit): Delete {
    return Delete(this).deleteWhere(init)
}

/**
 * Deletes a row from the table with the given primary key value.
 *
 * @param value the value of the primary key
 * @return the Delete object for method chaining
 */
fun Table.deletePrimary(value: Any): Delete {
    return Delete(this).deletePrimary(value)
}

/**
 * Persists the delete operation by executing the generated SQL query with the given `Connection`.
 *
 * @param conn The database connection to use for executing the delete operation.
 * @return [Result.success] Unit // [Result.failure]
 */
fun Delete.persist(conn: Connection): Result<Unit> {
    return runCatching {
        val (sql, args) = sqlArgs()
        conn.prepareStatement(sql).use { stmt ->
            setParameters(stmt, args)
            stmt.executeUpdate()
        }

        Unit
    }.mapCatching {
        Exception("Failed to execute delete operation: $it")
    }
}
