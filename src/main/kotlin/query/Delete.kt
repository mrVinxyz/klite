package query

import java.sql.Connection
import kotlin.use

/**
 * The `Deleter` class is responsible for generating DELETE SQL queries for a given `Table` and
 * executing them.
 *
 * @param table The table for which the DELETE query is generated.
 * @constructor Creates a `Deleter` instance for the specified `table`.
 * @property table The table for which the DELETE query is generated.
 * @property argsValues A mutable list to store the values of arguments used in the query.
 * @property condition The WHERE clause of the DELETE query.
 */
class Deleter(private val table: Table) {
    /** Mutable list to hold values of type Any for SQL query arguments. */
    private val argsValues = mutableListOf<Any>()

    /**
     * Represents a private nullable property that stores the condition for a SQL query. The
     * condition is used in the `deleteWhere` and `sqlArgs` functions of the `Deleter` class.
     *
     * @property condition The condition for the SQL query as a `WhereArgs` object.
     * @see Deleter
     * @see WhereArgs
     */
    private var condition: WhereArgs? = null

    /**
     * Deletes rows from a table based on a given WHERE clause.
     *
     * @param init a lambda function that takes a `Where` object and defines the WHERE clause for
     *   the query
     * @return a `Deleter` object that can be used to further customize the DELETE query
     */
    fun deleteWhere(init: Where.() -> Unit): Deleter {
        val where = Where()
        init(where)

        condition = where.clausesArgs()

        return this
    }

    /**
     * Deletes a row from the database table based on the primary key value.
     *
     * @param value the value of the primary key
     * @return a [Deleter] object for chaining additional delete operations
     */
    fun <T : Any> deletePrimary(value: T?): Deleter {
        val primaryKey = table.primaryKey<T>()
        return deleteWhere { primaryKey equal value }
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
 * @return a `Deleter` instance that allows further operations on the delete statement.
 */
fun Table.deleteWhere(init: Where.() -> Unit): Deleter {
    return Deleter(this).deleteWhere(init)
}

/**
 * Deletes a row from the table with the given primary key value.
 *
 * @param value the value of the primary key
 * @return the Deleter object for method chaining
 */
fun Table.deletePrimary(value: Any): Deleter {
    return Deleter(this).deletePrimary(value)
}

/**
 * A type alias representing the result of a delete operation. It is a [Result] that returns [Unit].
 */
typealias DeleteResult = Result<Unit>

/**
 * Persists the delete operation by executing the SQL delete statement on the provided database
 * connection.
 *
 * @param conn the database connection on which to execute the delete operation
 * @return the result of the delete operation as a [DeleteResult] object
 */
fun Deleter.persist(conn: Connection): DeleteResult {
    return runCatching {
        val (sql, args) = sqlArgs()
        conn.prepareStatement(sql).use { stmt ->
            setParameters(stmt, args)
            stmt.executeUpdate()
        }

        Unit
    }
        .onFailure { Result.failure<Unit>(Exception("Failed to execute delete operation: [$it]")) }
}
