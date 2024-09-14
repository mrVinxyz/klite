package query

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement

/**
 * Represents a SQL query that can be executed against a database connection.
 *
 * @property sql The SQL statement of the query.
 * @property args The arguments to be used in the query.
 * @constructor Creates a Query object with the given SQL statement and arguments.
 */
class Query(val sql: String, vararg args: Any) {
    /**
     * Represents a list of arguments.
     *
     * This variable, `args`, is a property of the `Query` class. It is a read-only list
     * containing the arguments passed to the query. The `args` property is converted to a
     * `List` using the `toList()` extension function. The arguments are then stored as elements
     * in the resulting list.
     *
     * @property args The list of arguments.
     */
    val args = args.toList()

    /**
     * Executes a database query that does not require a return value.
     *
     * @param connection The connection to the database.
     * @return A Result object representing the success or failure of the query execution.
     */
    fun execute(connection: Connection): Result<Unit> {
        return runCatching {
            val stmt = connection.prepareStatement(sql)
            setParameters(stmt, args)
            stmt.executeUpdate()
        }
    }

    /**
     * Persists data into the database using the provided connection.
     *
     * @param conn The database connection to use for the persistence operation.
     * @return The newly generated ID of the persisted record, as either an `Int` or a `String`.
     * @throws Exception if there is an error executing the persistence operation.
     */
    fun insert(conn: Connection): Result<Any> {
        return runCatching {
            val stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
            setParameters(stmt, args)
            stmt.executeUpdate()

            val generatedKeys = stmt.generatedKeys

            if (!generatedKeys.next()) {
                Result.failure<Unit>(Exception("Failed to insert record: [${sql}] [${args}]"))
            }

            val generatedId = generatedKeys.getObject(1)

            if (generatedId == 0) {
                Result.failure<Unit>(Exception("Failed to insert record: [${sql}] [$args]"))
            }

            generatedId
        }
            .onFailure { Result.failure<Unit>(Exception("Failed to execute insert operation: [$it]")) }
    }

    fun selectOne(conn: Connection): Result<Any> {
        return runCatching {
            val stmt = conn.prepareStatement(sql)
            setParameters(stmt, args)

            val rs = stmt.executeQuery()
            if (!rs.next()) {
                Result.success<List<Any>>(emptyList())
            }

            val row = Row(rs)
            row
        }
            .onFailure { Result.failure<Unit>(Exception("Failed to execute selectOne operation: [$it]")) }
    }

    fun selectList(conn: Connection): Result<List<Any>> {
        return runCatching {
            val stmt = conn.prepareStatement(sql)
            setParameters(stmt, args)

            val rs = stmt.executeQuery()
            val rows = Rows(rs).iterator()

            val resultList = mutableListOf<Any>()
            while (rows.hasNext()) {
                val row = rows.next()
                resultList.add(row)
            }

            resultList
        }
            .onFailure { Result.failure<Unit>(Exception("Failed to execute selectList operation: [$it]")) }
    }

    /**
     * Sets the parameters of a prepared statement using the given list of values.
     *
     * @param stmt The prepared statement to set the parameters for.
     * @param args The list of parameters to set.
     */
    fun setParameters(stmt: PreparedStatement, args: List<Any>) = stmt.setParameters(args)
}

typealias Transaction<R> = ((Connection) -> R) -> R
