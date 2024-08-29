package query

import java.sql.Connection
import java.sql.Statement

/**
 * The `Inserter` class is responsible for building SQL INSERT statements for a given table.
 *
 * @property table the table for which the INSERT statement will be created
 * @property insertColumns a list of columns to be inserted
 * @property argsValues a list of values to be inserted into the columns
 */
class Inserter(val table: Table) {
    /**
     * Represents a mutable list of columns to be inserted into a table.
     *
     * This variable holds the list of columns that will be inserted into a table. It is a
     * [MutableList] of [Column] objects. The type parameter of [Column] can be any type of data
     * stored in the column.
     *
     * @see Column
     */
    private val insertColumns = mutableListOf<Column<*>>()

    /**
     * The `argsValues` variable is a mutable list of any type. It is a private property and is used
     * within the `Inserter` class. This list is used to store the values that are inserted into the
     * database table.
     *
     * The `argsValues` list is initialized as an empty mutable list. It is updated when the `set`
     * function, `values` function, or `values` function with a map parameter is called .
     *
     * The `set` function adds a value to the `argsValues` list when inserting a value for a
     * specific column into the table.
     *
     * The `values` function adds values to the `argsValues` list when inserting multiple values
     * into the table. It accepts a variable number of parameters of any type. If any parameter is
     * null, the corresponding column is added to a `nullColumns` list for removal from the
     * `insertColumns` list.
     *
     * The `values` function with a map parameter adds values to the `argsValues` list based on the
     * key-value pairs in the map. It iterates through the `insertColumns` list and retrieves the
     * value from the map based on the key of each column.
     *
     * The `sqlArgs` function returns a Pair of a SQL statement and a list of values. It creates a
     * SQL INSERT statement based on the table name and column names. The "?" placeholders are used
     * for values, which are retrieved from the `insertColumns` list and appended to the
     * `argsValues` list.
     */
    private val argsValues = mutableListOf<Any>()

    /**
     * Returns an `Inserter` object that can be used to construct an SQL INSERT statement for the
     * specified table. The `Inserter` object provides various methods to specify the columns and
     * values to be inserted.
     *
     * @param init The initialization block that takes an `Inserter` object as a parameter. This
     *   block is used to set the columns and values for the insertion.
     * @return An `Inserter` object.
     */
    fun insert(init: (Inserter) -> Unit): Inserter {
        return Inserter(table).apply(init)
    }

    /**
     * Inserts the given columns into the current Inserter instance.
     *
     * @param column the columns to insert
     * @return the current Inserter instance
     */
    fun insert(vararg column: Column<*>): Inserter {
        insertColumns.addAll(column)
        return this
    }

    /**
     * Sets a column with the specified value in the [Inserter].
     *
     * @param column the column to set the value for
     * @param value the value to set for the column
     * @return the [Inserter] object
     */
    operator fun <T> set(column: Column<*>, value: T?): Inserter {
        value?.let { v ->
            insertColumns.add(column)
            argsValues.add(v)
        }

        return this
    }

    /**
     * Inserts values into the table.
     *
     * @param value the values to be inserted into the table as varargs
     * @return an instance of Inserter for method chaining
     */
    fun values(vararg value: Any?): Inserter {
        val nullColumns = mutableListOf<Column<*>>()

        value.forEachIndexed { index, any ->
            if (any != null) argsValues.add(any) else nullColumns.add(insertColumns[index])
        }

        insertColumns.removeAll(nullColumns)

        return this
    }

    /**
     * Retrieves the column values from a given map and adds them to the `argsValues` list. Only the
     * values corresponding to the columns in the `insertColumns` list are added.
     *
     * @param map a map containing column names and their corresponding values
     * @return the Inserter object for method chaining
     */
    fun values(map: Map<String, Any?>): Inserter {

        insertColumns.forEach { column ->
            map[column.key()]?.let { value -> value.let { argsValues.add(value) } }
        }

        return this
    }

    /**
     * Generates SQL arguments for an insert statement.
     *
     * @return a Pair containing the SQL statement and a list of argument values
     */
    fun sqlArgs(): Pair<String, List<Any>> {
        val sql = StringBuilder()

        sql.append("INSERT INTO ")
        sql.append(table.name())
        sql.append(" (")

        insertColumns.forEachIndexed { index, column ->
            sql.append(column.key())
            if (index < insertColumns.size - 1) sql.append(", ")
        }

        sql.append(") VALUES (")
        sql.append(insertColumns.joinToString(", ") { "?" })
        sql.append(")")

        return Pair(sql.toString(), argsValues)
    }
}

/**
 * Inserts a new row into the table.
 *
 * @param init a function that initializes the `Inserter` object
 * @return an `Inserter` object
 */
fun Table.insert(init: Inserter.() -> Unit): Inserter {
    return Inserter(this).apply(init)
}

/**
 * Represents the result of an insert operation.
 *
 * @property value the map of column names to generated IDs
 */
typealias InsertResult = Result<Map<String, Int>>

/**
 * Persists the data in the database by executing an insert operation using the provided connection.
 *
 * @param conn the database connection to use for executing the insert operation
 * @return an [InsertResult] object representing the result of the insert operation
 */
fun Inserter.persist(conn: Connection): InsertResult {
    return runCatching {
        val (sql, args) = sqlArgs()
        val stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        setParameters(stmt, args)
        stmt.executeUpdate()

        val idName = this.table.primaryKey<Any>()?.key() ?: "id"
        val generatedId = stmt.generatedKeys.getInt(1)

        if (generatedId == 0) {
            Result.failure<Unit>(Exception("Failed to insert record: [${sql}] [$args]"))
        }

        mapOf(idName to generatedId)
    }
        .onFailure { Result.failure<Unit>(Exception("Failed to execute insert operation: [$it]")) }
}
