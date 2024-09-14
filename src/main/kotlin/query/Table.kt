package query

import java.math.BigDecimal
import java.sql.Connection

/**
 * Represents a table in a database.
 *
 * @property name the name of the table
 * @property columns a list of columns in the table
 * @property primaryKey the primary key column of the table
 */
abstract class Table(private val name: String) {
    /**
     * Represents a table in a database.
     *
     * @param name the name of the table
     */
    protected val columns = mutableListOf<Column<*>>()

    /**
     * Represents the primary key column in a table.
     *
     * This variable holds the primary key column for a table. The primary key column is used to
     * uniquely identify each row in the table. It is of type `Column<T>`, where `T` represents the
     * data type stored in the primary key column.
     *
     * @property primaryKey the primary key column
     */
    private var primaryKey: Column<*>? = null

    /**
     * Adds a new column to the table.
     *
     * @param T the type of data stored in the column
     * @param key the key name of the column
     * @return the created column of type Column<T>
     */
    protected inline fun <reified T : Any> column(key: String): Column<T> {
        val columnType =
            when (T::class) {
                String::class -> ColumnType.STRING
                Int::class -> ColumnType.INT
                Long::class -> ColumnType.LONG
                Float::class -> ColumnType.FLOAT
                Double::class -> ColumnType.DOUBLE
                BigDecimal::class -> ColumnType.DECIMAL
                Boolean::class -> ColumnType.BOOLEAN
                else -> throw IllegalArgumentException("Unsupported type: ${T::class}")
            }
        val column = Column<T>(key, columnType, this)
        columns.add(column)

        return column
    }

    /**
     * Retrieves the name of the object.
     *
     * @return The name of the object.
     */
    fun name(): String = name

    /**
     * Sets the primary key for the column.
     *
     * @return the column with the primary key set
     */
    protected fun <T : Any> Column<T>.setPrimaryKey(): Column<T> {
        primaryKey = this
        return this
    }

    /**
     * Returns the primary key column of the table.
     *
     * If the primary key has not been explicitly set, the method returns the first column in the
     * list of columns.
     *
     * @return the primary key column as a [Column] object
     */
    fun <T : Any> primaryKey(): Column<T> {
        if (primaryKey == null) primaryKey = columns.first()
        @Suppress("UNCHECKED_CAST") return primaryKey as Column<T>
    }

    /**
     * Retrieves a column of type String with the given key from the table.
     *
     * @param key the key name of the column
     * @return the created column of type Column<String>
     */
    protected fun text(key: String): Column<String> = column(key)

    /**
     * Adds a new Integer column to the table.
     *
     * @param key the key name of the column
     * @return the created column of type Column<Int>
     */
    protected fun integer(key: String): Column<Int> = column(key)

    /**
     * Creates a new column of type Long in the table.
     *
     * @param key the key name of the column
     * @return the created column of type Column<Long>
     */
    protected fun long(key: String): Column<Long> = column(key)

    /**
     * Creates a new column of type Float.
     *
     * @param key the key name of the column
     * @return the created column of type Column<Float>
     */
    protected fun float(key: String): Column<Float> = column(key)

    /**
     * Creates a new column of type Double with the given key name and adds it to the table.
     *
     * @param key the key name of the column
     * @return the created column of type Column<Double>
     */
    protected fun double(key: String): Column<Double> = column(key)

    /**
     * Creates a new Decimal column.
     *
     * @param key the key name of the column
     * @return the created column of type Column<BigDecimal>
     */
    protected fun decimal(key: String): Column<BigDecimal> = column(key)

    /**
     * Creates a new Boolean column with the specified key.
     *
     * @param key the key name of the column
     * @return the created column of type [Column]<[Boolean]>
     */
    protected fun boolean(key: String): Column<Boolean> = column(key)

    /**
     * Retrieves the list of columns in the table.
     *
     * @return the list of columns as a [List] of [Column] objects
     */
    fun getColumnsList(): List<Column<*>> = columns
}

/**
 * Inserts a new row into the table.
 *
 * @param init a function that initializes the `Insert` object
 * @return an `Insert` object
 */
fun Table.insert(init: (Insert) -> Unit): Insert {
    return Insert(this).apply(init)
}

/**
 * Extension function to initialize a Select for a table.
 *
 * @param columns Vararg of columns to be selected.
 * @return A configured [Select] instance.
 */
fun Table.select(vararg columns: Column<*>): Select {
    return Select(this).select(*columns)
}

/**
 * Updates the table with the specified changes.
 *
 * It takes a lambda function as a parameter which allows the caller to specify the changes to be made
 * to the table. The lambda function has a single argument of type `Update`. Within the lambda function,
 * the caller can use `Update` object to set the values of the columns to be updated and add conditions
 * for the update operation.
 *
 * @param init a lambda function that specifies the changes to be made to the table
 * @return an `Update` object that can be used to further modify the update operation
 */
fun Table.update(init: (Update) -> Unit): Update {
    return Update(this).apply(init)
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
 * Executes a SQL query to retrieve the count of rows in the table.
 *
 * @param conn the database connection used to execute the query
 * @return the result of the query
 */
fun Table.selectCount(conn: Connection): Result<Int> {
    val sql = "SELECT COUNT(*) FROM ${this.name()}"

    return runCatching {
        conn.prepareStatement(sql).use { stmt ->
            stmt.executeQuery().use { rs ->
                if (rs.next()) rs.getInt(1) else 0
            }
        }
    }
}

/**
 * Executes a SQL query to check if a record exists in the table based on the provided `column:value`.
 *
 * @param conn the database connection
 * @param column the column to check for existence
 * @return the result of the query, true if a record exists, false otherwise
 */
inline fun <reified T> Table.selectExists(conn: Connection, column: Column<Any>, value: T): Result<Boolean> {
    val sql = "SELECT EXISTS(SELECT 1 FROM ${this.name()} WHERE ${column.key()} = ? LIMIT 1)"

    return runCatching {
        conn.prepareStatement(sql).use { stmt ->
            stmt.setParameters(listOf(value))
            val rs = stmt.executeQuery()
            rs.next() && rs.getBoolean(1)
        }
    }
}
