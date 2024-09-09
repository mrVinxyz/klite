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
 * Executes a SQL query to retrieve the count of rows in the table.
 *
 * @param conn the database connection used to execute the query
 * @return a Result object - [Result.success] The number of records this given table has.
 * [Result.failure] Any error that may have occurred in the process.
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
 * Executes a SQL query to check if a record exists in the table based on the provided column value.
 *
 * @param conn the database connection
 * @param column the column to check for existence
 * @return the result of the query, true if a record exists, false otherwise
 */
fun Table.selectExists(conn: Connection, column: Column<Any>): Result<Boolean> {
    val sql = "SELECT EXISTS(SELECT 1 FROM ${this.name()} WHERE ${column.key()} = ? LIMIT 1)"

    return runCatching {
        conn.prepareStatement(sql).use { statement ->
            statement.setObject(1, column)
            val rs = statement.executeQuery()
            rs.next() && rs.getBoolean(1)
        }
    }
}
