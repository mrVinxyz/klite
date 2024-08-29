package query

/** Database column types. */
enum class ColumnType {
    /** Represents a [String] column. */
    STRING,

    /** Represents an [Int] column. */
    INT,

    /** Represents a [Long] column. */
    LONG,

    /** Represents a [Float] column. */
    FLOAT,

    /** Represents a [Double] column. */
    DOUBLE,

    /** Represents a [java.math.BigDecimal] column. */
    DECIMAL,

    /** Represents a [Boolean] column or [Int] `1 true` - `0 false`. */
    BOOLEAN,
}

/**
 * Represents a column in a table.
 *
 * @param T the type of data stored in the column
 * @property key the key name of the column
 * @property type the type of the column
 */
open class Column<T : Any>(
    private val key: String,
    private val type: ColumnType,
    private val table: Table,
) {
    /**
     * Returns the key of the column.
     *
     * @return the key of the column as a string
     */
    fun key(): String = key

    /**
     * Returns the type of the column.
     *
     * @return the column type
     */
    fun type(): ColumnType = type

    /**
     * Creates a new column with the given alias.
     *
     * @param alias the alias to assign to the column
     * @return a new column with the specified alias and the same type as the original column
     */
    fun asAlias(alias: String): Column<T> {
        return Column(alias, type, table)
    }

    /**
     * Retrieves the associated table for this column.
     *
     * @return The [Table] associated with this column.
     */
    fun table(): Table = table
}
