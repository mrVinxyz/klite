package query

/** Order clauses in SQL queries. */
enum class OrderType {
    /** `ASC` sql expression. */
    ASC,

    /** `DESC` sql expression. */
    DESC,
}

/**
 * Represents an order-by clause in an SQL query.
 *
 * The OrderBy class allows you to specify the ordering of query results based on one or more
 * columns. You can use the `asc()` and `desc()` methods to specify the order for each column. The
 * `toString()` method returns the SQL representation of the order-by clause.
 *
 * @property columnsBy a list of column-order type pairs
 */
class OrderBy {
    /**
     * Represents a list of column-order pairs for sorting. Each pair consists of a column and an
     * order type (ASC or DESC).
     */
    private val columnsBy = mutableListOf<Pair<Column<*>, OrderType>>()

    /**
     * Adds the column to the list of columns to be ordered in ascending order.
     *
     * This method adds the column to the list of columns to be ordered in ascending order in the
     * `OrderBy` class.
     *
     * @param T the type of data stored in the column
     */
    fun <T : Any> Column<T>.asc() {
        columnsBy.add(this to OrderType.ASC)
    }

    /**
     * Adds the current column to the list of columns to be ordered in descending order. The column
     * will be paired with the [OrderType.DESC] value in the [columnsBy] list.
     *
     * @param T the type of data stored in the column
     */
    fun <T : Any> Column<T>.desc() {
        columnsBy.add(this to OrderType.DESC)
    }

    /**
     * Returns a string representation of the object.
     *
     * The string representation is the ordered combination of the key of each column in the
     * `columnsBy` list, followed by the corresponding `OrderType` name. The key and order type are
     * separated by a space and each column representation is separated by a comma. The key of a
     * column is obtained by calling the `key()` function on the `Column` object.
     *
     * @return a string representation of the object
     */
    override fun toString(): String {
        return columnsBy.joinToString(", ") { (column, orderType) ->
            "${column.key()} ${orderType.name}"
        }
    }
}
