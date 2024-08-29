package query

/** Various types of join clauses in SQL. */
enum class JoinType {
    /** `LEFT` join type for SQL expression. */
    LEFT,

    /** `RIGHT` join type for SQL expression. */
    RIGHT,

    /** `INNER` join type for SQL expression. */
    INNER,

    /** `OUTER` join type for SQL expression. */
    OUTER,

    /** `FULL` join type SQL expression */
    FULL,
}

/**
 * Represents a join clause in a SQL query.
 *
 * The `Join` class is responsible for constructing a join clause in a SQL query. It takes the
 * required parameters for the join type, the original table and column, and the attached table and
 * column. It also provides a `toString()` method that returns the generated join clause as a
 * string.
 *
 * @property originalTable the original table to join
 * @property attachedTable the attached table to join
 * @property originalColumn the column in the original table to join
 * @property attachedColumn the column in the attached table to join
 * @property joinType the type of join to perform
 */
class Join(
    private val originalTable: Table,
    private val attachedTable: Table,
    private val originalColumn: Column<*>,
    private val attachedColumn: Column<*>,
    private val joinType: JoinType,
) {
    /**
     * The `joinClause` variable is a private property of the class `Join` and represents the SQL
     * join clause used to specify the relationship between tables in a database query. It is of
     * type `StringBuilder`.
     *
     * The `joinClause` property is initialized with an empty `StringBuilder` in the constructor of
     * the `Join` class.
     *
     * The `joinClause` property is used in the `toString` method of the `Join` class to generate
     * the string representation of the join object. The generated string representation includes
     * the join type, the name of the attached table, the "ON" keyword, and the join condition. The
     * join condition is in the form of "originalTable.originalColumn =
     * attachedTable.attachedColumn".
     *
     * The `joinClause` property is mutated in the `toString` method based on the `joinType`
     * property of the `Join` class. The join type is appended to the `joinClause` property based on
     * the value of the `joinType` property. The attached table name is appended to the `joinClause`
     * property using the `name` method of the attached table object. The "ON" keyword is appended
     * to the `joinClause` property. The original table name, original column key, attached table
     * name, and attached column key are appended to the `joinClause` property.
     *
     * The `joinClause` property is returned as a string in the `toString` method.
     */
    private var joinClause = StringBuilder()

    /**
     * Returns a string representation of the Join object.
     *
     * The string representation includes the join type ("LEFT JOIN", "RIGHT JOIN", "INNER JOIN",
     * "OUTER JOIN", or "FULL JOIN"), the name of the attached table, the "ON" keyword, and the join
     * condition in the form of "originalTable.originalColumn = attachedTable.attachedColumn".
     *
     * @return a string representation of the Join object
     */
    override fun toString(): String {
        when (joinType) {
            JoinType.LEFT -> joinClause.append("LEFT JOIN ")
            JoinType.RIGHT -> joinClause.append("RIGHT JOIN ")
            JoinType.INNER -> joinClause.append("INNER JOIN ")
            JoinType.OUTER -> joinClause.append("OUTER JOIN ")
            JoinType.FULL -> joinClause.append("FULL JOIN ")
        }

        joinClause.append(attachedTable.name())
        joinClause.append(" ON ")

        joinClause.append(originalTable.name())
        joinClause.append(".")
        joinClause.append(originalColumn.key())

        joinClause.append(" = ")
        joinClause.append(attachedTable.name())
        joinClause.append(".")
        joinClause.append(attachedColumn.key())

        return joinClause.toString()
    }
}
