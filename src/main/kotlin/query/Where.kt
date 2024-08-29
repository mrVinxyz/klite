package query

/**
 * Represents a type alias for WhereArgs, which is a pair of a SQL WHERE clause string and a list of
 * argument values.
 */
typealias WhereArgs = Pair<String, List<Any>>

/**
 * Writes the WHERE clause and argument values into the SQL string and argument list provided.
 *
 * @param sqlBuilder The StringBuilder to store the SQL string.
 * @param argsList The MutableList to store the argument values.
 */
fun WhereArgs.intoSqlArgs(sqlBuilder: StringBuilder, argsList: MutableList<Any>) {
    this.let { cond ->
        cond.first
            .takeIf { it.isNotEmpty() }
            ?.let {
                sqlBuilder.append(" WHERE ")
                sqlBuilder.append(it)
                argsList.addAll(cond.second)
            }
    }
}

/**
 * Represents a WHERE clause in a database query. This class allows you to construct a WHERE clause
 * by using column comparison operations. The constructed WHERE clause can be used in a query to
 * filter rows based on specific conditions.
 *
 * @property clauses the string representation of the clauses in the WHERE clause
 * @property args the list of argument values in the WHERE clause
 */
class Where {
    /**
     * Represents a class that holds a collection of SQL WHERE clauses.
     *
     * This class is used to construct SQL WHERE clauses using the DSL-style syntax. An instance of
     * this class is created with an initial empty string builder, and the clauses are appended to
     * this string builder using the infix functions defined in this class. The clauses can then be
     * retrieved using the [clausesArgs] function. The class also has a private mutable list for
     * storing the arguments associated with the clauses.
     *
     * @property clauses the string builder for storing the SQL WHERE clauses
     * @property args the mutable list for storing the arguments associated with the clauses
     */
    private val clauses = StringBuilder()

    /**
     * Represents a mutable list of arguments.
     *
     * This variable holds a list of arguments that can be used in various functions. The list is
     * initially empty and elements can be added to it using the `add` method. The type of objects
     * that can be added to the list is `Any`.
     *
     * @property args the mutable list of arguments
     */
    private val args = mutableListOf<Any>()

    /**
     * Adds an equality condition to the WHERE clause of a database query.
     *
     * @param value the value to compare with
     * @param T the type of the column
     */
    infix fun <T : Any> Column<T>.equal(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append("${this.key()} = ?")
            args.add(value)
        }
    }

    /**
     * Adds a LIKE clause to the WHERE condition of a SQL query.
     *
     * @param T the type of data stored in the column
     * @param value the value to match with the column's value
     *
     * Usage:
     * ```
     * column like value
     * ```
     */
    infix fun <T : Any> Column<T>.like(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append("${this.key()} LIKE ?")
            args.add(value)
        }
    }

    /**
     * Sets a like starts condition for the specified column.
     *
     * This method appends a "like starts" condition to the list of clauses in the `Where` class. If
     * the column already has existing clauses, an "AND" operator is appended before the new
     * condition. The column name and the entered value are combined to form the condition string in
     * the format "${column.key()} LIKE ?". The value is then appended to the list of arguments with
     * a "%" wildcard added at the end to represent the starts condition.
     *
     * @param T the type of data stored in the column
     * @param value the value to search for at the start of the column's value
     */
    infix fun <T : Any> Column<T>.likeStarts(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append("${this.key()} LIKE ?")
            args.add("$value%")
        }
    }

    /**
     * Appends a LIKE clause to the list of clauses and adds the corresponding argument to the list
     * of arguments if the value is not null. The LIKE clause checks if the value ends with the
     * specified pattern.
     *
     * @param value the value to check against
     */
    infix fun <T : Any> Column<T>.likeEnds(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append("${this.key()} LIKE ?")
            args.add("%$value")
        }
    }

    /**
     * Adds a clause to the WHERE statement of a SQL query.
     *
     * This method appends a clause to the WHERE statement of a SQL query using the LIKE operator.
     * The column to be compared is specified by the receiver (left side of the expression). The
     * value to be compared is specified by the `value` parameter (right side of the expression).
     * The `value` parameter is surrounded by wildcard characters (`%`) to allow partial matches.
     * The clause is added to the `clauses` property of the `Where` class, along with the
     * corresponding value to be used in the query.
     *
     * @param value the value to be compared with the column in the LIKE statement
     */
    infix fun <T : Any> Column<T>.likeContains(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append("${this.key()} LIKE ?")
            args.add("%$value%")
        }
    }

    /**
     * Represents a function that applies a custom function to a column and returns a new column
     * with the updated key.
     *
     * @param T the type of data stored in the column
     * @param column the column to apply the function to
     * @param fnName the name of the custom function to apply
     * @return a new column with the updated key that represents the custom function applied to the
     *   original column
     */
    fun <T : Any> fn(column: Column<T>, fnName: String): Column<T> {
        return Column("${fnName}(${column.key()})", column.type(), column.table())
    }

    /**
     * Returns the clauses and arguments for the WHERE clause of a SQL query.
     *
     * @return A pair consisting of the clauses as a string and the arguments as a list.
     */
    fun clausesArgs(): WhereArgs = Pair(clauses.toString(), args)
}
