package query

import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Represents a row in a database query result set.
 *
 * @constructor Creates a Row object with the given result set.
 * @property resultSet The result set containing the row data.
 */
class Row(val resultSet: ResultSet) {
    /**
     * Retrieves the value of the specified column from the current row of the result set.
     *
     * @param column the column object representing the desired column
     * @return the value of the column as type T
     * @suppress Specifies that an unchecked cast is performed
     */
    inline operator fun <reified T : Any> get(column: Column<T>): T {
        return when (column.type()) {
            ColumnType.INT -> resultSet.getInt(column.key()) as T
            ColumnType.STRING -> {
                val value = resultSet.getString(column.key())
                (value ?: "") as T
            }

            ColumnType.LONG -> resultSet.getLong(column.key()) as T
            ColumnType.FLOAT -> resultSet.getFloat(column.key()) as T
            ColumnType.DOUBLE -> resultSet.getDouble(column.key()) as T
            ColumnType.DECIMAL -> {
                val value = resultSet.getBigDecimal(column.key())
                (value ?: BigDecimal.ZERO) as T
            }

            ColumnType.BOOLEAN -> resultSet.getBoolean(column.key()) as T
        }
    }

    // TODO
//    inline operator fun <reified T : Any> get(column: Column<T>): T {
//        return when (T::class) {
//            Int::class -> resultSet.getInt(column.key()) as T
//            String::class -> {
//                val value = resultSet.getString(column.key())
//                (value ?: "") as T
//            }
//
//            Long::class -> resultSet.getLong(column.key()) as T
//            Float::class -> resultSet.getFloat(column.key()) as T
//            Double::class -> resultSet.getDouble(column.key()) as T
//            BigDecimal::class -> {
//                val value = resultSet.getBigDecimal(column.key())
//                (value ?: BigDecimal.ZERO) as T
//            }
//
//            Boolean::class -> resultSet.getBoolean(column.key()) as T
//            else -> error("Unsupported type: ${T::class}")
//        }
//    }

    inline operator fun <reified T : Any> get(column: String): T {
        return when (T::class) {
            Int::class -> resultSet.getInt(column) as T
            String::class -> {
                val value = resultSet.getString(column)
                (value ?: "") as T
            }

            Long::class -> resultSet.getLong(column) as T
            Float::class -> resultSet.getFloat(column) as T
            Double::class -> resultSet.getDouble(column) as T
            BigDecimal::class -> {
                val value = resultSet.getBigDecimal(column)
                (value ?: BigDecimal.ZERO) as T
            }

            Boolean::class -> resultSet.getBoolean(column) as T
            else -> error("Unsupported type: ${T::class}")
        }
    }
}

/**
 * Represents a collection of database rows obtained from a ResultSet.
 *
 * @param resultSet The ResultSet from which the rows are obtained.
 */
class Rows(private val resultSet: ResultSet) : Iterable<Row> {
    /**
     * Returns an iterator over the elements in the ResultSet. Each element in the iterator is a Row
     * object.
     *
     * @return an iterator of type Iterator<Row>
     */
    override fun iterator(): Iterator<Row> = resultSet.iterator()
}

/**
 * Returns an iterator over the rows of a ResultSet.
 *
 * This method extends the ResultSet class and allows it to be iterated over using a for-each loop
 * or other iteration methods. The iterator returns instances of the Row class, which represents a
 * single row in the ResultSet.
 *
 * @return an iterator of type Iterator<Row> that can be used to iterate over the rows of the
 *   ResultSet
 */
fun ResultSet.iterator(): Iterator<Row> =
    object : Iterator<Row> {
        /**
         * The `isHasNext` variable is a nullable Boolean that represents whether there is a next
         * row in a ResultSet. `True` indicates that there is a next row. `False` indicates that
         * there is no next row. `null` indicates that the value has not been read yet.
         */
        var isHasNext: Boolean? = null

        /**
         * Returns whether there is a next element in the iterator.
         *
         * This method checks if the `isHasNext` property is null. If it is not null, it has content
         * to be read, then returns the value of `isHasNext`(true). If it is null, it calls the
         * `next` function of the `iterator ` and assigns the result to `isHasNext`. It then returns
         * the value of `isHasNext`(True or False).
         *
         * @return true if there is a next element, false otherwise
         */
        override fun hasNext() = isHasNext ?: this@iterator.next().also { isHasNext = it }

        /**
         * Returns the next row in the ResultSet. It resets the `isHasNext` property to null to
         * indicate that the value has been read. Else it calls the `next` function of the
         * `iterator` and returns a [Row] object.
         *
         * @return the next row as a [Row] object
         * @throws NoSuchElementException if there are no more rows in the ResultSet
         */
        override fun next(): Row {
            val isHasNext = isHasNext?.also { isHasNext = null } ?: this@iterator.next()
            if (!isHasNext) throw NoSuchElementException("No more rows in ResultSet.")
            return Row(this@iterator)
        }
    }

/**
 * Sets the parameters of a prepared statement using the given list of values.
 *
 * @param params The list of parameters to set.
 */
fun PreparedStatement.setParameters(params: List<Any?>) {
    params.forEachIndexed { index, param ->
        when (param) {
            is String -> this.setString(index + 1, param)
            is Int -> this.setInt(index + 1, param)
            is Long -> this.setLong(index + 1, param)
            is Float -> this.setFloat(index + 1, param)
            is Double -> this.setDouble(index + 1, param)
            is BigDecimal -> this.setBigDecimal(index + 1, param)
            is Boolean -> this.setBoolean(index + 1, param)
            else -> this.setObject(index + 1, param)
        }
    }
}