package query.mapper

import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.ResultSet
import query.table.Column
import query.table.ColumnType

class Row(private val resultSet: ResultSet) {
    @Suppress("UNCHECKED_CAST")
    operator fun <T : Any> get(column: Column<T>): T {
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
}

class Rows(private val resultSet: ResultSet) : Iterable<Row> {
    override fun iterator(): Iterator<Row> = resultSet.iterator()
}

fun ResultSet.iterator(): Iterator<Row> =
    object : Iterator<Row> {
        var isHasNext: Boolean? = null

        override fun hasNext() = isHasNext ?: this@iterator.next().also { isHasNext = it }

        override fun next(): Row {
            val isHasNext = isHasNext?.also { isHasNext = null } ?: this@iterator.next()
            if (!isHasNext) throw NoSuchElementException("No more rows in ResultSet.")
            return Row(this@iterator)
        }
    }

internal fun setParameters(stmt: PreparedStatement, params: List<Any?>) {
    params.forEachIndexed { index, param ->
        when (param) {
            is String -> stmt.setString(index + 1, param)
            is Int -> stmt.setInt(index + 1, param)
            is Long -> stmt.setLong(index + 1, param)
            is Float -> stmt.setFloat(index + 1, param)
            is Double -> stmt.setDouble(index + 1, param)
            is BigDecimal -> stmt.setBigDecimal(index + 1, param)
            is Boolean -> stmt.setBoolean(index + 1, param)
            else -> stmt.setObject(index + 1, param)
        }
    }
}
