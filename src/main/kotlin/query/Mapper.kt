package query

import query.schema.Column
import query.schema.ColumnType
import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.ResultSet

class Row(val resultSet: ResultSet) {
    inline operator fun <reified T : Any> get(column: Column<T>): T =
        when (column.type()) {
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

    inline operator fun <reified T : Any> get(arg: Any): T =
        when (T::class) {
            Int::class -> when (arg) {
                is Int -> resultSet.getInt(arg)
                is String -> resultSet.getInt(arg)
                else -> error("Invalid type: $arg")
            } as T

            String::class -> when (arg) {
                is Int -> resultSet.getString(arg) ?: ""
                is String -> resultSet.getString(arg) ?: ""
                else -> error("Invalid type: $arg")
            } as T

            Long::class -> when (arg) {
                is Int -> resultSet.getLong(arg)
                is String -> resultSet.getLong(arg)
                else -> error("Invalid type: $arg")
            } as T

            Float::class -> when (arg) {
                is Int -> resultSet.getFloat(arg)
                is String -> resultSet.getFloat(arg)
                else -> error("Invalid type: $arg")
            } as T

            Double::class -> when (arg) {
                is Int -> resultSet.getDouble(arg)
                is String -> resultSet.getDouble(arg)
                else -> error("Invalid type: $arg")
            } as T

            BigDecimal::class -> when (arg) {
                is Int -> resultSet.getBigDecimal(arg) ?: BigDecimal.ZERO
                is String -> resultSet.getBigDecimal(arg) ?: BigDecimal.ZERO
                else -> error("Invalid type: $arg")
            } as T

            Boolean::class -> when (arg) {
                is Int -> resultSet.getBoolean(arg)
                is String -> resultSet.getBoolean(arg)
                else -> error("Invalid type: $arg")
            } as T

            else -> error("Unsupported type: ${T::class}")
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

fun PreparedStatement.setParameters(params: List<Any?>) =
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
