package query.table

import java.math.BigDecimal

abstract class Table(private val name: String) {
    protected val columns = mutableListOf<Column<*>>()
    private var primaryKey: Column<*>? = null

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
        val column = Column<T>(key, columnType)
        columns.add(column)
        column.setTable(this)

        return column
    }

    protected fun <T : Any> Column<T>.setPrimaryKey(): Column<T> {
        primaryKey = this
        return this
    }

    fun name(): String = name

    fun primaryKey(): Column<*>? = primaryKey

    fun columns(): List<Column<*>> = columns
}
