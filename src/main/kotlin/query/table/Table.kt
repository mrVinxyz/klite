package query.table

import query.insert.Inserter
import query.select.Selector
import query.update.Updater
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

    fun name(): String = name

    protected fun <T : Any> Column<T>.setPrimaryKey(): Column<T> {
        primaryKey = this
        return this
    }

    fun primaryKey(): Column<*>? = primaryKey

    fun columns(): List<Column<*>> = columns

    fun insert(init: Inserter.() -> Unit): Inserter {
        return Inserter(this).apply(init)
    }

    fun select(init: Selector.() -> Unit): Selector {
        return Selector(this).apply(init)
    }

    fun update(init: Updater.() -> Unit): Updater {
        return Updater(this).apply(init)
    }
}
