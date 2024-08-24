package query

enum class ColumnType {
    STRING,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    DECIMAL,
    BOOLEAN
}

open class Column<T : Any>(private val key: String, private val type: ColumnType) {
    private lateinit var table: Table

    fun key(): String = key

    fun type(): ColumnType = type

    fun asAlias(alias: String): Column<T> {
        return Column(alias, type)
    }

    fun setTable(table: Table) {
        this.table = table
    }

    fun getTable(): Table = table
}
