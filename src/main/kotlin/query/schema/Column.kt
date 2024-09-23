package query.schema

enum class ColumnType {
    STRING,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    DECIMAL,
    BOOLEAN,
}

//fun ColumnType.toSqlType(): String = {
//    return ""
//}

open class Column<T : Any>(
    private val key: String,
    private val type: ColumnType,
    private val table: Table,
) {
    fun key(): String = key
    fun type(): ColumnType = type
    fun table(): Table = table
}
