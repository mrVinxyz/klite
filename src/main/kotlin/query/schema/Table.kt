package query.schema

import query.Query
import query.expr.Delete
import query.expr.Insert
import query.expr.Select
import query.expr.Update
import query.expr.Where
import query.setParameters
import java.math.BigDecimal
import java.sql.Connection

abstract class Table(val tableName: String, protected val tablePrefix: Boolean = true) {
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

        val parsedKey = if (tablePrefix) tableName.plus("_").plus(key) else key
        val column = Column<T>(parsedKey, columnType, this)
        columns.add(column)

        return column
    }

    protected fun text(key: String): Column<String> = column(key)

    protected fun integer(key: String): Column<Int> = column(key)

    protected fun long(key: String): Column<Long> = column(key)

    protected fun float(key: String): Column<Float> = column(key)

    protected fun double(key: String): Column<Double> = column(key)

    protected fun decimal(key: String): Column<BigDecimal> = column(key)

    protected fun boolean(key: String): Column<Boolean> = column(key)

    protected fun <T : Any> Column<T>.setPrimaryKey(): Column<T> {
        primaryKey = this
        return this
    }

    fun getColumnsList(): List<Column<*>> = columns

    fun <T : Any> primaryKey(): Column<T> {
        if (primaryKey == null) primaryKey = columns.first()
        @Suppress("UNCHECKED_CAST") return primaryKey as Column<T>
    }
}

// TODO - In order for this to work, you'll need to implement some sort of builder interface in order to translate
// TODO - the columnType into database specific keyword; example: decimal to real.
//fun Table.createTable(): Query {
//    val sql = StringBuilder()
//    sql.append("CREATE TABLE IF NOT EXISTS ${this.tableName} (")
//
//    val columns = this.getColumnsList()
//    columns.forEachIndexed { index, column ->
//        sql.append("${column.key()} ${column.type().toString().uppercase()}")
//        if (index < columns.size - 1) sql.append(", ")
//    }
//
//    sql.append(");")
//
//    return Query(sql.toString())
//}

fun Table.insert(init: (Insert) -> Unit): Insert = Insert(this).apply(init)


fun Table.select(vararg columns: Column<*>): Select = Select(this).select(*columns)


fun Table.update(init: (Update) -> Unit): Update = Update(this).apply(init)


fun Table.deleteWhere(init: Where.() -> Unit): Delete = Delete(this).deleteWhere(init)


fun Table.deletePrimary(value: Any): Delete = Delete(this).deletePrimary(value)

fun Table.selectCount(conn: Connection): Result<Int> {
    val sql = "SELECT COUNT(*) FROM ${this.tableName}"

    return runCatching {
        conn.prepareStatement(sql).use { stmt ->
            stmt.executeQuery().use { rs ->
                if (rs.next()) rs.getInt(1) else 0
            }
        }
    }
}

inline fun <reified T> Table.selectExists(conn: Connection, column: Column<Any>, value: T): Result<Boolean> {
    val sql = "SELECT EXISTS(SELECT 1 FROM ${this.tableName} WHERE ${column.key()} = ? LIMIT 1)"

    return runCatching {
        conn.prepareStatement(sql).use { stmt ->
            stmt.setParameters(listOf(value))
            stmt.executeQuery().use { rs ->
                rs.next() && rs.getBoolean(1)
            }
        }
    }
}
