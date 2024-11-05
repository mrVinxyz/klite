package query.expr

import query.execMapOne
import query.schema.Column
import query.schema.Table
import java.sql.Connection

data class FilterResult(val ok: Boolean, val err: String? = null, val field: String? = null)

class Filter(val table: Table) {
    private val filterFunctions = mutableListOf<(Connection) -> FilterResult>()

    fun <T : Any> Column<T>.unique(
        value: T?,
        msg: String = "Record already exists"
    ) {
        val column = this
        filterFunctions.add { conn ->
            val exists = Select(table)
                .select()
                .exists { column eq value }
                .sqlArgs()
                .execMapOne(conn) { it.get<Int>(1) }
                .getOrThrow()

            if (exists == 1) {
                FilterResult(false, msg, key())
            } else {
                FilterResult(true)
            }
        }
    }

    fun <T : Any> uniqueComposite(
        vararg columns: Pair<Column<T>, T?>,
        msg: String = "Composite key already exists"
    ) {
        filterFunctions.add { conn ->
            val exists = Select(table)
                .select()
                .exists {
                    columns.forEach { (column, value) ->
                        column eq value
                    }
                }
                .sqlArgs()
                .execMapOne(conn) { it.get<Int>(1) }
                .getOrThrow()

            if (exists == 1) {
                FilterResult(
                    false,
                    msg,
                    columns.joinToString(",") { it.first.key() }
                )
            } else {
                FilterResult(true)
            }
        }
    }

    fun <T : Any> Column<T>.exists(
        foreignTable: Table,
        foreignColumn: Column<T>,
        value: T?,
        msg: String = "Referenced record doesn't exist"
    ) {
        filterFunctions.add { conn ->
            val exists = Select(foreignTable)
                .select()
                .exists { foreignColumn eq value }
                .sqlArgs()
                .execMapOne(conn) { it.get<Int>(1) }
                .getOrThrow()

            if (exists == 0) {
                FilterResult(false, msg, key())
            } else {
                FilterResult(true)
            }
        }
    }

    fun predicate(
        msg: String,
        selectBuilder: Select.() -> Unit
    ) {
        filterFunctions.add { conn ->
            val select = Select(table)
                .apply(selectBuilder)

            val exists = select
                .sqlArgs()
                .execMapOne(conn) { it.get<Int>(1) }
                .getOrThrow()

            if (exists == 1) {
                FilterResult(false, msg, null)
            } else {
                FilterResult(true, "", null)
            }
        }
    }

    fun execute(conn: Connection): FilterResult {
        filterFunctions.forEach { filterFn ->
            val result = filterFn(conn)
            if (!result.ok) {
                return result
            }
        }
        return FilterResult(true)
    }
}