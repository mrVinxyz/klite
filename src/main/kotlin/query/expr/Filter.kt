package query.expr

import query.execMapOne
import query.schema.Column
import query.schema.Table
import java.sql.Connection

data class FilterResolution(val ok: Boolean, val err: String)

class Filter(val table: Table) {
    private val filterFunctions = mutableListOf<(Connection) -> FilterResolution>()

    infix fun <T : Any> Column<T>.unique(value: T?) {
        val column = this
        filterFunctions.add { conn ->
            val exists = Select(table)
                .select()
                .exists { column eq value }
                .sqlArgs()
                .execMapOne(conn) { it.get<Int>(1) }
                .getOrThrow()

            if (exists == 1) {
                FilterResolution(false, "Record already exists")
            } else {
                FilterResolution(true, "")
            }
        }
    }

    fun execute(conn: Connection): FilterResolution {
        filterFunctions.forEach { filterFn ->
            val result = filterFn(conn)
            if (!result.ok) {
                return result
            }
        }
        return FilterResolution(true, "")
    }
}