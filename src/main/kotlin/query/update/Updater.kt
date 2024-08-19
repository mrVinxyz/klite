package query.update

import query.table.Column
import query.table.Table
import query.where.Where
import query.where.WhereArgs

class Updater(private val table: Table) {
    private val updateColumns = mutableListOf<Column<*>>()
    private val argsValues = mutableListOf<Any?>()
    private var condition: WhereArgs? = null

    fun update(init: (Updater) -> Unit): Updater {
        return Updater(table).apply(init)
    }

    operator fun <T : Any> set(column: Column<*>, value: T?): Updater {
        updateColumns.add(column)
        argsValues.add(value)

        return this
    }

    fun where(init: Where.() -> Unit): Updater {
        val where = Where()
        init(where)

        condition = where.clausesArgs()

        return this
    }

    fun sqlArgs(): Pair<String, List<Any?>> {
        val sql = StringBuilder()

        sql.append("UPDATE ")
        sql.append(table.name())
        sql.append(" SET ")

        updateColumns.forEachIndexed { index, column ->
            if (argsValues[index] == null) {
                sql.append(column.key())
                sql.append(" = COALESCE(?, ")
                sql.append(column.key())
                sql.append(")")
            } else {
                sql.append(column.key())
                sql.append(" = ?")
            }

            if (index < updateColumns.size - 1) sql.append(", ")
        }

        condition?.let {
            sql.append(" WHERE ")
            sql.append(it.first)
            argsValues.addAll(it.second)
        }

        return Pair(sql.toString(), argsValues)
    }
}
