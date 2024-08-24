package query

class Update(private val table: Table) {
    private val updateColumns = mutableListOf<Column<*>>()
    private val argsValues = mutableListOf<Any?>()
    private var condition: WhereArgs? = null

    fun update(init: (Update) -> Unit): Update {
        return Update(table).apply(init)
    }

    operator fun <T : Any> set(column: Column<*>, value: T?): Update {
        updateColumns.add(column)
        argsValues.add(value)

        return this
    }

    fun where(init: Where.() -> Unit): Update {
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
