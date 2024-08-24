package query

import java.sql.Connection

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

        condition?.let { cond ->
            cond.first.takeIf { it.isNotEmpty() }?.let {
                sql.append(" WHERE ")
                sql.append(it)
                argsValues.addAll(cond.second)
            }
        }

        return Pair(sql.toString(), argsValues)
    }
}

typealias UpdateResult = Result<Unit>

fun Updater.persist(conn: Connection): UpdateResult {
    return runCatching {
            val (sql, args) = sqlArgs()
            conn.prepareStatement(sql).use { stmt ->
                setParameters(stmt, args)
                stmt.executeUpdate()
            }

            Unit
        }
        .onFailure { Result.failure<Unit>(Exception("Failed to execute update operation: [$it]")) }
}
