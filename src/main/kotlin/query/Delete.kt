package query

import java.sql.Connection
import kotlin.use

class Deleter(private val table: Table) {
    private val argsValues = mutableListOf<Any?>()
    private var condition: WhereArgs? = null

    fun deleteWhere(init: Where.() -> Unit): Deleter {
        val where = Where()
        init(where)

        condition = where.clausesArgs()

        return this
    }

    fun <T : Any> deletePrimary(value: T): Deleter {
        val primaryKey = table.primaryKey<T>() ?: error("Primary key not found")

        return deleteWhere { primaryKey equal value }
    }

    fun sqlArgs(): Pair<String, List<Any?>> {
        val sql = StringBuilder()

        sql.append("DELETE FROM ")
        sql.append(table.name())

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

fun Table.deleteWhere(init: Where.() -> Unit): Deleter {
    return Deleter(this).deleteWhere(init)
}

fun Table.deletePrimary(value: Any): Deleter {
    return Deleter(this).deletePrimary(value)
}

typealias DeleteResult = Result<Unit>

fun Deleter.persist(conn: Connection): DeleteResult {
    return runCatching {
        val (sql, args) = sqlArgs()
        conn.prepareStatement(sql).use { stmt ->
            setParameters(stmt, args)
            stmt.executeUpdate()
        }

        Unit
    }
        .onFailure { Result.failure<Unit>(Exception("Failed to execute delete operation: [$it]")) }
}
