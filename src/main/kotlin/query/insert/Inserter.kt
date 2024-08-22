package query.insert

import java.sql.Connection
import java.sql.Statement
import query.mapper.setParameters
import query.table.Column
import query.table.Table

class Inserter(val table: Table) {
    private val insertColumns = mutableListOf<Column<*>>()
    private val argsValues = mutableListOf<Any>()

    fun insert(init: (Inserter) -> Unit): Inserter {
        return Inserter(table).apply(init)
    }

    fun insert(vararg column: Column<*>): Inserter {
        insertColumns.addAll(column)
        return this
    }

    operator fun <T> set(column: Column<*>, value: T?): Inserter {
        value?.let { v ->
            insertColumns.add(column)
            argsValues.add(v)
        }

        return this
    }

    fun values(vararg value: Any?): Inserter {
        val nullColumns = mutableListOf<Column<*>>()

        value.forEachIndexed { index, any ->
            if (any != null) argsValues.add(any) else nullColumns.add(insertColumns[index])
        }

        insertColumns.removeAll(nullColumns)

        return this
    }

    fun values(map: Map<String, Any?>): Inserter {

        insertColumns.forEach { column ->
            map[column.key()]?.let { value -> value.let { argsValues.add(value) } }
        }

        return this
    }

    fun sqlArgs(): Pair<String, List<Any>> {
        val sql = StringBuilder()

        sql.append("INSERT INTO ")
        sql.append(table.name())
        sql.append(" (")

        insertColumns.forEachIndexed { index, column ->
            sql.append(column.key())
            if (index < insertColumns.size - 1) sql.append(", ")
        }

        sql.append(") VALUES (")
        sql.append(insertColumns.joinToString(", ") { "?" })
        sql.append(")")

        return Pair(sql.toString(), argsValues)
    }
}

typealias InsertResult = Result<Map<String, Int>>

fun Inserter.persist(conn: Connection): InsertResult {
    return runCatching {
            val (sql, args) = sqlArgs()
            val stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
            setParameters(stmt, args)
            stmt.executeUpdate()

            val idName = this.table.primaryKey()?.key() ?: "id"
            val generatedId = stmt.generatedKeys.getInt(1)

            if (generatedId == 0) {
                Result.failure<Unit>(Exception("Failed to insert record: [${sql}] [$args]"))
            }

            mapOf(idName to generatedId)
        }
        .onFailure { Result.failure<Int>(Exception("Failed to execute insert operation: [$it]")) }
}
