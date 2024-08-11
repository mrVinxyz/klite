package query.insert

import java.sql.Connection
import java.sql.Statement
import query.mapper.setParameters
import query.table.Column
import query.table.Table

class Inserter(private val table: Table) {

    private val insertColumns = mutableListOf<Column<*>>()
    private val args = mutableListOf<Any>()

    operator fun <T> set(column: Column<*>, value: T?): Inserter {
        value?.let { v ->
            insertColumns.add(column)
            args.add(v)
        }

        return this
    }

    fun insert(init: (Inserter) -> Unit): Inserter {
        val inserter = Inserter(table)
        init(inserter)

        return inserter
    }

    fun insert(vararg column: Column<*>): Inserter {
        insertColumns.addAll(column)
        return this
    }

    fun values(vararg value: Any?): Inserter {
        value.forEachIndexed { index, any ->
            if (any != null) args.add(any) else insertColumns.removeAt(index)
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

        return Pair(sql.toString(), args)
    }

    fun persist(conn: Connection): Result<Int> {
        return runCatching {
                val (sql, args) = sqlArgs()
                val stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
                setParameters(stmt, args)
                stmt.executeUpdate()

                val generatedId = stmt.generatedKeys.getInt(1)

                if (generatedId == 0) {
                    Result.failure<Unit>(Exception("Failed to insert record: [${sql}] [$args]"))
                }

                generatedId
            }
            .onFailure {
                Result.failure<Int>(Exception("Failed to execute insert operation: [$it]"))
            }
    }
}
