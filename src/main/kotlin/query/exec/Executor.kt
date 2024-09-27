package query.exec

import query.Query
import query.Row
import query.Rows
import query.setParameters
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Statement
import kotlin.use

class Executor(val conn: Connection, val query: Query) {
    fun exec(): Result<Unit> =
        runCatching {
            conn.prepareStatement(query.sql).use { stmt ->
                stmt.setParameters(query.args)
                stmt.executeUpdate()
                Unit
            }
        }.onFailure {
            error(
                "An error occurred while executing the query:\n [SQL] $query.sql; [ARGS] $query.args; ${it.message}; ${it.message};"
            )
        }

    inline fun <reified T> execReturn(): Result<T> =
        runCatching {
            conn.prepareStatement(query.sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
                stmt.setParameters(query.args)
                stmt.executeUpdate()

                stmt.generatedKeys.use { rs ->
                    when (T::class) {
                        Int::class -> rs.getInt(1) as T
                        String::class -> {
                            val value = rs.getString(1)
                            (value ?: "") as T
                        }

                        Long::class -> rs.getLong(1) as T
                        Float::class -> rs.getFloat(1) as T
                        Double::class -> rs.getDouble(1) as T
                        BigDecimal::class -> {
                            val value = rs.getBigDecimal(1)
                            (value ?: BigDecimal.ZERO) as T
                        }

                        Boolean::class -> rs.getBoolean(1) as T
                        else -> error("Unsupported type: ${T::class};")
                    }
                }
            }
        }.onFailure {
            error(
                "An error occurred while persisting a record:\n [SQL] $query.sql; [ARGS] $query.args; ${it.message};"
            )
        }

    inline fun <reified R> execMapOne(mapper: (Row) -> R): Result<R> =
        runCatching {
            conn.prepareStatement(query.sql).use { stmt ->
                stmt.setParameters(query.args)
                val rs = stmt.executeQuery()
                val row = Row(rs)
                mapper(row)
            }
        }.onFailure {
            error(
                "An error occurred while selecting one record:\n [SQL] $query.sql; [ARGS] $query.args; ${it.message};",
            )
        }

    inline fun <reified R> execMapList(mapper: (Row) -> R): Result<List<R>> =
        runCatching {
            conn.prepareStatement(query.sql).use { stmt ->
                stmt.setParameters(query.args)

                val rs = stmt.executeQuery()
                val rows = Rows(rs).iterator()

                val resultList = mutableListOf<R>()
                while (rows.hasNext()) {
                    val row = rows.next()
                    resultList.add(mapper(row))
                }

                resultList
            }
        }.onFailure {
            error(
                "An error occurred while selecting many records:\n [SQL] $query.sql; [ARGS] $query.args; ${it.message};"
            )
        }
}
