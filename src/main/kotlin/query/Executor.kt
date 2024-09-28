package query

import java.sql.Connection
import java.sql.Statement

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
                "An error occurred while executing:\n[SQL] ${query.sql}\n[ARGS] ${query.args}\n${it.message};"
            )
        }

    inline fun <reified T : Any> execReturn(): Result<T> =
        runCatching {
            conn.prepareStatement(query.sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
                stmt.setParameters(query.args)
                stmt.executeUpdate()

                stmt.generatedKeys.use { rs ->
                    Row(rs).get<T>(1) as T
                }
            }
        }.onFailure {
            error(
                "An error occurred while executing and returning:\n[SQL] ${query.sql}\n[ARGS] ${query.args}\n${it.message};"
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
                "An error occurred while executing and mapping one:\n[SQL] ${query.sql}\n[ARGS] ${query.args}\n${it.message};",
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
                "An error occurred while executing and mapping list:\n[SQL] ${query.sql}\n[ARGS] ${query.args}\n${it.message};"
            )
        }
}