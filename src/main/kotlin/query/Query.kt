package query

import java.sql.Connection
import java.sql.Statement

class Query(val sql: String, vararg args: Any?) {
    val args = args.flatMap {
        if (it is List<*>) it else listOf(it)
    }

    fun sqlArgs(): Pair<String, List<Any?>> = Pair(sql, args)

    fun exec(conn: Connection): Result<Unit> =
        runCatching {
            conn.prepareStatement(sql).use { stmt ->
                stmt.setParameters(args)
                stmt.executeUpdate()
                Unit
            }
        }.onFailure {
            error(
                Exception(
                    "An error occurred while executing the query:\n [SQL] $sql; [ARGS] $args;",
                    it
                )
            )
        }

    fun persist(conn: Connection): Result<Int> =
        runCatching {
            conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
                stmt.setParameters(args)
                stmt.executeUpdate()

                stmt.generatedKeys.use { rs ->
                    if (rs.next()) rs.getInt(1).takeIf { !rs.wasNull() } ?: 0
                    else 0
                }
            }
        }.onFailure {
            error(
                Exception(
                    "An error occurred while persisting the query and retrieving generated keys:\n [SQL] $sql; [ARGS] $args",
                    it
                )
            )
        }

    inline fun <reified R> get(conn: Connection, mapper: (Row) -> R): Result<R> =
        runCatching {
            conn.prepareStatement(sql).use { stmt ->
                stmt.setParameters(args)
                val rs = stmt.executeQuery()
                val row = Row(rs)
                mapper(row)
            }
        }.onFailure {
            error(
                Exception(
                    "An error occurred while selecting the data:\n [SQL] $sql; [ARGS] $args",
                    it
                )
            )
        }

    inline fun <reified R> many(conn: Connection, mapper: (Row) -> R): Result<List<R>> =
        runCatching {
            conn.prepareStatement(sql).use { stmt ->
                stmt.setParameters(args)

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
                Exception(
                    "An error occurred while selecting the data:\n [SQL] $sql; [ARGS] $args",
                    it
                )
            )
        }

    fun toJsonStr(): String = "{sql:\"$sql\", args:$args}"

    override fun toString(): String = "SQL = $sql; ARGS = $args;"

    override fun hashCode(): Int {
        var result = sql.hashCode()
        result = 31 * result + args.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Query

        if (sql != other.sql) return false
        if (args != other.args) return false

        return true
    }
}