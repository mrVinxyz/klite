package query.expr

import query.Query
import query.exec.Executor
import query.schema.Column
import query.schema.Table
import java.sql.Connection

class Filter(val table: Table) {
    private var selectConditions = mutableListOf<Query>()

    infix fun <T : Any> Column<T>.exists(value: T?) {
        val existsQuery = Query(
            "SELECT EXISTS(SELECT 1 FROM ${table.tableName} WHERE ${this.key()} = ? LIMIT 1)",
            value
        )
        selectConditions.add(existsQuery)
    }

    fun intoSqlArgs(): Query {
        return Query(selectConditions.joinToString(" AND ") { it.sql })
    }

    fun execAll(conn: Connection): Result<Boolean> {
        selectConditions.forEach {
            Executor(conn, intoSqlArgs()).execReturn<Boolean>().fold(
                onSuccess = {
                    if (it == false) Result.success(false)
                },
                onFailure = { return Result.failure(it) }
            )
        }

        return Result.success(true)
    }
}