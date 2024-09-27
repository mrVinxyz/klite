package query.expr

import query.Query
import query.schema.Column
import query.schema.Table

class Filter(val table: Table) {
    private var selectConditions = mutableListOf<Query>()

    infix fun <T : Any> Column<T>.exists(value: T?) {
        val existsQuery = Query(
            "SELECT EXISTS(SELECT 1 FROM ${table.tableName} WHERE ${this.key()} = ? LIMIT 1)",
            value
        )
        selectConditions.add(existsQuery)
    }
}