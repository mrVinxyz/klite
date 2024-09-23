package query.expr

import query.schema.Column

enum class OrderType {
    ASC,
    DESC,
}

class OrderBy {
    private val columnsBy = mutableListOf<Pair<Column<*>, OrderType>>()

    fun <T : Any> Column<T>.asc() {
        columnsBy.add(this to OrderType.ASC)
    }

    fun <T : Any> Column<T>.desc() {
        columnsBy.add(this to OrderType.DESC)
    }

    fun orderByClause(): String {
        return columnsBy.joinToString(", ") { (column, orderType) ->
            "${column.key()} ${orderType.name}"
        }
    }
}
