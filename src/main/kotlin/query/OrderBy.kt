package query

enum class OrderType(name: String) {
    ASC("ASC"),
    DESC("DESC"),
}

class OrderBy {
    private val columnsBy = mutableListOf<Pair<Column<*>, OrderType>>()

    fun <T : Any> Column<T>.asc() {
        columnsBy.add(this to OrderType.ASC)
    }

    fun <T : Any> Column<T>.desc() {
        columnsBy.add(this to OrderType.DESC)
    }

    override fun toString(): String {
        return columnsBy.joinToString(", ") { (column, orderType) ->
            "${column.key()} ${orderType.name}"
        }
    }
}