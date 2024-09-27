package query

class Query(val sql: String, vararg args: Any?) {
    val args = args.flatMap {
        if (it is List<*>) it else listOf(it)
    }

    fun sqlArgs(): Pair<String, List<Any?>> = Pair(sql, args)

    override fun toString(): String = "SQL = $sql;\n ARGS = $args;"

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
