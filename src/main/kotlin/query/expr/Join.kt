package query.expr

import query.schema.Column

enum class JoinType {
    LEFT,
    RIGHT,
    INNER,
    OUTER,
    FULL,
}

typealias Clause = Triple<JoinType, Column<*>, Column<*>>

class Join {
    private var joinClause = StringBuilder()

    infix fun <T : Any> Column<T>.left(other: Column<*>) = infixClause(Triple(JoinType.LEFT, this, other))

    infix fun <T : Any> Column<T>.inner(other: Column<*>) = infixClause(Triple(JoinType.INNER, this, other))

    infix fun <T : Any> Column<T>.right(other: Column<*>) = infixClause(Triple(JoinType.RIGHT, this, other))

    infix fun <T : Any> Column<T>.outer(other: Column<*>) = infixClause(Triple(JoinType.OUTER, this, other))

    infix fun <T : Any> Column<T>.full(other: Column<*>) = infixClause(Triple(JoinType.FULL, this, other))

    private val infixClause: (clause: Clause) -> Unit = { clause ->
        joinClause.append(
            when (clause.first) {
                JoinType.LEFT -> " LEFT JOIN "
                JoinType.RIGHT -> " RIGHT JOIN "
                JoinType.INNER -> " INNER JOIN "
                JoinType.OUTER -> " OUTER JOIN "
                JoinType.FULL -> " FULL JOIN "
            }
        )

        joinClause.append(clause.second.table().tableName)
        joinClause.append(" ON ")
        joinClause.append(clause.second.table().tableName).append(".").append(clause.second.key())
        joinClause.append(" = ")
        joinClause.append(clause.third.table().tableName).append(".").append(clause.third.key())
    }

    fun joinClauses(): String = joinClause.toString()
}
