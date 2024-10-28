package query.expr

import query.schema.Column
import query.schema.Table

class Join(mainTable: Table) {
    enum class JoinType {
        LEFT,
        RIGHT,
        INNER,
        OUTER,
        FULL,
    }

    private var joinClause = StringBuilder()

    infix fun <T : Any> Column<T>.left(other: Column<*>) = infixClause(Triple(JoinType.LEFT, this, other))

    infix fun <T : Any> Column<T>.inner(other: Column<*>) = infixClause(Triple(JoinType.INNER, this, other))

    infix fun <T : Any> Column<T>.right(other: Column<*>) = infixClause(Triple(JoinType.RIGHT, this, other))

    infix fun <T : Any> Column<T>.outer(other: Column<*>) = infixClause(Triple(JoinType.OUTER, this, other))

    infix fun <T : Any> Column<T>.full(other: Column<*>) = infixClause(Triple(JoinType.FULL, this, other))

    private val infixClause: (clause: Triple<JoinType, Column<*>, Column<*>>) -> Unit = { clause ->
        val leftTable = if (clause.second.table() == mainTable) clause.second else clause.third
        val rightTable = if (clause.second.table() != mainTable) clause.second else clause.third

        // Append the JOIN type (LEFT, RIGHT, INNER, OUTER, FULL)
        joinClause.append(
            when (clause.first) {
                JoinType.LEFT -> " LEFT JOIN "
                JoinType.RIGHT -> " RIGHT JOIN "
                JoinType.INNER -> " INNER JOIN "
                JoinType.OUTER -> " OUTER JOIN "
                JoinType.FULL -> " FULL JOIN "
            }
        )

        // Append the correct tables and columns for the ON clause
        joinClause.append(rightTable.table().tableName)
        joinClause.append(" ON ")
        joinClause.append(leftTable.table().tableName).append(".").append(leftTable.key())
        joinClause.append(" = ")
        joinClause.append(rightTable.table().tableName).append(".").append(rightTable.key())
    }

    fun joinClauses(): String = joinClause.toString()
}
