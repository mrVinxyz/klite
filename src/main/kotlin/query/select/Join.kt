package query.select

import query.table.Column
import query.table.Table

enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    OUTER,
    FULL,
}

class Join(
    private val mainTable: Table,
    private val joinTable: Table,
    private val leftColumn: Column<*>,
    private val rightColumn: Column<*>,
    private var joinType: JoinType,
) {
    private var joinClause = StringBuilder()

    fun joinClauses(): String {
        when (joinType) {
            JoinType.INNER -> joinClause.append("INNER JOIN ")
            JoinType.LEFT -> joinClause.append("LEFT JOIN ")
            JoinType.RIGHT -> joinClause.append("RIGHT JOIN ")
            JoinType.OUTER -> joinClause.append("OUTER JOIN ")
            JoinType.FULL -> joinClause.append("FULL JOIN ")
        }

        joinClause.append(joinTable.name())
        joinClause.append(" ON ")

        joinClause.append(mainTable.name())
        joinClause.append(".")
        joinClause.append(leftColumn.key())

        joinClause.append(" = ")
        joinClause.append(joinTable.name())
        joinClause.append(".")
        joinClause.append(rightColumn.key())

        return joinClause.toString()
    }
}
