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
    private var joinType: JoinType,
) {
    private lateinit var leftColumn: Column<*>
    private lateinit var rightColumn: Column<*>
    private var joinClause = StringBuilder()

    infix fun <T : Any> Column<T>.on(other: Column<*>) {
        if (this.getTable() == mainTable && other.getTable() == joinTable) {
            leftColumn = this
            rightColumn = other
        } else if (this.getTable() == joinTable && other.getTable() == mainTable) {
            leftColumn = other
            rightColumn = this
        }
    }

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
