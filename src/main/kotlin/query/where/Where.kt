package query.where

import query.table.Column

typealias WhereArgs = Pair<String, List<Any>>

class Where {
    private val clauses = StringBuilder()
    private val args = mutableListOf<Any>()

    infix fun <T : Any> Column<T>.equal(value: T) {
        if (clauses.isNotEmpty()) {
            clauses.append(" AND ")
        }
        clauses.append("${this.key()} = ?")
        args.add(value)
    }

    infix fun <T : Any> Column<T>.like(value: T) {
        if (clauses.isNotEmpty()) {
            clauses.append(" AND ")
        }
        clauses.append("${this.key()} LIKE ?")
        args.add(value)
    }

    infix fun <T : Any> Column<T>.likeStarts(value: T) {
        if (clauses.isNotEmpty()) {
            clauses.append(" AND ")
        }
        clauses.append("${this.key()} LIKE ?")
        args.add("$value%")
    }

    infix fun <T : Any> Column<T>.likeEnds(value: T) {
        if (clauses.isNotEmpty()) {
            clauses.append(" AND ")
        }
        clauses.append("${this.key()} LIKE ?")
        args.add("%$value")
    }

    infix fun <T : Any> Column<T>.likeContains(value: T) {
        if (clauses.isNotEmpty()) {
            clauses.append(" AND ")
        }
        clauses.append("${this.key()} LIKE ?")
        args.add("%$value%")
    }

    fun <T : Any> fn(column: Column<T>, fnName: String): Column<T> {
        return Column("${fnName}(${column.key()})", column.type())
    }

    fun clausesArgs(): WhereArgs = Pair(clauses.toString(), args)
}
