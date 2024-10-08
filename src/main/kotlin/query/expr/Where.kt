package query.expr

import query.schema.Column

typealias WhereArgs = Pair<String, List<Any>>

fun WhereArgs.intoSqlArgs(sqlBuilder: StringBuilder, argsList: MutableList<Any>) {
    this.let { cond ->
        cond.first
            .takeIf { it.isNotEmpty() }
            ?.let {
                sqlBuilder.append(" WHERE ")
                sqlBuilder.append(it)
                argsList.addAll(cond.second)
            }
    }
}

class Where {
    private val clauses = StringBuilder()
    private val args = mutableListOf<Any>()

    val whereClause: () -> String = {
        clauses.toString().takeIf { it.isNotEmpty() }?.let {
            StringBuilder()
                .append(" WHERE ")
                .append(it)
                .toString()
        } ?: ""
    }

    infix fun <T : Any> Column<T>.eq(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append(this.key())
            clauses.append(" = ?")
            args.add(value)
        }
    }

    infix fun <T : Any> Column<T>.like(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append(this.key())
            clauses.append(" LIKE ?")
            args.add(value)
        }
    }

    infix fun <T : Any> Column<T>.likeStarts(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append(this.key())
            clauses.append(" LIKE ?")
            args.add("$value%")
        }
    }

    infix fun <T : Any> Column<T>.likeEnds(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append(this.key())
            clauses.append(" LIKE ?")
            args.add("%$value")
        }
    }

    infix fun <T : Any> Column<T>.likeContains(value: T?) {
        value?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append(this.key())
            clauses.append(" LIKE ?")
            args.add("%$value%")
        }
    }

    infix fun <T : Comparable<T>> Column<T>.between(range: Pair<Any?, Any?>?) {
        if (range != null) {
            val (min, max) = range
            if (min != null && max != null) {
                if (clauses.isNotEmpty()) {
                    clauses.append(" AND ")
                }
                clauses.append(this.key())
                clauses.append(" BETWEEN ? AND ?")
                args.add(min)
                args.add(max)
            }
        }
    }

    fun whereClauses(): WhereArgs = Pair(whereClause(), args)
}