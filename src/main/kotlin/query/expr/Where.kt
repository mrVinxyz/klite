package query.expr

import query.schema.Column

typealias WhereArgs = Pair<String, List<Any>>

class Where {
    private val clauses = StringBuilder() // SQL Where clause
    private val args = mutableListOf<Any>() // List of arguments

    // Grouping support
    private var groupLevel = 0 // Track the nesting level of parentheses
    private var isFirstInGroup = true // Track if we're the first in a group
    private var isInOrBlock = false // Track if we're inside an OR block

    // Helper function to build the WHERE clause
    private val whereClause: () -> String = {
        clauses.toString().takeIf { it.isNotEmpty() }?.let {
            StringBuilder()
                .append(" WHERE ")
                .append(it)
                .toString()
        } ?: ""
    }

    // Helper function to add operator clauses
    private fun addClause(column: Column<*>, operator: String, value: Any?) {
        if (clauses.isNotEmpty() && !isFirstInGroup) {
            clauses.append(if (isInOrBlock && groupLevel == 1) " OR " else " AND ")
        }
        clauses.append(column.key())
        clauses.append(" $operator")
        if (value != null) {
            clauses.append(" ?")
            args.add(value)
        }
        isFirstInGroup = false
    }

    // Comparison Operators
    infix fun <T : Any> Column<T>.eq(value: T?) {
        value?.let { addClause(this, "=", it) }
    }

    infix fun <T : Any> Column<T>.neq(value: T?) {
        value?.let { addClause(this, "<>", it) }
    }

    infix fun <T : Comparable<T>> Column<T>.lt(value: T?) {
        value?.let { addClause(this, "<", it) }
    }

    infix fun <T : Comparable<T>> Column<T>.gt(value: T?) {
        value?.let { addClause(this, ">", it) }
    }

    infix fun <T : Comparable<T>> Column<T>.lte(value: T?) {
        value?.let { addClause(this, "<=", it) }
    }

    infix fun <T : Comparable<T>> Column<T>.gte(value: T?) {
        value?.let { addClause(this, ">=", it) }
    }

    // Logical Operators
    fun or(init: Where.() -> Unit) {
        if (clauses.isNotEmpty() && !isFirstInGroup) {
            clauses.append(" AND ")
        }
        startGroup()
        val previousIsInOrBlock = isInOrBlock
        isInOrBlock = true
        init(this)
        isInOrBlock = previousIsInOrBlock
        endGroup()
    }

    fun and(init: Where.() -> Unit) {
        if (clauses.isNotEmpty() && !isFirstInGroup) {
            clauses.append(if (isInOrBlock && groupLevel == 1) " OR " else " AND ")
        }
        startGroup()
        val previousIsInOrBlock = isInOrBlock
        isInOrBlock = false
        init(this)
        isInOrBlock = previousIsInOrBlock
        endGroup()
    }

    fun not(init: Where.() -> Unit) {
        if (clauses.isNotEmpty() && !isFirstInGroup) {
            clauses.append(" AND ")
        }
        clauses.append("NOT ")
        startGroup()
        val previousIsInOrBlock = isInOrBlock
        isInOrBlock = false
        init(this)
        isInOrBlock = previousIsInOrBlock
        endGroup()
    }

    // Pattern Matching
    infix fun <T : Any> Column<T>.like(value: T?) {
        value?.let { addClause(this, "LIKE", it) }
    }

    infix fun <T : Any> Column<T>.notLike(value: T?) {
        value?.let { addClause(this, "NOT LIKE", it) }
    }

    infix fun <T : Any> Column<T>.likeStarts(value: T?) {
        value?.let { addClause(this, "LIKE", "$it%") }
    }

    infix fun <T : Any> Column<T>.likeEnds(value: T?) {
        value?.let { addClause(this, "LIKE", "%$it") }
    }

    infix fun <T : Any> Column<T>.likeContains(value: T?) {
        value?.let { addClause(this, "LIKE", "%$it%") }
    }

    infix fun <T : Comparable<T>> Column<T>.between(range: Pair<T?, T?>?) {
        range?.let { (min, max) ->
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

    // List Operators
    infix fun <T : Any> Column<T>.inList(values: List<T>?) {
        values?.takeIf { it.isNotEmpty() }?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append(this.key())
            clauses.append(" IN (")
            clauses.append(values.joinToString(", ") { "?" })
            clauses.append(")")
            args.addAll(values)
        }
    }

    infix fun <T : Any> Column<T>.notIn(values: List<T>?) {
        values?.takeIf { it.isNotEmpty() }?.let {
            if (clauses.isNotEmpty()) {
                clauses.append(" AND ")
            }
            clauses.append(this.key())
            clauses.append(" NOT IN (")
            clauses.append(values.joinToString(", ") { "?" })
            clauses.append(")")
            args.addAll(values)
        }
    }

    // Null Checks
    fun Column<*>.isNull() {
        addClause(this, "IS NULL", null)
    }

    fun Column<*>.isNotNull() {
        addClause(this, "IS NOT NULL", null)
    }

    // Helper functions for grouping
    private fun startGroup() {
        clauses.append("(")
        groupLevel++
        isFirstInGroup = true
    }

    private fun endGroup() {
        clauses.append(")")
        groupLevel--
        isFirstInGroup = false
    }

    // Helper functions for adding clauses
    fun whereClauses(): WhereArgs = Pair(whereClause(), args)
}

// Insert the where clause into the main SQL query
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