package query.expr

import query.Query
import query.execReturnKey
import query.schema.Column
import query.schema.Table
import java.sql.Connection

sealed class InsertResult {
    data class Success(
        val generatedId: Int? = null
    ) : InsertResult()

    data class FilterFailure(
        val error: FilterResult? = null,
    ) : InsertResult()

    data class DatabaseError(
        val exception: Exception
    ) : InsertResult()

    fun isSuccess() = this is Success
    fun isFilterFailure() = this is FilterFailure
    fun isDatabaseError() = this is DatabaseError

    fun getGeneratedIdOrNull() = (this as? Success)?.generatedId
    fun getErrorOrNull() = (this as? FilterFailure)?.error
    fun getExceptionOrNull() = (this as? DatabaseError)?.exception
}

class Insert(val table: Table) {
    private val insertColumns = mutableListOf<Column<*>>()
    private val args = mutableListOf<Any?>()
    internal var filter: Filter? = null

    fun insert(block: Insert.() -> Unit): Insert = apply(block)

    fun insert(vararg columns: Column<*>): Insert {
        insertColumns.addAll(columns)
        return this
    }

    infix fun <T : Any> Column<T>.to(value: T?) {
        insertColumns.add(this)
        args.add(value)
    }

    fun values(vararg value: Any?): Insert {
        require(value.size == insertColumns.size) { "Number of values must match number of columns" }
        args.addAll(value.toList())
        return this
    }

    fun values(map: Map<String, Any?>): Insert {
        insertColumns.forEach { column ->
            args.add(map[column.key()])
        }
        return this
    }

    fun filter(block: Filter.() -> Unit): Insert {
        filter = Filter(table)
        block(filter!!)
        return this
    }

    fun getFilter() = filter

    fun sqlArgs(): Query {
        require(insertColumns.isNotEmpty()) { "No columns specified for insert" }

        val sql = StringBuilder()

        sql.append("INSERT INTO ")
        sql.append(table.tableName)
        sql.append(" (")

        sql.append(insertColumns.joinToString(", ") { it.key() })

        sql.append(") VALUES (")
        sql.append(insertColumns.joinToString(", ") { "?" })
        sql.append(")")

        return Query(sql.toString(), args)
    }
}

fun Insert.persist(conn: Connection): InsertResult {
    filter?.let { ft ->
        val result = ft.execute(conn)
        if (!result.ok) {
            return InsertResult.FilterFailure(result)
        }
    }

    return try {
        val result = sqlArgs().execReturnKey(conn)
        when {
            result.isSuccess -> {
                val id = result.getOrNull()
                InsertResult.Success(id)
            }
            else ->  InsertResult.DatabaseError(result.exceptionOrNull() as java.lang.Exception)
        }
    } catch (e: Exception) {
        InsertResult.DatabaseError(e)
    }
}