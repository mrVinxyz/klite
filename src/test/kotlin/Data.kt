import query.schema.Table
import java.sql.Connection

object Accounts : Table("account") {
    val accountId = integer("id").setPrimaryKey()
    val balance = decimal("balance")
    val interestRate = float("interest_rate")
    val accountType = text("type")
    val isActive = boolean("is_active")
    val createdAt = long("created_at")
    val lastUpdatedAt = long("last_updated_at")
}

data class Account(
    val accountId: Int? = null,
    val balance: Double? = null,
    val interestRate: Float? = null,
    val accountType: String? = null,
    val isActive: Boolean? = null,
    val createdAt: Long? = null,
    val lastUpdatedAt: Long? = null
) {
    fun setBalance(value: Double) = this.copy(balance = value)
}

object Transaction : Table("transaction", false) {
    val transactionId = integer("transaction_id").setPrimaryKey()
    val transactionTo = text("transaction_to")
    val transactionFrom = text("transaction_from")
    val transactionAmount = decimal("transaction_amount")
    val transactionFee = double("transaction_fee")
    val transactionType = column<String>("transaction_type")
}

fun create(conn: Connection) {
    conn.createStatement().use { stmt ->
        stmt.execute("""""".trimIndent())
    }
}

fun feedUserTable(conn: Connection) {
    conn.createStatement().use { stmt ->
        stmt.execute("""""".trimIndent())
    }
}