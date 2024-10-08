import query.Executor
import query.Row
import query.exec
import query.schema.Table
import query.schema.createTable
import java.math.BigDecimal
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
    val balance: BigDecimal? = null,
    val interestRate: Float? = null,
    val accountType: String? = null,
    val isActive: Boolean? = null,
    val createdAt: Long? = null,
    val lastUpdatedAt: Long? = null,
)

fun Accounts.createAccountTable(conn: Connection) =
    this.createTable().exec(conn)

object Transactions : Table("account_transaction", false) {
    val transactionId = integer("transaction_id").setPrimaryKey()
    val transactionTo = text("transaction_to")
    val transactionFrom = text("transaction_from")
    val transactionAmount = decimal("transaction_amount")
    val transactionFee = double("transaction_fee")
    val transactionType = column<String>("transaction_type")
}

data class Transaction(
    val transactionId: Int? = null,
    val transactionTo: String? = null,
    val transactionFrom: String? = null,
    val transactionAmount: Double? = null,
    val transactionFee: Double? = null,
    val transactionType: String? = null,
)

fun Transactions.createTransactionTable(conn: Connection) =
    this.createTable().exec(conn)
