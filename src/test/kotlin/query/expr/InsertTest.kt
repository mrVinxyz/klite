package query.expr

import Account
import Accounts
import DB
import Transaction
import createAccountTable
import query.Executor
import query.Query
import query.schema.insert
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InsertTest {
    fun Accounts.create(account: Account): Query {
        return insert {
            it[accountId] = account.accountId
            it[balance] = account.balance
            it[interestRate] = account.interestRate
            it[accountType] = account.accountType
            it[createdAt] = account.createdAt
            it[isActive] = account.isActive
        }.intoSqlArgs()
    }

    val account = Account(
        accountId = 1,
        balance = 100.50,
        interestRate = 3.5f,
        accountType = "savings",
        isActive = true,
        createdAt = 1630425600000L,
        lastUpdatedAt = 1633117600000L
    )

    val transaction = Transaction(
        transactionId = 1,
        transactionTo = "@mrvin123",
        transactionFrom = "@acc456",
        transactionAmount = 100.50,
        transactionFee = 0.0,
        transactionType = "withdrawal",
    )

    @Test
    fun `test insert with lambda block`() {
        val query = Accounts.create(account)
        val assertQuery = Query(
            "INSERT INTO account (account_id, account_balance, account_interest_rate, account_type, account_is_active, account_created_at, account_last_updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            listOf(
                account.accountId,
                account.balance,
                account.interestRate,
                account.accountType,
                account.isActive,
                account.createdAt,
                account.lastUpdatedAt
            )
        )

        assertTrue(assertQuery == query)
    }

    @Test
    fun `test insert init block with values with null values`() {
        val accountNull = account.copy(createdAt = null, lastUpdatedAt = null)

        val query = Accounts.create(accountNull)

        val assertQuery = Query(
            "INSERT INTO account (account_id, account_balance, account_interest_rate, account_type, account_is_active) VALUES (?, ?, ?, ?, ?)",
            listOf(
                account.accountId,
                account.balance,
                account.interestRate,
                account.accountType,
                account.isActive,
            )
        )

        assertTrue(assertQuery == query)
    }

    @Test
    fun `test insert persist method`() {
        val db = DB()
        Accounts.createAccountTable(db.conn())

        val query = Accounts.create(account)
        val result = Executor(db.conn(), query).execReturn<Int>()

        assertTrue(result.isSuccess)
        assertEquals(result.getOrThrow(), 1)
    }

//    @Test
//    fun `test insert unique filter`() {
//        val db = DB()
//        Accounts.createAccountTable(db.conn())
//        Accounts.create(account)
//
//        Transactions.createTransactionTable(db.conn())
//
//        val createFilterUnique: (transaction: Transaction, accountId: Int) -> Insert = { transaction, accountId ->
//            Transactions.insert {
//                it[Transactions.transactionId] = transaction.transactionId
//                it[Transactions.transactionAmount] = transaction.transactionAmount
//                it[Transactions.transactionType] = transaction.transactionType
//                it[Transactions.transactionTo] = transaction.transactionTo
//                it[Transactions.transactionFrom] = transaction.transactionFrom
//                it[Transactions.transactionFee] = transaction.transactionFee
//            }.filter {
//                Accounts.accountId exists accountId
//            }
//        }
//
//        val filterQuery = createFilterUnique(transaction, account.accountId!!)
//    }
}