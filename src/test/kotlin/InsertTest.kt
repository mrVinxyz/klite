import query.Query
import query.expr.persist
import query.schema.insert
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InsertTest {
    val logSqlArgs: (Query) -> Unit = {
        println(it.toString())
        println("[HashCode] ${it.hashCode()}")
        println(it.toJsonStr())
    }

    @Test
    fun `test insert with lambda block`() {
        val account = Account(
            accountId = 1,
            balance = 100.50,
            interestRate = 3.5f,
            accountType = "savings",
            isActive = true,
            createdAt = 1630425600000L,
            lastUpdatedAt = 1633117600000L
        )

        val query = Accounts.insert {
            it[Accounts.accountId] = account.accountId
            it[Accounts.balance] = account.balance
            it[Accounts.interestRate] = account.interestRate
            it[Accounts.accountType] = account.accountType
            it[Accounts.isActive] = account.isActive
            it[Accounts.createdAt] = account.createdAt
            it[Accounts.lastUpdatedAt] = account.lastUpdatedAt
        }.intoSqlArgs()

        logSqlArgs(query)

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

        assertTrue(assertQuery.equals(query))
    }

    @Test
    fun `test insert init block with values with null values`() {}


    @Test
    fun `test insert persist method`() {
        lateinit var conn: Connection
        val account = Account(
            accountId = 1,
            balance = 100.50,
            interestRate = 3.5f,
            accountType = "savings",
            isActive = true,
            createdAt = 1630425600000L,
            lastUpdatedAt = 1633117600000L
        )

        val createdId = Accounts.insert {
            it[Accounts.accountId] = account.accountId
            it[Accounts.balance] = account.balance
            it[Accounts.interestRate] = account.interestRate
            it[Accounts.accountType] = account.accountType
            it[Accounts.isActive] = account.isActive
            it[Accounts.createdAt] = account.createdAt
            it[Accounts.lastUpdatedAt] = account.lastUpdatedAt
        }.persist(DB().conn()).getOrThrow()

        assertEquals(createdId, account.accountId)
    }
}