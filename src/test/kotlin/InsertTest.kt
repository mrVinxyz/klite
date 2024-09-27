import query.Query
import query.schema.insert
import kotlin.test.Test
import kotlin.test.assertTrue

fun Accounts.create(account: Account): Query {
    return insert {
        it[accountId] = account.accountId
        it[balance] = account.balance
        it[interestRate] = account.interestRate
        it[accountType] = account.accountType
        it[isActive] = account.isActive
        it[createdAt] = account.createdAt
        it[lastUpdatedAt] = account.lastUpdatedAt
    }.intoSqlArgs()
}

fun Accounts.createFilterUnique(account: Account): Query {
    return insert {
        it[accountId] = account.accountId
        it[balance] = account.balance
        it[interestRate] = account.interestRate
        it[accountType] = account.accountType
        it[isActive] = account.isActive
        it[createdAt] = account.createdAt
        it[lastUpdatedAt] = account.lastUpdatedAt
    }.filter {
        accountId exists account.accountId
        val query = intoSqlArgs()
        println(query.toString())
    }.intoSqlArgs()
}

class InsertTest {
    val logSqlArgs: (Query) -> Unit = {
        println(it.toString())
        println("HashCode = ${it.hashCode()}")
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
        val account = Account(
            accountId = 1,
            balance = 100.50,
            interestRate = 3.5f,
            accountType = "savings",
            isActive = true,
            createdAt = null,
            lastUpdatedAt = null
        )

        val query = Accounts.create(account)

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

    fun `test insert filter`() {
        val account = Account(
            accountId = 1,
            balance = 100.50,
            interestRate = 3.5f,
            accountType = "savings",
            isActive = true,
            createdAt = null,
            lastUpdatedAt = null
        )

        val query = Accounts.createFilterUnique(account)
    }


    @Test
    fun `test insert persist method`() {

    }
}