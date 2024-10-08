package query.expr

import Account
import Accounts
import createAccountTable
import query.Query
import query.schema.insert
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals

class InsertTest {

    val myAccount = Account(
        accountId = 1,
        balance = BigDecimal("100"),
        interestRate = 3.5f,
        accountType = "SAVINGS",
        isActive = true,
        createdAt = System.currentTimeMillis(),
        lastUpdatedAt = null
    )

    @Test
    fun `test basic insert query with lambda block`() {
        fun Accounts.create(account: Account): Query =
            insert {
                accountId to account.accountId
                balance to account.balance
                interestRate to account.interestRate
                accountType to account.accountType
                createdAt to account.createdAt
                isActive to account.isActive
            }.sqlArgs()

        val account = myAccount.copy()
        val query = Accounts.create(account)

        val assertQuery = Query(
            "INSERT INTO account (account_id, account_balance, account_interest_rate, account_type, account_created_at, account_is_active) VALUES (?, ?, ?, ?, ?, ?)",
            listOf(
                account.accountId,
                account.balance,
                account.interestRate,
                account.accountType,
                account.createdAt,
                account.isActive,
            )
        )

        assertEquals(assertQuery, query)
    }

    @Test
    fun `test insert with varargs selection`() {
        val query = Accounts. insert(
            Accounts.accountId,
            Accounts.balance
        )
            .sqlArgs()

        val expectedQuery = Query(
            "INSERT INTO account (account_id, account_balance) VALUES (?, ?)",
        )

        assertEquals(expectedQuery, query)
    }

    @Test
    fun `test insert with manually specified columns and values`() {
        val query = Accounts.insert {
            +Accounts.accountId
            +Accounts.balance
            values("123", 1000.0)
        }
            .sqlArgs()

        val expectedQuery = Query(
            "INSERT INTO account (account_id, account_balance) VALUES (?, ?)",
            listOf("123", 1000.0)
        )

        assertEquals(expectedQuery, query)
    }

    @Test
    fun `test insert with map of values`() {
        val valuesMap = mapOf(
            "account_id" to "123",
            "account_balance" to 1000.0
        )

        val query = Accounts.insert {
            +Accounts.accountId
            +Accounts.balance
            values(valuesMap)
        }
            .sqlArgs()

        val expectedQuery = Query(
            "INSERT INTO account (account_id, account_balance) VALUES (?, ?)",
            listOf("123", 1000.0)
        )

        assertEquals(expectedQuery, query)
    }


    @Test
    fun `test persist insert`() {
        val account = myAccount.copy()

        val query = Accounts.insert {
            Accounts.accountId to account.accountId
            Accounts.balance to account.balance
            Accounts.interestRate to account.interestRate
            Accounts.accountType to account.accountType
            Accounts.createdAt to account.createdAt
            Accounts.isActive to account.isActive
        }

        val result = Database.transaction {
            Accounts.createAccountTable(it)

            query.persist(it)
        }

        assertEquals(Result.success(1), result)
    }

    @Test
    fun `test insert lazily`() {
        val account = myAccount.copy()

        val query = Accounts.insert {
            +Accounts.accountId
            +Accounts.balance
            +Accounts.interestRate
            +Accounts.accountType
            +Accounts.createdAt
        }

        val queryWithValues = query.values(
            mapOf(
                Accounts.accountId.key() to account.accountId,
                Accounts.balance.key() to account.balance,
                Accounts.interestRate.key() to account.interestRate,
                Accounts.accountType.key() to account.accountType,
                Accounts.createdAt.key() to account.createdAt,
            )
        )

        val result = Database.transaction {
            Accounts.createAccountTable(it)

            queryWithValues.persist(it)
        }

        assertEquals(Result.success(1), result)
    }
}