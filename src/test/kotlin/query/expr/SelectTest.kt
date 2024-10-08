package query.expr

import query.schema.select
import kotlin.test.Test
import kotlin.test.assertEquals

class SelectTest {
    @Test
    fun `test basic select query`() {
        val query = Select(Accounts)
            .select(Accounts.accountId, Accounts.balance)
            .sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account"
        val expectedArgs = emptyList<Any>()

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test basic select query with lambda block`() {
        val query = Accounts.select {
            +Accounts.accountId
            +Accounts.balance
        }
            .sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account"
        val expectedArgs = emptyList<Any>()

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test basic select query with where clause`() {
        val query = Select(Accounts)
            .select(Accounts.accountId, Accounts.balance)
            .where {
                Accounts.accountId eq 1
            }
            .sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account WHERE account_id = ?"

        val expectedArgs = listOf(1)

        // Compare the generated query with the expected result
        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select query with multiple where clause`() {
        val query = Select(Accounts)
            .select(Accounts.accountId, Accounts.balance)
            .where {
                Accounts.accountId eq 1
                Accounts.balance between (1000 to 5000)
            }
            .sqlArgs()

        val expectedSql =
            "SELECT account_id, account_balance FROM account WHERE account_id = ? AND account_balance BETWEEN ? AND ?"
        val expectedArgs = listOf(1, 1000, 5000)

        // Compare the generated query with the expected result
        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select primary clause`() {
        val query = Select(Accounts)
            .selectPrimary(1) {
                +Accounts.accountId
                +Accounts.balance
            }.sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account WHERE account_id = ?"
        val expectedArgs = listOf(1)
        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select primary clause with extra where clauses`() {
        val query = Select(Accounts)
            .selectPrimary(1) {
                +Accounts.accountId
                +Accounts.balance
            }.where {
                Accounts.balance between (1000 to 5000)
            }.sqlArgs()

        val expectedSql =
            "SELECT account_id, account_balance FROM account WHERE account_id = ? AND account_balance BETWEEN ? AND ?"
        val expectedArgs = listOf(1, 1000, 5000)
        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select query with join on real transaction table`() {
        val query = Select(Accounts)
            .select(Accounts.accountId, Accounts.balance, Transactions.transactionId, Transactions.transactionAmount)
            .join {
                Transactions.transactionFrom inner Accounts.accountId
                //Accounts.accountId inner Transactions.transactionFrom
            }
            .where {
                Accounts.accountId eq 1
            }
            .sqlArgs()

        val expectedSql =
            "SELECT account_id, account_balance, transaction_id, transaction_amount FROM account INNER JOIN account_transaction ON account.account_id = account_transaction.transaction_from WHERE account_id = ?"

        val expectedArgs = listOf(1)

        // Compare the generated query with the expected result
        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }
}