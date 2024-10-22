package query.expr

import query.schema.select
import query.schema.selectPrimary
import kotlin.test.Test
import kotlin.test.assertEquals

class SelectTest {
    @Test
    fun `test basic select query`() {
        val query = Accounts
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
        }.sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account"
        val expectedArgs = emptyList<Any>()

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test basic select query with where clause`() {
        val query = Accounts
            .select(Accounts.accountId, Accounts.balance)
            .where { Accounts.accountId eq 1 }
            .sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account WHERE account_id = ?"
        val expectedArgs = listOf(1)

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select query with multiple where clause`() {
        val query = Accounts
            .select(Accounts.accountId, Accounts.balance)
            .where {
                Accounts.accountId eq 1
                Accounts.balance between (1000 to 5000)
            }
            .sqlArgs()

        val expectedSql =
            "SELECT account_id, account_balance FROM account WHERE account_id = ? AND account_balance BETWEEN ? AND ?"
        val expectedArgs = listOf(1, 1000, 5000)

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select primary clause`() {
        val query = Accounts
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
    fun `test select query with join`() {
        val queryA = Accounts
            .select(Accounts.accountId, Accounts.balance)
            .join(Transactions.transactionId, Transactions.transactionAmount) {
                Transactions.transactionFrom inner Accounts.accountId
            }
            .where { Accounts.accountId eq 1 }
            .sqlArgs()

        val queryB = Select(Accounts)
            .select(Accounts.accountId, Accounts.balance, Transactions.transactionId, Transactions.transactionAmount)
            .join { Transactions.transactionFrom inner Accounts.accountId }
            .where { Accounts.accountId eq 1 }
            .sqlArgs()

        val expectedSql =
            "SELECT account_id, account_balance, transaction_id, transaction_amount FROM account INNER JOIN account_transaction ON account.account_id = account_transaction.transaction_from WHERE account_id = ?"
        val expectedArgs = listOf(1)

        assertEquals(expectedSql, queryA.sql)
        assertEquals(expectedArgs, queryA.args)

        assertEquals(expectedSql, queryB.sql)
        assertEquals(expectedArgs, queryB.args)
    }

    @Test
    fun `test select query with multiple joins`() {
        val query = Accounts
            .select(Accounts.accountId, Transactions.transactionAmount)
            .join(Transactions.transactionId) {
                Transactions.transactionFrom inner Accounts.accountId
            }
            .join(Transactions.transactionTo) {
                Transactions.transactionTo left Accounts.accountId
            }
            .where { Accounts.accountId eq 1 }
            .sqlArgs()

        val expectedSql = "SELECT account_id, transaction_amount, transaction_id, transaction_to FROM account " +
                "INNER JOIN account_transaction ON account.account_id = account_transaction.transaction_from " +
                "LEFT JOIN account_transaction ON account.account_id = account_transaction.transaction_to " +
                "WHERE account_id = ?"
        val expectedArgs = listOf(1)

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select with order by clause`() {
        val query = Accounts
            .select(Accounts.accountId, Accounts.balance)
            .orderBy { Accounts.balance.desc() }.sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account ORDER BY account_balance DESC"
        val expectedArgs = emptyList<Any>()

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select with multiple order by columns`() {
        val query = Accounts
            .select(Accounts.accountId, Accounts.balance)
            .orderBy {
                Accounts.balance.desc()
                Accounts.accountId.asc()
            }
            .sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account ORDER BY account_balance DESC, account_id ASC"
        val expectedArgs = emptyList<Any>()

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select with limit and offset`() {
        val query = Accounts
            .select(Accounts.accountId, Accounts.balance)
            .limit(10)
            .offset(20)
            .sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account LIMIT ? OFFSET ?"
        val expectedArgs = listOf(10, 20)

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select with pagination`() {
        val query = Accounts
            .select(Accounts.accountId, Accounts.balance)
            .pagination(page = 2, pageSize = 5)
            .sqlArgs()

        val expectedSql = "SELECT account_id, account_balance FROM account LIMIT ? OFFSET ?"
        val expectedArgs = listOf(5, 5)

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }

    @Test
    fun `test select query with join and additional where clause`() {
        val query = Accounts
            .select(Accounts.accountId)
            .join(Transactions.transactionAmount) {
                Transactions.transactionFrom inner Accounts.accountId
            }
            .where { Transactions.transactionType eq "transfer" }
            .sqlArgs()

        val expectedSql =
            "SELECT account_id, transaction_amount FROM account INNER JOIN account_transaction ON account.account_id = account_transaction.transaction_from WHERE transaction_type = ?"
        val expectedArgs = listOf("transfer")

        assertEquals(expectedSql, query.sql)
        assertEquals(expectedArgs, query.args)
    }
}