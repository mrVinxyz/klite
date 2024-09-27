import query.schema.ColumnType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TableTest {

    @Test
    fun `test table name`() {
        assertEquals("account", Accounts.tableName)
        assertEquals("transaction", Transaction.tableName)
    }

    @Test
    fun `test column text is set`() {
        assertEquals("account_type", Accounts.accountType.key())
        assertEquals(ColumnType.STRING, Accounts.accountType.type())

        assertEquals("transaction_to", Transaction.transactionTo.key())
        assertEquals(ColumnType.STRING, Transaction.transactionTo.type())
    }

    @Test
    fun `test column integer is set`() {
        assertEquals("account_id", Accounts.accountId.key())
        assertEquals(ColumnType.INT, Accounts.accountId.type())

        assertEquals("transaction_id", Transaction.transactionId.key())
        assertEquals(ColumnType.INT, Transaction.transactionId.type())
    }

    @Test
    fun `test column long is set`() {
        assertEquals("account_created_at", Accounts.createdAt.key())
        assertEquals(ColumnType.LONG, Accounts.createdAt.type())
    }

    @Test
    fun `test column float is set`() {
        assertEquals("account_interest_rate", Accounts.interestRate.key())
        assertEquals(ColumnType.FLOAT, Accounts.interestRate.type())
    }

    @Test
    fun `test column double is set`() {
        assertEquals("transaction_fee", Transaction.transactionFee.key())
        assertEquals(ColumnType.DOUBLE, Transaction.transactionFee.type())
    }

    @Test
    fun `test column decimal is set`() {
        assertEquals("account_balance", Accounts.balance.key())
        assertEquals(ColumnType.DECIMAL, Accounts.balance.type())

        assertEquals("transaction_amount", Transaction.transactionAmount.key())
        assertEquals(ColumnType.DECIMAL, Transaction.transactionAmount.type())
    }

    @Test
    fun `test column boolean is set`() {
        assertEquals("account_is_active", Accounts.isActive.key())
        assertEquals(ColumnType.BOOLEAN, Accounts.isActive.type())
    }

    @Test
    fun `test column set primary key`() {
        assertEquals(Accounts.accountId, Accounts.primaryKey<Int>())
        assertEquals(Transaction.transactionId, Transaction.primaryKey<Int>())
    }

    @Test
    fun `test column get primary key`() {
        val accountPrimaryKey = Accounts.primaryKey<Int>()
        val transactionPrimaryKey = Transaction.primaryKey<Int>()

        assertNotNull(accountPrimaryKey)
        assertNotNull(transactionPrimaryKey)
        assertEquals("account_id", accountPrimaryKey.key())
        assertEquals("transaction_id", transactionPrimaryKey.key())
    }

    @Test
    fun `test table all columns`() {
        val accountColumns = Accounts.getColumnsList()
        val transactionColumns = Transaction.getColumnsList()

        assertEquals(7, accountColumns.size)
        assertTrue(accountColumns.any { it.key() == "account_id" })
        assertTrue(accountColumns.any { it.key() == "account_balance" })
        assertTrue(accountColumns.any { it.key() == "account_interest_rate" })
        assertTrue(accountColumns.any { it.key() == "account_type" })
        assertTrue(accountColumns.any { it.key() == "account_is_active" })
        assertTrue(accountColumns.any { it.key() == "account_created_at" })
        assertTrue(accountColumns.any { it.key() == "account_last_updated_at" })

        assertEquals(6, transactionColumns.size)
        assertTrue(transactionColumns.any { it.key() == "transaction_id" })
        assertTrue(transactionColumns.any { it.key() == "transaction_to" })
        assertTrue(transactionColumns.any { it.key() == "transaction_from" })
        assertTrue(transactionColumns.any { it.key() == "transaction_amount" })
        assertTrue(transactionColumns.any { it.key() == "transaction_fee" })
    }
}