package query.schema

import createAccountTable
import org.junit.jupiter.api.Test
import query.expr.persist
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TableTest {

    @Test
    fun `test table name`() {
        assertEquals("account", Accounts.tableName)
        assertEquals("transaction", Transactions.tableName)
    }

    @Test
    fun `test column text is set`() {
        assertEquals("account_type", Accounts.accountType.key())
        assertEquals(ColumnType.STRING, Accounts.accountType.type())
    }

    @Test
    fun `test column integer is set`() {
        assertEquals("account_id", Accounts.accountId.key())
        assertEquals(ColumnType.INT, Accounts.accountId.type())
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
        assertEquals("transaction_fee", Transactions.transactionFee.key())
        assertEquals(ColumnType.DOUBLE, Transactions.transactionFee.type())
    }

    @Test
    fun `test column decimal is set`() {
        assertEquals("account_balance", Accounts.balance.key())
        assertEquals(ColumnType.DECIMAL, Accounts.balance.type())
    }

    @Test
    fun `test column boolean is set`() {
        assertEquals("account_is_active", Accounts.isActive.key())
        assertEquals(ColumnType.BOOLEAN, Accounts.isActive.type())
    }

    @Test
    fun `test column set primary key`() {
        assertEquals(Accounts.accountId, Accounts.primaryKey<Int>())
    }

    @Test
    fun `test column get primary key`() {
        val accountPrimaryKey = Accounts.primaryKey<Int>()
        assertNotNull(accountPrimaryKey)
        assertEquals("account_id", accountPrimaryKey.key())
    }

    @Test
    fun `test table all columns`() {
        val accountColumns = Accounts.getColumnsList()

        assertEquals(7, accountColumns.size)
        assertTrue(accountColumns.any { it.key() == "account_id" })
        assertTrue(accountColumns.any { it.key() == "account_balance" })
        assertTrue(accountColumns.any { it.key() == "account_interest_rate" })
        assertTrue(accountColumns.any { it.key() == "account_type" })
        assertTrue(accountColumns.any { it.key() == "account_is_active" })
        assertTrue(accountColumns.any { it.key() == "account_created_at" })
        assertTrue(accountColumns.any { it.key() == "account_last_updated_at" })
    }

    @Test
    fun `test column name with table prefix`() {
        val tableWithPrefix = object : Table("account") {
            val accountId = column<Int>("id")
        }

        assertEquals("account_id", tableWithPrefix.accountId.key())
    }

    @Test
    fun `test column name without table prefix`() {
        val tableWithoutPrefix = object : Table("account", tablePrefix = false) {
            val accountId = column<Int>("id")
        }

        assertEquals("id", tableWithoutPrefix.accountId.key())
    }

    @Test
    fun `test createTable generates correct SQL`() {
        val query = Accounts.createTable()
        val expectedSQL = "CREATE TABLE IF NOT EXISTS account (account_id INTEGER PRIMARY KEY, account_balance NUMERIC, account_interest_rate REAL, account_type TEXT, account_is_active INTEGER, account_created_at INTEGER, account_last_updated_at INTEGER);"
        assertEquals(expectedSQL, query.sql)
    }

//    @Test
//    fun `should return the number of how many records are stored`(){
//        val db = DB()
//        Accounts.createAccountTable(db.conn())
//
//        Accounts.insert { it[Accounts.accountId] = 1 }.persist(db.conn())
//        Accounts.insert { it[Accounts.accountId] = 2 }.persist(db.conn())
//
//        val numRecords = Accounts.recordsCount(db.conn()).getOrThrow()
//        assertEquals(2, numRecords)
//    }
//
//    @Test
//    fun `should return if a given record exists`(){
//        val db = DB()
//        Accounts.createAccountTable(db.conn())
//        Accounts.insert { it[Accounts.accountId] = 1 }.persist(db.conn())
//
//        val existsA = Accounts.recordExistsBy(db.conn(), Accounts.accountId, 1).getOrThrow()
//        val existsB = Accounts.recordExists(db.conn(), 1).getOrThrow()
//        val notExists = Accounts.recordExists(db.conn(), 3).getOrThrow()
//
//        assertTrue(existsA)
//        assertTrue(existsB)
//        assertFalse(notExists)
//    }
}