package query.schema

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ColumnTest {
    @Test
    fun `test key method returns correct key`() {
        val column = Accounts.balance
        assertEquals("account_balance", column.key())
    }

    @Test
    fun `test type method returns correct type`() {
        val column = Accounts.balance
        assertEquals(ColumnType.DECIMAL, column.type())
    }

    @Test
    fun `test table method returns correct table`() {
        val column = Accounts.balance
        assertNotNull(column.table())
        assertEquals(Accounts, column.table())
    }
}