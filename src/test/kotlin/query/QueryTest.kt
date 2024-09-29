package query

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class QueryTest {
    @Test
    fun `should return SQL and args correctly`() {
        val query = Query("SELECT * FROM table WHERE id = ?", 1)
        val expectedSql = "SELECT * FROM table WHERE id = ?"
        val expectedArgs = listOf(1)

        val (sql, args) = query.sqlArgs()

        assertEquals(expectedSql, sql)
        assertEquals(expectedArgs, args)
    }

    @Test
    fun `should handle list of args in query`() {
        val query = Query("SELECT * FROM table WHERE id IN (?)", listOf(1, 2, 3))
        val expectedSql = "SELECT * FROM table WHERE id IN (?)"
        val expectedArgs = listOf(1, 2, 3)

        val (sql, args) = query.sqlArgs()

        assertEquals(expectedSql, sql)
        assertEquals(expectedArgs, args)
    }

    @Test
    fun `toString should format SQL and args correctly`() {
        val query = Query("SELECT * FROM table WHERE id = ?", 1)
        val expectedString = "SQL = SELECT * FROM table WHERE id = ?;\nARGS = [1];"

        assertEquals(expectedString, query.toString())
    }

    @Test
    fun `hashCode should be the same for identical queries`() {
        val query1 = Query("SELECT * FROM table WHERE id = ?", 1)
        val query2 = Query("SELECT * FROM table WHERE id = ?", 1)

        assertEquals(query1.hashCode(), query2.hashCode())
    }

    @Test
    fun `equals should return true for identical queries and false for different ones`() {
        val query1 = Query("SELECT * FROM table WHERE id = ?", 1)
        val query2 = Query("SELECT * FROM table WHERE id = ?", 1)
        val query3 = Query("SELECT * FROM table WHERE id = ?", 2)

        assertTrue(query1 == query2)
        assertNotEquals(query1, query3)
    }
}