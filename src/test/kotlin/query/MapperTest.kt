package query

import org.junit.jupiter.api.Assertions.assertFalse
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.mockito.kotlin.verify
import query.schema.Column
import query.schema.ColumnType
import query.schema.Table
import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RowTest {
    val mockTable = object : Table("mock"){}
    val mockRs = mock(ResultSet::class.java)

    @Test
    fun `get String`() {
        `when`(mockRs.getString("name")).thenReturn("Test")
        `when`(mockRs.getString(2)).thenReturn("Test")

        val byColumn = {
            val column = Column<String>("name", ColumnType.STRING, mockTable)
            val row = Row(mockRs)
            row[column]
        }

        val byColumnName = {
            val row = Row(mockRs)
            row.get<String>("name")
        }

        val byColumnPos = {
            val row = Row(mockRs)
            row.get<String>(2)
        }

        assertEquals("Test", byColumn())
        assertEquals("Test", byColumnName())
        assertEquals("Test", byColumnPos())
    }

    @Test
    fun `get Int`() {
        `when`(mockRs.getInt("id")).thenReturn(86)
        `when`(mockRs.getInt(1)).thenReturn(86)

        val byColumn = {
            val column = Column<Int>("id", ColumnType.INT, mockTable)
            val row = Row(mockRs)
            row[column]
        }

        val byColumnName = {
            val row = Row(mockRs)
            row.get<Int>("id")
        }

        val byColumnPos = {
            val row = Row(mockRs)
            row.get<Int>(1)
        }

        assertEquals(86, byColumn())
        assertEquals(86, byColumnName())
        assertEquals(86, byColumnPos())
    }

    @Test
    fun `get Long`() {
        `when`(mockRs.getLong("id")).thenReturn(123456789L)
        `when`(mockRs.getLong(4)).thenReturn(123456789L)

        val byColumn = {
            val column = Column<Long>("id", ColumnType.LONG, mockTable)
            val row = Row(mockRs)
            row[column]
        }

        val byColumnName = {
            val row = Row(mockRs)
            row.get<Long>("id")
        }

        val byColumnPos = {
            val row = Row(mockRs)
            row.get<Long>(4)
        }

        assertEquals(123456789L, byColumn())
        assertEquals(123456789L, byColumnName())
        assertEquals(123456789L, byColumnPos())
    }

    @Test
    fun `get Float`() {
        `when`(mockRs.getFloat("score")).thenReturn(98.6f)
        `when`(mockRs.getFloat(5)).thenReturn(98.6f)

        val byColumn = {
            val column = Column<Float>("score", ColumnType.FLOAT, mockTable)
            val row = Row(mockRs)
            row[column]
        }

        val byColumnName = {
            val row = Row(mockRs)
            row.get<Float>("score")
        }

        val byColumnPos = {
            val row = Row(mockRs)
            row.get<Float>(5)
        }

        assertEquals(98.6f, byColumn())
        assertEquals(98.6f, byColumnName())
        assertEquals(98.6f, byColumnPos())
    }

    @Test
    fun `get Double`() {
        `when`(mockRs.getDouble("balance")).thenReturn(12345.67)
        `when`(mockRs.getDouble(6)).thenReturn(12345.67)

        val byColumn = {
            val column = Column<Double>("balance", ColumnType.DOUBLE, mockTable)
            val row = Row(mockRs)
            row[column]
        }

        val byColumnName = {
            val row = Row(mockRs)
            row.get<Double>("balance")
        }

        val byColumnPos = {
            val row = Row(mockRs)
            row.get<Double>(6)
        }

        assertEquals(12345.67, byColumn())
        assertEquals(12345.67, byColumnName())
        assertEquals(12345.67, byColumnPos())
    }

    @Test
    fun `get BigDecimal`() {
        `when`(mockRs.getBigDecimal("amount")).thenReturn(BigDecimal.TEN)
        `when`(mockRs.getBigDecimal(3)).thenReturn(BigDecimal.TEN)

        val byColumn = {
            val column = Column<BigDecimal>("amount", ColumnType.DECIMAL, mockTable)
            val row = Row(mockRs)
            row[column]
        }

        val byColumnName = {
            val row = Row(mockRs)
            row.get<BigDecimal>("amount")
        }

        val byColumnPos = {
            val row = Row(mockRs)
            row.get<BigDecimal>(3)
        }

        assertEquals(BigDecimal.TEN, byColumn())
        assertEquals(BigDecimal.TEN, byColumnName())
        assertEquals(BigDecimal.TEN, byColumnPos())
    }

    @Test
    fun `get Boolean`() {
        `when`(mockRs.getBoolean("active")).thenReturn(true)
        `when`(mockRs.getBoolean(7)).thenReturn(true)

        val byColumn = {
            val column = Column<Boolean>("active", ColumnType.BOOLEAN, mockTable)
            val row = Row(mockRs)
            row[column]
        }

        val byColumnName = {
            val row = Row(mockRs)
            row.get<Boolean>("active")
        }

        val byColumnPos = {
            val row = Row(mockRs)
            row.get<Boolean>(7)
        }

        assertEquals(true, byColumn())
        assertEquals(true, byColumnName())
        assertEquals(true, byColumnPos())
    }
}

class RowsTest {
    @Test
    fun `test rows iteration`() {
        val mockRs = mock(ResultSet::class.java)

        `when`(mockRs.next()).thenReturn(true, true, false)

        `when`(mockRs.getInt("id")).thenReturn(1, 2)
        `when`(mockRs.getString("name")).thenReturn("Alice", "Bob")

        val rows = Rows(mockRs)
        val iterator = rows.iterator()

        val firstRow = iterator.next()
        assertEquals(1, firstRow.resultSet.getInt("id"))
        assertEquals("Alice", firstRow.resultSet.getString("name"))

        val secondRow = iterator.next()
        assertEquals(2, secondRow.resultSet.getInt("id"))
        assertEquals("Bob", secondRow.resultSet.getString("name"))

        assertFalse(iterator.hasNext())
    }
}

class PreparedStatementTest {
    @Test
    fun `test setParameters with multiple types`() {
        val mockPs = mock(PreparedStatement::class.java)

        val params = listOf<Any?>(
            "Test String", 42, 123456789L, 98.6f, 12345.67, BigDecimal.TEN, true, null
        )

        mockPs.setParameters(params)

        verify(mockPs).setString(1, "Test String")
        verify(mockPs).setInt(2, 42)
        verify(mockPs).setLong(3, 123456789L)
        verify(mockPs).setFloat(4, 98.6f)
        verify(mockPs).setDouble(5, 12345.67)
        verify(mockPs).setBigDecimal(6, BigDecimal.TEN)
        verify(mockPs).setBoolean(7, true)
        verify(mockPs).setObject(8, null)
    }
}

class ResultSetIteratorTest {
    val mockRs = mock(ResultSet::class.java)

    @Test
    fun `test iterator with multiple rows`() {
        `when`(mockRs.next()).thenReturn(true, true, true, false)

        val iterator = mockRs.iterator()

        assertTrue(iterator.hasNext())
        val row1 = iterator.next()
        assertTrue(row1.resultSet === mockRs)

        assertTrue(iterator.hasNext())
        val row2 = iterator.next()
        assertTrue(row2.resultSet === mockRs)

        assertTrue(iterator.hasNext())
        val row3 = iterator.next()
        assertTrue(row3.resultSet === mockRs)

        assertFalse(iterator.hasNext())

        assertFailsWith<NoSuchElementException> { iterator.next() }
    }

    @Test
    fun `test iterator with no rows`() {
        `when`(mockRs.next()).thenReturn(false)

        val iterator = mockRs.iterator()

        assertFalse(iterator.hasNext())

        assertFailsWith<NoSuchElementException> { iterator.next() }
    }
}