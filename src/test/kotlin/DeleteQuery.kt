import kotlin.test.Test
import kotlin.test.assertEquals
import query.Deleter
import query.persist

class DeleteQuery {
    @Test
    fun `test delete where query`() {
        val (sql, args) =
            Deleter(Users)
                .deleteWhere {
                    Users.id equal 1
                }
                .sqlArgs()

        assertEquals("DELETE FROM user WHERE user_id = ?", sql)
        assertEquals(listOf(1), args)
    }

    @Test
    fun `test delete primary query`() {
        val (sql, args) = Deleter(Users).deletePrimary(1).sqlArgs()

        assertEquals("DELETE FROM user WHERE user_id = ?", sql)
        assertEquals(listOf(1), args)
    }

    @Test
    fun `test delete persist method`() {
        val deleteQuery = Deleter(Users).deletePrimary(1)

        val db = Store()

        db.conn().use {
            createUserTable(it)
            feedUserTable(it)

            val result = deleteQuery.persist(it)
            assert(result.isSuccess)

            val deleteQueryAssert = "SELECT name FROM user WHERE user_id = 1"
            it.prepareStatement(deleteQueryAssert).use { stmt ->
                val rs = stmt.executeQuery()
                rs.next()

                assertEquals(0, rs.row)
            }
        }
    }
}
