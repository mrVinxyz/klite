import query.Query
import query.Row
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class QueryTest {
    val user = User(
        id = 1,
        name = "Johnny Appleseed",
        email = "john.appleseed@example.com",
        password = "password123",
        recordStatus = "active",
        createdAt = 1234567890
    )

    val insertQuery = Query(
        "INSERT INTO user (name, email, password, record_status, created_at) VALUES (?, ?, ?, ?, ?)",
        user.name ?: "",
        user.email ?: "",
        user.password ?: "",
        user.recordStatus ?: "",
        user.createdAt ?: 0
    )

    @Test
    fun `test insert query`() {
        Store().conn().use { conn ->
            createUserTable(conn)

            assertEquals(
                "INSERT INTO user (name, email, password, record_status, created_at) VALUES (?, ?, ?, ?, ?)",
                insertQuery.sql
            )

            assertTrue(
                listOf(user.name, user.email, user.password, user.recordStatus, user.createdAt) ==
                        insertQuery.args
            )

            insertQuery.insert(conn).fold(
                onSuccess = { id ->
                    assertTrue(id is Int)
                    assertTrue(id > 0)
                },
                onFailure = { error -> throw error }
            )
        }
    }

    @Test
    fun `test fetch query`() {
        Store().conn().use { conn ->
            createUserTable(conn)
            feedUserTable(conn)

            val fetchQuery = Query("SELECT * FROM user WHERE user_id = ?", 1)

            assertEquals("SELECT * FROM user WHERE user_id = ?", fetchQuery.sql)
            assertEquals(listOf(1), fetchQuery.args)

            val resultOne = fetchQuery.selectOne(conn)
            assertTrue(resultOne.isSuccess)
            resultOne.onSuccess {
                assertTrue(it is Row)

                assertEquals(1, it.get("user_id"))
                assertEquals("John Doe", it.get("name"))
            }

            val resultList = fetchQuery.selectList(conn)
            assertTrue(resultList.isSuccess)
            resultList.onSuccess {
                assertTrue(it.isNotEmpty())
                assertTrue(it.size == 1)
            }
        }
    }
}