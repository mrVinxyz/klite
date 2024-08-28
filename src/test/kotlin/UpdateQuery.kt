import kotlin.test.Test
import kotlin.test.assertEquals
import query.Updater
import query.persist
import kotlin.test.assertNotEquals

class UpdateQuery {
    private val user = User(1, "John Doe", "johndoe@email.com", "johndoe123", "active", 1234567890)
    private val userNull = User(1, "John Doe", "johndoe@email.com", "johndoe123", null, null)

    @Test
    fun `test update with init block`() {
        val (sql, args) =
            Updater(Users)
                .update {
                    it[Users.name] = user.name
                    it[Users.email] = user.email
                    it[Users.password] = user.password
                    it[Users.recordStatus] = user.recordStatus
                    it[Users.createdAt] = user.createdAt
                }
                .where { Users.id equal 1 }
                .sqlArgs()

        assertEquals(
            "UPDATE user SET name = ?, email = ?, password = ?, record_status = ?, created_at = ? WHERE user_id = ?",
            sql,
        )

        assertEquals(
            listOf(
                user.name as Any,
                user.email,
                user.password,
                user.recordStatus,
                user.createdAt,
                1,
            ),
            args,
        )
    }

    @Test
    fun `test update init block with null values`() {
        val (sql, args) =
            Updater(Users)
                .update {
                    it[Users.name] = userNull.name
                    it[Users.recordStatus] = userNull.recordStatus
                    it[Users.createdAt] = userNull.createdAt
                }
                .where { Users.id equal 1 }
                .sqlArgs()

        assertEquals(
            "UPDATE user SET name = ?, record_status = COALESCE(?, record_status), created_at = COALESCE(?, created_at) WHERE user_id = ?",
            sql,
        )

        assertEquals(
            listOf(userNull.name as Any, userNull.recordStatus, userNull.createdAt, 1),
            args,
        )
    }

    @Test
    fun `test update empty`() {
        val (sql, args) = Updater(Users).sqlArgs()

        assertEquals("UPDATE user SET ", sql)
        assertEquals(emptyList(), args)
    }

    @Test
    fun `test update persist method`() {
        var updateQuery =
            Updater(Users)
                .update {
                    it[Users.name] = "Jhon Doe"
                    it[Users.email] = user.email
                    it[Users.password] = user.password
                    it[Users.recordStatus] = user.recordStatus
                    it[Users.createdAt] = user.createdAt
                }
                .where { Users.id equal 1 }

        val db = Store()
        db.conn().use {
            createUserTable(it)
            feedUserTable(it)

            val result = updateQuery.persist(it)
            assert(result.isSuccess)

            val selectAssertQuery = "SELECT name FROM user WHERE user_id = 1"
            it.prepareStatement(selectAssertQuery).use { stmt ->
                val rs = stmt.executeQuery()
                rs.next()

                val updatedName = rs.getString("name")
                assertNotEquals(user.name, updatedName)
                assertEquals("Jhon Doe", updatedName)
            }
        }
    }
}
