import query.update.Updater
import kotlin.test.Test
import kotlin.test.assertEquals

class UpdateQuery {
    private val user = User(1, "John Doe", "johndoe@email.com", "johndoe123", "active", 1234567890)
    private val userNull = User(1, "John Doe", "johndoe@email.com", "johndoe123", null, null)

    @Test
    fun `test update`() {
        val (sql, args) = Updater(Users).update {
            it[Users.name] = user.name
            it[Users.email] = user.email
            it[Users.password] = user.password
            it[Users.recordStatus] = user.recordStatus
            it[Users.createdAt] = user.createdAt
        }.where {
            Users.id equal 1
        }.sqlArgs()

        assertEquals(
            "UPDATE user SET name = ?, email = ?, password = ?, record_status = ?, created_at = ? WHERE id = ?",
            sql
        )

        assertEquals(
            listOf(
                user.name as Any, user.email, user.password, user.recordStatus, user.createdAt, 1
            ),
            args
        )
    }
}