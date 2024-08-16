import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import query.insert.Inserter
import query.insert.persist

class InsertQuery {
    private val user = User(1, "John Doe", "johndoe@email.com", "johndoe123", "active", 1234567890)
    private val userNull = User(1, "John Doe", "johndoe@email.com", "johndoe123", null, null)

    @Test
    fun `test insert with init block`() {
        val (sql, args) =
            Inserter(Users)
                .insert {
                    it[Users.id] = user.id
                    it[Users.name] = user.name
                    it[Users.email] = user.email
                    it[Users.password] = user.password
                    it[Users.recordStatus] = user.recordStatus
                    it[Users.createdAt] = user.createdAt
                }
                .sqlArgs()

        assertEquals(
            "INSERT INTO user (id, name, email, password, record_status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            sql)

        assertEquals(
            listOf(
                user.id, user.name, user.email, user.password, user.recordStatus, user.createdAt),
            args)
    }

    @Test
    fun `test insert with vararg columns`() {
        val (sql, args) =
            Inserter(Users)
                .insert(
                    Users.id,
                    Users.name,
                    Users.email,
                    Users.password,
                    Users.recordStatus,
                    Users.createdAt)
                .values(
                    user.id,
                    user.name,
                    user.email,
                    user.password,
                    user.recordStatus,
                    user.createdAt)
                .sqlArgs()

        assertEquals(
            "INSERT INTO user (id, name, email, password, record_status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            sql)

        assertEquals(
            listOf(
                user.id, user.name, user.email, user.password, user.recordStatus, user.createdAt),
            args)
    }

    @Test
    fun `test insert with map`() {
        val (sql, args) =
            Inserter(Users)
                .insert(Users.id, Users.name, Users.email, Users.password)
                .values(
                    mapOf(
                        "id" to user.id,
                        "name" to user.name,
                        "email" to user.email,
                        "password" to user.password))
                .sqlArgs()

        assertEquals("INSERT INTO user (id, name, email, password) VALUES (?, ?, ?, ?)", sql)

        assertEquals(listOf(user.id, user.name, user.email, user.password), args)
    }

    @Test
    fun `test insert init block with values with null values`() {
        val (sql, args) =
            Inserter(Users)
                .insert {
                    it[Users.id] = userNull.id
                    it[Users.name] = userNull.name
                    it[Users.email] = userNull.email
                    it[Users.password] = userNull.password
                    it[Users.recordStatus] = userNull.recordStatus
                    it[Users.createdAt] = userNull.createdAt
                }
                .sqlArgs()

        assertEquals("INSERT INTO user (id, name, email, password) VALUES (?, ?, ?, ?)", sql)

        assertEquals(listOf(userNull.id, userNull.name, userNull.email, userNull.password), args)
    }

    @Test
    fun `test insert varargs columns with values with null values`() {
        val (sql, args) =
            Inserter(Users)
                .insert(
                    Users.id,
                    Users.name,
                    Users.email,
                    Users.password,
                    Users.recordStatus,
                    Users.createdAt)
                .values(
                    userNull.id,
                    userNull.name,
                    userNull.email,
                    userNull.password,
                    userNull.recordStatus,
                    userNull.createdAt)
                .sqlArgs()

        assertEquals("INSERT INTO user (id, name, email, password) VALUES (?, ?, ?, ?)", sql)

        assertEquals(listOf(userNull.id, userNull.name, userNull.email, userNull.password), args)
    }

    @Test
    fun `test insert map with null values`() {
        val (sql, args) =
            Inserter(Users)
                .insert(Users.id, Users.name, Users.email, Users.password)
                .values(
                    mapOf(
                        "id" to userNull.id,
                        "name" to userNull.name,
                        "email" to userNull.email,
                        "password" to userNull.password,
                        "record_status" to userNull.recordStatus,
                        "created_at" to userNull.createdAt))
                .sqlArgs()

        assertEquals("INSERT INTO user (id, name, email, password) VALUES (?, ?, ?, ?)", sql)

        assertEquals(listOf(userNull.id, userNull.name, userNull.email, userNull.password), args)
    }

    @Test
    fun `test sqlArgs with empty inserter`() {
        val (sql, args) = Inserter(Users).sqlArgs()

        assertEquals("INSERT INTO user () VALUES ()", sql)
        assertEquals(emptyList<Any>(), args)
    }

    @Test
    fun `test insert persist method`() {
        val mock = Database()

        val query =
            Inserter(Users).insert {
                it[Users.id] = user.id
                it[Users.name] = user.name
                it[Users.email] = user.email
                it[Users.password] = user.password
                it[Users.recordStatus] = user.recordStatus
                it[Users.createdAt] = user.createdAt
            }

        mock.conn().use {
            createUserTable(it)

            val result = query.persist(it)

            assert(result.isSuccess)
            result.onSuccess { id -> assertEquals(1, id) }
        }
    }
}
