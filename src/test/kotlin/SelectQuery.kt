import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import query.JoinType
import query.Selector
import query.get
import query.list

class SelectQuery {
    @Test
    fun `test regular select`() {
        val (sql, _) =
            Selector(Users).select(Users.id, Users.name, Users.email, Users.password).sqlArgs()

        assertEquals("SELECT user_id, name, email, password FROM user", sql)
    }

    @Test
    fun `test select all`() {
        val (sql, _) = Selector(Users).select().sqlArgs()

        assertEquals(
            "SELECT user_id, name, email, password, record_status, created_at FROM user", sql)
    }

    @Test
    fun `test select with where clause`() {
        val (sql, args) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where { Users.id equal 1 }
                .sqlArgs()

        assertEquals("SELECT user_id, name, email, password FROM user WHERE user_id = ?", sql)

        assertEquals(listOf(1), args)
    }

    @Test
    fun `test select with null where clause`() {
        val (sql, args) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where { Users.id equal null }
                .sqlArgs()

        assertEquals("SELECT user_id, name, email, password FROM user", sql)

        assertEquals(emptyList(), args)
    }

    @Test
    fun `test select with multiple where clauses`() {
        val (sql, args) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where {
                    Users.id equal 1
                    Users.email equal "john"
                }
                .sqlArgs()

        assertEquals(
            "SELECT user_id, name, email, password FROM user WHERE user_id = ? AND email = ?", sql)

        assertEquals(listOf(1, "john"), args)
    }

    @Test
    fun `test select with join clause`() {
        val (sql, _) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users, Users.id, Users.id)
                .sqlArgs()

        assertEquals(
            "SELECT user_id, name, email, password FROM user INNER JOIN user ON user.user_id = user.user_id",
            sql)
    }

    @Test
    fun `test select with multiple join clauses`() {
        val (sql, _) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users, Users.id, Users.id)
                .join(Users, Users.email, Users.email)
                .sqlArgs()

        assertEquals(
            "SELECT user_id, name, email, password FROM user " +
                "INNER JOIN user ON user.id = user.user_id " +
                "INNER JOIN user ON user.email = user.email",
            sql)
    }

    @Test
    fun `test different join types`() {
        val (sql, _) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users, Users.id, Users.id, JoinType.LEFT)
                .join(Users, Users.email, Users.email, JoinType.RIGHT)
                .join(Users, Users.password, Users.password, JoinType.OUTER)
                .join(Users, Users.recordStatus, Users.recordStatus, JoinType.FULL)
                .sqlArgs()

        assertEquals(
            "SELECT user_id, name, email, password FROM user " +
                "LEFT JOIN user ON user.user_id = user.user_id " +
                "RIGHT JOIN user ON user.email = user.email " +
                "OUTER JOIN user ON user.password = user.password " +
                "FULL JOIN user ON user.record_status = user.record_status",
            sql)
    }

    @Test
    fun `test select with custom function`() {
        val (sql, args) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where { fn(Users.email, "LOWER") equal "john" }
                .sqlArgs()

        assertEquals("SELECT user_id, name, email, password FROM user WHERE LOWER(email) = ?", sql)

        assertEquals(listOf("john"), args)
    }

    @Test
    fun `test select executor get one`() {
        val selectQuery =
            Selector(Users)
                .select(
                    Users.id,
                    Users.name,
                    Users.email,
                    Users.password,
                    Users.createdAt,
                    Users.recordStatus)
                .where { Users.id equal 1 }

        var user: User?

        val db = Database()
        db.conn().use { conn ->
            createUserTable(conn)
            feedUserTable(conn)

            user =
                selectQuery
                    .get(conn) {
                        User(
                            it[Users.id],
                            it[Users.name],
                            it[Users.email],
                            it[Users.password],
                            it[Users.recordStatus],
                            it[Users.createdAt])
                    }
                    .getOrThrow()
        }

        assertNotNull(user)
        assertEquals(1, user.id)

        db.cleanUp()
    }

    @Test
    fun `test select executor list`() {
        val db = Database()
        val selectQuery =
            Selector(Users)
                .select(
                    Users.id,
                    Users.name,
                    Users.email,
                    Users.password,
                    Users.createdAt,
                    Users.recordStatus)

        var users: List<User>

        db.conn().use { conn ->
            createUserTable(conn)
            feedUserTable(conn)

            users =
                selectQuery
                    .list(conn) {
                        User(
                            it[Users.id],
                            it[Users.name],
                            it[Users.email],
                            it[Users.password],
                            it[Users.recordStatus],
                            it[Users.createdAt])
                    }
                    .getOrThrow()
        }

        assertEquals(4, users.size)

        db.cleanUp()
    }
}
