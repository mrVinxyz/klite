import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import query.JoinType
import query.Select
import query.get
import query.list

class SelectQuery {
    @Test
    fun `test regular select`() {
        val (sql, _) =
            Select(Users).select(Users.id, Users.name, Users.email, Users.password).sqlArgs()

        Logger.info("[SQL] $sql")
        assertEquals("SELECT user_id, name, email, password FROM user", sql)
    }

    @Test
    fun `test select all`() {
        val (sql, _) = Select(Users).select().sqlArgs()

        Logger.info("[SQL] $sql")
        assertEquals(
            "SELECT user_id, name, email, password, record_status, created_at FROM user",
            sql,
        )
    }

    @Test
    fun `test select with where clause`() {
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where { Users.id eq 1 }
                .sqlArgs()

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals("SELECT user_id, name, email, password FROM user WHERE user_id = ?", sql)
        assertEquals(listOf(1), args)
    }

    @Test
    fun `test select with null where clause`() {
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where { Users.id eq null }
                .sqlArgs()

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals("SELECT user_id, name, email, password FROM user", sql)
        assertEquals(emptyList(), args)
    }

    @Test
    fun `test select with multiple where clauses`() {
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where {
                    Users.id eq 1
                    Users.email eq "john"
                }
                .sqlArgs()

        assertEquals(
            "SELECT user_id, name, email, password FROM user WHERE user_id = ? AND email = ?",
            sql,
        )

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals(listOf(1, "john"), args)
    }

    @Test
    fun `test select with join clause`() {
        val (sql, _) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users, Users.id, Users.id)
                .sqlArgs()

        Logger.info("[SQL] $sql")
        assertEquals(
            "SELECT user_id, name, email, password FROM user INNER JOIN user ON user.user_id = user.user_id",
            sql,
        )
    }

    @Test
    fun `test select with multiple join clauses`() {
        val (sql, _) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users, Users.id, Users.id)
                .join(Users, Users.email, Users.email)
                .sqlArgs()

        Logger.info("[SQL] $sql")
        assertEquals(
            "SELECT user_id, name, email, password FROM user " +
                "INNER JOIN user ON user.id = user.user_id " +
                "INNER JOIN user ON user.email = user.email",
            sql,
        )
    }

    @Test
    fun `test different join types`() {
        val (sql, _) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users, Users.id, Users.id, JoinType.LEFT)
                .join(Users, Users.email, Users.email, JoinType.RIGHT)
                .join(Users, Users.password, Users.password, JoinType.OUTER)
                .join(Users, Users.recordStatus, Users.recordStatus, JoinType.FULL)
                .sqlArgs()

        val rawSql =
            "SELECT user_id, name, email, password FROM user " +
                "LEFT JOIN user ON user.user_id = user.user_id " +
                "RIGHT JOIN user ON user.email = user.email " +
                "OUTER JOIN user ON user.password = user.password " +
                "FULL JOIN user ON user.record_status = user.record_status"

        Logger.info("[SQL] $sql")
        assertEquals(
            rawSql,
            sql,
        )
    }

    @Test
    fun `test select with custom function`() {
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where { fn(Users.email, "LOWER") eq "john" }
                .sqlArgs()

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals("SELECT user_id, name, email, password FROM user WHERE LOWER(email) = ?", sql)
        assertEquals(listOf("john"), args)
    }

    @Test
    fun `test select with limitOffset`() {
        val limitOffset = Pair(10, 5)
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .limit(limitOffset.first)
                .offset(limitOffset.second)
                .sqlArgs()

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals("SELECT user_id, name, email, password FROM user LIMIT ? OFFSET ?", sql)
        assertEquals(listOf(10, 5), args)
    }

    @Test
    fun `test select with null pagination`() {
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .limit(null)
                .offset(null)
                .sqlArgs()

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals("SELECT user_id, name, email, password FROM user", sql)
        assertEquals(emptyList(), args)
    }

    @Test
    fun `test select with pagination`() {
        data class Pagination(val limit: Int? = 10, val offset: Int? = 5)
        val pagination = Pagination()
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .pagination(pagination.limit, pagination.offset)
                .sqlArgs()

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals("SELECT user_id, name, email, password FROM user LIMIT ? OFFSET ?", sql)
        assertEquals(listOf(pagination.limit!!, pagination.offset!!), args)
    }

    @Test
    fun `test select with order by`() {
        val (sql, args) =
            Select(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .orderBy {
                    Users.id.asc()
                    Users.name.desc()
                }
                .sqlArgs()

        Logger.info("[SQL] $sql [ARGS] $args")
        assertEquals(
            "SELECT user_id, name, email, password FROM user ORDER BY user_id ASC, name DESC", sql)
        assertEquals(emptyList(), args)
    }

    @Test
    fun `test select executor get one`() {
        val selectQuery =
            Select(Users)
                .select(
                    Users.id,
                    Users.name,
                    Users.email,
                    Users.password,
                    Users.createdAt,
                    Users.recordStatus,
                )
                .where { Users.id eq 1 }

        var user: User?

        val db = Store()
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
                            it[Users.createdAt],
                        )
                    }
                    .getOrThrow()
        }

        Logger.info("[GetExecutor] $user")
        assertNotNull(user)
        assertEquals(1, user.id)
    }

    @Test
    fun `test select executor list`() {
        val db = Store()
        var users: List<User>

        db.conn().use { conn ->
            createUserTable(conn)
            feedUserTable(conn)

            users =
                Select(Users)
                    .select(
                        Users.id,
                        Users.name,
                        Users.email,
                        Users.password,
                        Users.createdAt,
                        Users.recordStatus,
                    )
                    .list(conn) {
                        User(
                            it[Users.id],
                            it[Users.name],
                            it[Users.email],
                            it[Users.password],
                            it[Users.recordStatus],
                            it[Users.createdAt],
                        )
                    }
                    .getOrThrow()
        }

        Logger.info("[ListExecutor] $users")

        assertNotEquals(0, users.size)
        assertEquals(4, users.size)
    }
}
