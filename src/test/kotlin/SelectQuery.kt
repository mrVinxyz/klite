import query.select.JoinType
import query.select.Selector
import kotlin.test.Test
import kotlin.test.assertEquals

class SelectQuery {
    @Test
    fun `test regular select`() {
        val (sql, _) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .sqlArgs()

        assertEquals(
            "SELECT id, name, email, password FROM user",
            sql
        )
    }

    @Test
    fun `test select all`() {
        val (sql, _) =
            Selector(Users)
                .select()
                .sqlArgs()

        assertEquals(
            "SELECT id, name, email, password, record_status, created_at FROM user",
            sql
        )
    }

    @Test
    fun `test select with where clause`() {
        val (sql, args) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where {
                    Users.id equal 1
                }
                .sqlArgs()

        assertEquals(
            "SELECT id, name, email, password FROM user WHERE id = ?",
            sql
        )

        assertEquals(
            listOf(1),
            args
        )
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
            "SELECT id, name, email, password FROM user WHERE id = ? AND email = ?",
            sql
        )

        assertEquals(
            listOf(1, "john"),
            args
        )
    }

    @Test
    fun `test select with join clause`() {
        val (sql, _) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users) {
                    Users.id on Users.id
                }
                .sqlArgs()

        assertEquals(
            "SELECT id, name, email, password FROM user INNER JOIN user ON user.id = user.id",
            sql
        )
    }

    @Test
    fun `test select with multiple join clauses`() {
        val (sql, _) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users) {
                    Users.id on Users.id
                }
                .join(Users) {
                    Users.email on Users.email
                }
                .sqlArgs()

        assertEquals(
            "SELECT id, name, email, password FROM user INNER JOIN user ON user.id = user.id INNER JOIN user ON user.email = user.email",
            sql
        )
    }

    @Test
    fun `test different join types`() {
        val (sql, _) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .join(Users, JoinType.LEFT) {
                    Users.id on Users.id
                }
                .join(Users, JoinType.RIGHT) {
                    Users.email on Users.email
                }
                .join(Users, JoinType.OUTER) {
                    Users.password on Users.password
                }
                .join(Users, JoinType.FULL) {
                    Users.recordStatus on Users.recordStatus
                }
                .sqlArgs()

        assertEquals(
            "SELECT id, name, email, password FROM user LEFT JOIN user ON user.id = user.id RIGHT JOIN user ON user.email = user.email OUTER JOIN user ON user.password = user.password FULL JOIN user ON user.record_status = user.record_status",
            sql
        )
    }

    @Test
    fun `test select with where and join clauses`() {
        val (sql, args) =
            Selector(Users)
                .select(Users.id, Users.name, Users.email, Users.password)
                .where {
                    Users.id equal 1
                }
                .join(Users) {
                    Users.id on Users.id
                }
                .sqlArgs()

        assertEquals(
            "SELECT id, name, email, password FROM user INNER JOIN user ON user.id = user.id WHERE id = ?",
            sql
        )

        assertEquals(
            listOf(1),
            args
        )
    }
}