import java.sql.Connection
import query.Table

// TODO use a pool so that we're able to use a in-memory database
object Users : Table("user") {
    val id = column<Int>("user_id").setPrimaryKey()
    val name = text("name")
    val email = text("email")
    val password = column<String>("password")
    val recordStatus = column<String>("record_status")
    val createdAt = column<Long>("created_at")
}

data class User(
    val id: Int?,
    val name: String?,
    val email: String?,
    val password: String?,
    val recordStatus: String?,
    val createdAt: Long?
)

fun createUserTable(conn: Connection) {
    conn.createStatement().use { stmt ->
        stmt.execute(
            """
                    CREATE TABLE user (
                        user_id INTEGER PRIMARY KEY,
                        name TEXT,
                        email TEXT,
                        password TEXT,
                        record_status TEXT,
                        created_at INTEGER
                    )
                    """
                .trimIndent())
    }
}

fun feedUserTable(conn: Connection) {
    conn.createStatement().use { stmt ->
        stmt.execute(
            """
                INSERT INTO user (user_id, name, email, password, record_status, created_at) VALUES
                (1, 'John Doe', 'john.doe@example.com', 'password1', 'active', CURRENT_TIMESTAMP),
                (2, 'Jane Smith', 'jane.smith@example.com', 'password2', 'active', CURRENT_TIMESTAMP),
                (3, 'Alice Johnson', 'alice.johnson@example.com', 'password3', 'active', CURRENT_TIMESTAMP),
                (4, 'Bob Brown', 'bob.brown@example.com', 'password4', 'active', CURRENT_TIMESTAMP)
            """
                .trimIndent())
    }
}
