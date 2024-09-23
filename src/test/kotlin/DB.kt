import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ArrayBlockingQueue

class DB {
    private var conn: Pool = Pool("jdbc:sqlite::memory:")

    fun conn(): Conn {
        return conn.acquire() ?: error("No connection available")
    }
}

class Pool(uri: String, poolSize: Int = 5) {
    private val connectionPool: ArrayBlockingQueue<Conn> = ArrayBlockingQueue(poolSize)

    init {
        repeat(poolSize) {
            val connection = DriverManager.getConnection(uri)
            connectionPool.offer(Conn(connection, this))
        }
    }

    fun acquire(): Conn? {
        return connectionPool.poll()
    }

    fun release(connection: Conn?) {
        connection?.let {
            if (!it.isClosed) {
                connectionPool.offer(it)
            }
        }
    }

    fun size(): Int {
        return connectionPool.size
    }
}

class Conn(private val connection: Connection, private val pool: Pool) : Connection by connection {
    override fun close() {
        pool.release(this)
    }
}