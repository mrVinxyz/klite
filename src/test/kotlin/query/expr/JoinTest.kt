package query.expr

import query.Query
import kotlin.test.Test
import query.schema.Table
import query.schema.select
import kotlin.test.assertEquals

// Test tables definition
object Departments : Table("department") {
    val id = integer("id").setPrimaryKey()
    val name = text("name")
    val locationId = integer("location_id")
}

object Locations : Table("location") {
    val id = integer("id").setPrimaryKey()
    val city = text("city")
    val country = text("country")
}

class JoinTest {
    private fun assertQuery(expected: String, actual: Query) {
        assertEquals(expected, actual.sql)
    }

    @Test
    fun `test inner join`() {
        val query = Employees
            .select()
            .join {
                Employees.department inner Departments.name
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee INNER JOIN department ON employee.department = department.name",
            query
        )
    }

    @Test
    fun `test left join`() {
        val query = Employees
            .select()
            .join {
                Employees.department left Departments.name
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee LEFT JOIN department ON employee.department = department.name",
            query
        )
    }

    @Test
    fun `test right join`() {
        val query = Employees
            .select()
            .join {
                Employees.department right Departments.name
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee RIGHT JOIN department ON employee.department = department.name",
            query
        )
    }

    @Test
    fun `test full join`() {
        val query = Employees
            .select()
            .join {
                Employees.department full Departments.name
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee FULL JOIN department ON employee.department = department.name",
            query
        )
    }

    @Test
    fun `test outer join`() {
        val query = Employees
            .select()
            .join {
                Employees.department outer Departments.name
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee OUTER JOIN department ON employee.department = department.name",
            query
        )
    }

    @Test
    fun `test multiple joins`() {
        val query = Employees
            .select()
            .join {
                Employees.department inner Departments.name
                Departments.locationId left Locations.id
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee " +
                    "INNER JOIN department ON employee.department = department.name " +
                    "LEFT JOIN location ON department.location_id = location.id",
            query
        )
    }

    @Test
    fun `test join with where clause`() {
        val query = Employees
            .select()
            .join {
                Employees.department inner Departments.name
            }
            .where {
                Locations.country eq "USA"
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee " +
                    "INNER JOIN department ON employee.department = department.name " +
                    "WHERE country = ?",
            query
        )
    }
}
