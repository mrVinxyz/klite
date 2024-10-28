package query.expr

import query.Query
import kotlin.test.Test
import query.schema.Table
import query.schema.select
import java.math.BigDecimal
import kotlin.test.assertEquals

// Test table definition
object Employees : Table("employee") {
    val id = integer("id").setPrimaryKey()
    val name = text("name")
    val age = integer("age")
    val salary = decimal("salary")
    val department = text("department")
    val isActive = boolean("is_active")
    val createdAt = long("created_at")
}

data class Employee(
    val id: Int? = null,
    val name: String? = null,
    val age: Int? = null,
    val salary: BigDecimal? = null,
    val department: String? = null,
    val isActive: Boolean? = null,
    val createdAt: Long? = null
)

class WhereTest {

    private fun assertQuery(expected: String, expectedArgs: List<Any>, actual: Query) {
        assertEquals(expected, actual.sql)
        assertEquals(expectedArgs, actual.args)
    }

    @Test
    fun `test equal operator`() {
        val query = Employees
            .select()
            .where {
                Employees.name eq "John"
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE name = ?",
            listOf("John"),
            query
        )
    }

    @Test
    fun `test multiple conditions with AND`() {
        val query = Employees
            .select()
            .where {
                Employees.name eq "John"
                Employees.age gte 30
                Employees.department eq "IT"
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE name = ? AND age >= ? AND department = ?",
            listOf("John", 30, "IT"),
            query
        )
    }

    @Test
    fun `test OR operator`() {
        val query = Employees
            .select()
            .where {
                or {
                    Employees.department eq "IT"
                    Employees.department eq "HR"
                }
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE (department = ? OR department = ?)",
            listOf("IT", "HR"),
            query
        )
    }

    @Test
    fun `test NOT operator`() {
        val query = Employees
            .select()
            .where {
                not {
                    Employees.department eq "IT"
                }
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE NOT (department = ?)",
            listOf("IT"),
            query
        )
    }

    @Test
    fun `test complex nested conditions`() {
        val query = Employees
            .select()
            .where {
                Employees.isActive eq true
                or {
                    Employees.department eq "IT"
                    and {
                        Employees.salary gte BigDecimal("50000")
                        Employees.age lt 40
                    }
                }
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE is_active = ? AND (department = ? OR (salary >= ? AND age < ?))",
            listOf(true, "IT", BigDecimal("50000"), 40),
            query
        )
    }

    @Test
    fun `test LIKE operators`() {
        val query = Employees
            .select()
            .where {
                Employees.name like "Jo%"
                Employees.department likeContains "dev"
                Employees.name likeStarts "A"
                Employees.department likeEnds "ops"
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE name LIKE ? AND department LIKE ? AND name LIKE ? AND department LIKE ?",
            listOf("Jo%", "%dev%", "A%", "%ops"),
            query
        )
    }

    @Test
    fun `test NULL checks`() {
        val query = Employees
            .select()
            .where {
                Employees.department.isNull()
                Employees.salary.isNotNull()
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE department IS NULL AND salary IS NOT NULL",
            emptyList(),
            query
        )
    }

    @Test
    fun `test IN operator`() {
        val departments = listOf("IT", "HR", "Finance")
        val query = Employees
            .select()
            .where {
                Employees.department inList departments
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE department IN (?, ?, ?)",
            departments,
            query
        )
    }

    @Test
    fun `test NOT IN operator`() {
        val departments = listOf("Sales", "Marketing")
        val query = Employees
            .select()
            .where {
                Employees.department notIn departments
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE department NOT IN (?, ?)",
            departments,
            query
        )
    }

    @Test
    fun `test BETWEEN operator`() {
        val salaryRange = Pair(BigDecimal("50000"), BigDecimal("100000"))
        val query = Employees
            .select()
            .where {
                Employees.salary between salaryRange
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE salary BETWEEN ? AND ?",
            listOf(salaryRange.first, salaryRange.second),
            query
        )
    }

    @Test
    fun `test comparison operators`() {
        val query = Employees
            .select()
            .where {
                Employees.age lt 30
                Employees.age lte 35
                Employees.salary gt BigDecimal("60000")
                Employees.salary gte BigDecimal("65000")
                Employees.department neq "Sales"
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee WHERE age < ? AND age <= ? AND salary > ? AND salary >= ? AND department <> ?",
            listOf(30, 35, BigDecimal("60000"), BigDecimal("65000"), "Sales"),
            query
        )
    }

    @Test
    fun `test null value handling`() {
        val query = Employees
            .select()
            .where {
                Employees.name eq null
                Employees.age gte null
                Employees.department inList null
                Employees.salary between null
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee",
            emptyList(),
            query
        )
    }

    @Test
    fun `test empty IN list handling`() {
        val query = Employees
            .select()
            .where {
                Employees.department inList emptyList()
            }
            .sqlArgs()

        assertQuery(
            "SELECT id, name, age, salary, department, is_active, created_at FROM employee",
            emptyList(),
            query
        )
    }
}