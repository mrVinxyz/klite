# Insert Documentation

The `Insert` class provides a fluent interface for building SQL INSERT statements. It offers two main approaches for defining columns and values to be inserted.

## Class Overview

```kotlin
class Insert(val table: Table)
```

The `Insert` class is initialized with a `Table` object representing the target table for the INSERT operation.

## Key Methods

### insert

```kotlin
fun insert(init: Insert.() -> Unit): Insert
```

This method allows for a DSL-style configuration of the INSERT statement.

### to

```kotlin
infix fun <T : Any> Column<T>.to(value: T?)
```

An infix function that eagerly associates a column with its value.

### unaryPlus

```kotlin
operator fun <T : Any> Column<T>.unaryPlus()
```

An operator function that lazily adds a column to the insert list.

### sqlArgs

```kotlin
fun sqlArgs(): Query
```

This method allows you to specify all the values for the previously defined columns in a single call. The number of provided values must match the number of defined columns.

### values

```kotlin
fun values(vararg value: Any?)
```

This method allows you to specify all the values for the previously defined columns in a single call. The number of provided values must match the number of defined columns.

### values

```kotlin
fun values(map: Map<String, Any?>)
```
This method allows values to be provided as a map, where keys are column names. It's useful when values are dynamically generated or come from a mapped source (like a JSON or configuration object).

## Usage Examples

### Eager Insertion (using `to`)

In this approach, columns and their corresponding values are associated immediately within the insert block. This is convenient when all values are readily available at the time of the insert operation.

```kotlin
fun Accounts.create(account: Account): Query =
    insert {
        accountId to account.accountId
        balance to account.balance
        interestRate to account.interestRate
        accountType to account.accountType
        createdAt to account.createdAt
        isActive to account.isActive
    }.sqlArgs()
```

This method creates a Query directly, which can then be executed or further processed. It ensures that all column-value pairs are set during the block execution.

### Lazy Insertion (using unary plus)

This approach separates column specification from value insertion, allowing more flexibility. You first define the columns to insert into, and later provide the values.

```kotlin
fun Accounts.prepareInsert(): Insert =
    insert {
        +accountId
        +balance
        +interestRate
        +accountType
        +createdAt
        +isActive
    }

// Later, you can add values:
val preparedInsert = Accounts.prepareInsert()
preparedInsert.apply {
    accountId to account.accountId
    balance to account.balance
    // Add other values as needed
}
val query = preparedInsert.sqlArgs()
```

This approach is useful when you need to declare the structure first, then retrieve or calculate the values separately. It also supports deferred insertion logic, where values can be added dynamically.

## Extension Function

```kotlin
fun Insert.persist(conn: Connection): Result<Int>
```

Executes the INSERT statement on the given database connection.

---
