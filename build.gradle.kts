import org.gradle.kotlin.dsl.test

plugins { kotlin("jvm") version ("2.0.20") }

group = "mrvin"

version = "0.1.0"

repositories { mavenCentral() }

val xerialJdbcVersion = "3.47.0.0"
val mockitoVersion = "5.4.0"
val hikariVersion = "5.1.0"

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.xerial:sqlite-jdbc:$xerialJdbcVersion")
    testImplementation("org.mockito.kotlin:mockito-kotlin:$mockitoVersion")
    testImplementation("com.zaxxer:HikariCP:$hikariVersion")
}

kotlin { jvmToolchain(21) }

tasks.test { useJUnitPlatform() }