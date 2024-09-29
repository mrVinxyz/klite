import org.gradle.kotlin.dsl.test

plugins { kotlin("jvm") version ("2.0.10") }

group = "mrvin"

version = "0.0.1"

repositories { mavenCentral() }

private val xerialJdbcVersion = "3.46.0.0"
private val mockitoVersion = "5.4.0"
private val logbackVersion = "1.4.14"

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.xerial:sqlite-jdbc:$xerialJdbcVersion")
    testImplementation("org.mockito.kotlin:mockito-kotlin:$mockitoVersion")
    testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
}

kotlin { jvmToolchain(21) }

tasks.test { useJUnitPlatform() }