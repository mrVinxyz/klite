import org.gradle.kotlin.dsl.test

plugins { kotlin("jvm") version ("2.0.0") }

group = "mrvin.ktstd"

version = "1.0-SNAPSHOT"

repositories { mavenCentral() }

private val xerialJdbcVersion = "3.46.0.0"
private val logbackVersion = "1.4.14"

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.xerial:sqlite-jdbc:$xerialJdbcVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
}

kotlin { jvmToolchain(20) }

tasks.test { useJUnitPlatform() }