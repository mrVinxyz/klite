plugins {
    kotlin("jvm") version("2.0.0")
    id("com.ncorti.ktfmt.gradle") version("0.19.0")
    id("io.gitlab.arturbosch.detekt") version("1.23.3")
}

group = "mrvin.ktstd"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

ktfmt {
    kotlinLangStyle()
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.xerial:sqlite-jdbc:3.46.0.1")
}

kotlin {
    jvmToolchain(20)
}

tasks.test {
    useJUnitPlatform()
}

tasks.register("lint") {
    group = "linting"
    description = "Lints the entire project using detekt"
    dependsOn("detekt")
}

tasks.register("fmt") {
    group = "formatting"
    description = "Formats the entire project using ktfmt"
    dependsOn("ktfmtFormat")
}