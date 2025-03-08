
plugins {
    kotlin("jvm") version "2.0.20"
    id("org.jetbrains.kotlin.plugin.serialization") version "1.9.21"
    id("maven-publish") // Add Maven Publish plugin
}

group = "pl.steclab"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    val ktorVersion = "3.1.1"
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-websockets:$ktorVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.8.0")
    testImplementation("io.kotest:kotest-assertions-core:5.8.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
    testImplementation("io.mockk:mockk:1.13.8")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}

// Publishing configuration for JitPack
publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = "surrealdb-kotlin"
            version = project.version.toString()

            from(components["java"]) // Include the main JAR

            // Optionally include sources and Javadoc
            artifact(tasks.create("sourcesJar", Jar::class) {
                archiveClassifier.set("sources")
                from(sourceSets["main"].allSource)
            })

            artifact(tasks.create("javadocJar", Jar::class) {
                archiveClassifier.set("javadoc")
                from(tasks.named("javadoc"))
            })
        }
    }
}

// Ensure Javadoc task is available (optional, but recommended)
tasks.withType<Javadoc> {
    isFailOnError = false // JitPack can fail on strict Javadoc errors, so relax this
}