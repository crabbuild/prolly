plugins {
    kotlin("jvm") version "2.2.21"
    `maven-publish`
}

group = "build.crab"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("net.java.dev.jna:jna:5.14.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
}

tasks.test {
    useJUnitPlatform()
}
