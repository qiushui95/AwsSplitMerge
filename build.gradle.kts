import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "org.example"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("com.amazonaws:aws-lambda-java-core:1.2.2")
    implementation("com.amazonaws:aws-lambda-java-events:3.11.0")
    runtimeOnly("com.amazonaws:aws-lambda-java-log4j2:1.5.1")

    implementation("software.amazon.awssdk:s3:2.18.34")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}

tasks.withType(Zip::class.java) {
    from("compileJava")
    from("compileKotlin")
    from("processResources")
    into("lib") {
        from("configurations.runtimeClasspath")
    }
}