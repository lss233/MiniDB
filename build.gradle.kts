import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
//    kotlin("jvm") version "1.7.20"
    id("org.jetbrains.kotlin.jvm") version "1.7.20"
    application
}

group = "com.lss233"
version = "1.0-SNAPSHOT"

repositories {
    maven (url = "https://lss233.littleservice.cn/repositories/minecraft")
    mavenCentral()
}

dependencies {
    implementation("io.netty:netty-all:4.1.84.Final")
    implementation("hu.webarticum:tree-printer:2.1.0")
    implementation("cn.hutool:hutool-all:5.8.9")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.4.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useTestNG()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("MainKt")
}