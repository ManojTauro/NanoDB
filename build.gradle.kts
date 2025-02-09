plugins {
    id("java")
}

group = "nanodb"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven(url = "https://projectlombok.org/https://projectlombok.org/"
)
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.projectlombok:lombok:1.18.24")
}

tasks.test {
    useJUnitPlatform()

    testLogging {
        events("passed", "skipped", "failed")
    }
}