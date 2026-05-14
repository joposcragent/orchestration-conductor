import nu.studer.gradle.jooq.JooqEdition
import org.jooq.meta.jaxb.Database
import org.jooq.meta.jaxb.Generate
import org.jooq.meta.jaxb.Generator
import org.jooq.meta.jaxb.Jdbc
import org.jooq.meta.jaxb.Target
import org.openapitools.generator.gradle.plugin.tasks.GenerateTask
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
	kotlin("jvm") version "2.2.21"
	kotlin("plugin.spring") version "2.2.21"
	id("org.springframework.boot") version "4.0.5"
	id("io.spring.dependency-management") version "1.1.7"
	id("nu.studer.jooq") version "9.0"
	id("org.openapi.generator") version "7.14.0"
	jacoco
}

group = "ru.sadovskie.leo.app.joposcragent"
version = "1.0.0"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

dependencyManagement {
	imports {
		mavenBom("org.springframework.cloud:spring-cloud-dependencies:2025.1.1")
	}
}

val openapiConductorDir = layout.buildDirectory.dir("generated/openapi-conductor").get().asFile.path
val openapiFeignDir = layout.buildDirectory.dir("generated/openapi-feign").get().asFile.path

tasks.register<GenerateTask>("openApiGenerateConductorModels") {
	generatorName.set("kotlin")
	val specFile = layout.projectDirectory.file("../../specifications/services/orchestration-conductor/openapi.yaml").asFile
	inputSpec.set(specFile.toURI().toString())
	outputDir.set(openapiConductorDir)
	packageName.set("ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi")
	apiPackage.set("ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.api")
	modelPackage.set("ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model")
	configOptions.set(
		mapOf(
			"serializationLibrary" to "jackson",
			"enumPropertyNaming" to "UPPERCASE",
			"useSpringBoot3" to "true",
			"documentationProvider" to "none",
		),
	)
	globalProperties.set(
		mapOf(
			"models" to "",
			"modelDocs" to "false",
			"apis" to "false",
			"supportingFiles" to "false",
		),
	)
}

tasks.register<GenerateTask>("openApiGenerateSettingsFeign") {
	generatorName.set("spring")
	val specFile = layout.projectDirectory.file("src/openapi/settings-manager-search-query.yaml").asFile
	inputSpec.set(specFile.toURI().toString())
	outputDir.set(openapiFeignDir)
	packageName.set("ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client")
	apiPackage.set("ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.api")
	modelPackage.set("ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.model")
	configOptions.set(
		mapOf(
			"library" to "spring-cloud",
			"useSpringBoot3" to "true",
			"documentationProvider" to "none",
			"dateLibrary" to "java8",
			"openApiNullable" to "false",
			"interfaceOnly" to "true",
			"skipDefaultInterface" to "true",
		),
	)
	globalProperties.set(
		mapOf(
			"models" to "",
			"modelDocs" to "false",
			"apis" to "",
		),
	)
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	implementation("org.springframework.kafka:spring-kafka")
	implementation("org.springframework.cloud:spring-cloud-starter-openfeign")
	implementation("io.github.openfeign:feign-okhttp")
	implementation("io.swagger.core.v3:swagger-annotations:2.2.22")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("tools.jackson.module:jackson-module-kotlin")
	implementation("org.jooq:jooq:3.19.31")

	jooqGenerator("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-actuator-test")
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("io.mockk:mockk-jvm:1.14.6")
	testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
	testImplementation("org.wiremock:wiremock-standalone:3.9.1")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

sourceSets["main"].kotlin.srcDir("$openapiConductorDir/src/main/kotlin")
sourceSets["main"].java.srcDir("$openapiFeignDir/src/main/java")
sourceSets["main"].java.srcDir("build/generated-src/jooq/main")

val jooqDbUrl = System.getenv("JOOQ_DB_URL") ?: "jdbc:postgresql://localhost:5432/joposcragent"
val jooqDbUser = System.getenv("JOOQ_DB_USER") ?: "postgres"
val jooqDbPassword = System.getenv("JOOQ_DB_PASSWORD") ?: "postgres"

jooq {
	version.set("3.19.31")
	edition.set(JooqEdition.OSS)
	configurations {
		create("main") {
			generateSchemaSourceOnCompilation.set(false)
			jooqConfiguration.apply {
				jdbc = Jdbc().apply {
					driver = "org.postgresql.Driver"
					url = jooqDbUrl
					user = jooqDbUser
					password = jooqDbPassword
				}
				generator = Generator().apply {
					database = Database().apply {
						name = "org.jooq.meta.postgres.PostgresDatabase"
						inputSchema = "orchestration"
						excludes = "flyway_schema_history"
					}
					generate = Generate().apply {
						isDeprecated = false
						isRecords = true
						isImmutablePojos = true
						isFluentSetters = true
					}
					target = Target().apply {
						packageName = "ru.sadovskie.leo.app.joposcragent.orchestrationconductor.jooq"
						directory = "build/generated-src/jooq/main"
					}
				}
			}
		}
	}
}

tasks.named("compileJava") {
	dependsOn("openApiGenerateSettingsFeign")
}

tasks.named("compileKotlin") {
	dependsOn("openApiGenerateConductorModels", "generateJooq", "openApiGenerateSettingsFeign")
}

val dockerImageRepository = System.getenv("IMAGE_NAME") ?: "joposcragent/${rootProject.name}"
val dockerImageTag = System.getenv("IMAGE_TAG") ?: project.version.toString()

tasks.bootBuildImage {
	imageName.set("$dockerImageRepository:$dockerImageTag")
	finalizedBy("bootBuildImageTagLatest")
}

tasks.register<Exec>("bootBuildImageTagLatest") {
	group = "container"
	description = "docker tag: помечает образ из bootBuildImage тегом latest"
	commandLine(
		"docker", "tag",
		"$dockerImageRepository:$dockerImageTag",
		"$dockerImageRepository:latest",
	)
}

tasks.register("buildImage") {
	group = "container"
	description = "Build OCI image via Buildpacks (bootBuildImage)."
	dependsOn("bootBuildImage")
}

tasks.withType<Test> {
	useJUnitPlatform()
	testLogging {
		events = setOf(TestLogEvent.FAILED, TestLogEvent.SKIPPED)
		exceptionFormat = TestExceptionFormat.FULL
	}
	finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
	dependsOn(tasks.test)
	reports {
		xml.required.set(true)
		html.required.set(true)
	}
	val excludes = listOf(
		"**/orchestrationconductor/openapi/**",
		"**/orchestrationconductor/jooq/**",
		"**/orchestrationconductor/client/**",
	)
	classDirectories.setFrom(
		sourceSets.main.get().output.classesDirs.map { dir ->
			fileTree(dir) { exclude(excludes) }
		},
	)
}

tasks.jacocoTestCoverageVerification {
	dependsOn(tasks.jacocoTestReport)
	violationRules {
		rule {
			limit {
				counter = "LINE"
				value = "COVEREDRATIO"
				minimum = "0.60".toBigDecimal()
			}
		}
	}
	classDirectories.setFrom(tasks.jacocoTestReport.get().classDirectories)
}

tasks.check {
	dependsOn(tasks.jacocoTestCoverageVerification)
}
