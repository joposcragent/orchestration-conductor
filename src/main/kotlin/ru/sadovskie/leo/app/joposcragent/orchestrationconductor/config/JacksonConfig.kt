package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule

@Configuration
class JacksonConfig {

	@Bean
	fun jsonMapper(): JsonMapper =
		JsonMapper.builder()
			.addModules(kotlinModule())
			.build()
}
