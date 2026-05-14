package ru.sadovskie.leo.app.joposcragent.orchestrationconductor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.openfeign.EnableFeignClients
import org.springframework.kafka.annotation.EnableKafka
@SpringBootApplication
@EnableFeignClients
@EnableKafka
class OrchestrationConductorApplication

fun main(args: Array<String>) {
	runApplication<OrchestrationConductorApplication>(*args)
}
