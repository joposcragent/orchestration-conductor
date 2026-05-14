package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.util.UUID

class JobPostingEvaluateOrchestrationServiceTest {

	private val jsonMapper = JsonMapper.builder().addModule(kotlinModule()).build()
	private val publisher = mockk<OrchestrationKafkaPublisher>(relaxUnitFun = true)
	private val service = JobPostingEvaluateOrchestrationService(publisher)

	@Test
	fun `ignores non succeeded status`() {
		val json = """{"jobUuid":"${UUID.randomUUID()}","status":"FAILED","jobPostingUuid":"${UUID.randomUUID()}"}"""
		service.onJobPostingCreateResult(jsonMapper.readTree(json))
		verify(exactly = 0) { publisher.publishJobPostingEvaluateBegin(any(), any(), any()) }
	}

	@Test
	fun `publishes evaluate begin on succeeded`() {
		val job = UUID.randomUUID()
		val posting = UUID.randomUUID()
		val json = """{"jobUuid":"$job","status":"SUCCEEDED","jobPostingUuid":"$posting"}"""
		service.onJobPostingCreateResult(jsonMapper.readTree(json))
		verify(exactly = 1) { publisher.publishJobPostingEvaluateBegin(any(), job, posting) }
	}
}
