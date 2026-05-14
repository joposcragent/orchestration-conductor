package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import org.springframework.stereotype.Service
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import tools.jackson.databind.JsonNode
import java.util.UUID

@Service
class JobPostingEvaluateOrchestrationService(
	private val publisher: OrchestrationKafkaPublisher,
) {

	fun onJobPostingCreateResult(payload: JsonNode) {
		val status = payload.path("status").asText(null) ?: return
		if (status != "SUCCEEDED") {
			return
		}
		val parentJobUuid = payload.uuid("jobUuid") ?: return
		val jobPostingUuid = payload.uuid("jobPostingUuid") ?: return
		val newJobUuid = UUID.randomUUID()
		publisher.publishJobPostingEvaluateBegin(
			jobUuid = newJobUuid,
			parentJobUuid = parentJobUuid,
			jobPostingUuid = jobPostingUuid,
		)
	}

	private fun JsonNode.uuid(field: String): UUID? =
		get(field)?.takeIf { !it.isNull && it.asText().isNotBlank() }?.let { UUID.fromString(it.asText()) }
}
