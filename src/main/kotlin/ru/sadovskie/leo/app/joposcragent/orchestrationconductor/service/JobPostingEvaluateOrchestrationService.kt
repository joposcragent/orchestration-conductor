package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

@Service
class JobPostingEvaluateOrchestrationService(
	private val publisher: OrchestrationKafkaPublisher,
	private val jsonMapper: JsonMapper,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	fun onJobPostingCreateResult(payload: JsonNode) {
		val status = payload.path("status").asText(null) ?: run {
			if (log.isDebugEnabled) {
				log.debug(
					"job-posting-create-result: missing status in payload={}",
					jsonMapper.writeValueAsString(payload),
				)
			}
			return
		}
		if (status != "SUCCEEDED") {
			log.info("job-posting-create-result: skip orchestration, status={} (expected SUCCEEDED)", status)
			return
		}
		val parentJobUuid = payload.uuid("jobUuid") ?: run {
			if (log.isDebugEnabled) {
				log.debug(
					"job-posting-create-result: missing jobUuid in payload={}",
					jsonMapper.writeValueAsString(payload),
				)
			}
			return
		}
		val jobPostingUuid = payload.uuid("jobPostingUuid") ?: run {
			if (log.isDebugEnabled) {
				log.debug(
					"job-posting-create-result: missing jobPostingUuid in payload={}",
					jsonMapper.writeValueAsString(payload),
				)
			}
			return
		}
		val newJobUuid = UUID.randomUUID()
		log.info(
			"job-posting-create-result: enqueue evaluate-begin newJobUuid={} parentJobUuid={} jobPostingUuid={}",
			newJobUuid,
			parentJobUuid,
			jobPostingUuid,
		)
		publisher.publishJobPostingEvaluateBegin(
			jobUuid = newJobUuid,
			parentJobUuid = parentJobUuid,
			jobPostingUuid = jobPostingUuid,
		)
	}

	private fun JsonNode.uuid(field: String): UUID? =
		get(field)?.takeIf { !it.isNull && it.asText().isNotBlank() }?.let { UUID.fromString(it.asText()) }
}
