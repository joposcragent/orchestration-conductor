package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.CollectionBatchOrchestrationService
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.JobPostingEvaluateOrchestrationService
import tools.jackson.databind.json.JsonMapper

@Component
class CollectionBatchBeginKafkaListener(
	private val jsonMapper: JsonMapper,
	private val collectionBatchOrchestrationService: CollectionBatchOrchestrationService,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(
		topics = [OrchestrationKafkaTopics.COLLECTION_BATCH],
		groupId = "\${app.kafka.batch-consumer-group}",
	)
	fun onMessage(record: ConsumerRecord<String, String>) {
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: return
		if (type != OrchestrationMessageTypes.COLLECTION_BATCH_BEGIN) {
			return
		}
		val root = runCatching { jsonMapper.readTree(record.value()) }.getOrElse {
			log.warn("collection-batch-begin: invalid json: {}", it.message)
			return
		}
		val payload = root.get("payload") ?: run {
			log.warn("collection-batch-begin: missing payload")
			return
		}
		collectionBatchOrchestrationService.onCollectionBatchBegin(payload)
	}

	private fun parseTypeFromJson(json: String): String? =
		runCatching {
			jsonMapper.readTree(json).path("headers").path("type").asText(null)
		}.getOrNull()
}

@Component
class JobPostingCreateResultKafkaListener(
	private val jsonMapper: JsonMapper,
	private val jobPostingEvaluateOrchestrationService: JobPostingEvaluateOrchestrationService,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(
		topics = [OrchestrationKafkaTopics.JOB_POSTING_CREATE],
		groupId = "\${app.kafka.create-result-consumer-group}",
	)
	fun onMessage(record: ConsumerRecord<String, String>) {
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: return
		if (type != OrchestrationMessageTypes.JOB_POSTING_CREATE_RESULT) {
			return
		}
		val root = runCatching { jsonMapper.readTree(record.value()) }.getOrElse {
			log.warn("job-posting-create-result: invalid json: {}", it.message)
			return
		}
		val payload = root.get("payload") ?: run {
			log.warn("job-posting-create-result: missing payload")
			return
		}
		jobPostingEvaluateOrchestrationService.onJobPostingCreateResult(payload)
	}

	private fun parseTypeFromJson(json: String): String? =
		runCatching {
			jsonMapper.readTree(json).path("headers").path("type").asText(null)
		}.getOrNull()
}
