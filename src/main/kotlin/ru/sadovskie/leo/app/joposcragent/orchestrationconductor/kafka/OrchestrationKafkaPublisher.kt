package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper
import tools.jackson.databind.node.ObjectNode
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.util.UUID

@Component
class OrchestrationKafkaPublisher(
	private val kafkaTemplate: KafkaTemplate<String, String>,
	private val jsonMapper: JsonMapper,
) {

	fun publishEnvelope(
		topic: String,
		messageKey: String,
		type: String,
		payload: ObjectNode,
	) {
		val createdAt = OffsetDateTime.now().toString()
		val schemaVersion = "1.0"
		val headersNode = jsonMapper.createObjectNode().apply {
			put("key", messageKey)
			put("createdAt", createdAt)
			put("type", type)
			put("schemaVersion", schemaVersion)
		}
		val root = jsonMapper.createObjectNode().apply {
			set("headers", headersNode)
			set("payload", payload)
		}
		val json = jsonMapper.writeValueAsString(root)
		val record = ProducerRecord(topic, messageKey, json)
		record.headers().add(RecordHeader("type", type.toByteArray(StandardCharsets.UTF_8)))
		record.headers().add(RecordHeader("key", messageKey.toByteArray(StandardCharsets.UTF_8)))
		record.headers().add(RecordHeader("createdAt", createdAt.toByteArray(StandardCharsets.UTF_8)))
		record.headers().add(RecordHeader("schemaVersion", schemaVersion.toByteArray(StandardCharsets.UTF_8)))
		kafkaTemplate.send(record)
	}

	fun publishCollectionBatchBegin(jobUuid: UUID, isManual: Boolean = true) {
		val key = jobUuid.toString()
		val payload = jsonMapper.createObjectNode().apply {
			put("jobUuid", jobUuid.toString())
			if(isManual) {
				putObject("context").put("manual", true)
			}
		}
		publishEnvelope(
			topic = OrchestrationKafkaTopics.COLLECTION_BATCH,
			messageKey = key,
			type = OrchestrationMessageTypes.COLLECTION_BATCH_BEGIN,
			payload = payload,
		)
	}

	fun publishCollectionQueryBegin(
		jobUuid: UUID,
		query: String,
		searchQueryUuid: UUID,
		lazy: Boolean,
		parentJobUuid: UUID? = null,
		isManual: Boolean = false,
	) {
		val key = jobUuid.toString()
		val payload = jsonMapper.createObjectNode().apply {
			put("jobUuid", jobUuid.toString())
			if (parentJobUuid != null) {
				put("parentJobUuid", parentJobUuid.toString())
			}
			put("entityUuid", searchQueryUuid.toString())
			put("query", query)
			put("searchQueryUuid", searchQueryUuid.toString())
			put("lazy", lazy)
			if(isManual) {
				putObject("context").put("manual", true)
			}
		}
		publishEnvelope(
			topic = OrchestrationKafkaTopics.COLLECTION_QUERY,
			messageKey = key,
			type = OrchestrationMessageTypes.COLLECTION_QUERY_BEGIN,
			payload = payload,
		)
	}

	fun publishCollectionBatchResult(
		parentJobUuid: UUID,
		status: String,
		resultMessage: String,
	) {
		val key = parentJobUuid.toString()
		val payload = jsonMapper.createObjectNode().apply {
			put("jobUuid", parentJobUuid.toString())
			put("status", status)
			set("result", jsonMapper.createObjectNode().put("message", resultMessage))
		}
		publishEnvelope(
			topic = OrchestrationKafkaTopics.COLLECTION_BATCH,
			messageKey = key,
			type = OrchestrationMessageTypes.COLLECTION_BATCH_RESULT,
			payload = payload,
		)
	}

	fun publishJobPostingEvaluateBegin(
		jobUuid: UUID,
		parentJobUuid: UUID,
		jobPostingUuid: UUID,
	) {
		val key = jobUuid.toString()
		val payload = jsonMapper.createObjectNode().apply {
			put("jobUuid", jobUuid.toString())
			put("parentJobUuid", parentJobUuid.toString())
			put("entityUuid", jobPostingUuid.toString())
			put("jobPostingUuid", jobPostingUuid.toString())
		}
		publishEnvelope(
			topic = OrchestrationKafkaTopics.JOB_POSTING_EVALUATE,
			messageKey = key,
			type = OrchestrationMessageTypes.JOB_POSTING_EVALUATE_BEGIN,
			payload = payload,
		)
	}
}
