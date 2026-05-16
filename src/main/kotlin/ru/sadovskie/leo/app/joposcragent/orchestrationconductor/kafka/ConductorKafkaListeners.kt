package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.CollectionBatchOrchestrationService
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.CollectionQueryResultOrchestrationService
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.JobPostingEvaluateOrchestrationService
import tools.jackson.databind.JsonNode
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
		log.debugKafkaInbound(record)
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: run {
				if (log.isDebugEnabled) {
					log.debug("collection-batch-begin: no message type in headers or json, key={}", record.key())
				}
				return
			}
		if (type != OrchestrationMessageTypes.COLLECTION_BATCH_BEGIN) {
			if (log.isDebugEnabled) {
				log.debug(
					"collection-batch-begin: ignored message type={} topic={} key={}",
					type,
					record.topic(),
					record.key(),
				)
			}
			return
		}
		val root = runCatching { jsonMapper.readTree(record.value()) }.getOrElse {
			log.warn("collection-batch-begin: invalid json: {}", it.message)
			return
		}
		val payload = root.kafkaMessagePayloadOrNull() ?: run {
			log.warn("collection-batch-begin: missing or invalid body")
			return
		}
		log.info(
			"collection-batch-begin: dispatching parentJobKey={} topic={} type={}",
			record.key(),
			record.topic(),
			type,
		)
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
		log.debugKafkaInbound(record)
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: run {
				if (log.isDebugEnabled) {
					log.debug("job-posting-create-result: no message type in headers or json, key={}", record.key())
				}
				return
			}
		if (type != OrchestrationMessageTypes.JOB_POSTING_CREATE_RESULT) {
			if (log.isDebugEnabled) {
				log.debug(
					"job-posting-create-result: ignored message type={} topic={} key={}",
					type,
					record.topic(),
					record.key(),
				)
			}
			return
		}
		val root = runCatching { jsonMapper.readTree(record.value()) }.getOrElse {
			log.warn("job-posting-create-result: invalid json: {}", it.message)
			return
		}
		val payload = root.kafkaMessagePayloadOrNull() ?: run {
			log.warn("job-posting-create-result: missing or invalid body")
			return
		}
		log.info(
			"job-posting-create-result: dispatching jobKey={} topic={} type={}",
			record.key(),
			record.topic(),
			type,
		)
		jobPostingEvaluateOrchestrationService.onJobPostingCreateResult(payload)
	}

	private fun parseTypeFromJson(json: String): String? =
		runCatching {
			jsonMapper.readTree(json).path("headers").path("type").asText(null)
		}.getOrNull()
}

@Component
class CollectionQueryResultKafkaListener(
	private val jsonMapper: JsonMapper,
	private val collectionQueryResultOrchestrationService: CollectionQueryResultOrchestrationService,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(
		topics = [OrchestrationKafkaTopics.COLLECTION_QUERY],
		groupId = "\${app.kafka.collection-query-result-consumer-group}",
	)
	fun onMessage(record: ConsumerRecord<String, String>) {
		log.debugKafkaInbound(record)
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: run {
				if (log.isDebugEnabled) {
					log.debug("collection-query-result: no message type in headers or json, key={}", record.key())
				}
				return
			}
		if (type != OrchestrationMessageTypes.COLLECTION_QUERY_RESULT) {
			if (log.isDebugEnabled) {
				log.debug(
					"collection-query-result: ignored message type={} topic={} key={}",
					type,
					record.topic(),
					record.key(),
				)
			}
			return
		}
		val root = runCatching { jsonMapper.readTree(record.value()) }.getOrElse {
			log.warn("collection-query-result: invalid json: {}", it.message)
			return
		}
		val payload = root.kafkaMessagePayloadOrNull() ?: run {
			log.warn("collection-query-result: missing or invalid body")
			return
		}
		log.info(
			"collection-query-result: dispatching jobKey={} topic={} type={}",
			record.key(),
			record.topic(),
			type,
		)
		collectionQueryResultOrchestrationService.onCollectionQueryResult(payload)
	}

	private fun parseTypeFromJson(json: String): String? =
		runCatching {
			jsonMapper.readTree(json).path("headers").path("type").asText(null)
		}.getOrNull()
}

private fun Logger.debugKafkaInbound(record: ConsumerRecord<String, String>) {
	if (!isDebugEnabled) {
		return
	}
	val headersJoined = record.headers().joinToString(prefix = "[", postfix = "]") { h ->
		"${h.key()}=${h.value().toString(Charsets.UTF_8)}"
	}
	debug(
		"Kafka inbound topic={} partition={} offset={} key={} headers={} value={}",
		record.topic(),
		record.partition(),
		record.offset(),
		record.key(),
		headersJoined,
		record.value(),
	)
}

private fun JsonNode.kafkaMessagePayloadOrNull(): JsonNode? {
	val headers = get("headers")
	val payload = get("payload")
	return when {
		headers != null && headers.isObject && payload != null && payload.isObject -> payload
		isObject -> this
		else -> null
	}
}
