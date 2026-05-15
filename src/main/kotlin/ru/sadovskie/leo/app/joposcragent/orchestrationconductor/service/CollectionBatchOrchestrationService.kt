package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.SettingsSearchQueryFeignClient
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

@Service
class CollectionBatchOrchestrationService(
	private val searchQueryClient: SettingsSearchQueryFeignClient,
	private val publisher: OrchestrationKafkaPublisher,
	private val jsonMapper: JsonMapper,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	fun onCollectionBatchBegin(payload: JsonNode) {
		val parentJobUuid = payload.uuid("jobUuid") ?: run {
			if (log.isDebugEnabled) {
				log.debug(
					"collection-batch-begin: missing jobUuid in payload={}",
					jsonMapper.writeValueAsString(payload),
				)
			}
			return
		}
		try {
			val response = searchQueryClient.listSearchQueries(true)
			val queries = response.body ?: emptyList()
			if (log.isDebugEnabled) {
				log.debug(
					"listSearchQueries(activeOnly=true): status={} count={} body={}",
					response.statusCode,
					queries.size,
					jsonMapper.writeValueAsString(queries),
				)
			}
			if (queries.isEmpty()) {
				log.info(
					"collection-batch-begin: no active search queries, publishing CANCELED parentJobUuid={}",
					parentJobUuid,
				)
				publisher.publishCollectionBatchResult(
					parentJobUuid = parentJobUuid,
					status = "CANCELED",
					resultMessage = "Не найдено активных поисковых запросов",
				)
				return
			}
			log.info(
				"collection-batch-begin: publishing {} collection-query-begin child job(s) parentJobUuid={}",
				queries.size,
				parentJobUuid,
			)
			for (item in queries) {
				val childUuid = UUID.randomUUID()
				val lazy = item.isLazyScraping == true
				publisher.publishCollectionQueryBegin(
					jobUuid = childUuid,
					query = item.query,
					searchQueryUuid = item.uuid,
					lazy = lazy,
					parentJobUuid = parentJobUuid,
				)
			}
		} catch (e: Exception) {
			log.error("collection-batch-begin failed for parentJobUuid={}", parentJobUuid, e)
			val msg = e.toString() + (e.cause?.let { "\nCause: $it" } ?: "")
			log.info(
				"collection-batch-begin: publishing FAILED batch result parentJobUuid={}",
				parentJobUuid,
			)
			publisher.publishCollectionBatchResult(
				parentJobUuid = parentJobUuid,
				status = "FAILED",
				resultMessage = msg,
			)
		}
	}

	private fun JsonNode.uuid(field: String): UUID? =
		get(field)?.takeIf { !it.isNull && it.asText().isNotBlank() }?.let { UUID.fromString(it.asText()) }
}
