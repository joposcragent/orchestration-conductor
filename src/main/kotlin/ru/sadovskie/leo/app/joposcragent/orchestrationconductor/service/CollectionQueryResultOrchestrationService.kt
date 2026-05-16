package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import feign.FeignException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.asyncjobs.client.model.AsyncJobStatus
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.OrchestrationAsyncJobsCrudClient
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

@Service
class CollectionQueryResultOrchestrationService(
	private val asyncJobsCrudClient: OrchestrationAsyncJobsCrudClient,
	private val jsonMapper: JsonMapper,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	fun onCollectionQueryResult(payload: JsonNode) {
		val childJobUuid = payload.uuid("jobUuid") ?: run {
			if (log.isDebugEnabled) {
				log.debug(
					"collection-query-result: missing jobUuid payload={}",
					jsonMapper.writeValueAsString(payload),
				)
			}
			return
		}
		val jobResp = runCatching { asyncJobsCrudClient.getAsyncJob(childJobUuid) }
			.getOrElse { e ->
				log.warn("collection-query-result: get job failed jobUuid={} err={}", childJobUuid, e.message)
				return
			}
		if (jobResp.statusCode == HttpStatus.NOT_FOUND) {
			log.warn("collection-query-result: job not found jobUuid={}", childJobUuid)
			return
		}
		if (!jobResp.statusCode.is2xxSuccessful || jobResp.body == null) {
			log.warn(
				"collection-query-result: unexpected get status={} jobUuid={}",
				jobResp.statusCode,
				childJobUuid,
			)
			return
		}
		val parentUuid = jobResp.body!!.parentUuid ?: return
		val listResp = runCatching {
			asyncJobsCrudClient.listAsyncJobs(
				parentJobUuid = parentUuid,
				status = AsyncJobStatus.STARTED,
				size = 1,
				page = 1,
			)
		}.getOrElse { e ->
			log.warn("collection-query-result: list failed parentUuid={} err={}", parentUuid, e.message)
			return
		}
		if (!listResp.statusCode.is2xxSuccessful || listResp.body == null) {
			log.warn(
				"collection-query-result: unexpected list status={} parentUuid={}",
				listResp.statusCode,
				parentUuid,
			)
			return
		}
		if (listResp.body?.list?.isNotEmpty() == true) {
			return
		}
		log.info("Auto resolving parent task {}", parentUuid)
		val finishBody = mapOf(
			"result" to mapOf(
				"autoResolved" to true,
				"autoResolveSource" to childJobUuid.toString(),
			),
		)
		try {
			val fin = asyncJobsCrudClient.finishAsyncJob(parentUuid, "SUCCEEDED", finishBody)
			if (fin.statusCode == HttpStatus.NOT_FOUND) {
				log.error(
					"collection-query-result: parent finish returned 404 parentUuid={}",
					parentUuid,
				)
				return
			}
			if (fin.statusCode == HttpStatus.CONFLICT) {
				if (log.isDebugEnabled) {
					log.debug(
						"collection-query-result: parent finish 409 (idempotent) parentUuid={}",
						parentUuid,
					)
				}
				return
			}
			if (!fin.statusCode.is2xxSuccessful) {
				log.warn(
					"collection-query-result: unexpected finish status={} parentUuid={}",
					fin.statusCode,
					parentUuid,
				)
			}
		} catch (e: FeignException) {
			when (e.status()) {
				404 -> log.error(
					"collection-query-result: parent finish returned 404 parentUuid={}",
					parentUuid,
				)
				409 -> if (log.isDebugEnabled) {
					log.debug(
						"collection-query-result: parent finish 409 (idempotent) parentUuid={}",
						parentUuid,
					)
				}
				else -> throw e
			}
		}
	}

	private fun JsonNode.uuid(field: String): UUID? =
		get(field)?.takeIf { !it.isNull && it.asText().isNotBlank() }?.let { UUID.fromString(it.asText()) }
}
