package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.web

import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model.AsyncJobUuid
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model.SearchQueryItem
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.EnqueueService
import tools.jackson.databind.json.JsonMapper

@RestController
class EnqueueController(
	private val enqueueService: EnqueueService,
	private val jsonMapper: JsonMapper,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@PostMapping("/enqueue/collection-batch")
	fun enqueueCollectionBatch(): ResponseEntity<AsyncJobUuid> {
		if (log.isDebugEnabled) {
			log.debug("POST /enqueue/collection-batch")
		}
		val jobUuid = enqueueService.enqueueCollectionBatch()
		val body = AsyncJobUuid(jobUuid)
		if (log.isDebugEnabled) {
			log.debug("POST /enqueue/collection-batch -> 200 body={}", jsonMapper.writeValueAsString(body))
		}
		return ResponseEntity.ok(body)
	}

	@PostMapping("/enqueue/collection-query")
	fun enqueueCollectionQuery(@RequestBody body: SearchQueryItem): ResponseEntity<AsyncJobUuid> {
		if (log.isDebugEnabled) {
			log.debug("POST /enqueue/collection-query body={}", jsonMapper.writeValueAsString(body))
		}
		val jobUuid = enqueueService.enqueueCollectionQuery(body)
		val response = AsyncJobUuid(jobUuid)
		if (log.isDebugEnabled) {
			log.debug("POST /enqueue/collection-query -> 200 body={}", jsonMapper.writeValueAsString(response))
		}
		return ResponseEntity.ok(response)
	}
}
