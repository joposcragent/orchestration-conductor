package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.web

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model.AsyncJobUuid
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model.SearchQueryItem
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.EnqueueService

@RestController
class EnqueueController(
	private val enqueueService: EnqueueService,
) {

	@PostMapping("/enqueue/collection-batch")
	fun enqueueCollectionBatch(): ResponseEntity<AsyncJobUuid> {
		val jobUuid = enqueueService.enqueueCollectionBatch()
		return ResponseEntity.ok(AsyncJobUuid(jobUuid))
	}

	@PostMapping("/enqueue/collection-query")
	fun enqueueCollectionQuery(@RequestBody body: SearchQueryItem): ResponseEntity<AsyncJobUuid> {
		val jobUuid = enqueueService.enqueueCollectionQuery(body)
		return ResponseEntity.ok(AsyncJobUuid(jobUuid))
	}
}
