package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client

import org.springframework.cloud.openfeign.FeignClient
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.asyncjobs.client.model.AsyncJobItem
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.asyncjobs.client.model.AsyncJobList
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.asyncjobs.client.model.AsyncJobStatus
import java.util.UUID

@FeignClient(
	name = "orchestrationAsyncJobsCrud",
	url = "\${joposcragent.orchestration-async-jobs-crud.base-url}",
	contextId = "orchestrationAsyncJobsCrud",
	primary = false,
)
interface OrchestrationAsyncJobsCrudClient {

	@GetMapping("/async-jobs/{jobUuid}")
	fun getAsyncJob(@PathVariable jobUuid: UUID): ResponseEntity<AsyncJobItem>

	@GetMapping("/async-jobs/list")
	fun listAsyncJobs(
		@RequestParam parentJobUuid: UUID,
		@RequestParam status: AsyncJobStatus,
		@RequestParam size: Int,
		@RequestParam page: Int,
	): ResponseEntity<AsyncJobList>

	@PostMapping("/async-jobs/{jobUuid}/finish/{terminalStatus}")
	fun finishAsyncJob(
		@PathVariable jobUuid: UUID,
		@PathVariable terminalStatus: String,
		@RequestBody(required = false) body: Map<String, Any>?,
	): ResponseEntity<Void>
}
