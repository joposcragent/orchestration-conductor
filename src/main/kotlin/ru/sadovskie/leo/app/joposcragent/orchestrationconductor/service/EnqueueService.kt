package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import org.springframework.stereotype.Service
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model.SearchQueryItem
import java.util.UUID

@Service
class EnqueueService(
	private val publisher: OrchestrationKafkaPublisher,
) {

	fun enqueueCollectionBatch(): UUID {
		val jobUuid = UUID.randomUUID()
		publisher.publishCollectionBatchBegin(jobUuid)
		return jobUuid
	}

	fun enqueueCollectionQuery(body: SearchQueryItem): UUID {
		val jobUuid = UUID.randomUUID()
		val lazy = body.isLazyScraping ?: false
		publisher.publishCollectionQueryBegin(
			jobUuid = jobUuid,
			query = body.query,
			searchQueryUuid = body.uuid,
			lazy = lazy,
			parentJobUuid = null,
			isManual = true,
		)
		return jobUuid
	}
}
