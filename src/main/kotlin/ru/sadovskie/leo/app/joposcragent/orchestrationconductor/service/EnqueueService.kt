package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model.SearchQueryItem
import java.util.UUID

@Service
class EnqueueService(
	private val publisher: OrchestrationKafkaPublisher,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	fun enqueueCollectionBatch(): UUID {
		val jobUuid = UUID.randomUUID()
		publisher.publishCollectionBatchBegin(jobUuid)
		log.info("manual enqueue: collection-batch-begin jobUuid={}", jobUuid)
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
		log.info(
			"manual enqueue: collection-query-begin jobUuid={} searchQueryUuid={} lazy={}",
			jobUuid,
			body.uuid,
			lazy,
		)
		return jobUuid
	}
}
