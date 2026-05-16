package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.integration

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoBean
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.asyncjobs.client.model.AsyncJobItem
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.asyncjobs.client.model.AsyncJobList
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.asyncjobs.client.model.AsyncJobStatus
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.OrchestrationAsyncJobsCrudClient
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.SettingsSearchQueryFeignClient
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaTopics
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationMessageTypes
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.util.Collections
import java.util.UUID

@SpringBootTest
@Import(EmbeddedKafkaTestProducerConfig::class)
@EmbeddedKafka(
	partitions = 1,
	topics = [
		OrchestrationKafkaTopics.COLLECTION_BATCH,
		OrchestrationKafkaTopics.COLLECTION_QUERY,
		OrchestrationKafkaTopics.JOB_POSTING_CREATE,
	],
)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CollectionQueryResultKafkaIntegrationTest {

	companion object {
		@JvmStatic
		@DynamicPropertySource
		fun kafkaProps(r: DynamicPropertyRegistry) {
			r.add("app.kafka.batch-consumer-group") { "test-conductor-batch-${UUID.randomUUID()}" }
			r.add("app.kafka.create-result-consumer-group") { "test-conductor-create-${UUID.randomUUID()}" }
			r.add("app.kafka.collection-query-result-consumer-group") {
				"test-conductor-query-result-${UUID.randomUUID()}"
			}
		}
	}

	@Autowired
	private lateinit var kafkaTemplate: KafkaTemplate<String, String>

	@MockitoBean
	private lateinit var asyncJobsCrudClient: OrchestrationAsyncJobsCrudClient

	@MockitoBean
	private lateinit var searchQueryClient: SettingsSearchQueryFeignClient

	private val now: OffsetDateTime = OffsetDateTime.parse("2026-01-01T12:00:00Z")

	@Test
	fun `consumes query result and finishes parent when no started siblings`() {
		val child = UUID.randomUUID()
		val parent = UUID.randomUUID()
		val jobItem = AsyncJobItem(child, "async-job.collection-query", AsyncJobStatus.SUCCEEDED, now)
		jobItem.parentUuid = parent
		whenever(asyncJobsCrudClient.getAsyncJob(child)).thenReturn(ResponseEntity.ok(jobItem))
		whenever(
			asyncJobsCrudClient.listAsyncJobs(
				eq(parent),
				eq(AsyncJobStatus.STARTED),
				eq(1),
				eq(1),
			),
		).thenReturn(ResponseEntity.ok(AsyncJobList(Collections.emptyList())))
		whenever(asyncJobsCrudClient.finishAsyncJob(eq(parent), eq("SUCCEEDED"), any()))
			.thenReturn(ResponseEntity.ok().build())

		val body = """{"jobUuid":"$child"}"""
		val headers = listOf(
			RecordHeader(
				"type",
				OrchestrationMessageTypes.COLLECTION_QUERY_RESULT.toByteArray(StandardCharsets.UTF_8),
			),
			RecordHeader("key", child.toString().toByteArray(StandardCharsets.UTF_8)),
			RecordHeader("createdAt", "2026-01-01T12:00:00Z".toByteArray(StandardCharsets.UTF_8)),
			RecordHeader("schemaVersion", "1.0".toByteArray(StandardCharsets.UTF_8)),
		)
		kafkaTemplate.send(
			ProducerRecord(OrchestrationKafkaTopics.COLLECTION_QUERY, null, child.toString(), body, headers),
		).get()

		verify(asyncJobsCrudClient, timeout(15_000).times(1)).finishAsyncJob(
			eq(parent),
			eq("SUCCEEDED"),
			argThat { body ->
				@Suppress("UNCHECKED_CAST")
				val r = body?.get("result") as? Map<*, *> ?: return@argThat false
				r["autoResolved"] == true && r["autoResolveSource"] == child.toString()
			},
		)
	}

	@Test
	fun `consumes query result and skips finish when started siblings exist`() {
		val child = UUID.randomUUID()
		val parent = UUID.randomUUID()
		val jobItem = AsyncJobItem(child, "async-job.collection-query", AsyncJobStatus.SUCCEEDED, now)
		jobItem.parentUuid = parent
		val sibling = AsyncJobItem(UUID.randomUUID(), "async-job.collection-query", AsyncJobStatus.STARTED, now)
		whenever(asyncJobsCrudClient.getAsyncJob(child)).thenReturn(ResponseEntity.ok(jobItem))
		whenever(
			asyncJobsCrudClient.listAsyncJobs(
				eq(parent),
				eq(AsyncJobStatus.STARTED),
				eq(1),
				eq(1),
			),
		).thenReturn(ResponseEntity.ok(AsyncJobList(listOf(sibling))))

		val body = """{"jobUuid":"$child"}"""
		val headers = listOf(
			RecordHeader(
				"type",
				OrchestrationMessageTypes.COLLECTION_QUERY_RESULT.toByteArray(StandardCharsets.UTF_8),
			),
			RecordHeader("key", child.toString().toByteArray(StandardCharsets.UTF_8)),
			RecordHeader("createdAt", "2026-01-01T12:00:00Z".toByteArray(StandardCharsets.UTF_8)),
			RecordHeader("schemaVersion", "1.0".toByteArray(StandardCharsets.UTF_8)),
		)
		kafkaTemplate.send(
			ProducerRecord(OrchestrationKafkaTopics.COLLECTION_QUERY, null, child.toString(), body, headers),
		).get()

		Thread.sleep(2000)
		verify(asyncJobsCrudClient, never()).finishAsyncJob(any(), any(), any())
	}

	@Test
	fun `finish parent 409 is treated as success`() {
		val child = UUID.randomUUID()
		val parent = UUID.randomUUID()
		val jobItem = AsyncJobItem(child, "async-job.collection-query", AsyncJobStatus.SUCCEEDED, now)
		jobItem.parentUuid = parent
		whenever(asyncJobsCrudClient.getAsyncJob(child)).thenReturn(ResponseEntity.ok(jobItem))
		whenever(
			asyncJobsCrudClient.listAsyncJobs(
				eq(parent),
				eq(AsyncJobStatus.STARTED),
				eq(1),
				eq(1),
			),
		).thenReturn(ResponseEntity.ok(AsyncJobList(Collections.emptyList())))
		whenever(asyncJobsCrudClient.finishAsyncJob(eq(parent), eq("SUCCEEDED"), any()))
			.thenReturn(ResponseEntity.status(HttpStatus.CONFLICT).build())

		val body = """{"jobUuid":"$child"}"""
		val headers = listOf(
			RecordHeader(
				"type",
				OrchestrationMessageTypes.COLLECTION_QUERY_RESULT.toByteArray(StandardCharsets.UTF_8),
			),
			RecordHeader("key", child.toString().toByteArray(StandardCharsets.UTF_8)),
			RecordHeader("createdAt", "2026-01-01T12:00:00Z".toByteArray(StandardCharsets.UTF_8)),
			RecordHeader("schemaVersion", "1.0".toByteArray(StandardCharsets.UTF_8)),
		)
		kafkaTemplate.send(
			ProducerRecord(OrchestrationKafkaTopics.COLLECTION_QUERY, null, child.toString(), body, headers),
		).get()

		verify(asyncJobsCrudClient, timeout(15_000).times(1)).finishAsyncJob(eq(parent), eq("SUCCEEDED"), any())
	}

}
