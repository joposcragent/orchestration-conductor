package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.ResponseEntity
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.SettingsSearchQueryFeignClient
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.model.SearchQueriesItem
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.time.OffsetDateTime
import java.util.UUID

class CollectionBatchOrchestrationServiceTest {

	private val jsonMapper = JsonMapper.builder().addModule(kotlinModule()).build()
	private val searchClient = mockk<SettingsSearchQueryFeignClient>()
	private val publisher = mockk<OrchestrationKafkaPublisher>(relaxUnitFun = true)
	private val service = CollectionBatchOrchestrationService(searchClient, publisher, jsonMapper)

	@Test
	fun `publishes canceled when no active queries`() {
		val parent = UUID.randomUUID()
		every { searchClient.listSearchQueries(true) } returns ResponseEntity.ok(emptyList())

		service.onCollectionBatchBegin(jsonMapper.readTree("""{"jobUuid":"$parent"}"""))

		verify(exactly = 1) {
			publisher.publishCollectionBatchResult(parent, "CANCELED", "Не найдено активных поисковых запросов")
		}
		verify(exactly = 0) { publisher.publishCollectionQueryBegin(any(), any(), any(), any(), any()) }
	}

	@Test
	fun `fans out collection-query for each search query`() {
		val parent = UUID.randomUUID()
		val q1 = UUID.randomUUID()
		val q2 = UUID.randomUUID()
		val now = OffsetDateTime.parse("2026-01-01T12:00:00Z")
		val items = listOf(
			SearchQueriesItem(q1, "n1", "query1", 0.5, 0.5, true, false, now),
			SearchQueriesItem(q2, "n2", "query2", 0.6, 0.6, true, true, now),
		)
		every { searchClient.listSearchQueries(true) } returns ResponseEntity.ok(items)

		service.onCollectionBatchBegin(jsonMapper.readTree("""{"jobUuid":"$parent"}"""))

		verify(exactly = 1) { publisher.publishCollectionQueryBegin(any(), "query1", q1, false, parent) }
		verify(exactly = 1) { publisher.publishCollectionQueryBegin(any(), "query2", q2, true, parent) }
		verify(exactly = 0) { publisher.publishCollectionBatchResult(any(), any(), any()) }
	}

	@Test
	fun `publishes failed on downstream error`() {
		val parent = UUID.randomUUID()
		every { searchClient.listSearchQueries(true) } throws RuntimeException("boom")
		val msg = slot<String>()

		service.onCollectionBatchBegin(jsonMapper.readTree("""{"jobUuid":"$parent"}"""))

		verify(exactly = 1) { publisher.publishCollectionBatchResult(parent, "FAILED", capture(msg)) }
		assertTrue(msg.captured.contains("boom"))
	}
}
