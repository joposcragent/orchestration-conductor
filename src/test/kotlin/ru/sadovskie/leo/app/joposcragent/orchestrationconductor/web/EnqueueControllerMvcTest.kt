package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.web

import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.mockito.kotlin.any
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service.EnqueueService
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.util.UUID

@WebMvcTest(controllers = [EnqueueController::class])
class EnqueueControllerMvcTest {

	@Autowired
	private lateinit var mockMvc: MockMvc

	private val jsonMapper = JsonMapper.builder().addModule(kotlinModule()).build()

	@MockitoBean
	private lateinit var enqueueService: EnqueueService
	@Test
	fun `post collection batch returns job uuid`() {
		val u = UUID.randomUUID()
		given(enqueueService.enqueueCollectionBatch()).willReturn(u)

		mockMvc.perform(
			post("/enqueue/collection-batch").accept(MediaType.APPLICATION_JSON),
		)
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.jobUuid").value(u.toString()))
	}

	@Test
	fun `post collection query returns job uuid`() {
		val u = UUID.randomUUID()
		given(enqueueService.enqueueCollectionQuery(any())).willReturn(u)
		val body = mapOf(
			"uuid" to UUID.randomUUID().toString(),
			"query" to "q",
			"isLazyScraping" to true,
		)

		mockMvc.perform(
			post("/enqueue/collection-query")
				.contentType(MediaType.APPLICATION_JSON)
				.accept(MediaType.APPLICATION_JSON)
				.content(jsonMapper.writeValueAsString(body)),
		)
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.jobUuid").value(u.toString()))
	}
}
