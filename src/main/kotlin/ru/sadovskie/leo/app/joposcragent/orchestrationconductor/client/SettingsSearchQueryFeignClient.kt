package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client

import org.springframework.cloud.openfeign.FeignClient
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.api.SearchQueryApi

@FeignClient(
	name = "settingsSearchQuery",
	url = "\${joposcragent.settings-manager.base-url}",
	contextId = "settingsSearchQuery",
	primary = false,
)
interface SettingsSearchQueryFeignClient : SearchQueryApi
