package org.example

import com.squareup.moshi.Json
import com.squareup.moshi.JsonClass


@JsonClass(generateAdapter = true)
data class MergeConfig(
    @Json(name = "key")
    val key: String,
    @Json(name = "split")
    val split: List<SplitInfo>
)