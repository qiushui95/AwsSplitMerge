package org.example


import com.squareup.moshi.Json
import com.squareup.moshi.JsonClass

@JsonClass(generateAdapter = true)
data class SplitInfo(
    @Json(name = "key")
    val key: String,
    @Json(name = "size")
    val size: Long
)