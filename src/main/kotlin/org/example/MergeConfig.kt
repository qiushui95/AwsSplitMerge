package org.example


data class MergeConfig(
    val key: String="",
    val bucket: String="",
    val split: List<SplitInfo> = emptyList()
)