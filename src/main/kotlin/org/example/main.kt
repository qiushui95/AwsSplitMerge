package org.example

import org.codehaus.jackson.map.ObjectMapper
import java.io.File
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

fun main() {
    val json = "{\n" +
            "    \"key\": \"cdntest/418.xapk\",\n" +
            "    \"bucket\": \"res-southeast\",\n" +
            "    \"split\": [\n" +
            "        {\n" +
            "            \"key\": \"cdntest/418.xapk_split/418.xapk.0\",\n" +
            "            \"size\": 5242880\n" +
            "        }" +
            "    ]\n" +
            "}"


    val readValue = ObjectMapper().readValue(json, MergeConfig::class.java)

    println(readValue)
}
