package org.example

import java.io.File
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

fun main() {
    val splitList = listOf(
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.0"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.1"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.2"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.3"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.4"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.5"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.6"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.7"),
        File("C:\\Users\\97457\\Downloads\\40", "40.jpg.8"),
    )

    unzip(mergeSplit(splitList))
}

private fun mergeSplit(splitList: List<File>): File {
    val compressFile = File("C:\\Users\\97457\\Downloads", "merge.gzip")

    if (compressFile.exists()) compressFile.delete()

    compressFile.createNewFile()

    compressFile.outputStream().use { output ->
        splitList.onEach { splitFile ->
            splitFile.inputStream().use { input ->
                input.copyTo(output)
            }
        }.forEach {
//            it.delete()
        }
    }

    return compressFile
}

private fun unzip(compressFile: File) {
    val dstFile = File("C:\\Users\\97457\\Downloads", "cdntest/40.jpg")

    dstFile.parentFile.mkdirs()

    if (dstFile.exists()) dstFile.delete()

    dstFile.createNewFile()

    GZIPInputStream(compressFile.inputStream()).use { input ->
        dstFile.outputStream().use { output ->
            input.copyTo(output)
        }
    }

    compressFile.delete()
}