package org.example

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.squareup.moshi.Moshi
import kotlinx.coroutines.*
import okio.Buffer
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.util.*

class Handler : RequestStreamHandler {
    private companion object {
        const val MIN_BLOCK_SIZE = 5 * 1024 * 1024
    }

    private val configAdapter by lazy { Moshi.Builder().build().adapter(MergeConfig::class.java) }

    private data class MergeGroup(val partNumber: Int, val list: List<SplitInfo>)

    private val version = "8"

    private val timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    private fun getDateStr() {
        DateTime.now(timeZone).toString("yyyy-MM-dd HH:mm:ss:SSS")
    }

    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?): Unit = runBlocking {

        input ?: throw RuntimeException("input is null")
        context ?: throw RuntimeException("context is null")

        val mergeConfig = input.use { _ ->
            val buffer = Buffer()

            buffer.readFrom(input)

            configAdapter.fromJson(buffer)

        } ?: throw RuntimeException("mergeConfig is null")

        context.logger.log("version:$version,${getDateStr()}")

        context.logger.log("开始创建Client")

        val clientStart = System.currentTimeMillis()

        val s3Client = S3Client.builder()
            .build()

        context.logger.log("创建Client完成,耗时${System.currentTimeMillis() - clientStart}ms")

        val groupList = mutableListOf<MutableList<SplitInfo>>()

        mergeConfig.split.forEach { splitInfo ->
            val lastGroup = groupList.lastOrNull()

            if (lastGroup == null || lastGroup.sumOf { it.size } >= MIN_BLOCK_SIZE) {
                val newList = mutableListOf<SplitInfo>()

                groupList.add(newList)

                newList
            } else {
                lastGroup
            }.add(splitInfo)
        }

        val waitList = groupList.mapIndexed { index, splitInfos ->
            MergeGroup(index + 1, splitInfos)
        }

        val dir = File("/tmp/${context.awsRequestId.replace("-", "")}")

        dir.mkdirs()

        val createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
            .bucket(mergeConfig.bucket)
            .key(mergeConfig.key)
            .build()

        val uploadId = s3Client.createMultipartUpload(createMultipartUploadRequest).uploadId()

        val partList = waitList.map {
            doMerge(this, dir, uploadId, mergeConfig, s3Client, it)
        }.map { it.await() }

        val completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(partList)
            .build()

        val completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(mergeConfig.bucket)
            .key(mergeConfig.key)
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build()

        s3Client.completeMultipartUpload(completeMultipartUploadRequest)

        s3Client.close()

        dir.deleteRecursively()

        context.logger.log("version:6,${getDateStr()}")
    }

    private suspend fun doMerge(
        scope: CoroutineScope,
        dir: File,
        uploadId: String,
        config: MergeConfig,
        s3Client: S3Client,
        mergeGroup: MergeGroup
    ): Deferred<CompletedPart> {
        if (mergeGroup.list.size == 1) {
            return scope.doMerge(uploadId, config, s3Client, mergeGroup.partNumber, mergeGroup.list.first())
        }


        val splitFileList = mergeGroup.list.map {
            scope.async(Dispatchers.IO) {
                val dstFile = File(dir, it.key)

                dstFile.parentFile.mkdirs()

                dstFile.createNewFile()

                val request = GetObjectRequest.builder()
                    .bucket(config.bucket)
                    .key(it.key)
                    .build()

                s3Client.getObject(request).use { input ->
                    dstFile.outputStream().use { output ->
                        input.copyTo(output)
                    }
                }

                dstFile
            }
        }.map { it.await() }

        val dstFile = File(dir, "block.${mergeGroup.partNumber}")

        withContext(Dispatchers.IO) {
            dstFile.createNewFile()
        }

        dstFile.outputStream().use { output ->

            splitFileList.forEach { splitFile ->
                splitFile.inputStream().use { input ->
                    input.copyTo(output)
                }
            }
        }

        return scope.doMerge(uploadId, config, s3Client, mergeGroup.partNumber, dstFile)
    }

    private fun CoroutineScope.doMerge(
        uploadId: String,
        config: MergeConfig,
        s3Client: S3Client,
        partNumber: Int,
        splitInfo: SplitInfo
    ) = async(Dispatchers.IO) {
        val partRequest = UploadPartRequest.builder()
            .bucket(config.bucket)
            .key(config.key)
            .partNumber(partNumber)
            .uploadId(uploadId)
            .build()

        val splitRequest = GetObjectRequest.builder()
            .bucket(config.bucket)
            .key(splitInfo.key)
            .build()

        val eTag = s3Client.getObject(splitRequest).use { input ->
            s3Client.uploadPart(partRequest, RequestBody.fromInputStream(input, splitInfo.size)).eTag()
        }

        CompletedPart.builder().partNumber(partRequest.partNumber()).eTag(eTag).build()
    }

    private fun CoroutineScope.doMerge(
        uploadId: String,
        config: MergeConfig,
        s3Client: S3Client,
        partNumber: Int,
        dstFile: File
    ) = async(Dispatchers.IO) {
        val partRequest = UploadPartRequest.builder()
            .bucket(config.bucket)
            .key(config.key)
            .partNumber(partNumber)
            .uploadId(uploadId)
            .build()


        val eTag = s3Client.uploadPart(partRequest, RequestBody.fromFile(dstFile)).eTag()

        CompletedPart.builder().partNumber(partRequest.partNumber()).eTag(eTag).build()
    }
}