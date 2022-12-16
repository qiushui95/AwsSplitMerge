package org.example

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.squareup.moshi.Moshi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import okio.Buffer
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.io.InputStream
import java.io.OutputStream

class Handler : RequestStreamHandler {

    private val dloadDispatcher = Dispatchers.IO.limitedParallelism(20)

    private val configAdapter by lazy { Moshi.Builder().build().adapter(MergeConfig::class.java) }

    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?): Unit = runBlocking {

        input ?: throw RuntimeException("input is null")
        context ?: throw RuntimeException("context is null")

        val buffer = Buffer()

        buffer.readFrom(input)

        val mergeConfig = configAdapter.fromJson(buffer) ?: throw RuntimeException("mergeConfig is null")

        input.close()

        context.logger.log("version:6,${System.currentTimeMillis()}")

        context.logger.log("开始创建Client")

        val clientStart = System.currentTimeMillis()

        val s3Client = S3Client.builder()
            .build()

        context.logger.log("创建Client完成,耗时${System.currentTimeMillis() - clientStart}ms")

        checkExists(this, mergeConfig, s3Client)

        s3Client.close()

        context.logger.log("version:6,${System.currentTimeMillis()}")
    }

    private suspend fun checkExists(scope: CoroutineScope, config: MergeConfig, s3Client: S3Client) {
        val totalSize = config.split.sumOf { it.size }

        val attributesResponse = try {
            val getObjectAttributesRequest = GetObjectAttributesRequest.builder()
                .bucket(config.bucket)
                .key(config.key)
                .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                .build()
            s3Client.getObjectAttributes(getObjectAttributesRequest)
        } catch (ex: NoSuchKeyException) {
            doMerge(scope, config, s3Client)
            return
        }

        if (attributesResponse.objectSize() != totalSize) {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(config.bucket).key(config.key).build())
            doMerge(scope, config, s3Client)
        }
    }

    private suspend fun doMerge(scope: CoroutineScope, config: MergeConfig, s3Client: S3Client) {
        val createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
            .bucket(config.bucket)
            .key(config.key)
            .build()

        val createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest)

        val dloadJobList = config.split.mapIndexed { index, splitInfo ->
            val partNumber = index + 1

            val partRequest = UploadPartRequest.builder()
                .bucket(config.bucket)
                .key(config.key)
                .partNumber(partNumber)
                .uploadId(createMultipartUploadResponse.uploadId())
                .build()

            scope.dloadSplit(s3Client, config.bucket, splitInfo, partRequest)
        }

        val uploadPartList = dloadJobList.map { it.await() }

        val completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(uploadPartList)
            .build()

        val completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(config.bucket)
            .key(config.key)
            .uploadId(createMultipartUploadResponse.uploadId())
            .multipartUpload(completedMultipartUpload)
            .build()

        s3Client.completeMultipartUpload(completeMultipartUploadRequest)
    }

    private fun CoroutineScope.dloadSplit(
        s3Client: S3Client,
        bucket: String,
        info: SplitInfo,
        partRequest: UploadPartRequest
    ) = async(dloadDispatcher) {

        val splitRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(info.key)
            .build()

        s3Client.getObject(splitRequest).use { input ->
            val eTag = s3Client.uploadPart(partRequest, RequestBody.fromInputStream(input, info.size)).eTag()

            CompletedPart.builder().partNumber(partRequest.partNumber()).eTag(eTag).build()
        }
    }
}