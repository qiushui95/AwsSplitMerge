package org.example

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.S3Event
import com.squareup.moshi.Moshi
import kotlinx.coroutines.*
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.io.BufferedInputStream
import java.io.File
import java.io.InputStream
import java.io.ObjectInputStream
import java.nio.ByteBuffer
import java.util.UUID

class Handler : RequestHandler<S3Event, Unit> {
    private val configAdapter by lazy { Moshi.Builder().build().adapter(MergeConfig::class.java) }

    private val dloadDispatcher = Dispatchers.IO.limitedParallelism(10)

    override fun handleRequest(input: S3Event?, context: Context?): Unit = runBlocking {
        val start = System.currentTimeMillis()

        input ?: throw RuntimeException("input is null")
        context ?: throw RuntimeException("context is null")

        context.logger.log("version:5")

        context.logger.log("开始创建Client")

        val clientStart = System.currentTimeMillis()

        val s3Client = S3Client.builder()
            .build()

        context.logger.log("创建Client完成,耗时${System.currentTimeMillis() - clientStart}ms")

        val record = input.records.getOrNull(0) ?: throw RuntimeException("record is null")

        val srcBucket = record.s3.bucket.name
        val srcKey = record.s3.`object`.urlDecodedKey

        val jsonFileRequest = GetObjectRequest.builder()
            .bucket(srcBucket)
            .key(srcKey)
            .build()

        context.logger.log("开始读取配置")

        val configStart = System.currentTimeMillis()

        val config = s3Client.getObjectAsBytes(jsonFileRequest).asUtf8String()
            .run(configAdapter::fromJson)
            ?: throw RuntimeException("config is null")

        context.logger.log("配置读取完成,耗时${System.currentTimeMillis() - configStart}ms,开始下载文件")

        val dir = File("/tmp")

        val createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
            .bucket(srcBucket)
            .key(config.key)
            .build()

        val createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest)

        val dloadJobList = config.split.mapIndexed { index, splitInfo ->
            val partNumber = index + 1

            val partRequest = UploadPartRequest.builder()
                .bucket(srcBucket)
                .key(config.key)
                .partNumber(partNumber)
                .uploadId(createMultipartUploadResponse.uploadId())
                .build()

            dloadSplit(context.logger, s3Client, dir, srcBucket, splitInfo, partRequest)
        }

        val uploadPartList = dloadJobList.map { it.await() }

        val completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(uploadPartList)
            .build()

        val completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(srcBucket)
            .key(config.key)
            .uploadId(createMultipartUploadResponse.uploadId())
            .multipartUpload(completedMultipartUpload)
            .build()

        s3Client.completeMultipartUpload(completeMultipartUploadRequest)

        s3Client.close()

        context.logger.log("处理完成,耗时${System.currentTimeMillis() - start}ms")
    }


    private fun CoroutineScope.dloadSplit(
        logger: LambdaLogger,
        s3Client: S3Client,
        dir: File,
        bucket: String,
        info: SplitInfo,
        partRequest: UploadPartRequest
    ) = async(dloadDispatcher) {

        val start = System.currentTimeMillis()

        logger.log("开始下载${info.key}")

        val splitRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(info.key)
            .build()

        s3Client.getObject(splitRequest).use { input ->
            val eTag = s3Client.uploadPart(partRequest, RequestBody.fromInputStream(input, info.size)).eTag()

            CompletedPart.builder().partNumber(partRequest.partNumber()).eTag(eTag).build()
        }.apply {
            logger.log("下载${info.key}完成,耗时${System.currentTimeMillis() - start}")
        }
    }
}