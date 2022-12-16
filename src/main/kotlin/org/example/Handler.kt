package org.example

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.S3Event
import com.squareup.moshi.Moshi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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

    private val dloadDispatcher = Dispatchers.IO.limitedParallelism(5)

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

        val dloadStart = System.currentTimeMillis()

        val dloadJobList = config.split.map { dloadSplit(context.logger, s3Client, dir, srcBucket, it) }

        launch {
            dloadJobList.forEach { it.join() }

            context.logger.log("下载完成,耗时${System.currentTimeMillis() - dloadStart}ms")
        }

        val uploadPartList = config.split.map { dloadSplit(context.logger, s3Client, dir, srcBucket, it) }
            .onEach { it.join() }
            .mapIndexed { index, _ ->
                val partNumber = index + 1
                val request = UploadPartRequest.builder()
                    .bucket(srcBucket)
                    .key(config.key)
                    .partNumber(partNumber)
                    .uploadId(createMultipartUploadResponse.uploadId())
                    .build()

                val eTag = s3Client.uploadPart(request, RequestBody.fromFile(File(dir, config.split[index].key))).eTag()

                CompletedPart.builder().partNumber(partNumber).eTag(eTag).build()
            }


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
        info: SplitInfo
    ) = launch(dloadDispatcher) {

        val splitRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(info.key)
            .build()

        val dstFile = File(dir, info.key)

        val parentFile = dstFile.parentFile

        if (!parentFile.exists()) parentFile.mkdirs()

        if (dstFile.exists()) dstFile.delete()

        dstFile.createNewFile()

        s3Client.getObject(splitRequest).use { input ->
            dstFile.outputStream().use { output ->
                input.copyTo(output)
            }
        }

        if (dstFile.length() != info.size) throw RuntimeException("download size(${dstFile.length()}) != upload size(${info.size})")

    }
}