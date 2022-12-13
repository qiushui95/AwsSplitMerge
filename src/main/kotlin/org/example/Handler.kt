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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.File
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class Handler : RequestHandler<S3Event, Unit> {
    private val configAdapter by lazy { Moshi.Builder().build().adapter(MergeConfig::class.java) }

    private val dloadDispatcher = Dispatchers.IO.limitedParallelism(10)

    override fun handleRequest(input: S3Event?, context: Context?): Unit = runBlocking {
        val start = System.currentTimeMillis()

        input ?: throw RuntimeException("input is null")
        context ?: throw RuntimeException("context is null")

        context.logger.log("version:5")

        val accessKeyId = System.getenv("accessKeyId")
        val secretAccessKey = System.getenv("secretAccessKey")

        val credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey)

        context.logger.log("开始创建Client")

        val clientStart=System.currentTimeMillis()

        val s3Client = S3Client.builder()
            .region(Region.AP_SOUTHEAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build()

        context.logger.log("创建Client完成,耗时${System.currentTimeMillis()-clientStart}ms")

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

        val dloadStart = System.currentTimeMillis()

        config.split.map { dloadSplit(context.logger, s3Client, dir, srcBucket, it) }
            .forEach { it.join() }

        context.logger.log("下载完成,耗时${System.currentTimeMillis() - dloadStart}ms,开始合并文件")

        val mergeStart = System.currentTimeMillis()

        val compressFile = File(dir, "merge.gzip")

        if (compressFile.exists()) compressFile.delete()

        compressFile.createNewFile()

        compressFile.outputStream().use { output ->
            config.split.map { File(dir, it.key) }
                .onEach { splitFile ->
                    splitFile.inputStream().use { input ->
                        input.copyTo(output)
                    }
                }.forEach { it.delete() }
        }

        context.logger.log("合并完成,耗时${System.currentTimeMillis() - mergeStart}ms,开始解压文件")

        val unzipStart = System.currentTimeMillis()

        val dstFile = File(dir, config.key)

        dstFile.parentFile.mkdirs()

        if (dstFile.exists()) dstFile.delete()

        dstFile.createNewFile()

        GZIPInputStream(compressFile.inputStream()).use { input ->
            dstFile.outputStream().use { output ->
                input.copyTo(output)
            }
        }

        compressFile.delete()

        context.logger.log("解压完成,耗时${System.currentTimeMillis() - unzipStart}ms,开始上传文件")

        val uploadStart = System.currentTimeMillis()

        val putRequest = PutObjectRequest.builder()
            .bucket(srcBucket)
            .key(config.key)
            .build()

        s3Client.putObject(putRequest, RequestBody.fromFile(dstFile))

        context.logger.log("上传完成,耗时${System.currentTimeMillis() - uploadStart}ms")

        dstFile.delete()

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
        val startTime = System.currentTimeMillis()

        logger.log("[${info.key}]开始下载")

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

        logger.log("[${info.key}]下载完成,耗时${System.currentTimeMillis() - startTime}ms")

        if (dstFile.length() != info.size) throw RuntimeException("download size(${dstFile.length()}) != upload size(${info.size})")

    }

    private fun mergeSplit(splitList: List<File>) {

    }
}