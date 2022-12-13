package org.example

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.S3Event

class Handler : RequestHandler<S3Event, String> {
    override fun handleRequest(input: S3Event?, context: Context?): String {
        context?.logger?.log("version:5")

        return "success"
    }
}