package com.atyutin.utils

import mu.KLogger
import java.time.Duration
import java.time.ZonedDateTime

private const val CQL_PARAM = "?"

fun calculatePercent(current: Long, all: Long) =
    "$current/$all => ${String.format("%.2f", (current.toDouble() / all.toDouble()) * 100)}%"

fun operationLogging(current: Long, all: Long, text: String, log: KLogger, period: Int = 1000) {
    current.let { if ((it % period) == 0L) log.info { "$text: ${calculatePercent(it, all)}" } else Unit }
}

fun resultLogging(current: Int, all: Int, startTime: ZonedDateTime, log: KLogger) {
    log.info { "$current from $all records processed within ${Duration.between(startTime, ZonedDateTime.now()).seconds} seconds" }
}

fun loggingSql(cql: String, values: Set<Any>): String {
    var result: String = cql
    for (i in values) { result = result.replaceFirst(CQL_PARAM, if (i is String) "'$i'" else "$i") }
    return result
}
