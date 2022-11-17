package com.atyutin.config

import java.time.Duration

data class ExportConfig(
    val parallelism: Int = 1,
    val bufferSize: Int = 1000,
    val inputFile: String,
    val errorsOutputFile: String
)

data class CassandraConfig(
    val keyspaceFrom: String,
    val keyspaceTo: String,
    val datacenter: String,
    val username: String,
    val password: String
)

data class DeleteConfig(
    val delay: Duration,
    val parallelism: Int,
    val bufferSize: Int,
    val inputFile: String,
    val errorsOutputFile: String
)

data class DeleteOldMessageConfig(
    val delay: Duration,
    val parallelism: Int,
    val bufferSize: Int,
    val dryRun: Boolean,
    val snapshotAfter: Int,
    val inputFile: String,
    val errorsOutputFile: String
)
