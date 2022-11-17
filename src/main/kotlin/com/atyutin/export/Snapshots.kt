package com.atyutin.export

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.CassandraWriteSettings
import akka.stream.alpakka.cassandra.javadsl.CassandraFlow
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry
import akka.stream.alpakka.cassandra.javadsl.CassandraSource
import akka.stream.alpakka.csv.javadsl.CsvParsing
import akka.stream.javadsl.FileIO
import akka.stream.javadsl.Sink
import akka.stream.javadsl.Source
import akka.util.ByteString
import com.atyutin.config.CassandraConfig
import com.atyutin.config.ExportConfig
import com.atyutin.utils.operationLogging
import com.atyutin.utils.resultLogging
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.cql.Row
import com.typesafe.config.Config
import io.github.config4k.extract
import mu.KotlinLogging
import java.nio.file.Paths
import java.time.ZonedDateTime
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicLong

private val log = KotlinLogging.logger {}

private const val PERSISTENCE_ID = "persistence_id"
private const val SEQUENCE_NR = "sequence_nr"
private const val TIMESTAMP = "timestamp"
private const val META = "meta"
private const val META_SER_ID = "meta_ser_id"
private const val META_SER_MANIFEST = "meta_ser_manifest"
private const val SER_ID = "ser_id"
private const val SER_MANIFEST = "ser_manifest"
private const val SNAPSHOT = "snapshot"
private const val SNAPSHOT_DATA = "snapshot_data"

object Snapshots {

    fun export(system: ActorSystem, config: Config): CompletionStage<Int> {
        val snapshotsConfig: ExportConfig = config.extract("export.snapshots")
        val cassandraConfig: CassandraConfig = config.extract("project.cassandra")
        val sessionSettings = CassandraSessionSettings.create()
        val cassandraSession = CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
        val startTime = ZonedDateTime.now()
        val count = AtomicLong(0)

        val sql = "SELECT * FROM ${cassandraConfig.keyspaceFrom}.snapshots WHERE persistence_id=?;"

        fun selectSource(persistenceId: String?): Source<Row, NotUsed> = CassandraSource.create(cassandraSession, sql, persistenceId)

        val statementBinder: (Row, PreparedStatement) -> BoundStatement = { row: Row, preparedStatement: PreparedStatement ->
            preparedStatement.bind(
                row.getString(PERSISTENCE_ID),
                row.getLong(SEQUENCE_NR),
                row.getByteBuffer(META),
                row.getInt(META_SER_ID),
                row.getString(META_SER_MANIFEST),
                row.getInt(SER_ID),
                row.getString(SER_MANIFEST),
                row.getByteBuffer(SNAPSHOT),
                row.getByteBuffer(SNAPSHOT_DATA),
                row.getLong(TIMESTAMP)
            )
        }

        val writeFlow = CassandraFlow.create(
            cassandraSession,
            CassandraWriteSettings.defaults()
                .withParallelism(snapshotsConfig.parallelism),
            """
            INSERT INTO ${cassandraConfig.keyspaceTo}.snapshots (
            $PERSISTENCE_ID,
            $SEQUENCE_NR,
            $META,
            $META_SER_ID,
            $META_SER_MANIFEST,
            $SER_ID,
            $SER_MANIFEST,
            $SNAPSHOT,
            $SNAPSHOT_DATA,
            $TIMESTAMP)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            statementBinder
        )

        /**
         * Source queue to record persistenceIds that couldn't export.
         */
        val errorSource = Source.queue<String>(snapshotsConfig.bufferSize)
            .map { ByteString.fromString("$it\n") }
            .to(FileIO.toPath(Paths.get(snapshotsConfig.errorsOutputFile)))
            .run(system)

        /**
         * The function of logging errors that occur during data export.
         * persistenceId on which the error occurred is written to the file from the configuration [export.snapshots.errors-output-file]
         */
        fun <T> transferErrorLogging(
            count: AtomicLong,
            persistenceId: String?
        ) = { _: T, throwable: Throwable? ->
            count.updateAndGet { v -> v.plus(1) }
            if (throwable != null) {
                log.error(throwable) { "Error transfer snapshots from ${cassandraConfig.keyspaceFrom} to ${cassandraConfig.keyspaceTo} for `$persistenceId`" }
                errorSource.offer(persistenceId)
                0
            } else 1
        }

        /**
         * The function of logging the process of scanning data from a file.
         */
        fun operationLogging(count: AtomicLong, limit: Int): (t: Int) -> Int = {
            operationLogging(count.get(), limit.toLong(), "scanning record", log, 1000); it
        }

        return FileIO.fromPath(Paths.get(snapshotsConfig.inputFile))
            .via(CsvParsing.lineScanner())
            .runWith(Sink.fold(0) { i, _ -> i + 1 }, system)
            .thenCompose { limit ->
                log.info { "Start com.atyutin.export snapshots... Count export persistence_ids: $limit" }
                FileIO.fromPath(Paths.get(snapshotsConfig.inputFile))
                    .via(CsvParsing.lineScanner())
                    .map { line -> line.map { it.utf8String() }.firstOrNull() }
                    .mapAsync(snapshotsConfig.parallelism) { persistenceId ->
                        selectSource(persistenceId)
                            .map { it }
                            .via(writeFlow)
                            .runWith(Sink.seq(), system)
                            .handle(transferErrorLogging(count, persistenceId))
                            .thenApply(operationLogging(count, limit))
                    }
                    .runWith(Sink.fold(0) { i, b -> i + b }, system)
                    .whenComplete { data, throwable ->
                        if (throwable != null) log.error(throwable) { "Error transfer snapshots from ${cassandraConfig.keyspaceFrom} to ${cassandraConfig.keyspaceTo}" }
                        else resultLogging(data, limit, startTime, log)
                    }
            }
            .whenComplete { _, throwable ->
                if (throwable != null) log.error(throwable) { "Common error:" }
            }
    }
}
