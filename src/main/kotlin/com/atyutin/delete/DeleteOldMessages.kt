package com.atyutin.delete

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry
import akka.stream.alpakka.cassandra.javadsl.CassandraSource
import akka.stream.alpakka.csv.javadsl.CsvParsing
import akka.stream.javadsl.FileIO
import akka.stream.javadsl.Flow
import akka.stream.javadsl.Sink
import akka.stream.javadsl.Source
import akka.util.ByteString
import com.atyutin.config.CassandraConfig
import com.atyutin.config.DeleteOldMessageConfig
import com.atyutin.utils.loggingSql
import com.atyutin.utils.resultLogging
import com.datastax.oss.driver.api.core.cql.Row
import com.typesafe.config.Config
import io.github.config4k.extract
import mu.KotlinLogging
import java.nio.file.Paths
import java.time.ZonedDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicLong

private val log = KotlinLogging.logger {}

private const val SEQUENCE_NR = "sequence_nr"
private const val DEFAULT_VALUE = "-1"

object DeleteOldMessages {
    fun delete(system: ActorSystem, config: Config): CompletionStage<Int> {
        val deleteConfig: DeleteOldMessageConfig = config.extract("delete-old-messages")
        val cassandraConfig: CassandraConfig = config.extract("project.cassandra")
        val sessionSettings = CassandraSessionSettings.create()
        val cassandraSession = CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
        val startTime = ZonedDateTime.now()
        val count = AtomicLong(0)

        val snapshotSequenceNr = "SELECT sequence_nr FROM ${cassandraConfig.keyspaceFrom}.snapshots WHERE persistence_id = ?;"
        val deleteSnapshotsSql = "DELETE FROM ${cassandraConfig.keyspaceFrom}.snapshots WHERE persistence_id = ? AND sequence_nr <= ?;"
        val deleteMessagesSql = "DELETE FROM ${cassandraConfig.keyspaceFrom}.messages WHERE persistence_id = ? AND sequence_nr <= ? AND partition_nr IN (0,1);"

        /**
         * Source snapshots persistenceIds to be removed.
         */
        fun sourceSnapshots(persistenceId: String?): Source<Row, NotUsed> =
            CassandraSource.create(cassandraSession, snapshotSequenceNr, persistenceId)

        /**
         * Source queue to record persistenceIds that couldn't remove.
         */
        val errorSource = Source.queue<String>(deleteConfig.bufferSize)
            .map { ByteString.fromString("$it\n") }
            .to(FileIO.toPath(Paths.get(deleteConfig.errorsOutputFile)))
            .run(system)

        /**
         * Throttle Flow.
         */
        val throttleFlow = Flow.create<MutableCollection<ByteString>>().throttle(1, deleteConfig.delay)

        /**
         * The function of logging the process of scanning data from a file.
         */
        fun operationLogging(count: AtomicLong, limit: Int): (t: Int) -> Int = {
            com.atyutin.utils.operationLogging(count.get(), limit.toLong(), "scanning record", log, 1000); it
        }

        /**
         * The function of logging errors that occur when deleting data.
         * persistenceId on which the error occurred is written to the file from the configuration [delete.errors-output-file]
         */
        fun <T> deletionErrorLogging(
            count: AtomicLong,
            persistenceId: String?
        ) = { _: T, throwable: Throwable? ->
            count.updateAndGet { v -> v.plus(1) }
            if (throwable != null) {
                log.error(throwable) { "Error delete data for `$persistenceId`" }
                errorSource.offer(persistenceId)
                0
            } else 1
        }

        /**
         * Function for logging sql-queries that will be executed if [delete-old-messages.dry-run] is 'false'.
         */
        fun loggingSQL(
            persistenceId: String,
            sequenceNr: Long
        ): CompletableFuture<Any> = CompletableFuture.completedFuture(
            log.info {
                """
                ${loggingSql(deleteSnapshotsSql, setOf(persistenceId, sequenceNr))}
                ${loggingSql(deleteMessagesSql, setOf(persistenceId, sequenceNr))}
                """.trimIndent()
            }
        )

        fun calculateSequenceNr() = { value: Long -> if (value > deleteConfig.snapshotAfter) (value - deleteConfig.snapshotAfter) else -1 }

        fun deleteSnapshots(persistenceId: String?, sequenceNr: Long): CompletionStage<MutableList<Row>> =
            CassandraSource.create(cassandraSession, deleteSnapshotsSql, persistenceId, sequenceNr)
                .runWith(Sink.seq(), system)
                .whenComplete { _, throwable -> if (throwable != null) log.error(throwable) { "Error delete snapshots for persistenceId: `$persistenceId`" } }

        fun deleteMessages(persistenceId: String?, sequenceNr: Long): CompletionStage<MutableList<Row>> =
            CassandraSource.create(cassandraSession, deleteMessagesSql, persistenceId, sequenceNr)
                .runWith(Sink.seq(), system)
                .whenComplete { _, throwable -> if (throwable != null) log.error(throwable) { "Error delete messages for persistenceId: `$persistenceId`" } }

        return FileIO.fromPath(Paths.get(deleteConfig.inputFile))
            .via(CsvParsing.lineScanner())
            .runWith(Sink.fold(0) { i, _ -> i + 1 }, system)
            .thenCompose { limit ->
                log.info { "Start deletion data... Persistence_ids limits: $limit" }
                FileIO.fromPath(Paths.get(deleteConfig.inputFile))
                    .via(CsvParsing.lineScanner())
                    .via(throttleFlow)
                    .map { line -> line.map { it.utf8String() }.firstOrNull() ?: DEFAULT_VALUE }
                    .mapAsync(deleteConfig.parallelism) { persistenceId ->
                        sourceSnapshots(persistenceId)
                            .map { it.getLong(SEQUENCE_NR) }
                            .runWith(Sink.seq(), system)
                            .thenApply { seq -> if (seq.size > 0) seq.max() else -1 }
                            .thenApply(calculateSequenceNr())
                            .thenCompose { sequenceNr ->
                                // if [delete-old-messages.dry-run: true] will be only logging.
                                if (deleteConfig.dryRun) {
                                    loggingSQL(persistenceId, sequenceNr)
                                } else {
                                    deleteSnapshots(persistenceId, sequenceNr)
                                        .thenCompose { deleteMessages(persistenceId, sequenceNr) }
                                        .handle(deletionErrorLogging(count, persistenceId))
                                        .thenApply(operationLogging(count, limit))
                                }
                            }
                    }
                    .runWith(Sink.fold(0) { i, _ -> i + 1 }, system)
                    .whenComplete { data, throwable ->
                        if (throwable != null) log.error(throwable) { "Error delete data" }
                        else resultLogging(data, limit, startTime, log)
                    }
            }
            .whenComplete { _, throwable ->
                if (throwable != null) log.error(throwable) { "Common error:" }
            }
    }
}
