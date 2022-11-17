package com.atyutin.delete

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
import com.atyutin.config.DeleteConfig
import com.atyutin.utils.operationLogging
import com.atyutin.utils.resultLogging
import com.datastax.oss.driver.api.core.cql.Row
import com.typesafe.config.Config
import io.github.config4k.extract
import mu.KotlinLogging
import java.nio.file.Paths
import java.time.ZonedDateTime
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicLong

private val log = KotlinLogging.logger {}

object Delete {
    fun delete(system: ActorSystem, config: Config): CompletionStage<Int> {
        val deleteConfig: DeleteConfig = config.extract("delete")
        val cassandraConfig: CassandraConfig = config.extract("project.cassandra")
        val sessionSettings = CassandraSessionSettings.create()
        val cassandraSession = CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
        val startTime = ZonedDateTime.now()
        val count = AtomicLong(0)

        val deleteSnapshotsSql = "DELETE FROM ${cassandraConfig.keyspaceFrom}.snapshots WHERE persistence_id=?;"
        val deleteMessagesSql = "DELETE FROM ${cassandraConfig.keyspaceFrom}.messages WHERE persistence_id=? AND partition_nr IN (0,1);"

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
            operationLogging(count.get(), limit.toLong(), "scanning record", log, 1000); it
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

        fun deleteSnapshots(persistenceId: String?): CompletionStage<MutableList<Row>> =
            CassandraSource.create(cassandraSession, deleteSnapshotsSql, persistenceId)
                .runWith(Sink.seq(), system)
                .whenComplete { _, throwable -> if (throwable != null) log.error(throwable) { "Error delete snapshots for persistenceId: `$persistenceId`" } }

        fun deleteMessages(persistenceId: String?): CompletionStage<MutableList<Row>> =
            CassandraSource.create(cassandraSession, deleteMessagesSql, persistenceId)
                .runWith(Sink.seq(), system)
                .whenComplete { _, throwable -> if (throwable != null) log.error(throwable) { "Error delete messages for persistenceId: `$persistenceId`" } }

        return FileIO.fromPath(Paths.get(deleteConfig.inputFile))
            .via(CsvParsing.lineScanner())
            .runWith(Sink.fold(0) { i, _ -> i + 1 }, system)
            .thenCompose { limit ->
                log.info { "Start deletion data... Count deletion persistence_ids: $limit" }
                FileIO.fromPath(Paths.get(deleteConfig.inputFile))
                    .via(CsvParsing.lineScanner())
                    .via(throttleFlow)
                    .map { line -> line.map { it.utf8String() }.firstOrNull() }
                    .mapAsync(deleteConfig.parallelism) { persistenceId ->
                        deleteSnapshots(persistenceId)
                            .thenCompose { deleteMessages(persistenceId) }
                            .handle(deletionErrorLogging(count, persistenceId))
                            .thenApply(operationLogging(count, limit))
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
