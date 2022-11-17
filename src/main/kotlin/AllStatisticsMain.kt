import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry
import akka.stream.alpakka.cassandra.javadsl.CassandraSource
import akka.stream.javadsl.FileIO
import akka.stream.javadsl.Sink
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import mu.KotlinLogging.logger
import java.nio.file.Paths
import java.util.concurrent.CompletionStage

private val log = logger {}

fun main(args: Array<String>) {
    val system = ActorSystem.create("migration")
    val config = ConfigFactory.load()
    val keyspaceFrom = config.getString("project.cassandra.keyspace-from")
    val inputFile = "LotEntityAll.csv"
    val sessionSettings = CassandraSessionSettings.create()
    val cassandraSession = CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
    var count = 0L

    val saveToOutputFile: Sink<ByteString, CompletionStage<IOResult>> = FileIO.toPath(Paths.get(inputFile))

    CassandraSource.create(
        cassandraSession,
        "SELECT persistence_id, sequence_nr FROM $keyspaceFrom.messages;"
    )
        .map { Pair(it.getString("persistence_id"), it.getLong("sequence_nr")) }
        .filter { it?.first?.startsWith("LotEntity") ?: false && it?.second?.equals(1L) ?: false }
        .map { it.first }
        .map { ByteString.fromString("$it\n").also { count += 1; if ((count % 1000) == 0L) log.info { count } } }
        .runWith(saveToOutputFile, system)
        .whenComplete { _, throwable ->
            if (throwable != null) {
                log.error(throwable) { "Global error" }
            }
            system.terminate()
        }
}
