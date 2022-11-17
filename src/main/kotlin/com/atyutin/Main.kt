import akka.actor.ActorSystem
import com.atyutin.delete.Delete
import com.atyutin.delete.DeleteOldMessages
import com.atyutin.export.Messages
import com.atyutin.export.Snapshots
import com.typesafe.config.ConfigFactory
import java.util.concurrent.CompletableFuture.completedFuture

private val ERROR_MESSAGE = """
        You must specify a parameter to start the application:
        export_messages - export messages from one keyspace to other keyspace,
        export_snapshots - export snapshots from one keyspace to other keyspace,
        delete - delete data based on the generated file
        delete_old_messages - delete old messages and snapshots based on the generated file
""".trimIndent()

fun main(args: Array<String>) {
    val system = ActorSystem.create("migration")
    val config = ConfigFactory.load()

    if (args.isEmpty()) {
        println(ERROR_MESSAGE)
        system.terminate()
    } else {
        when (args[0]) {
            "export_messages" -> Messages.export(system, config)
            "export_snapshots" -> Snapshots.export(system, config)
            "delete" -> Delete.delete(system, config)
            "delete_old_messages" -> DeleteOldMessages.delete(system, config)
            else -> completedFuture(println(ERROR_MESSAGE))
        }
            .whenComplete { _, _ -> system.terminate() }
    }
}
