# Akka Cassandra Migration

## Как запустить программу?

1. Необходимо создать исполняемый jar файл, для этого необходимо выполнить команду `mvn clean package` и дождаться пока соберется jar файл `AkkaCassandraMigration-{application.version}-full.jar` в папке target.
2. Необходимо сформировать файл `application.conf` с настройками для запуска программы. Содержимое файла:

```HOCON
project.cassandra {
  keyspace-from = ""
  keyspace-to = ""
  datacenter = ""
  username = ""
  password = ""
}

datastax-java-driver {
  basic {
    request.timeout = 15 seconds
    contact-points = []
    load-balancing-policy.local-datacenter = ${project.cassandra.datacenter}
  }
  advanced {
    reconnect-on-init = true
    auth-provider {
      username = ${project.cassandra.username}
      password = ${project.cassandra.password}
    }
  }
}

delete {
  delay: 10 millis
  buffer-size: 1000
  parallelism: 8
  input-file: need_removed.csv
  errors-output-file: errors_need_removed.csv
}

delete-old-messages {
  delay: 10 millis
  buffer-size: 1000
  #Persistent entities saves snapshots after this number of persistent events.
  snapshot-after: 100
  # By default no deletes are executed and are instead logged at INFO. Set this to true
  # to actually do the deletes
  dry-run: true
  parallelism: 8
  input-file: need_removed.csv
  errors-output-file: errors_need_removed.csv
}

export {
  messages {
    parallelism: 8
    bufferSize: 1000
    input-file: messages_export.csv
    errors-output-file: errors_messages_export.csv
  }
  snapshots {
    parallelism: 8
    bufferSize: 1000
    input-file: snapshots_export.csv
    errors-output-file: errors_snapshots_export.csv
  }
}
```

Где: `project.cassandra.username`, `project.cassandra.password`, `project.cassandra.datacenter`, `project.cassandra.keyspace-from` `project.cassandra.keyspace-to` и `datastax-java-driver.basic.contact-points` параметры для подключения к CASSANDRA.

Пример заполнения:

```HOCON
project.cassandra.username = "cassandra"
project.cassandra.password = "cassandra"
project.cassandra.datacenter = datacenter1
project.cassandra.keyspace-from = "keyspace1"
project.cassandra.keyspace-to = "keyspace2"

datastax-java-driver.basic.contact-points = ["localhost:9042"]
```
3. С помощью команды `java -Dconfig.file=application.conf -jar AkkaCassandraMigration-{application.version}-full.jar export_messages` запускаем программу, которая сканирует построчно файл указанный в настройках `export.messages.input-file`, и по полученным persistence_id копируем все записи из таблицы `messages` из `keyspace-from` в `keyspace-to`.
4. С помощью команды `java -Dconfig.file=application.conf -jar AkkaCassandraMigration-{application.version}-full.jar export_snapshots` запускаем программу, которая сканирует построчно файл указанный в настройках `export.snapshots.input-file`, и по полученным persistence_id копируем все записи из таблицы `snapshots` из `keyspace-from` в `keyspace-to`.
5. С помощью команды `java -Dconfig.file=application.conf -jar AkkaCassandraMigration-{application.version}-full.jar delete`  запускаем программу, которая сканирует построчно файл указанный в настройках `delete.input-file`, и удаляет по полученным persistence_id информацию из cassandra в `keyspace-from`.
6. С помощью команды `java -Dconfig.file=application.conf -jar AkkaCassandraMigration-{application.version}-full.jar delete_old_messages` запускаем программу, которая сканирует построчно файл указанный в настройках `delete-old-messages.input-file`, затем сканирует текущие snapshots по заданному persistence_id из cassandra `keyspace-from`, и удаляется все записи из таблиц MESSAGES и SNAPSHOTS c sequence_nr меньше N -`delete-old-messages.snapshot-after` где N - sequence_nr последнего snapshot из cassandra в keyspace-from
