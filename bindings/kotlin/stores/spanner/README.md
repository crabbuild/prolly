# Kotlin Cloud Spanner store

Cloud Spanner implementation of the shared async store protocol using the
official Google Cloud SDK. The adapter offloads blocking SDK calls to a supplied
coroutine dispatcher and never closes the caller-owned `DatabaseClient` or
dispatcher.

Apply the three exact statements in `SPANNER_DDL` before serving requests.
