import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.TransportUtils
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import sql.Word
import sql.Words
import java.sql.Connection
import javax.net.ssl.SSLContext


/**
 * Created by mihael
 * on 02/01/2023 at 21:51
 * using IntelliJ IDEA
 */
fun main() {
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    Database.connect("jdbc:sqlite:words.db", "org.sqlite.JDBC")
    val fingerprint = "3df4fa90b5881028c97e84707032eaa5932c3313564c7f3cddfad519d5c1d4a2"
    val sslContext: SSLContext = TransportUtils.sslContextFromCaFingerprint(fingerprint)
    val credentialsProvider = BasicCredentialsProvider().apply {
        setCredentials(AuthScope.ANY, UsernamePasswordCredentials("elastic", "dm410UI16gGVSCIYt9-U"))
    }

    val restClient: RestClient = RestClient
        .builder(HttpHost("localhost", 9200, "https"))
        .setHttpClientConfigCallback {
            it.setSSLContext(sslContext).setDefaultCredentialsProvider(credentialsProvider)
        }
        .build()

    val transport: ElasticsearchTransport = RestClientTransport(restClient, JacksonJsonpMapper())
    val client = ElasticsearchClient(transport)

    transaction {
        SchemaUtils.createMissingTablesAndColumns(Words)
    }

    //storeWords(client)
    App(client).start()
}

/**
 * Store 100 000 words into both Elasticsearch and SQLite.
 *
 * @param client [ElasticsearchClient]
 */
fun storeWords(client: ElasticsearchClient) {
    val inputStream = object {}::class.java.getResourceAsStream("100_000.txt")?.bufferedReader()
    var line = inputStream?.readLine()
    var x = 0
    while (line != null) {
        val word = line
        if (!word.contains("#")) {
            insertES(client, WordDTO(word))
            insertIntoSQLite(word)
            println("Inserted ${++x} word.")
        }
        line = inputStream?.readLine()
    }
    println("Inserted $x words into Elasticsearch and sqlite.")
}

fun insertIntoSQLite(word: String) {
    transaction {
        Word.new {
            this.word = word
        }
    }
}

fun insertES(client: ElasticsearchClient, word: WordDTO) {
    client.index {
        it.index("words")
            .id(word.hashCode().toString())
            .document(word)
    }
}

data class WordDTO(@JsonProperty("word") val word: String?)