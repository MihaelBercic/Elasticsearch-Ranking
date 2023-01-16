import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.TransportUtils
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.annotation.JsonProperty
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import javax.net.ssl.SSLContext


/**
 * Created by mihael
 * on 02/01/2023 at 21:51
 * using IntelliJ IDEA
 */
fun main() {
    val fingerprint = "3df4fa90b5881028c97e84707032eaa5932c3313564c7f3cddfad519d5c1d4a2"
    val sslContext: SSLContext = TransportUtils.sslContextFromCaFingerprint(fingerprint)
    val credentialsProvider = BasicCredentialsProvider().apply {
        setCredentials(AuthScope.ANY, UsernamePasswordCredentials("elastic", "dm410UI16gGVSCIYt9-U"))
    }

    val restClient: RestClient = RestClient.builder(HttpHost("localhost", 9200, "http"))
        .build()
    val transport: ElasticsearchTransport = RestClientTransport(restClient, JacksonJsonpMapper())
    val client = ElasticsearchClient(transport)

    // storeWords(client)
    App(client).start()
}

/**
 * Store 100 000 words into both Elasticsearch and SQLite.
 *
 * @param client [ElasticsearchClient]
 */
fun storeWords(client: ElasticsearchClient) {
    runBlocking {
        val inputStream = object {}::class.java.getResourceAsStream("100_000.txt")?.bufferedReader()
        var line = inputStream?.readLine()
        var x = 0
        while (line != null) {
            val word = line
            if (!word.contains("#")) {
                launch {
                    insertES(client, WordDTO(word))
                    println("Inserted ${++x} word.")
                }
            }
            line = inputStream?.readLine()
        }
        println("Inserted $x wordsnto Elasticsearch and sqlite.")
    }
}

fun insertES(client: ElasticsearchClient, word: WordDTO) {
    client.index {
        it.index("words")
            .id(word.hashCode().toString())
            .document(word)
    }
}

/*
eyJ2ZXIiOiI4LjUuMyIsImFkciI6WyIxNzIuMTguMC4yOjkyMDAiXSwiZmdyIjoiM2RmNGZhOTBiNTg4MTAyOGM5N2U4NDcwNzAzMmVhYTU5MzJjMzMxMzU2NGM3ZjNjZGRmYWQ1MTlkNWMxZDRhMiIsImtleSI6Ik1rNi11b1VCaVhmU3pJZG45UkJjOk9BWXRxMTQ2UXE2aHdtSHYzTEU5bXcifQ==
 */

data class WordDTO(@JsonProperty("word") val word: String)