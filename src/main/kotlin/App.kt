import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.SearchResponse
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.awt.Dimension
import java.awt.event.KeyEvent
import java.awt.event.KeyListener
import javax.swing.DefaultListModel
import javax.swing.JFrame
import javax.swing.JLabel

/**
 * Created by mihael
 * on 02/01/2023 at 22:56
 * using IntelliJ IDEA
 */
class App(val client: ElasticsearchClient) {

    private val firstListModel = DefaultListModel<String>()

    fun start() {
        Comparison().apply {
            contentPane = this.mainPanel
            title = "Comparison app"
            size = Dimension(400, 300)
            isVisible = true
            defaultCloseOperation = JFrame.EXIT_ON_CLOSE

            list1.model = firstListModel

            textField1.addKeyListener(object : KeyListener {
                override fun keyReleased(e: KeyEvent?) {
                    val searchString = textField1.text
                    if (e?.keyCode == KeyEvent.VK_ENTER) {
                        println("------- SEARCH ------- \t $searchString")
                        runBlocking {
                            launch { searchElastic(esLabel, searchString) }
                        }
                    }
                }

                override fun keyTyped(e: KeyEvent?) {}

                override fun keyPressed(e: KeyEvent?) {}
            })
        }
    }

    fun searchElastic(label: JLabel, word: String) {
        val start = System.currentTimeMillis()
        val response: SearchResponse<WordDTO> = client.search(
            { sqb ->
                sqb.index("words").size(100)
                    .query {
                        it.matchPhrasePrefix { mppqb ->
                            mppqb.field("word").query(word).boost(1.5f)
                        }
                    }
            },
            WordDTO::class.java
        )

        val results = response.hits().hits().mapNotNull { it.source() to it.score() }
        firstListModel.clear()
        firstListModel.addAll(results.map { "${it.first}\t ${it.second}" })
        val end = System.currentTimeMillis()
        val total = end - start
        label.text = "Elasticsearch (${results.size}) - ${total}ms"
        println("ES: ${total}ms \t ${results.size}")
    }


}