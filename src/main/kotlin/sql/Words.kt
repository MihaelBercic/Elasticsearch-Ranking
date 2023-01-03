package sql

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable

/**
 * Created by mihael
 * on 02/01/2023 at 21:57
 * using IntelliJ IDEA
 */
object Words : IntIdTable("words") {
    val word = varchar("word", 100)
}

class Word(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<Word>(Words)

    var word by Words.word
}