package se.agfjord.ml

import com.google.gson.JsonElement
import com.google.gson.JsonParser
import java.io.File
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.nio.file.Paths
import java.io.BufferedReader
import java.io.Serializable
import java.nio.file.Files
import java.util.zip.GZIPInputStream
import kotlin.text.Charsets.UTF_8


class FilmCreator {

    private val jsonParser = JsonParser()
    private val stopWords = createStopWords()

    private fun createStopWords() = File("./data/stop_words.txt").readText(Charsets.UTF_8)
            .lines().map { it.toLowerCase() }

    fun createFilms(): List<Film> {
        val inputStream = File("./data/film_metadata.csv.gz").inputStream()
        val reader = GZIPInputStream(inputStream).bufferedReader(UTF_8).use { it.readText() }.reader()
        val csvParser = CSVParser(reader, CSVFormat.DEFAULT.withTrim()).toMutableList()
        csvParser.removeAt(0)
        return csvParser.mapNotNull { csvRecord -> createFilm(csvRecord) }
                .distinctBy { it.title }
    }

    private fun createFilm(row: CSVRecord): Film? {
        val genres = parseJsonGenres(row[3])
        if (genres.isEmpty()) {
            return null
        }
        val firstGenre = genres.first()
        val language = row[7]
        if (language != "en") {
            return null
        }
        val title = row[8]
        val description = removeStopWords(row[9])

        return Film(title, firstGenre.name, description)
    }

    private fun removeStopWords(text: String): String {
        val list = text.split(" ")
        return list.filter{ word -> isNotStopWord(word) }.joinToString(separator = " ")
    }

    private fun isNotStopWord(word: String) = !stopWords.contains(word.toLowerCase())

    private fun parseJsonGenres(json: String): List<Genre> {
        val jsonArray = jsonParser.parse(json).asJsonArray
        return jsonArray.map { jsonElement -> parseGenre(jsonElement) }
                .sortedBy { it.id }
    }

    private fun parseGenre(jsonElement: JsonElement): Genre {
        val jsonObject = jsonElement.asJsonObject
        val id  = jsonObject.get("id").asInt
        val name = jsonObject.get("name").asString

        return Genre(id, name)
    }

    data class Genre(val id: Int, val name: String)
    data class Film(val title: String, val genre: String, val description: String) : Serializable
}