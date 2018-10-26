package se.agfjord.ml

import com.google.gson.JsonParser
import java.io.File
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.nio.file.Paths
import java.io.BufferedReader
import java.nio.file.Files
import java.util.zip.GZIPInputStream
import kotlin.text.Charsets.UTF_8


class FilmCreator {

    private val jsonParser = JsonParser()

    fun createFilms(): List<Film> {
        val inputStream = File("./data/film_metadata.csv.gz").inputStream()
        val reader = GZIPInputStream(inputStream).bufferedReader(UTF_8).use { it.readText() }.reader()
        val csvParser = CSVParser(reader, CSVFormat.DEFAULT.withTrim()).toMutableList()
        csvParser.removeAt(0)
        return csvParser.mapNotNull { csvRecord -> createFilm(csvRecord) }
    }

    private fun createFilm(row: CSVRecord): Film? {
        val genres = parseJsonGenres(row[3])
        if (genres.isEmpty()) {
            return null
        }
        val firstGenre = genres.first()
        val title = row[8]
        val description = row[9]

        return Film(title, firstGenre, description)
    }

    private fun parseJsonGenres(json: String): List<String> {
        val jsonArray = jsonParser.parse(json).asJsonArray
        return jsonArray.map { jsonElement -> jsonElement.asJsonObject.get("name").asString }
    }

    data class Film(val title: String, val genre: String, val description: String)
}