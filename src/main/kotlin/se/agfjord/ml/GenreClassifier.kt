package se.agfjord.ml

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.slf4j.LoggerFactory

class GenreClassifier {

    private val filmCreator = FilmCreator()
    private val inverseGenreMap = HashMap<Double, String>()
    private val conf = SparkConf().setAppName("genre-classifier").setMaster("local[*]")
    private val sparkContext = JavaSparkContext(conf)

    private val logger = LoggerFactory.getLogger(javaClass)

    private fun predict(model: LogisticRegressionModel, text: String): String? {
        val tf = HashingTF(10000)
        val vector = tf.transform(text.split(" "))
        val prediction = model.predict(vector)
        return inverseGenreMap[prediction]
    }

    fun runKFold(amountOfChunks: Int, chunkToTest: Int): Double {
        val films = filmCreator.createFilms().sortedByDescending { it.title.hashCode() }
        val chunkSize = films.size / amountOfChunks
        val chunks = createChunks(films, chunkSize)
        val test = chunks[chunkToTest]
        chunks.removeAt(chunkToTest)
        val train = chunks.flatten()

        val rdds = sparkContext.parallelize(films)
        val genreMap = createGenreMap(train)
        val tf = HashingTF(10000)

        val features = rdds.map { film -> Pair(film.genre, tf.transform(film.description.split(" "))) }
        val trainingData = features.map { (genre, vector) -> LabeledPoint(genreMap[genre]!!, vector) }
        trainingData.cache()
        val model = LogisticRegressionWithLBFGS().setNumClasses(genreMap.keys.size).run(trainingData.rdd())
        
        var correct = 0
        for (film in test) {
            val prediction = predict(model, film.description)
            if (prediction == film.genre) {
                correct += 1
                println("Successfully predicted $prediction for '${film.title}'")
            } else {
                println("Failed to predict ${film.genre} for ${film.title}, instead predicted $prediction")
            }
        }
        val total = test.size
        val percentage = (correct / total.toDouble()) * 100
        val str = "$percentage"
        val dot = str.indexOf(".")
        val finalStr = str.substring(0, dot + 3)
        println("Of total $total, $finalStr were correctly predicted")

        return percentage
    }

    private tailrec fun createChunks(films: List<FilmCreator.Film>, chunkSize: Int, result: MutableList<List<FilmCreator.Film>> = mutableListOf()): MutableList<List<FilmCreator.Film>> {
        if(films.isEmpty()) {
            return result
        }
        val chunk = films.take(chunkSize)
        result.add(chunk)
        val remaining = films - chunk
        return createChunks(remaining, chunkSize, result)
    }

    private fun createGenreMap(films: List<FilmCreator.Film>): HashMap<String, Double> {
        val genres = films.map { it.genre }.toSet()
        val genreMap = HashMap<String, Double>()
        var counter = 0.0
        for(genre in genres) {
            genreMap.put(genre, counter)
            inverseGenreMap.put(counter, genre)
            counter += 1.0
        }

        return genreMap
    }
}