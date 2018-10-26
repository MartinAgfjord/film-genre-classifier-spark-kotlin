package se.agfjord.ml

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

class GenreClassifier {

    private val filmCreator = FilmCreator()
    private val inverseGenreMap = HashMap<Double, String>()
    private val model = createModel()

    fun predict(text: String): String? {
        val tf = HashingTF(10000)
        val vector = tf.transform(text.split(" "))
        val prediction = model.predict(vector)
        return inverseGenreMap[prediction]
    }

    private fun createModel(): LogisticRegressionModel {
        val conf = SparkConf().setAppName("genre-classifier").setMaster("local[*]")
        val sparkContext = JavaSparkContext(conf)
        val films = filmCreator.createFilms()
        val tf = HashingTF(10000)
        val pairs = films.map { film -> Pair(film.genre, film.description) }
        val rdds = sparkContext.parallelize(pairs)
        val genreMap = createGenreMap(pairs.map { it.first }.toSet())
        val features = rdds.map { (genre, description) -> Pair(genre, tf.transform(description.split(" "))) }
        val trainingData = features.map { foo -> LabeledPoint(genreMap[foo.first]!!, foo.second) }
        trainingData.cache()

        return LogisticRegressionWithLBFGS().setNumClasses(genreMap.keys.size).run(trainingData.rdd())
    }

    private fun createGenreMap(genres: Set<String>): HashMap<String, Double> {
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
