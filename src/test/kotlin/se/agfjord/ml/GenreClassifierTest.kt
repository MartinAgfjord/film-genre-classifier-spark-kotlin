package se.agfjord.ml

import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThan
import org.junit.Test

class GenreClassifierTest {

    companion object {
        private val underTest = GenreClassifier()
    }

    @Test
    fun `Film genre 10 fold cross validation`() {
        val p1 = underTest.runKFold(10, 0)
        assertThat(p1, `is`(greaterThan(0.99)))
        val p2 = underTest.runKFold(10, 1)
        assertThat(p2, `is`(greaterThan(0.99)))
        val p3 = underTest.runKFold(10, 2)
        assertThat(p3, `is`(greaterThan(0.99)))
        val p4 = underTest.runKFold(10, 3)
        assertThat(p4, `is`(greaterThan(0.99)))
        val p5 = underTest.runKFold(10, 4)
        assertThat(p5, `is`(greaterThan(0.99)))
        val p6 = underTest.runKFold(10, 5)
        assertThat(p6, `is`(greaterThan(0.99)))
        val p7 = underTest.runKFold(10, 6)
        assertThat(p7, `is`(greaterThan(0.99)))
        val p8 = underTest.runKFold(10, 7)
        assertThat(p8, `is`(greaterThan(0.99)))
        val p9 = underTest.runKFold(10, 8)
        assertThat(p9, `is`(greaterThan(0.99)))
        val p10 = underTest.runKFold(10, 9)
        assertThat(p10, `is`(greaterThan(0.99)))
    }

}