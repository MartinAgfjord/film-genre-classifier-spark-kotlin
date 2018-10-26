package se.agfjord.ml

import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test


class GenreClassifierTest {

    companion object {
        private val underTest = GenreClassifier()
    }

    @Test
    fun `Smoke test`() {
        val genre = underTest.predict("A mischievous young boy, Tom Sawyer, witnesses a murder by the deadly Injun Joe. Tom becomes friends with Huckleberry Finn, a boy with no future and no family. Tom has to choose between honoring a friendship or honoring an oath because the town alcoholic is accused of the murder. Tom and Huck go through several adventures trying to retrieve evidence.")
        assertThat(genre, `is`("Action"))
    }

}
