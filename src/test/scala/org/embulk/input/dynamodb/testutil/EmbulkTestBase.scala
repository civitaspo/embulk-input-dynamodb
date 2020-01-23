package org.embulk.input.dynamodb.testutil


import org.embulk.input.dynamodb.DynamodbInputPlugin
import org.embulk.spi.InputPlugin
import org.embulk.test.TestingEmbulk
import org.junit.{After, Rule}


class EmbulkTestBase
{
    @Rule
    def embulk: TestingEmbulk = TestingEmbulk.builder()
        .registerPlugin(classOf[InputPlugin], "dynamodb", classOf[DynamodbInputPlugin])
        .build()

    @After
    def destroyEmbulk(): Unit = {
        embulk.destroy()
    }
}
