package com.jtLiBrain.examples.mongo

import java.util.logging.{Level, Logger}

import org.mongodb.scala.{Completed, Document, MongoClient, Observer}
import org.scalatest.FunSuite
import Helpers._

/**
  * 1. http://mongodb.github.io/mongo-scala-driver/2.8/getting-started/quick-tour/
  * 2. https://github.com/mongodb/mongo-scala-driver/blob/master/examples/src/test/scala/tour/QuickTour.scala
  */
class ScalaDriverExample extends FunSuite {
  def initMongoClient(): MongoClient = {
    // close mongo log
    val mongoLogger = Logger.getLogger("org.mongodb.driver")
    mongoLogger.setLevel(Level.OFF)

    val uri: String = "mongodb+srv://mymongo:mymongo@cluster0-mtsah.mongodb.net/mylearn?retryWrites=true&w=majority"
    MongoClient(uri)
  }

  def closeMongoClient(client: MongoClient): Unit = {
    client.close()
  }

  test("insertOne") {
    val mongoClient = initMongoClient()

    val database = mongoClient.getDatabase("mylearn")
    val collection = database.getCollection("testSet")

    /*
      {
         "name" : "MongoDB",
         "type" : "database",
         "count" : 1,
         "info" : {
                     x : 203,
                     y : 102
                   }
      }
     */
    val doc = Document(
      "_id" -> 0,
      "name" -> "MongoDB",
      "type" -> "database",
      "count" -> 1,
      "info" -> Document("x" -> 203, "y" -> 102)
    )

    collection.insertOne(doc).results()

    closeMongoClient(mongoClient)
  }

  test("find.first") {
    val mongoClient = initMongoClient()

    val database = mongoClient.getDatabase("mylearn")
    val collection = database.getCollection("testSet")

    collection.find.first().printResults()

    closeMongoClient(mongoClient)
  }

  /**
    * can't work
    */
  test("Observable.subscribe") {
    val mongoClient = initMongoClient()

    val database = mongoClient.getDatabase("mylearn")
    val collection = database.getCollection("testSet")

    val doc = Document(
      "_id" -> 1,
      "name" -> "Java",
      "type" -> "lan",
      "count" -> 1,
      "info" -> Document("x" -> 203, "y" -> 102)
    )

    val observable = collection.insertOne(doc)

    observable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println("Inserted:" + result.toString())

      override def onError(e: Throwable): Unit = println("Failed")

      override def onComplete(): Unit = println("Completed")
    })

    closeMongoClient(mongoClient)
  }

  test("insertMany") {
    val mongoClient = initMongoClient()

    val database = mongoClient.getDatabase("mylearn")
    val collection = database.getCollection("testSet")

    // now, lets add lots of little documents to the collection so we can explore queries and cursors
    val documents: IndexedSeq[Document] = (1 to 100) map { i: Int => Document("i" -> i) }
    val insertObservable = collection.insertMany(documents)

    val insertAndCount = for {
      insertResult <- insertObservable
      countResult <- collection.countDocuments()
    } yield countResult

    println(s"total # of documents after inserting 100 small ones (should be 101):  ${insertAndCount.headResult()}")

    closeMongoClient(mongoClient)
  }

  test("drop") {
    val mongoClient = initMongoClient()

    val database = mongoClient.getDatabase("mylearn")
    val collection = database.getCollection("testSet")

    collection.drop().results()

    closeMongoClient(mongoClient)
  }
}
