package com.jtLiBrain.examples.mongo

import org.mongodb.scala._

/**
  * 1. http://mongodb.github.io/mongo-scala-driver/2.8/getting-started/quick-tour/
  */
object Example {
  def main(args: Array[String]): Unit = {
//    System.setProperty("org.mongodb.async.type", "netty")

//    val uri: String = "mongodb+srv://mymongo:mymongo@cluster0-mtsah.mongodb.net/mytest?retryWrites=true&w=majority"
    val mongoClient: MongoClient = MongoClient("mongodb://localhost:27017")


    val database = mongoClient.getDatabase("mytest")

    val collection = database.getCollection("students")

    val doc = Document("_id" -> 0, "name" -> "MongoDB", "type" -> "database",
      "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))

    val observable = collection.insertOne(doc)

    observable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println("Inserted")

      override def onError(e: Throwable): Unit = println("Failed")

      override def onComplete(): Unit = println("Completed")
    })

    mongoClient.close()
  }
}
