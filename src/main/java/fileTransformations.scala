import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by rajsarka on 11/10/2016.
  */
object fileTransformations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("pasrser-test")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    //    val hc = new HiveContext(sc)

    Logger.getRootLogger().setLevel(Level.ERROR)

    val dir = "./src/main/resources/Daimler"


    val listOfDates = getListOfFolder(dir)

    for (date <- listOfDates) {
      val listOfFolders = getListOfFolder(date.getAbsolutePath)
      //      val dateString = date.getName

      for (folder <- listOfFolders) {
        val lineNumber = folder.getName.substring(4, 8) // 8 is exclusive

        val listOfFiles = getListOfFiles(folder.getAbsolutePath)
        listOfFiles.foreach(file => {

          if (file.getName == "BALogFile.txt") {
            println("Opening file BALog")
            val BALogRDD = sc.textFile(file.getAbsolutePath)
            val header = BALogRDD.first()
            val headerArray = header.split(";")
            val headerLength = headerArray.size
            val indices = ArrayBuffer.empty[Int]
            var index = 0
            while (headerArray.indexOf("Message number", index) != -1) {
              index = headerArray.indexOf("Message number", index)
              indices += index
              index += 1
            }
            val index_limits = {
              indices.slice(1, indices.length)
                .map(numb => {
                  numb - 1
                })
            }

            //            println(indices)
            //            println(index_limits)

            val RDDLength = (BALogRDD.count() - 1).toInt
            val currRow = new AtomicInteger(0)
            val currCol = new AtomicInteger(0)

            val masterArray = ArrayBuffer.fill(headerLength, RDDLength)("")

            val rowData = BALogRDD.filter(_ != header)
              .map(_ + "a")
              .map(line => {
                val someArray = line.split(";")
                val slicedArray = someArray.slice(0, someArray.size - 1)
                slicedArray
              })
            //              .collect()


            //            println(rowData.count())

            val rowDataTranspose = rowData.collect() // it is not transposed for this test case
            val rowDataTranspose_rows = rowDataTranspose.length // The size is 3 -> number of lines in input file

            val arrayToWrite = ArrayBuffer.empty[Array[String]]
            index_limits += headerLength
            //            println(index_limits)
            var row_count_arrayToWrite = 0

            for (k <- 0 until rowDataTranspose_rows) {
              var temp_count = 0
              //              println("Enter K loop")
              for (i <- index_limits) {
                //                println("Enter i loop")
                var value = ""
                for (j <- temp_count until i) {
                  //                  println("Enter j loop")
                  // until is exclusive of the last element
                  value += rowDataTranspose(k)(j) + ";"
                }
                //                arrayToWrite += value.split(",")
                value = lineNumber + value
                println(value)
                row_count_arrayToWrite += 1
                temp_count = i
              }
            }
            /*/*            val rowData = BALogRDD.filter( _ != header)
                          .map(_ + "a")
                          .map(line => {
                            val someArray = line.split(";")
                            val slicedArray = someArray.slice(0, someArray.size - 1)
                            val rowList = new ListBuffer[String]
                            var indexCount = 0
                            while(indexCount + structureSize - 1 < slicedArray.size){
                              rowList += lineNumber + slicedArray.slice(indexCount, structureSize + indexCount).mkString(";")*/

            //                  hc.sql("insert into table values(" + rowList(0),  + ")");


                              indexCount += structureSize
                            }
                            rowList
                        }).repartition(1)
                        rowData.saveAsTextFile("D:\\Daimler\\src\\main\\resources\\testOutput\\" + date.getName + "\\" + folder.getName)*/


          }

          else if (file.getName == "DosKonfigLog.txt") {
            println("Opening file Dos")
            val BALogRDD = sc.textFile(file.getAbsolutePath)
            val header = BALogRDD.first()
            val headerArray = header.split(";")
            val headerLength = headerArray.size
            val indices = ArrayBuffer.empty[Int]
            var index = 0
            while (headerArray.indexOf("Message number", index) != -1) {
              index = headerArray.indexOf("Message number", index)
              indices += index
              index += 1
            }
            val index_limits = {
              indices.slice(1, indices.length)
                .map(numb => {
                  numb - 1
                })
            }

            //            println(indices)
            //            println(index_limits)

            val RDDLength = (BALogRDD.count() - 1).toInt
            val currRow = new AtomicInteger(0)
            val currCol = new AtomicInteger(0)

            val masterArray = ArrayBuffer.fill(headerLength, RDDLength)("")

            val rowData = BALogRDD.filter(_ != header)
              .map(_ + "a")
              .map(line => {
                val someArray = line.split(";")
                val slicedArray = someArray.slice(0, someArray.size - 1)
                slicedArray
              })
            //              .collect()


            //            println(rowData.count())

            val rowDataTranspose = rowData.collect() // it is not transposed for this test case
            val rowDataTranspose_rows = rowDataTranspose.length // The size is 3 -> number of lines in input file

            val arrayToWrite = ArrayBuffer.empty[Array[String]]
            index_limits += headerLength
            //            println(index_limits)
            var row_count_arrayToWrite = 0

            for (k <- 0 until rowDataTranspose_rows) {
              var temp_count = 0
              //              println("Enter K loop")
              for (i <- index_limits) {
                //                println("Enter i loop")
                var value = ""
                for (j <- temp_count until i) {
                  //                  println("Enter j loop")
                  // until is exclusive of the last element
                  value += rowDataTranspose(k)(j) + ";"
                }
                //                arrayToWrite += value.split(",")
                value = lineNumber + value
                println(value)
                row_count_arrayToWrite += 1
                temp_count = i
              }
            }

          }
        })
      }
    }


  }

  def getListOfFolder(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      val folders = d.listFiles.filter(_.isDirectory).toList
      folders
    } else {
      List[File]()
    }
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      val files = d.listFiles.filter(_.isFile).toList
      files
    } else {
      List[File]()
    }
  }

}
