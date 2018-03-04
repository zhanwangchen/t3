package eu.proteus.solma.CluStream

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

/**
  * Created by Ariane on 08.07.2017.
  * This class is to use the CluStream local
  */
object CluStreamLocalTest {
  /**
    * main to start a local test run
    * @param args -> use cluster version to use args
    */
  def main(args: Array[String]): Unit = {
    /*
    val points1: Seq[DenseVector] = Seq(
      (DenseVector(2.0, 3.0)),
      (DenseVector(3.0, 2.0)),
      (DenseVector(7.0, 8.0)),
      (DenseVector(8.0, 7.0)),
      (DenseVector(5.0, 8.0)),
      (DenseVector(6.0, 7.0)),
      (DenseVector(7.0, 6.0)),
      (DenseVector(8.0, 5.0)),
      (DenseVector(100.0, 8.0)),
      (DenseVector(6.0, 70.0)),
      (DenseVector(100.0, 6.0)),
      (DenseVector(100.0, 5.0))
    )*/
    // centerpoint, c_n, sumOfPoints, n, avgDist
    val centers : Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]=Seq(
      (DenseVector(25.00, 0.62,  60.0) ,1,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(20.00, 0.70, 100.0),2,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
        (DenseVector(32.004, 0.84, 100.0),3,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(42.008, 0.84, 100.0) ,4,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
        (DenseVector(9.0, 0.055, 100.0),5,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(-0.0087, 0.055, 100.0),6,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis())
    )
   // create a streaming environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val trainingFile : String = "C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\CS_bigDS_settings.csv"

    val evalDataStream: DataStream[DenseVector] = streamingEnv.readTextFile(filePath = trainingFile).map{ x=>
        val splitData = x.split(" ").map(x => x.toDouble)
        DenseVector(splitData)
    }
    // create a CluStream instance
    val clu = new CluStream()
      .setWindowSize(500)
      .setMaximalClusterRange(1.3)

    //hand over the data to the CluStream instance
    val finalCenters = clu.cluster(evalDataStream,centers)
    val Sink = finalCenters.writeAsText("src/main/resources/CS_results.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()
  }
}
