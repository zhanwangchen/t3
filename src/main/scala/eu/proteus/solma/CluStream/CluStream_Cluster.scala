package eu.proteus.solma.CluStream

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
  * Created by Ariane on 16.08.2017.
  * class to execute CluStream on the Cluster
  */
object CluStream_Cluster {
  //TODO parameter map for input parameters, centers?
  /* parameters to set
 args0  = Parallelism Environment (Int)
 args1   = input file path
 args2   = WindowSize (Int)
 args3   = MaximalClusterRange (Double)
 args4 = output file path
*/

  def main(args: Array[String]): Unit = {

    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    try{
      streamingEnv.setParallelism(args.apply(0).toInt)
    }
    val trainingFile : String = args.apply(1)

    val evalDataStream: DataStream[DenseVector] = streamingEnv.readTextFile(filePath = trainingFile).map{
      x=>
        val splitData = x.split(" ").map(x => x.toDouble)
        DenseVector(splitData)
    }

    // centerpoint, c_n, sumOfPoints, n, avgDist
    val centers : Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]=Seq(
      (DenseVector(25.00, 0.62,  60.0) ,1,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(20.00, 0.70, 100.0),2,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(32.004, 0.84, 100.0),3,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(42.008, 0.84, 100.0) ,4,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(9.0, 0.055, 100.0),5,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis()),
      (DenseVector(-0.0087, 0.055, 100.0),6,DenseVector(0.0, 0.0,0.0),0,0, System.currentTimeMillis())
    )
    // create a clu instance
    val clu = new CluStream()
      try {
        clu.setWindowSize(args.apply(2).toInt)
      }
    try {
      clu.setMaximalClusterRange(args.apply(3).toDouble)
    }

    // sent initial centers and datastream to cluStream instance
    val cluresult = clu.cluster(evalDataStream,centers)
    val Sink = cluresult.writeAsText(args.apply(4), WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()

  }
}
