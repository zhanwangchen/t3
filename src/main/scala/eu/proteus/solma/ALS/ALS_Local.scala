package eu.proteus.solma.ALS

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
// this class is used to test ALS with PS local
object ALS_Local {

  /**
    * method that starts the computation
    * @param args not used
    * @see ALS_Cluster
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val als = new ALS_PS()
      .setWindowSize(400)
      .setLabeledVectorOutput(false)
      .setPSParallelism(1)
      .setWorkerParallelism(1)

    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/proteus-solma-development/src/main/resources/ALSTest.csv")
      .map{ x: String =>
        val test = x.replaceAll(",", "").split(" ")
        // 0 = Label
        val rul = test.apply(0).replace("LabeledVector(", "").replace(",","").replace(" ", "").toDouble
        val dense1 = test.apply(1).replace("DenseVector(", "").replace(",","").replace(" ", "").toDouble
        val weights = new Array[Double](7)
        weights.update(0, dense1)
        var j = 1
        for(i<-2 to test.length-1){
          val weight = test.apply(i).replace("))", "").toDouble
          weights.update(j, weight )
          j = j+1
        }
        LabeledVector(rul,DenseVector(weights))
      }

    val resultSGDPS = als.fitAndPredict(labeledDataStream)
    val Sink = resultSGDPS.map(x=>x).writeAsText("/tmp/ALS_resfinal.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()

  }

}