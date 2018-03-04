package eu.proteus.solma

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
  * Created by Ariane on 15.08.2017.
  */
object SummerieTest {

  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(4)

    val sum = new SummaryFunctionWithPS()
      .setWindowSize(100)
      .setPSParallelism(1)



    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\CS_bigDS_settings.csv")
      .map{ x: String =>
        val test = x.replaceAll(",", "").split(" ")
        // 0 = Label
       // val rul = test.apply(0).replace("LabeledVector(", "").replace(",","").replace(" ", "").toDouble
        val rul = -1.0
        val dense1 = test.apply(0).replace("DenseVector(", "").replace(",","").replace(" ", "").toDouble
        val weights = new Array[Double](3)
        weights.update(0, dense1)
        var j = 1
        for(i<-1 to test.length-1){
          val weight = test.apply(i).replace("))", "").toDouble

          weights.update(j, weight )
          j = j+1
        }

        LabeledVector(rul,DenseVector(weights))
      }

    val resultSum = sum.summarize(labeledDataStream)
    val Sink = resultSum.writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\Sum_result.txt", WriteMode.OVERWRITE).setParallelism(1)
    streamingEnv.execute()

  }

}