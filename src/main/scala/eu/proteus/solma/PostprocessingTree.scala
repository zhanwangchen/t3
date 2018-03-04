package eu.proteus.solma

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala._

/**
  * Created by Ariane on 07.09.2017.
  */
object PostprocessingTree {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    //streamingEnv.setParallelism(1)

    // read preprocessed data, tree requires LabeledVector as dataformat
    val labeledDataStream: DataSet[LabeledVector] = env.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\HT_bigDS_Adult_Test.txt").map { x =>
      val test = x.replaceAll(",", "").split(" ")
      val rul = test.apply(0).replace("LabeledVector(", "").replace(",", "").replace(" ", "").toDouble
      val dense1 = test.apply(1).replace("DenseVector(", "").replace(",", "").replace(" ", "").toDouble
      val weights = new Array[Double](10)
      weights.update(0, dense1)
      var j = 1
      for (i <- 2 until test.length) {
        val weight = test.apply(i).replace("))", "").toDouble
        weights.update(j, weight)
        j = j + 1
      }
      LabeledVector(rul, DenseVector(weights))
    }



    val labeledDataStream1: DataSet[LabeledVector] = env.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\HT_results.txt").map { x =>

      val test = x.replaceAll(",", "").replace(")", "").split(" ")
      val rul = test.apply(0).replace("Left((LabeledVector(", "").replace(",", "").replace(" ", "").toDouble
      val dense1 = test.apply(1).replace("DenseVector(", "").replaceAll(",", "").replace(" ", "").toDouble
      val weights = new Array[Double](10)
      weights.update(0, dense1)
      var j = 1
      for (i <- 2 until 11) {
        println(i + test.apply(i))
        val weight = test.apply(i).toDouble
        weights.update(j, weight)
        j = j + 1
      }
      println(LabeledVector(rul, DenseVector(weights)))
      LabeledVector(rul, DenseVector(weights))
    }

    val result = labeledDataStream.join(labeledDataStream1).where(x => x.vector.toString).equalTo(y=> y.vector.toString).map{tuple =>
      var value = ""
      if(tuple._1.label ==  tuple._2.label){
        if(tuple._1.label == 1.0) value = "tp"
        else value = "tn"
      }
      (value, 1)
    }.groupBy(0).sum(1)

    result.print()

    }
}
