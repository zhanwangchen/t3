package eu.proteus.solma.HoeffdingTree

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Created by Ariane on 02.08.2017.
  * This class is to use the HoeffdingTree local
  */
object HoeffdingTreeLocalTest {

  // possible input data set
  /*val points1: Seq[LabeledVector] = Seq(
    LabeledVector( 0.0,(DenseVector(0.0, 2.0, 1.0, 0.0))),
    LabeledVector( 0.0,(DenseVector(0.0, 2.0, 1.0, 1.0))),
    LabeledVector( 1.0,(DenseVector(1.0, 2.0, 1.0, 0.0))),
    LabeledVector( 1.0,(DenseVector(2.0, 1.0, 1.0, 0.0))),
    LabeledVector( 1.0,(DenseVector(2.0, 0.0, 0.0, 0.0))),
    LabeledVector( 0.0,(DenseVector(2.0, 0.0, 0.0, 1.0))),
    LabeledVector( 0.0,(DenseVector(1.0, 0.0, 0.0, 1.0))),
    LabeledVector( 1.0,(DenseVector(0.0, 1.0, 1.0, 0.0))),
    LabeledVector( 1.0,(DenseVector(0.0, 0.0, 0.0, 0.0))),
    LabeledVector( 1.0,(DenseVector(2.0, 1.0, 0.0, 0.0))),
    LabeledVector( 1.0,(DenseVector(0.0, 1.0, 0.0, 1.0))),
    LabeledVector( 1.0,(DenseVector(1.0, 1.0, 1.0, 1.0))),
    LabeledVector( 0.0,(DenseVector(1.0, 2.0, 0.0, 0.0))),
    LabeledVector( 1.0,(DenseVector(2.0, 1.0, 1.0, 1.0)))
  )*/

  /**
    *
    * @param args command line, not used here
    * @see eu.proteus.solma.HoeffdingTree.HT_Cluster if you like to use command line
    */
  def main(args: Array[String]): Unit = {

    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //streamingEnv.setParallelism(1)

    // read preprocessed data, tree requires LabeledVector as dataformat
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\HT_bigDS_AdultTree_Train.txt") //HT_smallDS.txt" )//
      .map{ x: String =>
        val test = x.replaceAll(",", "").split(" ")
        val rul = test.apply(0).replace("LabeledVector(", "").replace(",","").replace(" ", "").toDouble
        val dense1 = test.apply(1).replace("DenseVector(", "").replace(",","").replace(" ", "").toDouble
        val weights = new Array[Double](10)
        weights.update(0, dense1)
        var j = 1
        for(i<-2 until test.length){
          val weight = test.apply(i).replace("))", "").toDouble
          weights.update(j, weight )
          j = j+1
        }
        LabeledVector(rul,DenseVector(weights))
      }

    // create a new tree instance
    val tree = new HoeffdingTree()
      .setWorkerParallelism(2)
      .setWindowSize(1000)
      .setLabeledVectorOutput(true)

    //give known data to tree instance to build a tree
    val result = tree.fit(labeledDataStream)

    // write result to a txt-file
    val Sink = result.writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\HT_results.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()
  }
}
