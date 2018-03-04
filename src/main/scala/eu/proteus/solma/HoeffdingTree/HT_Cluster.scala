package eu.proteus.solma.HoeffdingTree

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
  * Created by Ariane on 30.08.2017.
  * for cluster usage
  */
object HT_Cluster {

  /**
    * @param args command line, currently available params :
    *   args0  = Parallelism Environment (Int)
    * args1   = input file path
    * args2   = WindowSize (Int)
    * args3   = PSParallel
    * args4   = Worker Parallel
    * args5   = output file path
    * args6   = LabeledVectorOutput (boolean)
    * args7   = treebound (Double)
    * add more if necessary
    */


  def main(args: Array[String]): Unit = {

    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

    try{
      streamingEnv.setParallelism(args.apply(0).toInt)
    }
    val trainingFile : String = args.apply(1)

    //read preprocessed data, tree requires LabeledVector as dataformat
    // ATTENTION!! Changing the number of attributes requires to change
    // --> val weights = new Array[Double](12 <--- this value )
    val evalDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile(filePath = trainingFile)
      .map{ x: String =>
      val test = x.replaceAll(",", "").split(" ")
      // 0 = Label
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

    // create tree instance with parameters, try, if not, do nothing
    val tree = new HoeffdingTree()
    try{
      tree.setWindowSize(args.apply(2).toInt)
    }
    try{
      tree.setPSParallelism(args.apply(3).toInt)
    }
    try{
      tree.setWorkerParallelism(args.apply(4).toInt)
    }
    try{
      tree.setLabeledVectorOutput(args.apply(6).toBoolean)
    }
    try{
      tree.setTreeBound(args.apply(7).toDouble)
    }

    // hand over data to the tree instance to build tree
    val resultingtree = tree.fit(evalDataStream)
    // write to file
    val Sink = resultingtree.writeAsText(args.apply(5), WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()
  }
}