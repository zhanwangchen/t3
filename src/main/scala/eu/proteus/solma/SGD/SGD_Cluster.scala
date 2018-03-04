package eu.proteus.solma.SGD

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

// this class is used to test SGD Cluster

object SGD_Cluster {

  /** args0  = Parallelism Environment (Int)
    * args1   = input file path
    * args2   = WindowSize (Int)
    * args3   = output file path
    * args4   = InterationWaitTime
    * args5   = PSParallelism
    * args6   = WorkerParallelism
    * @param args see above
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    try {
      streamingEnv.setParallelism(args.apply(0).toInt)
    }
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile(args.apply(1))
      .map { x: String =>
        val test = x.replaceAll(",", "").split(" ")
        // 0 = Label
        val rul = test.apply(0).replace("LabeledVector(", "").replace(",", "").replace(" ", "").toDouble
        val dense1 = test.apply(1).replace("DenseVector(", "").replace(",", "").replace(" ", "").toDouble
        val weights = new Array[Double](7)
        weights.update(0, dense1)
        var j = 1
        for (i <- 2 to test.length - 1) {
          val weight = test.apply(i).replace("))", "").toDouble
          weights.update(j, weight)
          j = j + 1
        }
        LabeledVector(rul, DenseVector(weights))
      }

    val initialweights = WeightVector(DenseVector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 0.0)
    //start GradientDescent
    val sgd = new GradientDescentStreamPS()
    // window 500 best for TurboFan Data
    try {
      sgd.setWindowSize(args.apply(2).toInt)
    }
    try{
      sgd.setIterationWaitTime(args.apply(4).toLong)
    }
    try{
      sgd.setPSParallelism(args.apply(5).toInt)
    }
    try{
      sgd.setWorkerParallelism(args.apply(6).toInt)
    }
    val resultSGDPS = sgd.optimize(labeledDataStream, initialweights)
    val Sink = resultSGDPS.map(x => x).writeAsText(args.apply(3), WriteMode.OVERWRITE).setParallelism(1)

   streamingEnv.execute()
  }

}