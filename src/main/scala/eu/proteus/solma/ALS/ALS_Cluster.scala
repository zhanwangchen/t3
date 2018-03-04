package eu.proteus.solma.ALS

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
// this class is used to test ALS with PS on the cluster
object ALS_Cluster {

  /** args0 = Parallelism Environment (Int)
    * args1   = WindowSize (Int)
    * args2   = setLabeledVectorOutput (bool)
    * args3   = input file path
    * args4   = output file path
    * args5   = IterationWaitTime
    * args6   = PSParallelism
    * args7   = WorkerParallelism
    * @param args currently settable variables see above, add more if required
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    try {
      streamingEnv.setParallelism(args.apply(0).toInt)
    }

    val als = new ALS_PS()

    try{
      als.setIterationWaitTime(args.apply(5).toLong)
    }
    try{
      als.setPSParallelism(args.apply(6).toInt)
    }
    try{
      als.setWorkerParallelism(args.apply(7).toInt)
    }
    try{
       als.setWindowSize(args.apply(1).toInt)
      }
    try {
      als.setLabeledVectorOutput(args.apply(2).toBoolean)
    }

    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile(args.apply(3))
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
    val Sink = resultSGDPS.writeAsText(args.apply(4), WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()

  }

}