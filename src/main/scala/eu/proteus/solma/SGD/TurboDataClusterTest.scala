package eu.proteus.solma.SGD

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.optimization._
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//commented
//This class is used to test if SGD works okay on the cluster

object TurboDataClusterTest {
  def main(args: Array[String]) {

    // input files, normed datasets
    val trainingFile = args.apply(0)
    val outputfile = args.apply(8)
   // create ExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(args.apply(2).toInt)

    // read in trainingset and add "column" names
    val trainingData = env.readCsvFile[(CMAPSSData)](
      trainingFile,
      fieldDelimiter = " ",
      pojoFields = Array("id", "cycle", "settings1", "settings2", "s2", "s3", "s4", "s6", "s7", "s8", "s9", "s11", "s12", "s13", "s14", "s15", "s17", "s18", "s21", "rul"))

    // transfer data into LV
    val trainingDataLV = trainingData.map { tuple =>
        LabeledVector(tuple.rul, DenseVector(tuple.featuresNoRul.toArray))
    }

    // start time for MLR
   val time1 = System.currentTimeMillis()
    // inialize MLP with parameters
    val mlr = MultipleLinearRegression()
      .setLearningRateMethod(LearningRateMethod.Default)
      .setStepsize(0.0001).setIterations(10000)
      .setConvergenceThreshold(0.1)

    mlr.fit(trainingDataLV)
    val weights = mlr.weightsOption.get.collect()
    // end of MLR
    val time2 = System.currentTimeMillis()

    // create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamingEnv.setParallelism(args.apply(3).toInt)
    streamingEnv.setBufferTimeout(args.apply(1).toInt)

    //prepare dara
    val data = trainingDataLV.collect()
    val evalDataStream: DataStream[LabeledVector] = streamingEnv.fromCollection(data)
    val weightsEmpty = WeightVector(DenseVector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),0.0)
    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    // start time for SGD
    val time3 = System.currentTimeMillis()
    //start SGD
    val sgd2 = new GradientDescentStreamPS()
      .setWindowSize(args.apply(5).toInt)
      .setRegularizationPenalty(L1Regularization)
      .setIterationWaitTime(4000)
      .setWorkerParallelism(args.apply(6).toInt)
      .setPSParallelism(args.apply(7).toInt)

    val resultsSGD = sgd2.optimize(evalDataStream,weightsEmpty)
    val Sink = resultsSGD.map(x=>x).writeAsText(outputfile, WriteMode.OVERWRITE).setParallelism(1)
    val time4 = System.currentTimeMillis()
    // val lossInital = trainingDataLV.map(x => lossFunction.lossGradient(x,weights.last)).sum(0).print()

    streamingEnv.execute()
    println(time4-time3+"SGD")
    println(time2-time1+"MLR")

  }

  class CMAPSSData {

    var id: Int = _
    var cycle: Int = _
    var settings1: Double = _
    var settings2: Double = _
    var s2: Double = _
    var s3: Double = _
    var s4: Double = _
    var s6: Double = _
    var s7: Double = _
    var s8: Double = _
    var s9: Double = _
    var s11: Double = _
    var s12: Double = _
    var s13: Double = _
    var s14: Double = _
    var s15: Double = _
    var s17: Double = _
    var s18: Double = _
    var s21: Double = _
    var rul: Double = _


    def featuresNoRul: List[Double] = {
      val a1 = List(cycle.toDouble, s3, s4, s9, s14, s15, s21)
      a1
    }

  }
}