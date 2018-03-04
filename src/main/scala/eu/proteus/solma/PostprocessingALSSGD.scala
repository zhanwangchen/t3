package eu.proteus.solma

import eu.proteus.solma.SGD.SGD_Local.CMAPSSData
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.common.{LabeledVector, WeightVector}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.optimization.{GenericLossFunction, LearningRateMethod, LinearPrediction, SquaredLoss}
import org.apache.flink.ml.regression.MultipleLinearRegression

/**
  * Created by Ariane on 10.09.2017.
  * The MLR model to evaluate ALS and SGD
  */
object PostprocessingALSSGD {

  def main(args: Array[String]) {
    // input files SGD
    val trainingFileSGD = "C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\TurboFanComplet.csv"

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging;

    // read in trainingset incl column names
    val trainingDataSGD = env.readCsvFile[(CMAPSSData)](
      trainingFileSGD,
      fieldDelimiter = " ",
      pojoFields = Array("id", "cycle", "settings1", "settings2", "s3", "s4", "s9", "s14", "s15", "s21", "rul"))

    // create a LabelVector DataSet
    val trainingDataLVSGD = trainingDataSGD.map { tuple =>
      LabeledVector(tuple.rul, DenseVector(tuple.featuresNoRul.toArray))
    }
    //Batch MLR to check
   // inialize MLP with parameters
    val mlr = new MultipleLinearRegression()
      .setStepsize(0.0006)
      .setIterations(10000)
      .setConvergenceThreshold(0.01)
      .setLearningRateMethod(LearningRateMethod.Default)

    mlr.fit(trainingDataLVSGD)
    val weights = mlr.weightsOption.get.collect()
    val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)
    val lossInital = trainingDataLVSGD.map(x => lossFunction.lossGradient(x, weights.last)).sum(0).collect()
    val lossInital1 = trainingDataLVSGD.map(x => lossFunction.lossGradient(x, WeightVector(DenseVector(-0.0012950549712493543, -0.04466702984299608, -0.047494934401347604, -0.04752942189336458, -0.0805761489347408, -0.015343046123735228, 0.002952884512694476),0.5430807785244716))).sum(0).collect()
    val lossInital2 = trainingDataLVSGD.map(x => lossFunction.lossGradient(x,WeightVector(DenseVector(-0.0012093630009395644, -0.010426632310269348, -0.015867922050479568, -0.0064880913347248785, -0.05894693680002703, -0.017164234685221613, 0.005524787860328461),0.4252698805160335))).sum(0).collect()
    val lossInital3 = trainingDataLVSGD.map(x => lossFunction.lossGradient(x,WeightVector(DenseVector(-0.001153371943427156, -0.01417424527222574, -0.01968704726890816, -0.010148224016852066, -0.061794468787218136, -0.014223735902931946, 0.0025318407673747025),0.4285620927317347))).sum(0).collect()
    val lossInital4 = trainingDataLVSGD.map(x => lossFunction.lossGradient(x, WeightVector(DenseVector(-0.001105483354229232, -0.01003604129451444, -0.015280897736523538, -0.005758846956578995, -0.056891305420558314, -0.017309120405969304, 0.005983204744891729),0.4131380816057076))).sum(0).collect()
    println(lossInital1)
    println(lossInital2)
    println(lossInital3)
    println(lossInital4)
    env.execute()
  }
}
