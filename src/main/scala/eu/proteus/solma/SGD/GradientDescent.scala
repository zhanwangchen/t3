package eu.proteus.solma.SGD

/**
  * Created by Ariane on 03.06.2017.
  * source code for batch taken from:
  * https://github.com/apache/flink/tree/master/flink-libraries/flink-ml/src/main/scala/org/apache/flink/ml/optimization
  */

import eu.proteus.solma.pipeline.StreamTransformer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.common.{Parameter, _}
import org.apache.flink.ml.math._
import org.apache.flink.ml.optimization.IterativeSolver._
import org.apache.flink.ml.optimization.LearningRateMethod.LearningRateMethodTrait
import org.apache.flink.ml.optimization.Solver.{LossFunction, RegularizationConstant, RegularizationPenaltyValue}
import org.apache.flink.ml.pipeline.Estimator
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, createTypeInformation}
import org.slf4j.Logger

object GradientDescent {

  // taken from: https://github.com/TU-Berlin-DIMA/proteus-solma/blob/development/src/main/scala/eu/proteus/solma/sax/SAX.scala
  /**
    * Class logger.
    */
  private val Log: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)
  /**
    * Iteration Counter
    */
  private val iterationCounter: Int = 1

  /**
    * Case object to define the step size for opt search
    */
  case object LearningRate extends Parameter[Int] {

    /**
      * Default step size
      * TODO note: start n0, than get smaller n0/iter
      */
    private val DefaultLearningRate: Int = 1

    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(LearningRate.DefaultLearningRate)

  } // end LearningRate

  /**
    * Transform the input datastream in a KeyedStream. The method will reject data that is not
    * associated with a key.
    *
    * @param input The input datastream with tuples (weightName, value)
    *              what do wie need for SDG? Weights, learningrate, iteration > weight = key?
    * @tparam T The payload type.
    * @return A KeyedStream.
    */
  private[GradientDescent] def toKeyedStream[T](
                                     input: DataStream[(T, Int)]): KeyedStream[(T, Int), Int] = {

    input match {
      case tuples: DataStream[(T, Int)] => {
        implicit val typeInfo = TypeInformation.of(classOf[(Any, Int)])
        tuples.asInstanceOf[DataStream[(T, Int)]].keyBy(t => t._2)
      }
      case _ => {
        throw new UnsupportedOperationException(
          "Cannot build a keyed stream from the given data type")
      }
    }

  }
}


/**
  * Gradient Descent transformer.
 */
class GradientDescent extends StreamTransformer[GradientDescent] with Estimator[GradientDescent] {

  import eu.proteus.solma.SGD.GradientDescent.Log
  import org.apache.flink.ml.optimization._

  /** Provides a solution for the given optimization problem
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param initialWeights The initial weights that will be optimized
    * @return The weights, optimized for the provided data.
    */
  def optimize(
                       // stream
  data: DataSet[LabeledVector],
  initialWeights: Option[DataSet[WeightVector]]): DataSet[WeightVector] = {

  val numberOfIterations: Int = parameters(Iterations)
  val convergenceThresholdOption: Option[Double] = parameters.get(ConvergenceThreshold)
  val lossFunction = parameters(LossFunction)
  val learningRate = parameters(LearningRate)
  val regularizationPenalty = parameters(RegularizationPenaltyValue)
  val regularizationConstant = parameters(RegularizationConstant)
  val learningRateMethod = parameters(LearningRateMethodValue)

    /** Creates a DataSet with one zero vector. The zero vector has dimension d, which is given
      * by the dimensionDS.
      *
      * @param dimensionDS DataSet with one element d, denoting the dimension of the returned zero
      *                    vector
      * @return DataSet of a zero vector of dimension d
      */
    def createInitialWeightVector(dimensionDS: DataSet[Int]): DataSet[WeightVector] = {
      dimensionDS.map {
        dimension =>
          val values = Array.fill(dimension)(0.0)
          WeightVector(DenseVector(values), 0.0)
      }
    }

    /** Creates initial weights vector, creating a DataSet with a WeightVector element
      *
      * @param initialWeights An Option that may contain an initial set of weights
      * @param data The data for which we optimize the weights
      * @return A DataSet containing a single WeightVector element
      */
    def createInitialWeightsDS(initialWeights: Option[DataSet[WeightVector]],
                               data: DataSet[LabeledVector]): DataSet[WeightVector] = {
      // number of attributes
      val dimensionsDS = data.map(_.vector.size).reduce((a, b) => b)

      initialWeights match {
        // Ensure provided weight vector is a DenseVector
        case Some(wvDS) =>
          wvDS.map {
            wv => {
              val denseWeights = wv.weights match {
                case dv: DenseVector => dv
                case sv: SparseVector => sv.toDenseVector
              }
              WeightVector(denseWeights, wv.intercept)
            }
          }
        case None => createInitialWeightVector(dimensionsDS)
      }
    }

    // Initialize weights
    // createInitialWeightsDS comes from IterationSolver > but we use a Pipeline, how do we get the data
    val initialWeightsDS: DataSet[WeightVector] = createInitialWeightsDS(initialWeights, data)




  // Perform the iterations
  convergenceThresholdOption match {
  // No convergence criterion
  case None =>
  optimizeWithoutConvergenceCriterion(
  data,
  initialWeightsDS,
  numberOfIterations,
  regularizationPenalty,
  regularizationConstant,
  learningRate,
  lossFunction,
  learningRateMethod)
  case Some(convergence) =>
  optimizeWithConvergenceCriterion(
  data,
  initialWeightsDS,
  numberOfIterations,
  regularizationPenalty,
  regularizationConstant,
  learningRate,
  convergence,
  lossFunction,
  learningRateMethod)
}
}
/**
  /**
    * Average of the training set.
    */
  private[sdg] var trainingAvg : Option[Double] = None

  /**
    * Standard deviation of the training set.
    */
  private[sax] var trainingStd : Option[Double] = None

  /**
    * Sets the size of the words for SAX.
    * @param size The size of the words.
    * @return A configured [[GradientDescent]].
    */
  def setWordSize(size: Int) : SAX = {
    this.parameters.add(SAX.WordSize, size)
    this
  }
*/


  /**
    * Get the learning rate.
    * @return The learning rate.
    */
  def getLearningRate() :  Int = {
    this.parameters.get(eu.proteus.solma.SGD.GradientDescent.LearningRate).get
  }

  /**
    * Sets the size of the words for SAX.
    * @param stepsize The size of the words.
    * @return A configured [[GradientDescent]].
    */
  def setLearningRate(stepsize : Int) : eu.proteus.solma.SGD.GradientDescent = {
    this.parameters.add(eu.proteus.solma.SGD.GradientDescent.LearningRate, stepsize)
    this
  }



  /**
    * Print the internal parameters to the logger output.
    */
  def printInternalParameters() : Unit = {
    Log.info("LearningRate: " + this.parameters.get(eu.proteus.solma.SGD.GradientDescent.LearningRate))

  }

  /**
    * Apply helper.
    * @return A new [[GradientDescent]].
    */
  def apply(): eu.proteus.solma.SGD.GradientDescent = {
    new eu.proteus.solma.SGD.GradientDescent()
  }



/** Base class which performs Stochastic Gradient Descent optimization using mini batches.
  *
  * For each labeled vector in a mini batch the gradient is computed and added to a partial
  * gradient. The partial gradients are then summed and divided by the size of the batches. The
  * average gradient is then used to updated the weight values, including regularization.
  *
  * At the moment, the whole partition is used for SGD, making it effectively a batch gradient
  * descent. Once a sampling operator has been introduced, the algorithm can be optimized
  *
  *  The parameters to tune the algorithm are:
  *                      [[Solver.LossFunction]] for the loss function to be used,
  *                      [[Solver.RegularizationPenaltyValue]] for the regularization penalty.
  *                      [[Solver.RegularizationConstant]] for the regularization parameter,
  *                      [[IterativeSolver.Iterations]] for the maximum number of iteration,
  *                      [[IterativeSolver.LearningRate]] for the learning rate used.
  *                      [[IterativeSolver.ConvergenceThreshold]] when provided the algorithm will
  *                      stop the iterations if the relative change in the value of the objective
  *                      function between successive iterations is is smaller than this value.
  *                      [[IterativeSolver.LearningRateMethodValue]] determines functional form of
  *                      effective learning rate.
  */




  def optimizeWithConvergenceCriterion(
                                        dataPoints: DataSet[LabeledVector],
                                        initialWeightsDS: DataSet[WeightVector],
                                        numberOfIterations: Int,
                                        regularizationPenalty: RegularizationPenalty,
                                        regularizationConstant: Double,
                                        learningRate: Double,
                                        convergenceThreshold: Double,
                                        lossFunction: LossFunction,
                                        learningRateMethod: LearningRateMethodTrait)
  : DataSet[WeightVector] = {
    // We have to calculate for each weight vector the sum of squared residuals,
    // and then sum them and apply regularization
    val initialLossSumDS = calculateLoss(dataPoints, initialWeightsDS, lossFunction)

    // Combine weight vector with the current loss
    val initialWeightsWithLossSum = initialWeightsDS.mapWithBcVariable(initialLossSumDS){
      (weights, loss) => (weights, loss)
    }

    val resultWithLoss = initialWeightsWithLossSum.iterateWithTermination(numberOfIterations) {
      weightsWithPreviousLossSum =>

        // Extract weight vector and loss
        val previousWeightsDS = weightsWithPreviousLossSum.map{_._1}
        val previousLossSumDS = weightsWithPreviousLossSum.map{_._2}

        val currentWeightsDS = SGDStep(
          dataPoints,
          previousWeightsDS,
          lossFunction,
          regularizationPenalty,
          regularizationConstant,
          learningRate,
          learningRateMethod)

        val currentLossSumDS = calculateLoss(dataPoints, currentWeightsDS, lossFunction)

        // Check if the relative change in the loss is smaller than the
        // convergence threshold. If yes, then terminate i.e. return empty termination data set
        val termination = previousLossSumDS.filterWithBcVariable(currentLossSumDS){
          (previousLoss, currentLoss) => {
            if (previousLoss <= 0) {
              false
            } else {
              scala.math.abs((previousLoss - currentLoss)/previousLoss) >= convergenceThreshold
            }
          }
        }

        // Result for new iteration
        (currentWeightsDS.mapWithBcVariable(currentLossSumDS)((w, l) => (w, l)), termination)
    }
    // Return just the weights
    resultWithLoss.map{_._1}
  }

  def optimizeWithoutConvergenceCriterion(
                                           data: DataSet[LabeledVector],
                                           initialWeightsDS: DataSet[WeightVector],
                                           numberOfIterations: Int,
                                           regularizationPenalty: RegularizationPenalty,
                                           regularizationConstant: Double,
                                           learningRate: Double,
                                           lossFunction: LossFunction,
                                           optimizationMethod: LearningRateMethodTrait)
  : DataSet[WeightVector] = {
    initialWeightsDS.iterate(numberOfIterations) {
      weightVectorDS => {
        SGDStep(data,
          weightVectorDS,
          lossFunction,
          regularizationPenalty,
          regularizationConstant,
          learningRate,
          optimizationMethod)
      }
    }
  }

  /** Performs one iteration of Stochastic Gradient Descent using mini batches
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param currentWeights A Dataset with the current weights to be optimized as its only element
    * @param lossFunction The loss function to be used
    * @param regularizationPenalty The regularization penalty to be used
    * @param regularizationConstant The regularization parameter
    * @param learningRate The effective step size for this iteration
    * @param learningRateMethod The learning rate used
    *
    * @return A Dataset containing the weights after one stochastic gradient descent step
    */
  private def SGDStep(
                       data: DataSet[(LabeledVector)],
                       currentWeights: DataSet[WeightVector],
                       lossFunction: LossFunction,
                       regularizationPenalty: RegularizationPenalty,
                       regularizationConstant: Double,
                       learningRate: Double,
                       learningRateMethod: LearningRateMethodTrait)
  : DataSet[WeightVector] = {

    data.mapWithBcVariable(currentWeights){
      (data, weightVector) => (lossFunction.gradient(data, weightVector), 1)
    }.reduce{
      (left, right) =>
        val (leftGradVector, leftCount) = left
        val (rightGradVector, rightCount) = right

        // make the left gradient dense so that the following reduce operations (left fold) reuse
        // it. This strongly depends on the underlying implementation of the ReduceDriver which
        // always passes the new input element as the second parameter
        val result = leftGradVector.weights match {
          case d: DenseVector => d
          case s: SparseVector => s.toDenseVector
        }

        // Add the right gradient to the result
        BLAS.axpy(1.0, rightGradVector.weights, result)
        val gradients = WeightVector(
          result, leftGradVector.intercept + rightGradVector.intercept)

        (gradients , leftCount + rightCount)
    }.mapWithBcVariableIteration(currentWeights){
      (gradientCount, weightVector, iteration) => {
        val (WeightVector(weights, intercept), count) = gradientCount

        BLAS.scal(1.0/count, weights)

        val gradient = WeightVector(weights, intercept/count)
        val effectiveLearningRate = learningRateMethod.calculateLearningRate(
          learningRate,
          iteration,
          regularizationConstant)

        val newWeights = takeStep(
          weightVector.weights,
          gradient.weights,
          regularizationPenalty,
          regularizationConstant,
          effectiveLearningRate)

        WeightVector(
          newWeights,
          weightVector.intercept - effectiveLearningRate * gradient.intercept)
      }
    }
  }

  /** Calculates the new weights based on the gradient
    *
    * @param weightVector The weights to be updated
    * @param gradient The gradient according to which we will update the weights
    * @param regularizationPenalty The regularization penalty to apply
    * @param regularizationConstant The regularization parameter
    * @param learningRate The effective step size for this iteration
    * @return Updated weights
    */
  def takeStep(
                weightVector: Vector,
                gradient: Vector,
                regularizationPenalty: RegularizationPenalty,
                regularizationConstant: Double,
                learningRate: Double
              ): Vector = {
    regularizationPenalty.takeStep(weightVector, gradient, regularizationConstant, learningRate)
  }

  /** Calculates the regularized loss, from the data and given weights.
    *
    * @param data A Dataset of LabeledVector (label, features) pairs
    * @param weightDS A Dataset with the current weights to be optimized as its only element
    * @param lossFunction The loss function to be used
    * @return A Dataset with the regularized loss as its only element
    */
  private def calculateLoss(
                             data: DataSet[LabeledVector],
                             weightDS: DataSet[WeightVector],
                             lossFunction: LossFunction)
  : DataSet[Double] = {
    data.mapWithBcVariable(weightDS){
      (data, weightVector) => (lossFunction.loss(data, weightVector), 1)
    }.reduce{
      (left, right) => (left._1 + right._1, left._2 + right._2)
    }.map {
      lossCount => lossCount._1 / lossCount._2
    }
  }
}


