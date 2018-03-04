package eu.proteus.solma.SGD

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{LabeledVector, Parameter, WeightVector, WithParameters}
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}
import org.apache.flink.ml.optimization.LearningRateMethod.LearningRateMethodTrait
import org.apache.flink.ml.optimization.{RegularizationPenalty, _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/*
  * Created by Ariane on 08.06.2017.
  */

class GradientDescentStream extends WithParameters with Serializable{
  // optimize funcction called on this class
  def optimize(evalDataStream : DataStream[LabeledVector], initalWeights: WeightVector) : DataStream[String] = {
  // create a keyedStream
  val partitionedStream: KeyedStream[(LabeledVector, Int), Int] = evalDataStream.map(x=>(x,0)).keyBy(x => x._2)


  // this Windowfunction searches for the optimal weight in a partition of user set windowsize for the user set iterations
  val StepFunction = new WindowFunction[(LabeledVector, Int), (WeightVector, Int), Int, GlobalWindow] {

      override def apply(
                        key: Int,
                        window: GlobalWindow,
                        input: Iterable[(LabeledVector, Int)], out: Collector[(WeightVector, Int)]): Unit = {

        var currentWeigths = initalWeights
        var currentloss = input.map(x => getLossFunction().loss(x._1,currentWeigths)).sum
        var newLoss = 0.0
        var i = 0

       while(i <= getIterations() && currentloss>newLoss) {

         i = i+1
         if(i>1) {
           currentloss = newLoss
         }

          val gradientCount = input.map {
            x => (getLossFunction().lossGradient(x._1, currentWeigths), 1)
          }.reduce {
            (left, right) =>
              val ((lossleft, leftGradVector), leftCount) = left
              val ((lossright, rightGradVector), rightCount) = right

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

              val loss = lossleft + lossright

              ((loss, gradients), leftCount + rightCount)
          } // reduce gradientCount(gradient(WV), count (Int))

          // "weight the summed up weights and biases"
          BLAS.scal(1.0 / gradientCount._2, gradientCount._1._2.weights)
          //gradientCount._1._2.weights = result  of scal
          // gradient == Gradient of MiniBatch
          val gradient = WeightVector(gradientCount._1._2.weights, gradientCount._1._2.intercept / gradientCount._2)

          val effectiveLearningRate = 0.01
          //todo check this function, really small bias 0,44 vs. 0,0546 (???)
          /*getLearningRateMethod().calculateLearningRate(getStepSize(), getIterations(),getRegularizationConstant())
*/
          val newWeights = getRegularizationPenalty().takeStep(currentWeigths.weights, gradient.weights, getRegularizationConstant(), getStepSize())

          val newIntercept = currentWeigths.intercept - effectiveLearningRate * gradient.intercept


          val UpdatedWeigths = WeightVector(newWeights, newIntercept)

          newLoss = input.map(x => getLossFunction().loss(x._1,UpdatedWeigths)).sum

         if(currentloss>newLoss){
          currentWeigths = UpdatedWeigths
          }
        }// while

      out.collect(currentWeigths, key)


    } //StepFunction apply
  }//StepFunction

    val newWeigthsPartioned: DataStream[(WeightVector, Int)] = partitionedStream.countWindow(getWindowSize()).apply(StepFunction)

    //todo GlobalWindow, fold

    var weights : DataStream[WeightVector] = newWeigthsPartioned.keyBy(x=>x._2).countWindow(2).reduce{ (left, right) =>

      BLAS.scal(0.5,left._1.weights)
      val result = left._1.weights match {
        case d: DenseVector => d
        case s: SparseVector => s.toDenseVector
      }
      BLAS.axpy(0.5, right._1.weights, result)
    val gradients = WeightVector(
      result, (left._1.intercept + right._1.intercept)/2)
      (gradients,left._2)}.map(x => x._1)


    return weights.flatMap(new FlatMapFunction[WeightVector, String] {
      override def flatMap(t: WeightVector, collector: Collector[String]): Unit = {

          collector.collect(t.toString)
          collector.collect("ITERATIONSSTREAM")

      }
    })//.flatMap{_.isRight => _.toString}//(new FlatMapFunction[] {})
  }


  def setRegularizationConstant(regularizationConstant: Double): GradientDescentStream = {
    parameters.add(GradientDescentStream.RegularizationConstant, regularizationConstant)
    this
  }

  /**
    * Get the stepsize as double
    * @return The stepsize
    */
  def getRegularizationConstant() : Double  = {
    this.parameters.get(GradientDescentStream.RegularizationConstant).get
  }


  //def setRegularizationPenalty(L1Regularization: L1Regularization.type) = ???
  def setRegularizationPenalty(regularizationPenalty: RegularizationPenalty) : GradientDescentStream = {
    parameters.add(GradientDescentStream.RegularizationPenaltyValue, regularizationPenalty)
    this
  }

  /**
    * Get the stepsize as double
    * @return The stepsize
    */
  def getRegularizationPenalty() : RegularizationPenalty  = {
    this.parameters.get(GradientDescentStream.RegularizationPenaltyValue).get
  }



  def setLearningRateMethod(learningRateMethod: LearningRateMethodTrait ) : GradientDescentStream = {
    this.parameters.add(GradientDescentStream.LearningRateMethodValue, learningRateMethod)
    this
  }

  /**
    * Get the stepsize as double
    * @return The stepsize
    */
  def getLearningRateMethod() : LearningRateMethodTrait  = {
    this.parameters.get(GradientDescentStream.LearningRateMethodValue).get
  }

  def setConvergenceThreshold(threshold: Double) : GradientDescentStream = {
    this.parameters.add(GradientDescentStream.ConvergenceThreshold, threshold)
    this
  }

  /**
    * Get the stepsize as double
    * @return The stepsize
    */
  def getConvergenceThreshold() : Double = {
    this.parameters.get(GradientDescentStream.ConvergenceThreshold).get
  }


  /*
  * Getter and Setter for lossfunction
   */

  def setLossFunction(lossFunction: GenericLossFunction): GradientDescentStream = {
    this.parameters.add(GradientDescentStream.LossFunction, lossFunction)
    this
  }

  /**
    * Get the lossfunction
    * @return The lossfunction
    */
  def getLossFunction() : GenericLossFunction = {
    this.parameters.get(GradientDescentStream.LossFunction).get
  }

  /*
  * Getter and Setter for the stepsize
   */

  def setStepsize(stepsize : Double) : GradientDescentStream = {
    this.parameters.add(GradientDescentStream.StepSize, stepsize)
    this
  }

  /**
    * Get the stepsize as double
    * @return The stepsize
    */
  def getWindowSize() : Int = {
    this.parameters.get(GradientDescentStream.WindowSize).get
  }

  def setWindowSize(windowsize : Int) : GradientDescentStream = {
    this.parameters.add(GradientDescentStream.WindowSize, windowsize)
    this
  }

  /**
    * Get the stepsize as double
    * @return The stepsize
    */
  def getStepSize() : Double = {
    this.parameters.get(GradientDescentStream.StepSize).get
  }

  /*
  * Getter and Setter for the nummer of iterations incl a default value
   */

  def setIterations(iter : Int) : GradientDescentStream = {
    this.parameters.add(GradientDescentStream.Iteration, iter)
    this
  }

  /**
    * Get the numbers of iterations
    * @return number iterations
    */
  def getIterations() : Int = {
    this.parameters.get(GradientDescentStream.Iteration).get
  }

   // equals constructor
  def apply() : GradientDescentStream = new GradientDescentStream()
}


// Single instance
object GradientDescentStream {

  case object ConvergenceThreshold extends Parameter[Double] {

    /**
      * Default convergence threshold.
      */
    private val DefaultThreshold: Double = 0.0

    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(ConvergenceThreshold.DefaultThreshold)
  }

  case object RegularizationPenaltyValue extends Parameter[RegularizationPenalty] {

    /**
      * Default convergence threshold.
      */
    private val DefaultRegularizationPenalty: RegularizationPenalty = NoRegularization

    /**
      * Default value.
      */
    override val defaultValue: Option[RegularizationPenalty] = Some(RegularizationPenaltyValue.DefaultRegularizationPenalty)
  }


  case object RegularizationConstant extends Parameter[Double] {

    /**
      * Default stepsize constant.
      */
    private val DefaultRegularizationConstant: Double = 0.01

    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(RegularizationConstant.DefaultRegularizationConstant)
  }

  case object StepSize extends Parameter[Double] {

    /**
      * Default stepsize constant.
      */
    private val DefaultStepSize: Double = 0.1

    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(StepSize.DefaultStepSize)
  }

  case object WindowSize extends Parameter[Int] {

    /**
      * Default windowsize constant : taking each and every data point into account
      */
    private val DefaultWindowSize: Int = 1

    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(WindowSize.DefaultWindowSize)
  }

  case object Iteration extends Parameter[Int] {

    /**
      * Default iteration constant.
      */
    private val DefaultIteration: Int = 100

    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(Iteration.DefaultIteration)
  }

  case object LossFunction extends Parameter[GenericLossFunction] {

    /**
      * Default loss function
      */
    private val DefaultLossFunction: GenericLossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)

    /**
      * Default values
      */
    override val defaultValue: Option[GenericLossFunction] = Some(LossFunction.DefaultLossFunction)
  }

  case object LearningRateMethodValue extends Parameter[LearningRateMethodTrait] {

    /**
      * Default LearningRateMethod
      */
    private val DefaultLearningRateMethod: LearningRateMethodTrait = LearningRateMethod.Default

    /**
      * Default values
      */
    override val defaultValue: Option[LearningRateMethodTrait] = Some(LearningRateMethodValue.DefaultLearningRateMethod)
  }

}