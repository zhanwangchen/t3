package eu.proteus.solma.SGD
/**
  * Created by Ariane on 02.07.2017.
  * sources:
  * Parameter Server: https://github.com/gaborhermann/flink-parameter-server
  * Source code mainly taken over from:
  * GD - Batch: https://github.com/apache/flink/blob/master/flink-libraries/flink-ml/src/main/scala/org/apache/flink/ml/optimization/GradientDescent.scala
  */
import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{WithParameters, _}
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}
import org.apache.flink.ml.optimization.LearningRateMethod.LearningRateMethodTrait
import org.apache.flink.ml.optimization.{RegularizationPenalty, _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable
import org.apache.flink.ml.math.Breeze._

class GradientDescentStreamPS extends WithParameters with Serializable{
  // data types for parameter server communication
  type P = WeightVector
  //      id, workerId, weigthvector
  type WorkerIn = (Int, Int, WeightVector)
  type WorkerOut = (Boolean, Array[Int], WeightVector)
  type WOut = (LabeledVector, Int)

  /**
    *
    * @param evalDataStream the evaluation stream of LabeledVectors
    * @param initalWeights the inital weights, can be a zero vector if no training happened before
    * @return
    */
  def optimize(evalDataStream : DataStream[LabeledVector], initalWeights: WeightVector) : DataStream[String] = {
    //add a "problem" id
    //TODO in general this could be done in a more efficient way, i.e. solving several problems in the same time
    val partitionedStream = evalDataStream.map(x=>(x,0))
    //prepare PS
    SGDParameterServerLogic.params.put(0,initalWeights)

    // send the stream to the parameter server, define communication and Worker and Server instances
    val outputDS =
      transform(partitionedStream,
        SGDWorkerLogic,
        SGDParameterServerLogic,
        (x: (Boolean, Array[Int], WeightVector)) => x match {
          case (true, Array(partitionId, id), emptyVector) => Math.abs(id.hashCode())
          case (false, Array(partitionId, id), update) => Math.abs(id.hashCode())
        },
        (x: (Int, Int, WeightVector)) => x._2,
        this.getWorkerParallelism(),
        this.getPSParallelism(),
        new WorkerReceiver[WorkerIn, P] {
          override def onPullAnswerRecv(msg: WorkerIn, pullHandler: PullAnswer[P] => Unit): Unit = {
            pullHandler(PullAnswer(msg._1, msg._3))
          }
        },
        new WorkerSender[WorkerOut, P] {
          // output of Worker
          override def onPull(id: Int, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((true, Array(partitionId, id), WeightVector(new DenseVector(Array(0.0)),0.0)))
          }

          override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((false, Array(partitionId, id), deltaUpdate))
          }
        },
        new PSReceiver[(Boolean, Array[Int], WeightVector), P] {
          override def onWorkerMsg(msg: (Boolean, Array[Int], WeightVector), onPullRecv: (Int, Int) => Unit, onPushRecv: (Int, P) => Unit): Unit = {
            msg match {
              case (true, Array(partitionId, id), update) =>
                onPullRecv(id, partitionId)
              case (false, Array(partitionId, id), update) =>
                onPushRecv(id, update)
            }
          }
        },
        new PSSender[WorkerIn, P] {
          override def onPullAnswer(id: Int,
                                    value: P,
                                    workerPartitionIndex: Int,
                                    collectAnswerMsg: ((Int, Int, WeightVector)) => Unit): Unit = {
            collectAnswerMsg((id, workerPartitionIndex, value))
          }
        },
        this.getIterationWaitTime()
      )

    val y = outputDS.flatMap(new FlatMapFunction[Either[(LabeledVector,Int),String], String] {
      override def flatMap(t: Either[(LabeledVector, Int), String], collector: Collector[String]): Unit = {
        if(t.isRight && !getLabeledVectorOutput()){
          collector.collect(t.toString)
        }
        if(t.isLeft && getLabeledVectorOutput()) collector.collect(t.toString)
      }
    })
    y
  }
  /** --------------GETTER AND SETTER ----------------------------------------------------------------
    * Get the WorkerParallism value  with Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(GradientDescentStreamPS.WorkerParallism).get
  }
  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism as Int value, default = 4
    * @return current GradientDescentStreamPS
    */
  def setWorkerParallelism(workerParallism: Int): GradientDescentStreamPS = {
    this.parameters.add(GradientDescentStreamPS.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(GradientDescentStreamPS.PSParallelism).get
  }
   /**
    * Set the SeverParallism value as Int
    * @param psParallelism number of parallel task of parameter server
    * @return
    */
  def setPSParallelism(psParallelism: Int): GradientDescentStreamPS = {
    parameters.add(GradientDescentStreamPS.PSParallelism, psParallelism)
    this
  }
  /**
    * Set the RegularizationConstant as double
    * @param regularizationConstant contant value
    * @return current GradientDescentStreamPS
    */
  def setRegularizationConstant(regularizationConstant: Double): GradientDescentStreamPS = {
    parameters.add(GradientDescentStreamPS.RegularizationConstant, regularizationConstant)
    SGDParameterServerLogic.setRegularizationConstant(regularizationConstant)
    SGDWorkerLogic.setRegularizationConstant(regularizationConstant)
    this
  }
  /**
    * Get the RegularizationConstant as double
    * @return The RegularizationConstant
    */
  def getRegularizationConstant(): Double  = {
    this.parameters.get(GradientDescentStreamPS.RegularizationConstant).get
  }
   /**Variable to stop the calculation in stream environment
    * set iterationwaitingtime
    * @param iterationWaitTime
    */
  def setIterationWaitTime(iterationWaitTime: Long) : GradientDescentStreamPS = {
    parameters.add(GradientDescentStreamPS.IterationWaitTime, iterationWaitTime)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(GradientDescentStreamPS.IterationWaitTime).get
  }
   /**
    * Set the RegularizationPenalty
    * @param regularizationPenalty
    * @return current GradientDescentStreamPS
    */
  def setRegularizationPenalty(regularizationPenalty: RegularizationPenalty) : GradientDescentStreamPS = {
    parameters.add(GradientDescentStreamPS.RegularizationPenaltyValue, regularizationPenalty)
    SGDParameterServerLogic.setRegularizationPenalty(regularizationPenalty)
    SGDWorkerLogic.setRegularizationPenalty(regularizationPenalty)
    this
  }
  /**
    * Get the RegularizationPenalty
    * @return The RegularizationPenalty
    */
  def getRegularizationPenalty() : RegularizationPenalty  = {
    this.parameters.get(GradientDescentStreamPS.RegularizationPenaltyValue).get
  }
   /**
    * Set the learningrateMethod
    * @param learningRateMethod
    * @return
    */
  def setLearningRateMethod(learningRateMethod: LearningRateMethodTrait ) : GradientDescentStreamPS = {
    this.parameters.add(GradientDescentStreamPS.LearningRateMethodValue, learningRateMethod)
    this
  }
  /**
    * Get the learningrate Method (trait used)
    * @return The learningrate Method
    */
  def getLearningRateMethod() : LearningRateMethodTrait  = {
    this.parameters.get(GradientDescentStreamPS.LearningRateMethodValue).get
  }
   /**
    * Set lossfunction for GradientDescentStreamPS
    * @param lossFunction
    * @return current GradientDescentStreamPS
    */
  def setLossFunction(lossFunction: GenericLossFunction): GradientDescentStreamPS = {
    this.parameters.add(GradientDescentStreamPS.LossFunction, lossFunction)
    SGDWorkerLogic.setLossFunction(lossFunction)
    this
  }
  /**
    * Get the lossfunction
    * @return The lossfunction
    */
  def getLossFunction(): GenericLossFunction = {
    this.parameters.get(GradientDescentStreamPS.LossFunction).get
  }
  /**
    * set a new windowsize
    * @param windowsize
    */
  def setWindowSize(windowsize : Int) : GradientDescentStreamPS = {
    this.parameters.add(GradientDescentStreamPS.WindowSize, windowsize)
    SGDWorkerLogic.setWindowSize(windowsize)
    this
  }
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(GradientDescentStreamPS.WindowSize).get
  }
   /**
    * SEt the learning rate
    * @param rate learning rate as double value
    * @return current GradientDescentStreamPS
    */
  def setLearningRate(rate : Double) : GradientDescentStreamPS = {
    this.parameters.add(GradientDescentStreamPS.LearningRate, rate)
    SGDParameterServerLogic.setLearningRate(rate)
    SGDWorkerLogic.setLearningRate(rate)
    this
  }
  /**
    * Get the learningrate as double, start with small values
    * @return The learningrate
    */
  def getLearningRate() : Double = {
    this.parameters.get(GradientDescentStreamPS.LearningRate).get
  }
  /**
    * Set the required output format
    * @return either the Labeled datapoints (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): GradientDescentStreamPS = {
    this.parameters.add(GradientDescentStreamPS.LabeledVectorOutput, yes)
    SGDWorkerLogic.setLabelOut(yes)
    SGDParameterServerLogic.setLabeloutput(yes)
    this
  }
  /**
    * Get the current output format
    * @return
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(GradientDescentStreamPS.LabeledVectorOutput).get
  }
  /**
    * Get the Default Label which is used for unlabeld data points as double
    * @return The defaultLabel
    */
  def getDefaultLabel(): Double  = {
    this.parameters.get(GradientDescentStreamPS.DefaultLabel).get
  }
  /**Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double) : GradientDescentStreamPS = {
    parameters.add(GradientDescentStreamPS.DefaultLabel, label)
    SGDWorkerLogic.setLabel(label)
    this
  }

//---------------------------------WORKER--------------------------------

  val SGDWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut]{
    // Worker variables, have same default values as SGD
    private var regularizationPenalty: RegularizationPenalty = L1Regularization
    private var lossFunction =  GenericLossFunction(SquaredLoss, LinearPrediction)
    private var regularizationConstant = 0.01
    private var effectiveLearningRate = 0.00001
    private var window = 100
    private var defaultLabel = -1.0
    private var labelOut = false
    // free parameters
    private val dataQ = new mutable.Queue[(LabeledVector, Int)]()
    private val dataQPull = new mutable.Queue[(LabeledVector, Int)]()
    private val dataPredict = new mutable.Queue[(LabeledVector, Int)]()


    /**
      * Set Windowsize
      * @param windowsize
      * @return
      */
    def setWindowSize(windowsize : Int) : Int = {
      this.window = windowsize
      this.window
    }
    /**
      * Set lossfunction
      * @param lossFunction
      * @return
      */
    def setLossFunction(lossFunction: GenericLossFunction ): GenericLossFunction = {
      this.lossFunction = lossFunction
      this.lossFunction
    }
    /**
      * Set RegularizationPenalty
      * @param regularizationPenalty
      * @return
      */
    def setRegularizationPenalty(regularizationPenalty: RegularizationPenalty) : RegularizationPenalty = {
      this.regularizationPenalty = regularizationPenalty
      this.regularizationPenalty
    }
    /**
      * Set RegularizationConstant
      * @param regularizationConstant
      * @return
      */
    def setRegularizationConstant(regularizationConstant: Double): Double = {
      this.regularizationConstant = regularizationConstant
      this.regularizationConstant
    }
    /**
      * Set Learningrate
      * @param rate
      * @return
      */
    def setLearningRate(rate : Double) : Double = {
      this.effectiveLearningRate = rate
      this.effectiveLearningRate
    }
    /**
      * Set Learningrate
      * @param label
      * @return
      */
    def setLabel(label : Double) : Double = {
      this.defaultLabel = label
      this.defaultLabel
    }
    /**
      * Set the output format
      * @param yes
      * @return
      */
    def setLabelOut(yes : Boolean) : Boolean = {
      this.labelOut = yes
      this.labelOut
    }

    /**
      * this method handels arriving data
      * @param data arriving data point
      * @param ps parameter Server client
      */
    override def onRecv(data: (LabeledVector, Int), ps: ParameterServerClient[P, WOut]): Unit = {
      // data in queue until parameter is pulled
      if(data._1.label != -1.0) this.dataQ.enqueue(data)
      if(data._1.label == -1.0 && labelOut) this.dataPredict.enqueue(data)
      if(this.dataQ.size >= this.window ) {
       // already collected data is put into dataQPull-Queue so dataQ starts again with 0
       val dataPart = dataQ.dequeueAll(x => x._2 == data._2 )
        dataPart.map(x => dataQPull.enqueue(x))
        //sent pull request
        ps.pull(data._2)
      }
    }

    /**
      * Here the unlabeld data get labeled with the latest weights
      * @param UpdatedWeigths latest weigths
      * @param paramId problem id
      * @param ps parameterServer Client
      */
    def labelData(UpdatedWeigths: P,paramId : Int,ps: ParameterServerClient[P, WOut] ) = {
      if(this.dataPredict.nonEmpty) {
        val xPredict = this.dataPredict.dequeueAll(x => x._2 == paramId)
        xPredict.foreach(x => this.dataPredict.enqueue(x))
        if (xPredict.size > 0) {
          xPredict.foreach { x =>
            val WeightVector(weights, weight0) = UpdatedWeigths
            val dotProduct = x._1.vector.asBreeze.dot(weights.asBreeze)
            ps.output(LabeledVector((dotProduct + weight0), x._1.vector), paramId)
          }
        }
      }
      else{
        val release = this.dataPredict.dequeueAll(x => x._2 == paramId)
        release.foreach(x => this.dataPredict.enqueue(x))
      }
    }

    /**
      *
      * @param input the datapoints to train
      * @param currentWeigths the current weights from the parameter server
      * @return
      */
    def calcPartSol(input : mutable.Seq[(LabeledVector, Int)], currentWeigths: P, ps: ParameterServerClient[P, WOut]) : WeightVector = {

      val gradient = input.map {
        x => (this.lossFunction.lossGradient(x._1, currentWeigths), 1)
      }.reduce {
        (left, right) =>
          val ((lossleft, leftGradVector), leftCount) = left
          val ((lossright, rightGradVector), rightCount) = right

          // make the left gradient dense so that the following reduce operations (left fold) reuse it
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
      } // end reduce gradient(loss,gradient(WV), count (Int))

      // scal the gradient among  the observation count
      BLAS.scal(1.0 / gradient._2, gradient._1._2.weights)
      // regularizationPenalty.takeStep(weightVector, gradient.weights, regularizationConstant, learningRate)
      val newWeights = this.regularizationPenalty.takeStep(currentWeigths.weights, gradient._1._2.weights, this.regularizationConstant, effectiveLearningRate)
      val newIntercept = currentWeigths.intercept - this.effectiveLearningRate * (gradient._1._2.intercept/gradient._2)
      val UpdatedWeigths = WeightVector(newWeights, newIntercept)

      val gradientNew = input.map {
        x => (this.lossFunction.loss(x._1, UpdatedWeigths))}.sum

      var gradientFinal = WeightVector(DenseVector(Array[Double](0.0)),0.0)
      //update only if we could improve previous weights
      if(gradient._1._1 > gradientNew) {
        gradientFinal = WeightVector(gradient._1._2.weights, gradient._1._2.intercept/gradient._2)
        if(labelOut) labelData(UpdatedWeigths, input.head._2, ps)
      }
      gradientFinal
    }

    /**
      * defines the process if PullRecv from ParameterServer arrives
      * @param paramId problem id
      * @param paramValue weights from parameter server
      * @param ps parameter server client
      */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
      // dequeue all with key == paramId  dequeueAll(x=> boolean)
      val xs = this.dataQPull.dequeueAll(x => x._2 == paramId)
      val xs_Sized = xs.splitAt(this.window)
      xs_Sized._2.foreach(x => this.dataQPull.enqueue(x))

      if (xs_Sized._1.size > 0) {
        if(paramValue.weights.size < xs_Sized._1.last._1.vector.size) {
          val emptyWeights = Array.fill[Double](xs_Sized._1.last._1.vector.size)(0.0)
          val upgrade = calcPartSol(xs_Sized._1, WeightVector(DenseVector(emptyWeights), 0.0), ps: ParameterServerClient[P, WOut])
          ps.push(paramId,upgrade)
        }
        else{
        val upgrade = calcPartSol(xs_Sized._1, paramValue, ps: ParameterServerClient[P, WOut])
        ps.push(paramId, upgrade)
        }//else
      }//if
    }//onPull
  }//Worker
//--------------------------------------SERVER------------------------------------------------------------------
  val SGDParameterServerLogic = new ParameterServerLogic[P, String] {
    // Server variables, have same default values as SGD
    private var regularizationPenalty: RegularizationPenalty = L1Regularization
    private var regularizationConstant = 0.01
    private var effectiveLearningRate = 0.00001
    private var labelOut = false

    val params = new mutable.HashMap[Int, WeightVector]()

  /**
    * Set the regularizationPenalty
    * @param regularizationPenalty
    * @return
    */
    def setRegularizationPenalty(regularizationPenalty: RegularizationPenalty) : RegularizationPenalty = {
      this.regularizationPenalty = regularizationPenalty
      this.regularizationPenalty
    }
  /**
    * Set the regularization constant
    * @param regularizationConstant
    * @return
    */
    def setRegularizationConstant(regularizationConstant: Double): Double = {
      this.regularizationConstant = regularizationConstant
      this.regularizationConstant
    }
  /**
    * set the learning rate : double
    * @param rate
    * @return
    */
    def setLearningRate(rate : Double) : Double = {
      this.effectiveLearningRate = rate
      this.effectiveLearningRate
    }
  /**
    * Set the output format
    * @param yes
    * @return
    */
  def setLabeloutput(yes : Boolean) : Boolean = {
    this.labelOut = yes
    this.labelOut
  }

  /**
    * Searches for the current weights of the problem id and sends them to the worker with the partition index
    * @param id problem id
    * @param workerPartitionIndex index of worker for answer
    * @param ps parameter server client
    */
  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      val param = this.params.getOrElseUpdate(id, WeightVector(DenseVector(Array[Double](0.0)),0.0))
      ps.answerPull(id, param, workerPartitionIndex)
    }

  /**
    * calculates the update when receiving a push from a worker
    * @param id problem id
    * @param deltaUpdate gradient weights
    * @param ps parameter server
    */
    override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {
      //WeightVector(DenseVector(Array[Double](0.0)),0.0) == DEFAULT PUSH
     if(deltaUpdate != WeightVector(DenseVector(Array[Double](0.0)),0.0)){

      val param = this.params.getOrElseUpdate(id, WeightVector(DenseVector(Array[Double](0.0)),0.0))

       try {
         val newWeights = this.regularizationPenalty.takeStep(param.weights, deltaUpdate.weights, this.regularizationConstant, this.effectiveLearningRate)
         val newIntercept = param.intercept - this.effectiveLearningRate * deltaUpdate.intercept

         val UpdatedWeigths = WeightVector(newWeights, newIntercept)
         params.update(id, UpdatedWeigths)
         if(!labelOut) ps.output(params(id).toString)
       }
       catch{
         case foo : IllegalArgumentException => {
           params.update(id, deltaUpdate)
           if(!labelOut) ps.output(params(id).toString)
         }
       }
     }//if
   }//onPushRecv
  }//PS

  // equals constructor
  def apply() : GradientDescentStreamPS = new GradientDescentStreamPS()
} // end class


// Single instance
object GradientDescentStreamPS {

  case object LabeledVectorOutput extends Parameter[Boolean] {

    /**
      * Default outputformat
      */
    private val DefaultLabeledVectorOutput: Boolean = false

    /**
      * Default value.
      */
    override val defaultValue: Option[Boolean] = Some(LabeledVectorOutput.DefaultLabeledVectorOutput)
  }

  case object IterationWaitTime extends Parameter[Long] {

    /**
      * Default time - needs to be set in any case to stop this calculation
      */
    private val DefaultIterationWaitTime: Long = 4000

    /**
      * Default value.
      */
    override val defaultValue: Option[Long] = Some(IterationWaitTime.DefaultIterationWaitTime)
  }

  case object RegularizationPenaltyValue extends Parameter[RegularizationPenalty] {

    /**
      * Default RegularizationPenaltyValue
      */
    private val DefaultRegularizationPenalty: RegularizationPenalty = L1Regularization

    /**
      * Default value.
      */
    override val defaultValue: Option[RegularizationPenalty] = Some(RegularizationPenaltyValue.DefaultRegularizationPenalty)
  }

  case object RegularizationConstant extends Parameter[Double] {

    /**
      * Default RegularizationConstant
      */
    private val DefaultRegularizationConstant: Double = 0.01

    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(RegularizationConstant.DefaultRegularizationConstant)
  }

  case object LearningRate extends Parameter[Double] {

    /**
      * Default stepsize constant.
      */
    private val DefaultLearningRate: Double = 0.00001

    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(LearningRate.DefaultLearningRate)
  }

  case object WindowSize extends Parameter[Int] {
    /**
      * Default windowsize constant : taking each and every data point into account
      */
    private val DefaultWindowSize: Int = 100
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(WindowSize.DefaultWindowSize)
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

  case object WorkerParallism extends Parameter[Int] {

    /**
      * Default parallelism
      */
    private val DefaultWorkerParallism: Int = 4

    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(WorkerParallism.DefaultWorkerParallism)
  }

  case object PSParallelism extends Parameter[Int] {
    /**
      * Default server parallelism.
      */
    private val DefaultPSParallelism: Int = 4

    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(PSParallelism.DefaultPSParallelism)
  }

  case object DefaultLabel extends Parameter[Double] {
    /**
      * Default label
      */
    private val DefaultLab: Double = -1.0
    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(DefaultLabel.DefaultLab)
  }

}


