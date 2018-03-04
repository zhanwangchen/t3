package eu.proteus.solma.ALS

import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{WithParameters, _}
import org.apache.flink.ml.math
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/*
 *Ariane 11.08.2017
 *Formular : weights = (sum_n(x_n*y_n) - n*avg_x*avg*y) / (sum_n(x^2) - n*(avg_x)^2)
 * sources: Parameter Server: https://github.com/gaborhermann/flink-parameter-server
*/

class ALS_PS extends WithParameters with Serializable{
  // weights and the count of obs
  type P = (WeightVector, Int)
  type WorkerIn = (Int, Int, (WeightVector, Int)) // id, workerId, weigthvector
  type WorkerOut = (Boolean, Array[Int], (WeightVector, Int))
  type WOut = (LabeledVector, Int)

  /** fit with labeled data, predicts unlabeled data
    * handeles both labeld and unlabeled data
    * @param evalDataStream datastream of LabeledVectors
    * @return either the model or the predicted LabelVectors
    */
  def fitAndPredict(evalDataStream : DataStream[LabeledVector]) : DataStream[String] = {
    //add a "problem" id
    //TODO in general this could be done in a more efficient way, i.e. solving several problems in the same time
    val partitionedStream = evalDataStream.map(x=>(x,0))
    // send the stream to the parameter server, define communication and Worker and Server instances
    val outputDS =
      transform(partitionedStream,
        ALSWorkerLogic,
        ALSParameterServerLogic,
        (x: (Boolean, Array[Int], (WeightVector, Int))) => x match {
          case (true, Array(partitionId, id), emptyVector) => Math.abs(id.hashCode())
          case (false, Array(partitionId, id), update) => Math.abs(id.hashCode())
        },
        (x: (Int, Int, (WeightVector, Int))) => x._2,
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
            collectAnswerMsg((true, Array(partitionId, id), (WeightVector(new DenseVector(Array(0.0)),0.0),0)))
          }

          override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((false, Array(partitionId, id), deltaUpdate))
          }
        },
        new PSReceiver[(Boolean, Array[Int], (WeightVector, Int)), P] {
          override def onWorkerMsg(msg: (Boolean, Array[Int], (WeightVector, Int)), onPullRecv: (Int, Int) => Unit, onPushRecv: (Int, P) => Unit): Unit = {
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
                                    collectAnswerMsg: ((Int, Int, (WeightVector, Int))) => Unit): Unit = {
            collectAnswerMsg(id, workerPartitionIndex, value)
          }
        },
        this.getIterationWaitTime()
      )

    val y = outputDS.flatMap(new FlatMapFunction[Either[(LabeledVector,Int),String], String] {
      override def flatMap(t: Either[(LabeledVector, Int), String], collector: Collector[String]): Unit = {
        if(getLabeledVectorOutput() == true && t.isLeft) collector.collect(t.toString)
        if(getLabeledVectorOutput() == false && t.isRight) collector.collect(t.toString)
      }
    })

    y
  }

   //--------------GETTER AND SETTER ----------------------------------------------------------------
  /** Get the WorkerParallism value as Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(ALS_PS.WorkerParallism).get
  }
  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): ALS_PS = {
    this.parameters.add(ALS_PS.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(ALS_PS.PSParallelism).get
  }
  /**
    * Set the SeverParallism value as Int
    * @param psParallelism serverparallelism as Int
    * @return
    */
  def setPSParallelism(psParallelism : Int): ALS_PS = {
    parameters.add(ALS_PS.PSParallelism, psParallelism)
    this
  }
  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : ALS_PS = {
    parameters.add(ALS_PS.IterationWaitTime, iterationWaitTime)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(ALS_PS.IterationWaitTime).get
  }
  /**
    * set a new windowsize
    * @param windowsize
    */
  def setWindowSize(windowsize : Int) : ALS_PS = {
    this.parameters.add(ALS_PS.WindowSize, windowsize)
    ALSWorkerLogic.setWindowSize(this.getWindowSize())
    this
  }
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(ALS_PS.WindowSize).get
  }
  /**
    * Get the Default Label which is used for unlabeld data points as double
    * @return The defaultLabel
    */
  def getDefaultLabel(): Double  = {
    this.parameters.get(ALS_PS.DefaultLabel).get
  }
  /**Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double) : ALS_PS = {
    parameters.add(ALS_PS.DefaultLabel, label)
    ALSWorkerLogic.setLabel(label)
    this
  }
  /**
    * Set the required output format
    * @return either the Labeled datapoints (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): ALS_PS = {
    this.parameters.add(ALS_PS.LabeledVectorOutput, yes)
    this
  }
  /**
    * Get the current output format
    * @return
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(ALS_PS.LabeledVectorOutput).get
  }

  //---------------------------------WORKER--------------------------------
  val ALSWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut]{
    // Worker variables, have same default values as ALS
    private var window = 100
    private var label = -1.0
    private var doLabeling = false
    // free parameters
    private val dataFit = new mutable.Queue[(LabeledVector, Int)]()
    private val dataPredict = new mutable.Queue[(DenseVector, Int)]()
    private val dataPredictPull = new mutable.Queue[(DenseVector, Int)]()
    /**
      * Set Parameters
      */
    /**
      * Set the windowsize
      * @param windowsize
      * @return
      */
    def setWindowSize(windowsize : Int) : Int = {
      this.window = windowsize
      this.window
    }
    /**Set the Default Label
      * Variable to define the unlabeled observation that can be predicted later
      * @param label the value of the defaul label
      * @return the default label
      */
    def setLabel(label : Double) : Double = {
      this.label = label
      this.label
    }

    /**
      * set to true if prediction is required,else false
      * @param predictionOutput
      * @return
      */
    def setDoLabeling(predictionOutput : Boolean) : Boolean = {
      this.doLabeling = predictionOutput
      this.doLabeling
    }

    /**
      * Method handles data receives
      * @param data the datapoint arriving on the worker
      * @param ps parameter server client
      */
      override def onRecv(data: (LabeledVector, Int), ps: ParameterServerClient[P, WOut]): Unit = {
      // if for unlabeled data
      if(data._1.label == this.label && this.doLabeling){
        val dataDenseVector = data._1.vector match {
          case d: DenseVector => d
          case s: SparseVector => s.toDenseVector
        }
        this.dataPredict.enqueue((dataDenseVector, data._2))
          if(this.dataPredict.size >= this.window*0.5) {
            // already collected data is put into dataQPull-Queue so dataQ starts again with 0
            val dataPart = this.dataPredict.dequeueAll(x => x._2 == data._2)
            dataPart.map(x => this.dataPredictPull.enqueue(x))
            //sent pull request
            ps.pull(data._2)
          }
        // for labeled data
      } else{
        if(data._1.label != this.label)  this.dataFit.enqueue(data)
        if(this.dataFit.size >= this.window ) {
          // already collected data is put into dataQPull-Queue so dataQ starts again with 0
          val dataPart = this.dataFit.dequeueAll(x => x._2 == data._2 )
          val statistics = getStatistics(dataPart)
          val update = calcNewWeigths(statistics._1, statistics._2, statistics._3, statistics._4, statistics._5)
          ps.push(data._2,update)
        }
      }
    }

    /**
      * aggregates the values of a set of datapoints to calculate the LS weights
      * @param trainSet Set of labeled data points
      * @return tuple of aggregated values : (count, x           , xy     , x*x     , y)
      */
    def getStatistics(trainSet: mutable.Seq[(LabeledVector, Int)]) : (Int, math.Vector, math.Vector, DenseVector, Double) = {

      val statistics = trainSet.map { x =>

        val xyVector = x._1.vector.copy
        // x = a*x
        BLAS.scal(x._1.label, xyVector)
        val squaredx = x._1.vector.copy

        val squaredx2 = squaredx.map{ x =>
          (x._2 * x._2)
        }
        val squaredx3 = DenseVector(squaredx2.toArray)
        //count, x           , xy     , x^2     , y
        (1, x._1.vector.copy, xyVector, squaredx3, x._1.label)
      }.reduce { (x, y) =>
        val count = x._1 + y._1
        val Ysummed = x._5 + y._5
        // y =     a  * x +  y
        BLAS.axpy(1.0, x._2, y._2)
        BLAS.axpy(1.0, x._3, y._3)
        BLAS.axpy(1.0, x._4, y._4)
        (count, y._2, y._3, y._4, Ysummed)
      }
      statistics
    }

    /**
      * Method uses aggregations to calculate the new weights
      * @param count count of observations
      * @param xSummed sum of all x
      * @param xysummed sum of all x*y
      * @param xSquaredSum sum of all x*x
      * @param ySummed sum of all y
      * @return LS weightvector
      */

    def calcNewWeigths(count : Int, xSummed : math.Vector, xysummed : math.Vector, xSquaredSum: DenseVector, ySummed: Double): (WeightVector, Int) = {
      // Formular : weights = (sum_n(x_n*y_n) - n*avg_x*avg*y) / (sum_n(x^2) - n*(avg_x)^2)
      // statistics : count(1)r, sum_x (2)r          , sum_ xy(3)     , sum_x^2(4)r     , sum_y (5)
      // n*avg_x*avg*y = dummy
      val x_mean = xSummed.copy
      BLAS.scal(1.0/count, x_mean)

      val y_mean = ySummed/count
      val nxAVGyAVG = x_mean.copy
      // x = a*x
      BLAS.scal(y_mean*count, nxAVGyAVG)

      // sum_n(x_n*y_n) = partOne
      val partOne = xysummed.copy
      BLAS.axpy(-1.0, nxAVGyAVG, partOne)
      // (sum_n(x_n*y_n) - n*avg_x*avg*y) = partOne

      //(sum_n(x^2) - n*(avg_x)^2)
      // sum_n(x^2)
      val partTwo = xSquaredSum.copy
      //n*(avg_x)^2)
      val XmeanSquared = x_mean.copy
      val XmeanSquared2 = XmeanSquared.map{ x =>
        (x._2 * x._2)
      }
      val XmeanSquared3 = DenseVector(XmeanSquared2.toArray)
      BLAS.scal(count, XmeanSquared3)
      BLAS.axpy(-1.0, XmeanSquared3, partTwo)

      // part1 / part2
      val weights1 = partOne.copy
      val weights2 = weights1.map{ x =>
        if(x._2 != 0.0 && partTwo.apply(x._1) != 0.0 )
          (x._2/partTwo.apply(x._1))
        else (0.0)
      }

      val weights3 = DenseVector(weights2.toArray)

      val bias = y_mean - weights3.dot(x_mean)
      //weights of the current window
      val weights = WeightVector((weights3), bias)
      (weights, count)
    }

    /**
      * defines PullRecv from ParameterServer, labels the unseen datapoints
      * @param paramId problem id
      * @param paramValue current weights from PS
      * @param ps parameter server client
      */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
      // dequeue all with key == paramId  dequeueAll(x=> boolean)
      if(this.dataPredictPull.nonEmpty && (paramValue._2>0)) {
        val xPredict = this.dataPredictPull.dequeueAll(x => x._2 == paramId)
        val xs_Sized = xPredict.splitAt(this.window)
        xs_Sized._2.foreach(x => this.dataPredictPull.enqueue(x))
        if (xs_Sized._1.size > 0) {
          xs_Sized._1.foreach { x =>
            val WeightVector(weights, weight0) = paramValue._1
            val dotProduct = x._1.asBreeze.dot(weights.asBreeze)
            ps.output(LabeledVector((dotProduct + weight0), x._1), paramId)
          }
        }
      }
      else{
        val release = this.dataPredictPull.dequeueAll(x => x._2 == paramId)
        release.foreach(x => this.dataPredict.enqueue(x))
      }
    }//onPull
  }//Worker
  //-----------------------------------------------------SERVER-----------------------------------------
  val ALSParameterServerLogic = new ParameterServerLogic[P, String] {

    val params = new mutable.HashMap[Int, (WeightVector, Int)]()

    /**
      * On Pull Receive this method searches for the current weight of this problem id and send them
      * to the worker
      * @param id problem id
      * @param workerPartitionIndex the worker id to remember where the answer needs to be send to
      * @param ps parameter server
      */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      val param = this.params.getOrElseUpdate(id, (WeightVector(DenseVector(Array[Double](0.0)),0.0),0))
      ps.answerPull(id, param, workerPartitionIndex)
    }

    /**
      * this method merges the the current weights with the updated weights with regards to the number of
      * observation forming each weight
      * @param id problem id
      * @param deltaUpdate updated weights from a worker
      * @param ps parameter server
      */
    override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {
      if(deltaUpdate._2 >= 0){
        val param = this.params.getOrElseUpdate(id, (WeightVector(DenseVector(Array[Double](0.0)),0.0),0))
        if(param._2 > 0){
          val count = param._2+deltaUpdate._2
          //scal down the current weights
          //x = a*x
          BLAS.scal(param._2.toDouble/count.toDouble, param._1.weights)
          // y = a*x + y
          BLAS.axpy(deltaUpdate._2.toDouble/count.toDouble, deltaUpdate._1.weights, param._1.weights)
          val bias = (param._2*param._1.intercept/count) + (deltaUpdate._2*deltaUpdate._1.intercept/count)

          val UpdatedWeigths = WeightVector(param._1.weights, bias)
          this.params.update(id, (UpdatedWeigths, count))
          ps.output(this.params(id).toString)
        }//if
        else{
          this.params.update(id, deltaUpdate)
          ps.output(this.params(id).toString)
        }
      }
    }//onPushRecv
  }//PS

  // equals constructor
  def apply() : ALS_PS = new ALS_PS()
} // end class


// Single instance
object ALS_PS {

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
      * Default Label for unseen data
      */
    private val LabelDefault: Double = -1.0

    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(DefaultLabel.LabelDefault)
  }

  case object LabeledVectorOutput extends Parameter[Boolean] {
    /**
      * Default output are the Weights
      */
    private val DefaultLabeledVectorOutput: Boolean = false
    /**
      * Default value.
      */
    override val defaultValue: Option[Boolean] = Some(LabeledVectorOutput.DefaultLabeledVectorOutput)
  }

}