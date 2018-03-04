package eu.proteus.solma


import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{WithParameters, _}
import org.apache.flink.ml.math.BLAS
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by Ariane on 02.07.2017.
  * sources:
  * Parameter Server: https://github.com/gaborhermann/flink-parameter-server
  */
class SummaryFunctionWithPS extends WithParameters with Serializable{
  // weights and the count of obs
  type P = Array[Double]
  type WorkerIn = (Int, Int, Array[Double]) // id, workerId, weigthvector
  type WorkerOut = (Boolean, Array[Int], Array[Double])
  type WOut = (List[(Array[Double])])
  var paramsGlobal = new mutable.HashMap[Int, Array[Double]]()

  // optimize function called on this class
  def summarize(evalDataStream : DataStream[LabeledVector]) : DataStream[String] = {
    // add key to data
    val partitionedStream = evalDataStream.map(x=>(x,0))

    val outputDS =
      transform(partitionedStream,
        SumWorkerLogic,
        SumParameterServerLogic,
        (x: (Boolean, Array[Int], Array[Double])) => x match {
          case (true, Array(partitionId, id), emptyVector) => Math.abs(id.hashCode())
          case (false, Array(partitionId, id), update) => Math.abs(id.hashCode())
        },
        (x: (Int, Int, Array[Double])) => x._2,
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
            collectAnswerMsg((true, Array(partitionId, id), new Array[Double](1)))
          }

          override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((false, Array(partitionId, id), deltaUpdate))
          }
        },
        new PSReceiver[(Boolean, Array[Int], Array[Double]), P] {
          override def onWorkerMsg(msg: (Boolean, Array[Int], Array[Double]), onPullRecv: (Int, Int) => Unit, onPushRecv: (Int, P) => Unit): Unit = {
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
                                    collectAnswerMsg: ((Int, Int, Array[Double])) => Unit): Unit = {
            collectAnswerMsg(id, workerPartitionIndex, value)
          }
        },
        this.getIterationWaitTime()
      )

    val y = outputDS.flatMap(new FlatMapFunction[Either[List[Array[Double]],String], String] {

      override def flatMap(t: Either[List[(Array[Double])], String], collector: Collector[String]): Unit = {

        if(t.isRight){
           collector.collect(t.toString)
        }}
    })

    y

  }

  /** --------------GETTER AND SETTER ----------------------------------------------------------------
    * Get the WorkerParallism value  with Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(SummaryFunctionWithPS.WorkerParallism).get
  }
  /**
    * Set the WorkerParallelism value as Int
    */
  def setWorkerParallelism(workerParallism: Int): SummaryFunctionWithPS = {
    this.parameters.add(SummaryFunctionWithPS.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(SummaryFunctionWithPS.PSParallelism).get
  }
  /**
    * Set the SeverParallism value as Int
    */
  def setPSParallelism(psParallelism: Int): SummaryFunctionWithPS = {
    parameters.add(SummaryFunctionWithPS.PSParallelism, psParallelism)
    this
  }
  /**
    * Set the RegularizationConstant as double
    */


  /** Set the IterationWaitTime
    * Variable to stop the calculation in stream environment
    */
  def setIterationWaitTime(iterationWaitTime: Long) : SummaryFunctionWithPS = {
    parameters.add(SummaryFunctionWithPS.IterationWaitTime, iterationWaitTime)
    this
  }
  /**
    * Get the IterationWaitTime
    * @return The IterationWaitTime
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(SummaryFunctionWithPS.IterationWaitTime).get
  }
  /**
    * Set the Windowsize
    */
  def setWindowSize(windowsize : Int) : SummaryFunctionWithPS = {
    this.parameters.add(SummaryFunctionWithPS.WindowSize, windowsize)
    SumWorkerLogic.setWindowSize(this.getWindowSize())
    this
  }
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(SummaryFunctionWithPS.WindowSize).get
  }

  def getStatistics(id : Int, x : String) : Double  = {
    x match {
      case "Mean" => this.paramsGlobal.getOrElse(id, new Array[Double](5)).apply(1)
      case "Max"  => this.paramsGlobal.getOrElse(id, new Array[Double](5)).apply(4)
      case "Min"  => this.paramsGlobal.getOrElse(id, new Array[Double](5)).apply(3)
      case "Var"  => this.paramsGlobal.getOrElse(id, new Array[Double](5)).apply(2)
      case "Count" => this.paramsGlobal.getOrElse(id, new Array[Double](5)).apply(0)

    }
  }



  //---------------------------------WORKER--------------------------------

  val SumWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut]{
    // Worker variables, have same default values as SGD
    private var window = 100
    // free parameters
    private val dataQ = new mutable.Queue[(LabeledVector, Int)]()
    private val dataQPull = new mutable.Queue[(LabeledVector, Int)]()
    private val statistics = new mutable.HashMap[Int, (Array[Double])]()
    /**
      * Set Parameters
      */
    def setWindowSize(windowsize : Int) : Int = {
      this.window = windowsize
      this.window
    }

    override def onRecv(data: (LabeledVector, Int), ps: ParameterServerClient[P, WOut]): Unit = {

      dataQ.enqueue(data)
      if (dataQ.size >= window) {
        // already collected data is put into dataQPull-Queue so dataQ starts again with 0

        val dataPart = dataQ.dequeueAll(x => x._2 == data._2)
        getStatisticBasics(dataPart)
        statistics.foreach { x =>
          ps.push(x._1, x._2)
        }

      }

    }
    //Vector[Double], Vector[Double],
    def getStatisticBasics(trainSet: mutable.Seq[(LabeledVector, Int)])  = {
      var count = 0
      //val variables : Int = trainSet.head._1.vector.size+1
      //val statistics = new mutable.HashMap[Int, (Array[Double])]()

      val vector = trainSet.clone().map { x =>
        count = count + 1
        //attributes to vector
        (x._1.vector, x._1.vector.copy)
      }

      val vector1 = vector.map(y => y._2)
      val vector2 = vector.map(y => y._1)

      val attArray = vector1.reduce{ (x,y)=>
        // sum up all vectors = 1 left
        BLAS.axpy(1.0, x, y)
        y
      }

      // foreach attribute (0,1,...n)
      attArray.toArray.foreach{ x =>
        val value = statistics.getOrElseUpdate(x._1, new Array[Double](5))

        // sum divided by count
        val mean = x._2/count.toDouble
        value.update(0,count)
        value.update(1,mean)
        value.update(3,mean)
        value.update(4,mean)
        statistics.update(x._1, value)
      }



      vector2.foreach{ b =>
        b.toArray.foreach{ y =>

          val min = statistics.getOrElseUpdate(y._1, new Array[Double](5)).apply(3)
          val max = statistics.getOrElseUpdate(y._1, new Array[Double](5)).apply(4)
          var variance = statistics.getOrElseUpdate(y._1, new Array[Double](5)).apply(2)
          val mean = statistics.getOrElseUpdate(y._1, new Array[Double](5)).apply(1)


          variance = variance + ((y._2- mean)*(y._2- mean))
          statistics.apply(y._1).update(2, variance)
          if(y._2.toDouble < min) statistics.apply(y._1).update(3, y._2.toDouble)
          println("max" + y._2.toDouble +","+ max + "," + y)
          if(y._2.toDouble > max)  statistics.apply(y._1).update(4, y._2.toDouble)
        }
      }

      statistics.foreach { attribute =>
        val variance = attribute._2.apply(2) / attribute._2.apply(0)
        attribute._2.update(2, variance)
      }

    }

    // defines PullRecv from ParameterServer
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {

      // do nothing
    }//onPull
  }//Worker
  //-----------------------------------------------------SERVER-----------------------------------------
  val SumParameterServerLogic = new ParameterServerLogic[P, String] {

    private val params = new mutable.HashMap[Int, Array[Double]]()
    var counter = 0

    def getParams( i : Int) : Array[Double] = {
      val output = params.get(i)
      output match {
        case None => new Array[Double](5)
        case Some(output) => output
      }
    }

    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      val param = params.getOrElseUpdate(id, new Array[Double](5))
      ps.answerPull(id, param, workerPartitionIndex)
    }

    override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {

      val currentValue = params.get(id)
      currentValue match {
        case None => params.update(id, deltaUpdate)

        case Some( currentValue) =>
          val addCount = deltaUpdate.apply(0)
          //counter = counter +1
          var oldCount = 0.0
          try {
            oldCount = currentValue.apply(0)
          }

          for (i <- 0 to currentValue.length - 1) {
            var update = 0.0
            if (i == 0) {
              update = addCount + oldCount
            }
            if (i == 1 && currentValue.apply(1) != deltaUpdate.apply(1)) {
              update = currentValue.apply(i) * oldCount / (oldCount + addCount) + deltaUpdate.apply(i) * addCount / (oldCount + addCount)
            }
            if (i == 2) update = currentValue.apply(2) * oldCount / (oldCount + addCount) + deltaUpdate.apply(2) * addCount / (oldCount + addCount)

            if (i == 3 && currentValue.apply(3) > deltaUpdate.apply(3)) {
              update = deltaUpdate.apply(3)
            }

            if (i == 4 && currentValue.apply(4) < deltaUpdate.apply(4)) {

              update = deltaUpdate.apply(4)
            }
            if (update != 0.0) currentValue.update(i, update)
          }
          params.update(id, currentValue)

          //if(counter == 10) {
          var y = " "
          params.foreach{x =>
            y = y +  "Sum for " + x._1 + ":"
            paramsGlobal.update(x._1, x._2)
            for (j <- 0 to x._2.length - 1) {
              y = y + x._2.apply(j) + ","

              if(j == x._2.length - 1 ) y = y + "\n"
            }}
          //counter = 0
          ps.output(y)
        // }
      }

    }//onPushRecv
  }//PS

  // equals constructor
  def apply() : SummaryFunctionWithPS = new SummaryFunctionWithPS()
} // end class


// Single instance
object SummaryFunctionWithPS{

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

}