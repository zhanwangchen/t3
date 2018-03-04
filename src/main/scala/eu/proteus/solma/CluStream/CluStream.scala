package eu.proteus.solma.CluStream

import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps.entities.PullAnswer
import hu.sztaki.ilab.ps.{PSReceiver, PSSender, WorkerReceiver, WorkerSender, _}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{LabeledVector, Parameter, WithParameters}
import org.apache.flink.ml.math.{BLAS, DenseVector}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable

/**
  * Created by Ariane on 10.07.2017.
  */
class CluStream extends WithParameters with Serializable{

  // data types for parameter server communication
  type P = Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]
                  // id, workerId, vector
  type WorkerIn = (Int, Int, Seq[(DenseVector, Int, DenseVector, Long, Double, Long)])
  type WorkerOut = (Boolean, Array[Int], Seq[(DenseVector, Int, DenseVector, Long, Double, Long)])
  type WOut = (LabeledVector, Int)

  /**
    * main method of this algorithm that handles the cluster job
    * @param evalDataStream DataStream with Densevector
    * @param initalClusterCenters a set of initial cluster centers
    * @return a DataStream of Strings either containing labeled data or the cluster centers
    */
  def cluster(evalDataStream: DataStream[DenseVector], initalClusterCenters: Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]): DataStream[String] = {

    //add a "problem" id
    //TODO in general this could be done in a more efficient way, i.e. solving several problems in the same time
    val partitionedStream = evalDataStream.map(x => (x, 0))
    //add the initial weigths to the parameter server and set the number of clusters
    CluParameterServerLogic.params.put(0, initalClusterCenters)
    CluParameterServerLogic.setNumberOfClusters(initalClusterCenters.size)

    // send the stream to the parameter server, define communication and Worker and Server instances
    val outputDS =
      transform(partitionedStream,
        CluWorkerLogic,
        CluParameterServerLogic,
        (x: (Boolean, Array[Int], Seq[(DenseVector, Int, DenseVector, Long, Double, Long)])) => x match {
          case (true, Array(partitionId, id), emptyVector) => Math.abs(id.hashCode())
          case (false, Array(partitionId, id), update) => Math.abs(id.hashCode())
        },
        (x: (Int, Int, Seq[(DenseVector, Int, DenseVector, Long, Double, Long)])) => x._2,
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
            collectAnswerMsg(true, Array(partitionId, id), Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]((DenseVector(Array.fill[Double](2)(0.0)),0,DenseVector(Array.fill[Double](1)(0.0)),0,0.0, System.currentTimeMillis())))
          }
          override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((false, Array(partitionId, id), deltaUpdate))
          }
        },
        new PSReceiver[(Boolean, Array[Int], Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]), P] {
          override def onWorkerMsg(msg: (Boolean, Array[Int], Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]), onPullRecv: (Int, Int) => Unit, onPushRecv: (Int, P) => Unit): Unit = {
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
                                    collectAnswerMsg: ((Int, Int, Seq[(DenseVector, Int, DenseVector, Long, Double, Long)])) => Unit): Unit = {
            collectAnswerMsg((id, workerPartitionIndex, value))
          }
        },
        this.getIterationWaitTime()
      )
    // prepare output
    val y = outputDS.flatMap(new FlatMapFunction[Either[(LabeledVector, Int), String], String] {
      override def flatMap(t: Either[(LabeledVector, Int), String], collector: Collector[String]): Unit = {
        if(getLabeledVectorOutput() == true && t.isLeft) collector.collect(t.toString)
        if(getLabeledVectorOutput() == false && t.isRight) collector.collect(t.toString)
      }
    })
    y
  }
//----------------------GETTER AND SETTER --------------------------------------------------------------------------
  /**
    * Get the Windowsize    *
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(CluStream.WindowSize).get
  }
  /**
    * Set the window size
    * @param windowsize int value of windowsize
    * @return
    */
  def setWindowSize(windowsize: Int): CluStream = {
    this.parameters.add(CluStream.WindowSize, windowsize)
    CluWorkerLogic.setWindowSize(windowsize)
    this
  }
  /**
    * Get the workerparallism value with Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(CluStream.WorkerParallism).get
  }
  /**
    * Set the Worker Parallelism
    * @param workerParallism Int Value
    * @return
    */
  def setWorkerParallelism(workerParallism: Int): CluStream = {
    this.parameters.add(CluStream.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the Sever parallism value with Int value
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(CluStream.PSParallelism).get
  }
  /**
    * Set the Server Parallelism
    * @param psParallelism Int value of parallelism
    * @return
    */
  def setPSParallelism(psParallelism: Int): CluStream = {
    parameters.add(CluStream.PSParallelism, psParallelism)
    this
  }
  /**
    * get the waiting time
    * Variable to stop the calculation in stream environment
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(CluStream.IterationWaitTime).get
  }
  /**
    * Set the waiting time before server closes
    * @param iterationWaitTime
    * @return current CluStream
    */
  def setIterationWaitTime(iterationWaitTime: Long) : CluStream = {
    this.parameters.add(CluStream.IterationWaitTime, iterationWaitTime)
    this
  }
  /**
    * Get the MaximalClusterRange
    * radius of the average standard variance to accept points in
    * @return The range as Double
    */
  def getMaximalClusterRange(): Double = {
    this.parameters.get(CluStream.MaximalClusterRange).get
  }
  /**
    * Set the tolerance margin as a radius were points are accepted in the cluster
    * @param range Double value, % over average distance
    * @return current CluStream instance
    */
  def setMaximalClusterRange(range: Double): CluStream = {
    this.parameters.add(CluStream.MaximalClusterRange, range)
    CluWorkerLogic.setMaximalClusterRange(range)
    this
  }
  /**
    * Set the required output format
    * either the Labeled datapoints (true) or the model (false)
    * @return current CluStream instance
    */
  def setLabeledVectorOutput(yes: Boolean): CluStream = {
    this.parameters.add(CluStream.LabeledVectorOutput, yes)
    this
  }
  /**
    * Get the current output format
    * @return Boolean value of output
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(CluStream.LabeledVectorOutput).get
  }

//-----------------------------------------------WORKER------------------------------------------------------------------
  val CluWorkerLogic = new WorkerLogic[(DenseVector, Int), P, WOut] {

    var maxClass = 0
    var window = 100
    var clusterRange = 1.5
    val dataQ = new mutable.Queue[(DenseVector, Int)]()
    val dataQPull = new mutable.Queue[(DenseVector, Int)]()
    var counter = 0
   //--------------------------GETTER SETTER------------------------------------------------------------------------------
    /**
      * Set the Windowsize
      * @return worker logic
      */
    def setWindowSize(windowsize: Int): Unit = {
      this.window = windowsize
      this
    }
    /**
      * Set the MaximalClusterRange
      * @return worker logic
      * */
    def setMaximalClusterRange(range: Double): Unit = {
      this.clusterRange = range
      this
    }

    /**
      *  Method handles the receive of the data per worker
      * @param data one datapoint
      * @param ps parameter server client
      */
    override def onRecv(data: (DenseVector, Int), ps: ParameterServerClient[P, WOut]): Unit = {
      // data in queue until data points equal to windowsize
      this.dataQ.enqueue(data)
      if (this.dataQ.size >= this.window){
        // than parameters are pulled
        val dataPart = this.dataQ.dequeueAll(x => x._2 == data._2)
        dataPart.map(x => this.dataQPull.enqueue(x))
        ps.pull(data._2)
      }
    }
    /**
      * this Method calculates for a set of points the distance to a set of cluster points
      * @param points
      * @param paramValue
      * @return  Tuple3: foreach point(0) the nearest cluster (1) and the minimal distance (2)
      */
    def calcDistances(points: Seq[(DenseVector, Int)], paramValue: Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]) : Seq[(DenseVector, Int, Double)] = {
      val result = new mutable.Queue[(DenseVector, Int, Double)]
      if(points.nonEmpty && paramValue.nonEmpty){

      for(i <- 0 until points.length){
        var minSum = (0.0,0)

        for(j <- 0 until paramValue.length){
          val point = points.apply(i)._1
          val center = paramValue.apply(j)._1

          val sum = math.sqrt(calcDistance(point, center))

          if(j==0){
            minSum = (sum,j)
          }// if
          else{
            if(minSum._1>sum){
              minSum = (sum,j)
            }
          }
        }
        result.enqueue((points.apply(i)._1,paramValue.apply(minSum._2)._2,minSum._1))
      }}
      result
    }
    /**
      * this Method calculates the euclidean distance between two points
      * @param point the current point
      * @param center the center point
      * @return the distance as Double
      */
    def calcDistance(point: DenseVector, center: DenseVector) : Double = {
      var sum = 0.0
      for(k <- 0 until center.size) {
        sum = sum + math.pow((point.apply(k) - center.apply(k)), 2)
      }
      sum = math.sqrt(sum)
      sum
    }
    /**
      * Updates each Cluster points with regardsto the new assigned points
      * @param PointsAssignedToCurrentCenter
      * @param currentCenter
      * @return the updates cluster values incl. sum, variance and n (number of points)
      */
    def updateClusterValues(PointsAssignedToCurrentCenter: Seq[(DenseVector, Int, Double)], currentCenter : (DenseVector, Int, DenseVector, Long, Double, Long) ):(DenseVector, Int, DenseVector, Long, Double, Long) = {
      if(PointsAssignedToCurrentCenter.nonEmpty) {
        var newCount = 0
        var avgDistance = 0.0
        val newSum = DenseVector(Array.fill[Double](PointsAssignedToCurrentCenter.head._1.size)(0.0))
        var distance_n = 0.0
        //aggregate information from point collection
        PointsAssignedToCurrentCenter.map { point =>
          BLAS.axpy(1.0, point._1, newSum)
          newCount = newCount + 1
        }
        //create new center as average from sum values
        val newCenter = newSum.copy
        BLAS.scal((1 / newCount.toDouble), newCenter)

        // to define the radius of the center, calculate the distance from each point to the new Center
        distance_n = PointsAssignedToCurrentCenter.map { point =>
          calcDistance(point._1, newCenter)
        }.sum

        avgDistance = (distance_n / newCount.toDouble)

        (newCenter, currentCenter._2, newSum, newCount, avgDistance, System.currentTimeMillis())
      }
      else{
        currentCenter
      }
    }
    /**
      * this method calc the new clusters to push to the server
      * @param pointsAssignToCenter the points of the current window incl the closest cluster
      * @param centers the old centers
      * @return the updated centers
      */
    def calcNewCenters(pointsAssignToCenter: Seq[(DenseVector, Int, Double)], centers: Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]): Seq[(DenseVector, Int, DenseVector, Long, Double, Long)] = {
      val result = new mutable.Queue[(DenseVector, Int, DenseVector, Long, Double, Long)]()
      val PointsAssignedToClusterQ = new mutable.Queue[(DenseVector, Int, Double)]
      // all points to Queue
      pointsAssignToCenter.foreach(point => PointsAssignedToClusterQ.enqueue(point))
      // find the biggest clusterid
      val maxClassTest = centers.maxBy(x => x._2)._2
      if(maxClassTest > this.maxClass) this.maxClass = maxClassTest

      for(i<-0 to centers.length-1) {
        val currentCenter = centers.apply(i)
        //all points that are assigned to the currentcenter = Cluster
        val PointsAssignedToCurrentCenter = PointsAssignedToClusterQ.dequeueAll(y => y._2 == currentCenter._2)

        if (PointsAssignedToCurrentCenter.nonEmpty) {
          // find the points outside the distance
          val pointsOutsideTheDistance = PointsAssignedToCurrentCenter.filter(x => x._3 > (currentCenter._5 * clusterRange))
          var pointsWithinTheDistance = PointsAssignedToCurrentCenter.filter(x => x._3 <= currentCenter._5 * clusterRange)
          if (pointsOutsideTheDistance.nonEmpty) {
            // calc a possible new clusters
            val UpdatedCluster = updateClusterValues(pointsWithinTheDistance, currentCenter)

            val clusterOption = updateClusterValues(pointsOutsideTheDistance, (DenseVector(Array.fill[Double](currentCenter._3.size)(0.0)), maxClass + 1, DenseVector(Array.fill[Double](currentCenter._3.size)(0.0)), 0, 0.0, System.currentTimeMillis()))

            // calculate the distance between both centers
            val distance = calcDistance(clusterOption._1, UpdatedCluster._1)
            // if the distance between the new centers is smaller than the double cluster Range
            // which means the centers are very close, just take all the points into account
            if (UpdatedCluster._5 == 0.0 || distance < UpdatedCluster._5 * (2 * clusterRange - 1)) {
              val UpdatedCluster = updateClusterValues(PointsAssignedToCurrentCenter, currentCenter)
              result.enqueue(UpdatedCluster)
            }
            else {
              //check for all points out of the distance which cluster is closer
              val pointsAssignTwoCenter = calcDistances(pointsOutsideTheDistance.map(x=> (x._1,x._2)),Seq(clusterOption,UpdatedCluster ))
              val pointsAssignTwoCenterOld = pointsAssignTwoCenter.filter(y => y._2 == currentCenter._2)
              val pointsAssignTwoCenterNew = pointsAssignTwoCenter.filter(y => y._2 == (this.maxClass+1))

              if(pointsAssignTwoCenterNew.nonEmpty) {
                this.maxClass = this.maxClass + 1
                val UpdatedClusterNew = updateClusterValues(pointsAssignTwoCenterNew, UpdatedCluster)
                result.enqueue(UpdatedClusterNew)
              }
              if(pointsAssignTwoCenterOld.nonEmpty) {
                for (i <- 0 to pointsAssignTwoCenterOld.length-1) {
                  pointsWithinTheDistance = pointsWithinTheDistance :+ pointsAssignTwoCenterOld.apply(i)
                }
                val FinalUpdatedCluster = updateClusterValues(pointsWithinTheDistance, currentCenter)
                result.enqueue(FinalUpdatedCluster)
              }
            } //else
          } // if
          else{
            val UpdatedCluster = updateClusterValues(PointsAssignedToCurrentCenter, currentCenter)
            result.enqueue(UpdatedCluster)
          }
        } // 2. if
        else{
          result.enqueue((DenseVector(Array.fill[Double](currentCenter._3.size)(0.0)),currentCenter._2,DenseVector(Array.fill[Double](currentCenter._3.size)(0.0)), 0, 0.0, currentCenter._6))
        }
      }
      result
    }
    /**
      * defines the process in case of an PullRecv from ParameterServer
      * calls the cluster methods and labels unseen data after the cluster process
      * @param paramId problem id
      * @param paramValue the current cluster centers
      * @param ps parameter server client
      */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
      // dequeue all with key == paramId  dequeueAll(x=> boolean)
      val xs = this.dataQPull.dequeueAll(x => x._2 == paramId)
      val x = xs.splitAt(this.window)
      x._2.foreach(x=> this.dataQPull.enqueue(x))
      if (x._1.nonEmpty) {
        val pointsAssignToCenter = calcDistances(x._1,paramValue)
        val updatedCenters = calcNewCenters(pointsAssignToCenter,paramValue)
        ps.push(paramId, updatedCenters)
        // worker can now label the unseen datapoints
        pointsAssignToCenter.map { x =>
        val points = LabeledVector(x._2, x._1)
        ps.output(points, paramId)
        }
      }
    }
  }

  //--------------------------------SERVER------------------------------------------------
  val CluParameterServerLogic = new ParameterServerLogic[P, String] {

    val params = new mutable.HashMap[Int, Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]]
    var numberOfClusters = 0

    /**
      * Set the number of initial clusters
      * @param number the number of input 
      * @return
      */
    def setNumberOfClusters(number: Int): Int = {
      this.numberOfClusters = number
      this.numberOfClusters
    }
    /**
      * Pullreceive server searches for the current state of parameters corresponding to this id
      * @param id problem id
      * @param workerPartitionIndex worker index for answer
      * @param ps parameterServer
      */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {

      var param = params.getOrElseUpdate(id, Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]((DenseVector(Array.fill[Double](2)(0.0)),0,DenseVector(Array.fill[Double](1)(0.0)),0,0.0, System.currentTimeMillis())))
      // in case the current params are smaller than the initial ones, refill the list with
      // the latest old clusters
      if(param.nonEmpty && param.length >= this.numberOfClusters) {
        // in case number of cluster become to huge for the calculation only tripple of the initial clusters are sent
        if(param.length > this.numberOfClusters*3){
          ps.answerPull(id, param.sortBy(x=>x._6).reverse.take(this.numberOfClusters*3) , workerPartitionIndex)
          //TODO maybe take the older completely out as old clusters
        }
        else ps.answerPull(id, param, workerPartitionIndex)
      }
      else{
        val oldClusters = params.getOrElseUpdate(id+1, Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]((DenseVector(Array.fill[Double](2)(0.0)),0,DenseVector(Array.fill[Double](1)(0.0)),0,0.0, System.currentTimeMillis()))).sortBy(x => x._6)
        var i = oldClusters.length-1
        while(param.length < numberOfClusters && i > 0 ){
          param = param :+ oldClusters.apply(i)
          i = i-1
        }
        ps.answerPull(id, param, workerPartitionIndex)
      }
    }

    /**
      * Updates each Cluster points with regards to the new assigned points
      * @param UpdateCenter Push centers
      * @param currentCenter current centers on parameter server
      * @return the updates cluster values incl. sum, variance and n (number of points)
      */
    def updateClusterValues(UpdateCenter:  (DenseVector, Int, DenseVector, Long, Double, Long), currentCenter : (DenseVector, Int, DenseVector, Long, Double, Long) ):(DenseVector, Int, DenseVector, Long, Double, Long) = {

      if(UpdateCenter.equals(currentCenter)) return currentCenter
      if(UpdateCenter._1.equals(DenseVector(0.0, 0.0, 0.0))) return currentCenter
      if(currentCenter._1.equals(DenseVector(0.0, 0.0, 0.0))) return UpdateCenter
      else{
        val newSum = currentCenter._3.copy
        //aggregate information from point collection
        BLAS.axpy(1.0, UpdateCenter._3, newSum)
        val newCount = currentCenter._4 + UpdateCenter._4
        //create new center as average from sum values
        var newCenter = DenseVector(0.0, 0.0, 0.0)
        if(newSum.equals(DenseVector(0.0, 0.0, 0.0)))  newCenter = DenseVector(0.0, 0.0, 0.0)
        else {
          newCenter = newSum.copy
          BLAS.scal((1 / newCount.toDouble), newCenter)
        }
        // to define the radius of the center, calculate the distance from each point to the new Center
        var a = 0.0
        var b = 0.0
       if(currentCenter._5 == 0.0 || currentCenter._4 == 0)  a = 0.0
       else a = currentCenter._5 * (currentCenter._4/newCount.toDouble)

       if(UpdateCenter._5 == 0.0 || UpdateCenter._4 == 0)  b = 0.0
       else b = UpdateCenter._5 * (UpdateCenter._4/newCount.toDouble)

        val avgDistance = (a + b)

        (newCenter, UpdateCenter._2, newSum, newCount, avgDistance, UpdateCenter._6)
      }
    }

    /**
      * calculates the euclidean distance between two points
      * @param center1 one center point
      * @param center2 second center point
      * @return euclidean distance
      */
    def calcDistance(center1: DenseVector, center2: DenseVector) : Double = {
      var sum = 0.0
      for(k <- 0 to center2.size-1) {
        sum = sum + math.pow((center1.apply(k) - center2.apply(k)), 2)
      }
      sum = math.sqrt(sum)
      sum
    }
    /**
      * this method checks if redundant cluster exist
      * @param clusters current merged cluster
      * @return set of clusters with removed redundancy
      */
    def removeRedundancy(clusters: mutable.Queue[(DenseVector, Int, DenseVector, Long, Double, Long)]): ((DenseVector, Int, DenseVector, Long, Double, Long), mutable.Queue[((DenseVector, Int, DenseVector, Long, Double, Long), Double)]) = {
      var cluster2 = new mutable.Queue[((DenseVector, Int, DenseVector, Long, Double, Long), Double)]

      clusters.foreach{x =>  cluster2.enqueue((x,0.0))}
      val clusterToCheck = cluster2.dequeue()._1
      var result = clusterToCheck.copy()

      // calculate the distance between the cluster to check if they are different enough
        cluster2 = cluster2.map{x =>
          val dis = calcDistance(x._1._1,clusterToCheck._1)
          (x._1,dis)
        }

      // only consider the cluster which cluster point are within the avgDistance of the current center to check
        val mergeClusters = cluster2.dequeueAll{ x =>
          x._2 <= (clusterToCheck._5)
        }
      // and merge them into one cluster
        for(j<-0 to mergeClusters.length-1){
            val clusterCurrent = result
            val clusterNew = updateClusterValues(clusterCurrent,mergeClusters.apply(j)._1)
            result = clusterNew
          }
      (result, cluster2)
    }
    /**
      * merge of update and previous version
      * @param param current centers
      * @param deltaUpdate updated centers
      * @return a merged set of clusters
      */
    def merge(param: Seq[(DenseVector, Int, DenseVector, Long, Double, Long)], deltaUpdate: P) : Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]= {
      var clusters = new mutable.Queue[(DenseVector, Int, DenseVector, Long, Double, Long)]
      val clusters2 = new mutable.Queue[(DenseVector, Int, DenseVector, Long, Double, Long)]
      val updateClusters = new mutable.Queue[(DenseVector, Int, DenseVector, Long, Double, Long)]

      deltaUpdate.foreach(x => updateClusters.enqueue(x))

      for (i <- 0 to param.length - 1) {
          var ClusterToCheck = param.apply(i)
          val correspondingID = updateClusters.dequeueAll(x => x._2 == ClusterToCheck._2)

        for(j<-0 to correspondingID.length-1){
          val clusterCurrent = ClusterToCheck
          val clusterNew = updateClusterValues(clusterCurrent,correspondingID.apply(j))
          ClusterToCheck = clusterNew
        }
         clusters.enqueue(ClusterToCheck)
      }

      updateClusters.foreach(x => clusters.enqueue(x))

      var redo = true
      while (redo) {

          val ReducedResult = removeRedundancy(clusters)
          clusters2.enqueue(ReducedResult._1)
          if (ReducedResult._2.isEmpty || ReducedResult._2.size < 2) {
            redo = false
            if (ReducedResult._2.size == 1) clusters2.enqueue(ReducedResult._2.head._1)
          } //if
          else clusters = ReducedResult._2.map(x => x._1)

      }//while
      clusters2.dequeueAll(x => true)
    }

    /**
      * method define the procedure when receiving a push message
      * @param id problem id
      * @param deltaUpdate the updated cluster sets
      * @param ps parameter server
      */
    override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {
      val param = params.getOrElseUpdate(id, Seq[(DenseVector, Int, DenseVector, Long, Double, Long)]((DenseVector(Array.fill[Double](2)(0.0)),0,DenseVector(Array.fill[Double](1)(0.0)),0,0.0, System.currentTimeMillis())))
      val update = merge(param, deltaUpdate)
      params.update(id, update)
      var outputCounter = numberOfClusters-1
      if(params(id).length < numberOfClusters)  outputCounter = params(id).length-1

      var output = "Current Clusters: "
      for(i<-0 to outputCounter){
        val sortedParams = params(id).sortBy(x => x._4).reverse

       if(sortedParams.apply(i)._4 > getWindowSize()){
        output = output+"ClusterId :"+sortedParams.apply(i)._2+" Centre: "+sortedParams.apply(i)._1 +" Count: "+sortedParams.apply(i)._4
        if(i<sortedParams.length-1) output = output +";"
     }
    }
      ps.output(output)
    }
  }
  // equals constructor
  def apply() : CluStream = new CluStream()
}

// Single instance
object CluStream {

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

  case object MaximalClusterRange extends Parameter[Double] {
    /**
      * Default range constant : considering a outer boarder of 0.5 above avg
      */
    private val DefaultMaximalRange: Double = 1.5
    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(MaximalClusterRange.DefaultMaximalRange)
  }

  case object WorkerParallism extends Parameter[Int] {
    /**
      * Default workerParallism.
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

  case object LabeledVectorOutput extends Parameter[Boolean] {
    /**
      * Default output are the cluster centers
      */
    private val DefaultLabeledVectorOutput: Boolean = false
    /**
      * Default value.
      */
    override val defaultValue: Option[Boolean] = Some(LabeledVectorOutput.DefaultLabeledVectorOutput)
  }
}
