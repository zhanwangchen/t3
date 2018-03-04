package eu.proteus.solma.HoeffdingTree

import java.util
import breeze.numerics.log2
import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps.entities.PullAnswer
import hu.sztaki.ilab.ps.{PSReceiver, PSSender, WorkerReceiver, WorkerSender, _}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{LabeledVector, Parameter, WithParameters}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable

/**
  * Created by Ariane on 09.08.2017.
  */
class HoeffdingTree extends WithParameters with Serializable {

  // data types for parameter server communication
  type P = Tree
  //              id, workerId, decision tree
  type WorkerIn = (Int, Int, Tree)
  type WorkerOut = (Boolean, Array[Int], Tree)
  type WOut = (LabeledVector, Int)

  /**  main method of this algorithm that handles the tree job
    * @param evalDataStream : Datastream to analyse, LabeledVector
    * @return a datastream String containing the tree
   */
  def fit(evalDataStream: DataStream[LabeledVector]): DataStream[String] = {

    //add a "problem" id
    //TODO in general this could be done in a more efficient way, i.e. solving several problems in the same time
    val partitionedStream = evalDataStream.map(x => (x, 0))

    // send the stream to the parameter server, define communication and Worker and Server instances
    val outputDS =
      transform(partitionedStream,
        HTWorkerLogic,
        HTParameterServerLogic,
        (x: (Boolean, Array[Int], Tree)) => x match {
          case (true, Array(partitionId, id), pull) => Math.abs(id.hashCode())
          case (false, Array(partitionId, id), update) => Math.abs(id.hashCode())
        },
        (x: (Int, Int, Tree)) => x._2,
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
            collectAnswerMsg(true, Array(partitionId, id), new Tree())
          }

          override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((false, Array(partitionId, id), deltaUpdate))
          }
        },
        new PSReceiver[(Boolean, Array[Int], Tree), P] {
          override def onWorkerMsg(msg: (Boolean, Array[Int], Tree), onPullRecv: (Int, Int) => Unit, onPushRecv: (Int, P) => Unit): Unit = {
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
                                    collectAnswerMsg: ((Int, Int, Tree)) => Unit): Unit = {
            collectAnswerMsg((id, workerPartitionIndex, value))
          }
        },
        this.getIterationWaitTime()
      )

    // prepare output
    val y = outputDS.flatMap(new FlatMapFunction[Either[(LabeledVector, Int), String], String] {
      override def flatMap(t: Either[(LabeledVector, Int), String], collector: Collector[String]): Unit = {
        if(t.isRight && getLabeledVectorOutput()==false) collector.collect(t.toString)
        if(t.isLeft && getLabeledVectorOutput()== true) collector.collect(t.toString)
      }
    })

    y
  }
  //----------------------GETTER AND SETTER --------------------------------------------------------------------------
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(HoeffdingTree.WindowSize).get
  }
  /**
    * set a new windowsize
    * @param windowsize
    */
  def setWindowSize(windowsize: Int) : HoeffdingTree = {
    this.parameters.add(HoeffdingTree.WindowSize, windowsize)
    HTWorkerLogic.setWindowSize(windowsize)
    this
  }
  /**
    * Get the workerparallism value
    * @return The number of worker parallelism : int
    */
  def getWorkerParallelism(): Int = {
    this.parameters.get(HoeffdingTree.WorkerParallism).get
  }
  /**
    * set workerparallelism
    * @param workerParallism
    */
  def setWorkerParallelism(workerParallism: Int) : HoeffdingTree = {
    this.parameters.add(HoeffdingTree.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the severparallism value with Int values    *
    * @return The number of server parallelism
    */
  def getPSParallelism(): Int = {
    this.parameters.get(HoeffdingTree.PSParallelism).get
  }
  /**
    * set psParallelism
    * @param psParallelism Int value to set psparallelism to
    */
  def setPSParallelism(psParallelism: Int) : HoeffdingTree = {
    parameters.add(HoeffdingTree.PSParallelism, psParallelism)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime(): Long = {
    this.parameters.get(HoeffdingTree.IterationWaitTime).get
  }
  /**
    * set iterationwaitingtime
    * @param iterationWaitTime
    */
  def setIterationWaitTime(iterationWaitTime: Long) : HoeffdingTree = {
    this.parameters.add(HoeffdingTree.IterationWaitTime, iterationWaitTime)
    this
  }
  /**
    * Get the TreeBound to control when splits are necessary
    * @return The allowed % of wrong classifications
    */
  def getTreeBound(): Double = {
    this.parameters.get(HoeffdingTree.TreeBound).get
  }
  /**
    * set psParallelism
    * @param bound
    */
  def setTreeBound(bound: Double): HoeffdingTree = {
    this.parameters.add(HoeffdingTree.TreeBound, bound)
    HTWorkerLogic.setBound(bound)
    this
  }
  /**
    * Set the required output format
    * @return either the Labeled datapoints (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): HoeffdingTree = {
    this.parameters.add(HoeffdingTree.LabeledVectorOutput, yes)
    HTWorkerLogic.setLabelOut(yes)
    HTParameterServerLogic.setLabeloutput(yes)
    this
  }
  /**
    * Get the current output format
    * @return
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(HoeffdingTree.LabeledVectorOutput).get
  }
  /**
    * Get the Default Label which is used for unlabeld data points as double
    * @return The defaultLabel
    */
  def getDefaultLabel(): Double  = {
    this.parameters.get(HoeffdingTree.DefaultLabel).get
  }
  /**Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double) : HoeffdingTree = {
    parameters.add(HoeffdingTree.DefaultLabel, label)
    HTWorkerLogic.setLabel(label)
    this
  }

  //------------------------------------------------WORKER------------------------------------------------------------------
  val HTWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut] {

    var window = 0
    var label = -1.0
    var labeloutput = false
    var treeBound = 0.1
    val dataQ = new mutable.Queue[(LabeledVector, Int)]()
    val dataQPull = new mutable.Queue[(LabeledVector, Int)]()
    val dataQPredict = new mutable.Queue[(LabeledVector, Int)]()
    val nodeStatistics = new mutable.HashMap[Double, Int]

    //--------------------------GETTER SETTER------------------------------------------------------------------------------
    /**
      * Set the Windowsize
      * @param windowsize the windowvalue to update
      **/
    def setWindowSize(windowsize: Int): Int = {
      this.window = windowsize
      this.window
    }
    /**
      * Set the default
      * @param label to identify unseen datapoints
      **/
    def setLabel(label: Double): Double = {
      this.label = label
      this.label
    }
    /**
      * Set the treebound
      * @param bound the % of missclassification that is allowed
      **/
    def setBound(bound: Double): Double = {
      this.treeBound = bound
      this.treeBound
    }
    /**
      * Set the labeloutput
      * @param yes
      * @return
      */
    def setLabelOut(yes : Boolean):Boolean ={
      this.labeloutput = yes
      this.labeloutput
    }

    /**
      * Method handles  the receive of the data per worker
      * collects data and prepares statistics
      * sent the pull missing
      * @param data the input stream
      * @param ps the ParameterServerClient
      */
    override def onRecv(data: (LabeledVector, Int), ps: ParameterServerClient[P, WOut]): Unit = {
       //TODO one Tree per data._2, handle several problems
      // data in queue until data points equal to windowsize
      // than parameters are pulled
      if(data._1.label != this.label){
      this.dataQ.enqueue(data)
      val param = this.nodeStatistics.getOrElseUpdate(data._1.label, 0)
      val count = param + 1
      this.nodeStatistics.update(data._1.label, (count))
      // if more than one label has been observed, pull the parameters
      if (this.dataQ.size >= this.window && this.nodeStatistics.size > 1) {
        val dataPart = this.dataQ.dequeueAll(x => x._2 == data._2)
        dataPart.map(x => this.dataQPull.enqueue(x))
        ps.pull(data._2)
      }
    }
      if(data._1.label == this.label && this.labeloutput == true){
       this.dataQPredict.enqueue(data)
      }
    }
    /**
      * this method checks if a further split on the input node is necessary,
      * if so it calls
      * @see splitTree again
      * @param node the node to check
      */
    def check(node : TreeNode) = {
      val obs = node.getObservationCounter
      // we check how many labels  exists in the node and filter out labels that appear less than the treebound
      val nodesToSplit = node.getNodeStatistics
        .map(x => (x._1,x._2,x._2.toDouble/obs))
        .filter(y => (y._3 > this.treeBound && y._3 <= 1.0))
      // we only split further if we have more than one label left after we applied the treebound
      if(nodesToSplit.size > 1) {
        val splitAtt = calcGain(node.getParam3, obs)
        splitTree(splitAtt, node)
      }
    }

    /**
      *  add that many child nodes to the current node as the split attribute offers and updates the node values
      *  @param gainMap the highest gain value possible via a split
      *  @param node the current node
      * */
    def splitTree(gainMap: (Double, Int), node : TreeNode): Unit ={
      val splitter : List[(Double, Int)] = node.getParams2.get(gainMap._2).get
      if(splitter.nonEmpty && splitter.size > 1){
        for(i<-0 until splitter.size){
        val x = splitter.apply(i)
        val nodeNew = new TreeNode(node, gainMap._2, x._1, 0)
        node.addChild(nodeNew)
        check(nodeNew)
        }
      }
    }

    /**
      *  calculates the gain among the observations seen
      *  @param params3 statistic of observations
      *  @param size the total number of observations
      * */
    def calcGain(params3: mutable.HashMap[(Double, Int, Double), (Int, Int)], size: Int): (Double, Int) = {
      val gainMap = params3
        .map{ x=>
        val a = x._2._1.toDouble
        val b = x._2._2.toDouble
        val gain = (a/size) *(-b/a * log2((b)/a))
        (x._1._2,(x._1._1,x._1._3), gain)
      }.groupBy(x => x._1)
        .map{ x=>
        val totalGain = x._2.reduce((x,y) => (x._1, x._2,x._3+y._3))
        ( totalGain._3, x._1)
      }.minBy(x => x._1)
      gainMap
    }

    def findLabel(node: TreeNode) : Double = {
      var labelReturn = 1.0
      var counter = node.getObservationCounter
      if(counter == 0) counter = node.getNodeStatistics.map(x => x._2).sum

      if(node.getNodeStatistics.size > 1) {
        val label = node.getNodeStatistics.map(x => (x._1, x._2, x._2.toDouble / counter)).reduce[(Double, Int, Double)] { (L, R) =>
          if (L._3 > R._3) L
          else R
        }
        labelReturn = label._1
      }
      else{
        if(node.getNodeStatistics.isEmpty){
           labelReturn = findLabel(node.getParent)
        }
        else labelReturn = node.getNodeStatistics.head._1
      }
      labelReturn
    }

    /**
      * Adds unseen datapoints to the tree and returns their new label
      *
      * @param data the unseen datapoint
      * @param tree the current tree
      * @return the label of the data
      */
    def addunseenDatapointToTree(data: (LabeledVector, Int), tree : Tree): Double = {
      var node = tree.root
      var dataCheck = data._1.label
      var done1 = false
      while ((!node.getIsLeaf || !node.getChildren.isEmpty) && !done1) {
        val check: util.ArrayList[TreeNode] = node.getChildren
        var i = -1
        var done2 = false
        while (i < check.size()-1 && !done2) {
          i += 1
          if (check.get(i).getId()._2 == data._1.vector.apply(check.get(i).getId()._1)) {
            node = check.get(i)
            done2 = true
            if((node.getIsLeaf() || node.getChildren.isEmpty)) {
              dataCheck = findLabel(node)
              done1 = true
            }
          } // if in for
          if(i == check.size()-1 && !done2){
            val newNode = new TreeNode(node, check.get(i).getId()._1,data._1.vector.apply(check.get(i).getId()._1),0)
            node.addChild(newNode)
            done2 = true
            done1 = true
          }
        } //for
      } //while
      dataCheck
    } // end sort

    /**
      * Method to label unseen datapoints,if new label is not default label,data point is sent to output
      * @param ps parameter server instance
      * @param paramId problem id
      * @param tree current tree
      */

    def labelDataPoints(ps: ParameterServerClient[P, (LabeledVector, Int)], paramId: Int, tree : Tree) = {
      val data = this.dataQPredict.dequeueAll(x => x._1.label == -1)
      if(data.nonEmpty) {
        data.map { x =>
          val label: Double = addunseenDatapointToTree(x, tree)
          if (label != -1) ps.output(LabeledVector(label, x._1.vector), x._2)
          else this.dataQPredict.enqueue(x)
        }
      }
    }

    /**
      * defines what is done on PullRecv from ParameterServer and after update push result back to ps
      *
      * @param paramId the problem id
      * @param paramValue the tree pulled from the PS
      * @param ps the PS instance
      *  */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
      // dequeue all with key == paramId
      // dequeueAll(x=> boolean)
      val xs = dataQPull.dequeueAll(x => x._2 == paramId)
      val x = xs.splitAt(window)
      x._2.foreach(x => dataQPull.enqueue(x))

      if (x._1.nonEmpty) {
        x._1.foreach(x => paramValue.addDatapointToTree(x))
        if(!paramValue.leaveList.isEmpty){
        for(i<-0 until paramValue.leaveList.size()){
          if(paramValue.leaveList.get(i).getNodeStatisticSize > 1){
             val node = paramValue.leaveList.get(i)
            if(node != null) check(node)
            else check(paramValue.root)
          }
        }
      }else{
          check(paramValue.root)
        }
      } // if

      ps.push(paramId, paramValue)
      if(dataQPredict.size > 0) labelDataPoints(ps, paramId, paramValue)
    } //if
  }

  //----------------------------------SERVER------------------------------------------------
  val HTParameterServerLogic = new ParameterServerLogic[P, String] {

    val params = new mutable.HashMap[Int, Tree]
    var labeloutput = false

    /**
      * Set the labeloutput, prevent unnecessary parameter server outputs
      * @param yes boolean to add either model (false) or labelvectors (true)
      * @return
      */
    def setLabeloutput(yes : Boolean) : Boolean ={
      this.labeloutput = yes
      this.labeloutput
    }
    /**
      * Pullreceive server searches for the current state of parameters corresponding to this id
    * @param id the problem id
      *@param workerPartitionIndex to which worker sent pullAnswer
      *@param ps the parameter server instance
    */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      val param = params.getOrElseUpdate(id, new Tree())
      ps.answerPull(id, param, workerPartitionIndex)
    }

    /**
      * Pushreceive server updates old value with new
      * @param id the problem id
      *@param deltaUpdate to which worker sent pullAnswer
      *@param ps the parameter server instance
      */
    override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {
      //TODO no intelligent merge worked out, took to long
      val param = params.getOrElseUpdate(id, new Tree())
      if(param.level <= deltaUpdate.level) {
        params.update(id, deltaUpdate)
        if(!labeloutput) ps.output(deltaUpdate.toString)
      }
      else{
          params.update(id, param)
          if(!labeloutput) ps.output(param.toString)
      }
    }
  }
    // equals constructor
    def apply(): HoeffdingTree = new HoeffdingTree()
}
// Single instance
object HoeffdingTree {

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

  case object TreeBound extends Parameter[Double] {
    /**
      * Default bound constant : 10% failsure is okay
      */
    private val DefaultTreeBound: Double = 0.1
    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(TreeBound.DefaultTreeBound)
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
}