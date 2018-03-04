package eu.proteus.solma.HoeffdingTree

import java.util

import org.apache.flink.ml.common.LabeledVector

import scala.collection.mutable

/**
  * Created by Ariane on 02.08.2017.
  * class defining the nodes of the Decision Tree
  */
class TreeNode(parent1 : TreeNode){

  // id = (column number, value)
  private var id = (0, 0.0)
  private var Obscounter = 0
  private var parent = parent1
  private var isLeaf: Boolean = true
  private var children = new util.ArrayList[TreeNode]
  private var datapoints = Seq.empty[(LabeledVector)]
  //nodeStatistics : (label, count of observations)
  private var nodeStatistics = new mutable.HashMap[Double, Int]
  //param2 : (column, [value : count])
  private var params2 = new mutable.HashMap[Int, List[(Double, Int)]]
  //param3 : ( (label, column, value) ,  (count_param2, count) )
  private var params3 = new mutable.HashMap[(Double, Int, Double), (Int, Int)]

  //---------------------GETTER & SETTER ------------------------------------------

  /**
    * get the nodeStatistics
    * @return nodeStatistics
    */
  def getNodeStatistics: mutable.HashMap[Double, Int] = {
    this.nodeStatistics
  }
  /**
    * Set nodeStatistics
    * @param map HashMap of label counts
    * @return updated TreeNode
    */
  def setNodeStatistics (map : mutable.HashMap[Double, Int]) : TreeNode = {
    this.nodeStatistics = map
    this
  }
  /**
    * Set param2 (column, [value : count])
    * @param map the map for the node
    * @return updated TreeNode
    */
  def setParam2 (map : mutable.HashMap[Int, List[(Double, Int)]]) : TreeNode = {
    this.params2 = map
    this
  }
  /**
    * Set param3 ( (label, column, value) ,  (count_param2, count) )
    * @param map the map for the node
    * @return updated TreeNode
    */
  def setParam3 (map : mutable.HashMap[(Double, Int, Double), (Int, Int)]) : TreeNode = {
    this.params3 = map
    this
  }
  /**
    * get isLeaf flag
    * @return boolean flag
    */
  def getIsLeaf(): Boolean = {
    this.isLeaf
  }
  /**
    * get param2, a map of all values a column can have(which has been seen)
    * @return  map (column, [value : count])
    */
  def getParams2: mutable.HashMap[Int, List[(Double, Int)]] = {
    this.params2
  }
  /**
    * get param3 ( (label, column, value) ,  (count_param2, count) )
    * @return  map ( (label, column, value) ,  (count_param2, count) )
    */
  def getParam3: mutable.HashMap[(Double, Int, Double), (Int, Int)] = {
    this.params3
  }
  /**
    * higher the observationcounter by 1
    * @return new observation counter
    */
  def higherObservationCounter: Int = {
    this.Obscounter = this.Obscounter +1
    this.Obscounter
  }
  /**
    * Get node id
    * @return node id as Tuple2(column number, value)
    */
  def getId(): (Int, Double) ={
    (this.id._1 , this.id._2)
  }
  /**
    * adds datapoints to the node and controlls update of node statistics
    * @param point a datapoint to analyse
    */
  def addDatapoint(point : LabeledVector): TreeNode = {
    // if node == toot or the point matches the node add point to node datapoints and update its statistics
    if(this.getId()._1 == -1 ||this.getId()._2 == point.vector.apply(this.getId()._1) ) {
      this.datapoints = this.datapoints :+ point
      this.higherObservationCounter
      createMaps(this, point)
    }
    else{
      //this datapoint does not belong here, we do nothing
    }
    this
  }
  /**
    * Get the children of the node
    *  @return List of nodes related to the parent node
    */
  def getChildren: util.ArrayList[TreeNode] = this.children
  /**
    * Get the parent of the node
    *  @return parent node of this node
    */
  def getParent: TreeNode = this.parent
  /**
    * Get the number of datapoints == observation Counter
    *  @return the number of datapoints
    */
  def getDatapointSize: Int = this.datapoints.size
  /**
    * Get observation Counter == the number of datapoints
    *  @return the number of datapoints
    */
  def getObservationCounter: Int = this.Obscounter
  /**
    *Set the observation counter
    * @param count int value of seen observations
    * @return updated node
    */
  def setObservationCounter(count : Int ): TreeNode = {
    this.Obscounter = count
    this
  }
  /**
    * get all datapoints currently in the node
    * @return List of datapoints beloning to the node
    */
  def getDatapoints: Seq[LabeledVector] = this.datapoints
  /**
    *Set a seq of datapoints that belong to the ndoe
    * @param data datapoints
    * @return updated node
    */
  def setDatapoints (data : Seq[LabeledVector]) : TreeNode= {
    this.datapoints = data
    this
  }
  /**
    *Set a List of childnode to the current node
    * @param children int value of seen observations
    * @return updated node
    */
  def setChildren(children: util.ArrayList[TreeNode]): TreeNode = {
    for( i <- 1 until children.size()) {
      this.addChild(children.get(i))
    }
    this
  }
  /**
    *Set the leaf flag
    * @param isLeaf boolean value (true if node is leaf, false else)
    * @return updated node
    */
  def setIsLeaf(isLeaf: Boolean): TreeNode = {
    this.isLeaf = isLeaf
    this
  }
  /**
    * Get a observation count from the node's statistic
    * @param key the label value
    * @return count of seen obs of this label
    */
  def getNodeStatisticsByKey(key : Double): Int = {
    val count = this.nodeStatistics.getOrElseUpdate(key,0)
    count
  }
  /**
    * Get the number of different labels of this problem
    * @return numbers of different labels
    */
  def getNodeStatisticSize: Int = {
    val sizeStat = this.nodeStatistics.size
    sizeStat
  }
  /**
    * Update a value of the node's statistics
    * @param update
    */
  def updateNodeStatistics(update: (Double, Int)): TreeNode = {
    this.nodeStatistics.update(update._1, update._2)
    this
  }
  /**
    * get the nodes' identifier
    * @return node id
    */
  override def toString: String = this.id.toString

  // constructor for root node
  def this() {
    this(null)
  this.id = (-1, 0.0)
  this.isLeaf -> false
}
  //constructor
  def this(parent1 : TreeNode, col : Int , value : Double, count : Int ){
   this(parent1)
    this.id = (col, value)
    this.Obscounter = count
  }
  /**
    * adds children to the the node
    * @param child the node to add as child to the current node
    */
  def addChild(child: TreeNode): Unit = {
    if (!this.getChildrenId.contains(child.id)) {
      this.children.add(child)
      this.isLeaf = false
      child.isLeaf = true
      child.parent = this
      this.getDatapoints.foreach(x => child.addDatapoint(x))
    }
    else{
      //TODO solve merging problems
      /*for(i<-0 until this.getChildren.size()){
        if(this.getChildren.get(i).getId()==child.getId()){
          child.getDatapoints.foreach(x => this.getChildren.get(i).addDatapoint(x))
          checkChildren(this.getChildren.get(i),child.getChildren)
        }
      }*/
    }
  }

  /**
    * Check if id already is child befroe adding a node twice
    * remove duplicates
    * @return a List of all Children id
    */
  def getChildrenId: util.ArrayList[(Int, Double)] = {
    val id = new util.ArrayList[(Int, Double)]
    val duplicates = new util.ArrayList[TreeNode]()
    import scala.collection.JavaConversions._
    for (node : TreeNode <- this.getChildren) {
      if (!id.contains(node.id)) id.add(node.id)
      else{
        duplicates.addAll(this.getChildren.iterator().toList.filter(x => x.getId()==node.id))
      }
    }
    if(duplicates.nonEmpty){
      //remove duplicates
      for(i<-1 until duplicates.size()){
        duplicates.get(i).getDatapoints.foreach(x => duplicates.head.addDatapoint(x))
        this.getChildren.remove(duplicates.get(i))
      }
    }
    id
  }

  /** this method updates the node maps needed to define the split attribute
    * everytime a new datapoint is added
    * @param node current treenode
    * @param point data point added to the node
    */
  def createMaps(node: TreeNode, point: LabeledVector): Unit = {

    val param = node.getNodeStatistics.getOrElseUpdate(point.label, 0)
    val count = param + 1
    node.getNodeStatistics.update(point.label, (count))

    val vector = point.vector.toArray

    for (k <- 0 to vector.length - 1) {
      var param2 = node.params2.getOrElseUpdate(k, List((vector.apply(k)._2, 0)))

      if (param2.filter(x => x._1 == vector.apply(k)._2).nonEmpty) {
        param2 = param2.map { x =>
          var count = x._2
          if (x._1 == vector.apply(k)._2) {
            count = count + 1
          }
          (x._1, count)
        }
      }
      else {
        param2 = param2 :+ (vector.apply(k)._2, 1)
      }
      node.params2.update(k, param2)
    }

    val vectorp = point.vector.toArray

    vectorp.foreach{ z =>
      node.params2.filter(x => x._1==z._1).foreach{y =>
        y._2.filter(d => d._1 == z._2).foreach{k =>
          val oldCount = node.params3.getOrElseUpdate((point.label, z._1, z._2), (k._2,0))
          val count = oldCount._2+1
          node.params3.update((point.label, z._1, z._2), (k._2,count))
        }
      }
    }
  }

}