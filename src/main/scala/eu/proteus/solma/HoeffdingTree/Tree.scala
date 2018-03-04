package eu.proteus.solma.HoeffdingTree

import java.util

import org.apache.flink.ml.common.LabeledVector

/**
  * Created by Ariane on 02.08.2017.
  * class for Decision Tree
  */
class Tree {

  var root = new TreeNode()
  var leaveList = new util.ArrayList[TreeNode]()
  private val leaveList2 = new util.ArrayList[TreeNode]()
  var level = 0

  /** Checks if node is a leaf, else checks node's children
    * @param node the node to check if it is a leaf
    * @return
    */
  def getLeaf(node: TreeNode) = {
    if(node.getChildren.isEmpty || node.getIsLeaf()){
      node.setIsLeaf(true)
      this.leaveList2.add(node)
    }
    else{
      for(i<-0 until node.getChildren.size()) {
      this.getLeaves(node.getChildren.get(i))
      }
    }
  }

  /**
    * Checks if node's children are leaves
    * @param node node which children are to check if they are leaves
    */
  def getLeafs2(node: TreeNode) : Unit = {

    for(i<-0 until node.getChildren.size()) {
      this.getLeaves(node.getChildren.get(i))
    }
  }

  /**
    * Checks if node is a leaf, else checks node's children
    * @param node the node to check if it is a leaf
    * @return rekursive call
    */
  def getLeaves(node : TreeNode) = {
    if( node.getIsLeaf()) {
      this.leaveList2.add(node)
    }
    else {
      this.getLeafs2(node)
    }
  }

  /** Adds a datapoint to the tree till it reaches a leave
    * updates node parameters
    * @param data the datapoint which is added to the tree
    */
  def addDatapointToTree(data: (LabeledVector, Int)): Unit = {
    var node = this.root
    var levelcheck = 0
    var done1 = false
    node.addDatapoint(data._1)
    while ((!node.getIsLeaf || !node.getChildren.isEmpty) && !done1) {
      val check: util.ArrayList[TreeNode] = node.getChildren
      levelcheck = levelcheck+1
      var done2 = false
      var i = -1
      while (i < check.size()-1 && !done2) {
        i += 1
        if (check.get(i).getId()._2 == data._1.vector.apply(check.get(i).getId()._1)) {
          node.addDatapoint(data._1)
          node = check.get(i)
          done2 = true
          if(node.getIsLeaf() || node.getChildren.isEmpty ) {
            node.setIsLeaf(true)
            this.leaveList.add(node)
            if(levelcheck > this.level){
              this.level = levelcheck
            }
            done1 = true
          }
        } // if in for
        if(i == check.size()-1 && !done2){
          val newNode = new TreeNode(node, check.get(i).getId()._1,data._1.vector.apply(check.get(i).getId()._1),0)
          node.addChild(newNode)
          done2 = true
          done1 = true
        }
      }//for
    } //while
  } // end sort

  /**
    * Method runs over tree and collects all paths for output
    * @return String containing all path of the tree from leave to root
    */
  override def toString: String = {
    var item = "Tree Paths: "
    this.leaveList.clear()
    if( !this.root.getChildren.isEmpty ){
     for(i <- 0 until this.root.getChildren.size()) {
       this.leaveList2.clear()
       val node = this.root.getChildren.get(i)
       this.getLeaf(node)
       for(i <- 0 until leaveList2.size()) {
         var node = leaveList2.get(i)
         var levelcheck = 1
         var stop = true
         while(stop){
           item = " " + item + node.getId() + ","
           levelcheck += 1
           node.setDatapoints(Seq.empty[LabeledVector])
           node = node.getParent
           if(node == null){
             stop = false
             if(levelcheck > this.level ) this.level = levelcheck
           }
         }
         item = item + "\n"
       }//else
     }
    }// for children root
    else item = item + "tree with no children"

    //output
    item
  }
}