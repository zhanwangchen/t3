package eu.proteus.solma.SGD

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

// this class is used to test SGD local

object SGD_Local {
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment


    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDALSTurboFan.txt")
      .map{ x: String =>
        val test = x.replaceAll(",", "").split(" ")
        // 0 = Label
        val rul = test.apply(0).replace("LabeledVector(", "").replace(",","").replace(" ", "").toDouble
        val dense1 = test.apply(1).replace("DenseVector(", "").replace(",","").replace(" ", "").toDouble
        val weights = new Array[Double](7)
        weights.update(0, dense1)
        var j = 1
        for(i<-2 to test.length-1){
          val weight = test.apply(i).replace("))", "").toDouble
          weights.update(j, weight )
          j = j+1
        }
       LabeledVector(rul,DenseVector(weights))
      }


    val initialweights = WeightVector(DenseVector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),0.0)

    //start GradientDescent
    val time3 = System.currentTimeMillis()

    val sgd = new GradientDescentStreamPS()
      .setWindowSize(1000)
      .setLabeledVectorOutput(true)

    val resultSGDPS = sgd.optimize(labeledDataStream, initialweights)
    val Sink = resultSGDPS.writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDresults.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()
  }

  class CMAPSSData {

    var id: Int = _
    var cycle: Int = _
    var settings1: Double = _
    var settings2: Double = _
    var s2: Double = _
    var s3: Double = _
    var s4: Double = _
    var s6: Double = _
    var s7: Double = _
    var s8: Double = _
    var s9: Double = _
    var s11: Double = _
    var s12: Double = _
    var s13: Double = _
    var s14: Double = _
    var s15: Double = _
    var s17: Double = _
    var s18: Double = _
    var s21: Double = _
    var rul: Double = _
    var settingRegime: Double = _
    var bin: Double =_
    var PC2: Double = _
    var PC3: Double = _
    var PC4: Double = _
    var PC6: Double = _
    var PC7: Double = _
    var PC1: Double = _
    var PC5: Double = _


    def features : List[Double] = {
      val a1 = List(cycle.toDouble, s3,s4,s9,s14,s15,s21,rul)
      return a1
    }

    def featuresNoRul : List[Double] = {
      val a1 = List(cycle.toDouble, s3,s4,s9,s14,s15,s21)
      return a1
    }
    // for TurboPC
    def featuresPC : List[Double] = {
      val a1 = List(settingRegime, PC1, PC2, PC3, PC4, PC5, PC6, PC7)
      return a1
    }


    override def toString = "CMAPSSData [id=" + id + ", cycle=" + cycle + ", settings1=" + settings1 + ", settings2=" + settings2 +
      ", s2=" + s2 + ", s3=" + s3 + ", s4=" + s4 + ", s6=" + s6 + ", s7=" + s7 + ", s8=" + s8 + ", s9=" + s9  + ", s11=" + s11 + ", s12=" +
      s12 + ", s13=" + s13 + ", s14=" + s14 + ", s15=" + s15  + ", s17=" + s17 + ", s18=" + s18 +", s21=" + s21 +", rul=" + rul+ "]";
  }

}