package eu.proteus.solma.SLOG
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
object SLOG_Cluster2 extends  Serializable {
  private val serialVersionUID = 6529646098267711690L

  /**
    * method that starts the computation
    * @param args not used
    * @see ALS_Cluster
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val pipeline = new SLOG()

    //val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDALSTurboFan.txt")
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("/share/flink/onlineML/onlineML_regression20th.csv") //SGDALSTurboFan.txt
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
    //      pipeline.train(labeledDataStream)
    //val result = pipeline.predict(labeledDataStream)
        val result = pipeline.fitAndPredict(labeledDataStream)
    val Sink = result.map(x=>x).writeAsText("/share/flink/onlineML/SLOG_resultWeights.03.03.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()

  }

}