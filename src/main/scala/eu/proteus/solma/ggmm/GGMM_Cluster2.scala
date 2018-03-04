package eu.proteus.solma.ggmm
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
object GGMM_Cluster2 {
  private val serialVersionUID = 6529685098267711691L


  /**
    * method that starts the computation
    * @param args not used
    * @see ALS_Cluster
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

    var WindowSize  = 1000
    var LabeledVectorOutput = true
    var WorkerParallelism = 200
    var PSParallelism = 3
    var streamingEnvParallelism = 3
    var filename = "/share/hadoop/tmp/cassini-big.csv"
    //"hdfs:/onlineML/onlineMLBigSVm43.5.csv"
    var out = "/share/flink/onlineML/GGMM_resultWeights.04.3.txt"
    //"/share/flink/onlineML/svmOutOnRev43.5.txt"
    var outParallelism = 1
    try{
      if(args.length==7){
        println("using given parameters!")
        // 1 1 1 50 hdfs:/onlineML/onlineMLBig3.csv
        //40 2 2 50 /share/flink/onlineML/onlineMLBigSVm43.5v2.csv /share/flink/onlineML/svmOutOnRev43.5v2.txt 1
        //40 2 2 50 /share/flink/onlineML/onlineMLBigSVm43.5.csv /share/flink/onlineML/svmOutOnRev43.5.txt 1

        //40 2 2 50 /share/flink/onlineML/onlineML_regression20th.csv /share/flink/onlineML/ORR_resultWeights.24.12.txt 1
        WorkerParallelism = args.apply(0).toInt
        PSParallelism = args.apply(1).toInt
        streamingEnvParallelism = args.apply(2).toInt
        WindowSize=args.apply(3).toInt
        filename = args.apply(4)
        out = args.apply(5)
        outParallelism = args.apply(6).toInt
      }else{
        println("using default parameters!")
      }
    }

    val pipeline = new GGMM()

    //val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDALSTurboFan.txt")
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile(filename) //SGDALSTurboFan.txt
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
    val result = pipeline.fitAndPredict(labeledDataStream)
    //    val resultSGDPS = pipeline.fitAndPredict(labeledDataStream)
    val Sink = result.map(x=>x).writeAsText(out, WriteMode.OVERWRITE).setParallelism(outParallelism)

    streamingEnv.execute()

  }

}
