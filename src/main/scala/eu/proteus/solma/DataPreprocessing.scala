import eu.proteus.solma.SGD.SGD_Local.CMAPSSData
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

/**
  * Created by Ariane on 14.08.2017.
  * class is used to process the data into the required format
  */

object DataPreprocessing {

  def main(args: Array[String]) {
    // input files
    val trainingFile = "C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\TrainDSnormIDCYCLE.csv"
    val testFile = "C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\TestDSnormIDCYCLE.csv"
    val rulFile = "C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\TurboFan_RUL_FD001.txt"
    val trainingFileSGD = "C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\TurboFanComplet.csv"

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging;

    // read in trainingset incl column names
    val trainingDataSGD = env.readCsvFile[(CMAPSSData)](
      trainingFileSGD,
      fieldDelimiter = " ",
      pojoFields = Array("id", "cycle", "settings1", "settings2","s3", "s4", "s9", "s14", "s15", "s21", "rul"))

    // create a LabelVector DataSet
    val trainingDataLVSGD = trainingDataSGD.map { tuple =>
      LabeledVector(tuple.rul, DenseVector(tuple.featuresNoRul.toArray))
    }

    // read in trainingset incl column names
    val trainingData = env.readCsvFile[(CMAPSSData)](
      trainingFile,
      fieldDelimiter = " ",
      pojoFields = Array("id", "cycle", "settingRegime", "PC1", "PC2", "PC3", "PC4", "PC5", "PC6", "PC7"))

    // add rul to dataset
    // step 1: max cycles
    val maxCycles = trainingData.map { x => (x.id, x.cycle) }.groupBy(0).max(1)
    // step 2: join maxCycles und trainingData an x.id, additional calc minmax values
    val trainingDataLabeled = trainingData.join(maxCycles).where("id").equalTo(0).map { x =>
      x._1.rul = x._2._2 - x._1.cycle
      LabeledVector(x._1.rul, DenseVector(x._1.featuresPC.toArray))
    }

    val testData = env.readCsvFile[(CMAPSSData)](
      testFile,
      fieldDelimiter = " ",
      pojoFields = Array("id", "cycle", "settingRegime", "PC1", "PC2", "PC3", "PC4", "PC5", "PC6", "PC7"))

    var counter = 0

    val testRul = env.readTextFile(rulFile).setParallelism(1).map { x =>
      counter = counter + 1
      val rul = x.replaceAll(" ","").toInt
      (counter, rul)
    }.setParallelism(1)

    // step 1: max cycles
    val maxCyclesTest = testData.map { x => (x.id, x.cycle) }.groupBy(0).max(1)

    val result = maxCyclesTest.join(testRul).where(0).equalTo(0).map(x => (x._1._1, x._1._2, x._2._2))

    val TestDataWithSolution = testData.join(result).where("id").equalTo(0).map{ x =>
      val realrul = x._2._3+(x._2._2-x._1.cycle)
      (x._1, realrul)
    }
    val testDataLV = TestDataWithSolution
      .map { tuple =>
        (LabeledVector(-1, (DenseVector(tuple._1.featuresPC.toArray))), (tuple._1.id, tuple._1.cycle, tuple._2))
      }

   var data: Seq[LabeledVector] = trainingDataLabeled.collect()
    val test: Seq[LabeledVector] = testDataLV.map(x => x._1).collect()

    for (i <- 0 to test.length - 1) {
      data = data :+ test.apply(i)
    }

    val trainingDataMerge = env.fromCollection(data)


  // val Sink1 = trainingDataMerge.map(x=> x).writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\TurboFanforALS.txt", WriteMode.OVERWRITE).setParallelism(1)
  // val Sink2 = testDataLV.writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\PCTestDataALSIDCyclesRUL.txt", WriteMode.OVERWRITE).setParallelism(1)
  // val Sink3 = trainingDataLabeled.writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\PCTrainALSLabeled.txt", WriteMode.OVERWRITE).setParallelism(1)
   val Sink4 = trainingDataLVSGD.map(x=> x).writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDALSTurboFan.txt", WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
  }
}