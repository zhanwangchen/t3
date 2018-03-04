
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
/**
  * Created by Ariane on 05.09.2017.
  * this class was used to preprocess the dataset adult.txt into a dataset that can be observed by the tree
  * so each attribute is mapped into a categorical value
  */
object DataPreprocessingTree {
  /*
  0 age: continuous.
  1 workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
    2 fnlwgt: continuous. > dropped as not understood
    3 education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
  4 education-num: continuous. > taken and eduction dropped
  5 marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
  6 occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
  7 relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
  8 race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
  9 sex: Female, Male.
  10 capital-gain: continuous.
  11 capital-loss: continuous.
  12 hours-per-week: continuous.
13 native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
*/

  def map(age: Int) : Double ={
    var age2 = 0.0
    if(age < 25)                age2 = 1.0
    if(age >= 25 && age < 35 )  age2 = 2.0
    if(age >= 35 && age < 45 )  age2 = 3.0
    if(age >= 45 && age < 55)    age2 = 4.0
    if(age >= 55 && age < 65)    age2 = 5.0
    if(age >= 65)                age2 = 6.0
    age2
  }

  def mapLoss(age: Int) : Double ={
    var age2 = 0.0
    if(age < 20000)                age2 = 1.0
    if(age >= 20000 && age < 50000 )  age2 = 2.0
    if(age >= 50000 )  age2 = 3.0
    age2
  }

  def mapTime(value: Int) : Double ={
    var age2 = 0.0
    if(value < 35)                age2 = 1.0
    if(value >= 35 && value < 45 )  age2 = 2.0
    if(value>= 45 && value < 60 )  age2 = 3.0
    if(value > 59)                  age2 = 4.0
    age2
  }

  def mapWorkclass(workclass: String) : Double ={
    var value = 0.0
    if(workclass == "Private")                value = 1.0
    if(workclass == "Self-emp-not-inc" )      value = 2.0
    if(workclass == "Self-emp-inc" )  value = 3.0
    if(workclass == "Federal-gov" )    value = 4.0
    if(workclass  == "Local-gov" )    value = 5.0
    if(workclass  == "State-gov" ) value = 6.0
    if(workclass  == "Without-pay" ) value = 7.0
    if(workclass  == "Never-worked" ) value = 8.0
    value
  }

  def mapJob(job: String) : Double ={
    var value = 0.0
    if(job == "Private")                value = 1.0
    if(job == "Self-emp-not-inc" )      value = 2.0
    if(job == "Self-emp-inc" )  value = 3.0
    if(job == "Federal-gov" )    value = 4.0
    if(job == "Local-gov" )    value = 5.0
    if(job == "State-gov" ) value = 6.0
    if(job == "Without-pay" ) value = 7.0
    if(job == "Never-worked" ) value = 8.0
    value
  }

  def mapRace(job: String) : Double ={
    var value = 0.0
    if(job == "White")                value = 1.0
    if(job == "Asian-Pac-Islander" )      value = 2.0
    if(job == "Amer-Indian-Eskimo" )  value = 3.0
    if(job == "Black" )    value = 4.0
    if(job == "Other" )    value = 5.0
    value

  }

  def mapOccupation(obj: String) : Double ={
    var value = 0.0
    if(obj == "Tech-support")                value = 1.0
    if(obj == "Craft-repair" )      value = 2.0
    if(obj == "Other-service" )  value = 3.0
    if(obj == "Sales" )    value = 4.0
    if(obj == "Exec-managerial" )    value = 5.0
    if(obj == "Prof-specialty" ) value = 6.0
    if(obj == "Handlers-cleaners" ) value = 7.0
    if(obj == "Machine-op-inspct" ) value = 8.0
    if(obj == "Adm-clerical" )    value = 9.0
    if(obj == "Farming-fishing" )    value = 10.0
    if(obj == "Transport-moving" ) value = 11.0
    if(obj == "Priv-house-serv" ) value = 12.0
    if(obj == "Protective-serv" ) value = 13.0
    if(obj == "Armed-Forces" ) value = 14.0
    value
  }

  def mapCountry(obj: String) : Double ={
    var value = 0.0
    // EU
    if(obj == "England" || obj == "Germany" || obj == "Greece" || obj == "Portugal" || obj == "Poland"
      || obj == "Italy" || obj == "Ireland" || obj == "France" || obj == "Hungary" || obj == "Scotland" || obj == "Holand-Netherlands" )                value = 1.0
    // North America
    if(obj == "United-States" || obj == "Canada" ||obj == "Haiti" || obj == "Guatemala"
     ||obj == "El-Salvador" || obj == "Honduras" || obj == "Trinadad&Tobago" ||obj == " Nicaragua")      value = 2.0
    // Asia
    if(obj == "Cambodia" || obj == "Iran" || obj == "Vietnam" || obj == "Laos" ||
      obj == "Philippines" || obj == "Taiwan" || obj == "Thailand")  value = 3.0
    // sourthn America
    if(obj == "Puerto-Rico" || obj == "Cuba" || obj == "Jamaica" || obj == "Mexico" || obj == "Dominican-Republic" ||
      obj == "Ecuador" || obj == "Columbia" || obj == "Peru")    value = 4.0
    if(obj == "India" )    value = 5.0
    if(obj == "Japan" ) value = 6.0
    if(obj == "China" ) value = 7.0
    value
  }

    def mapMaritalState(workclass: String) : Double ={
    var age2 = 0.0
    if(workclass == "Married-civ-spouse")                age2 = 1.0
    if(workclass == "Divorced" )      age2 = 2.0
    if(workclass == "Never-married" )  age2 = 3.0
    if(workclass == "Separated" )    age2 = 4.0
    if(workclass  == "Widowed" )    age2 = 5.0
    if(workclass  == "Married-spouse-absent" ) age2 = 6.0
    if(workclass  == "Married-AF-spouse" ) age2 = 7.0
    age2
  }

  def mapRelationship(obj: String) : Double = {
     var value = 0.0
    if(obj == "Wife")                value = 1.0
    if(obj == "Own-child" )      value = 2.0
    if(obj == "Husband" ) {
      value = 3.0
    }
    if(obj == "Not-in-family" )    value = 4.0
    if(obj == "Other-relative" )    value = 5.0
    if(obj == "Unmarried" )   value = 6.0
    value

  }

  def main(args: Array[String]) {
      // input files for tree
      val trainingFile = "C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\HT_bigDS_Adult.txt"

      val env = ExecutionEnvironment.getExecutionEnvironment
      env.getConfig.disableSysoutLogging

      val dataset = env.readTextFile(trainingFile).map{ x =>
        val fin = new Array[Double](13)
        val values = x.replace(" ", "").split(",")
        if(values.length == 15) {
          try {
            val age = values.apply(0).toInt
            val agefin = map(age)
            fin.update(0, agefin)
          }
          catch {
            //case NumberFormatException => fin.update(0, 0.0)
            case _ => fin.update(0, 0.0)
          }

          val workclass = values.apply(1)
          val wc = mapWorkclass(workclass)
          fin.update(1, wc)

          try {
            val eduNum = values.apply(4).toDouble
            fin.update(2, eduNum)
          }
          catch {
            case _ => fin.update(2, 0.0)
          }

          val martialState = values.apply(5)
          val ws = mapMaritalState(martialState)
          fin.update(3, ws)

          val relationship = values.apply(7)
          val rel = mapRelationship(relationship)
          fin.update(4, rel)

          val race = values.apply(8)
          val ra = mapRace(race)
          fin.update(5, ra)

          val gender = values.apply(9)
          if (gender == "Female"){
          fin.update(6,1.0)}
          else{
            fin.update(6,0.0)}

           try {
            val gain = values.apply(10).toInt
            val gaind = mapLoss(gain)
            fin.update(7, gaind)
          }
          catch {
           case _ => fin.update(8, 0.0)
          }

          try {
            val loss = values.apply(11).toInt
            val lossd = mapLoss(loss)
            fin.update(8, lossd)
          }
          catch {
            case _ => fin.update(9, 0.0)
          }

          try {
            val time = values.apply(12).toInt
            val wt = mapTime(time)
            fin.update(9,wt)
          }
          catch {
            case _ => fin.update(10, 0.0)
          }

          val country = values.apply(13)
          val cou = mapCountry(country)
          fin.update(10, cou)


          val income = values.apply(14)
          if (income == ">50K"){ fin.update(12, 1.0)
          fin.update(11,1.0)}
          else{
          fin.update(11, 0.0)}
        }
        LabeledVector(fin.apply(11), DenseVector(fin.slice(0,10)))
      }

    val Sink = dataset.writeAsText("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\HT_bigDS_AdultTree_Train.txt", WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
    }

    }
