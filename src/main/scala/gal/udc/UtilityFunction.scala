package gal.udc

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.csvread
import breeze.linalg.csvwrite
import breeze.linalg.sum
import java.io.InputStreamReader
import java.util.ArrayList

import PositiveMissingNegative._

trait UtilityFunction extends Serializable
{
  def getValue(elem1:MixedData, elem2:MixedData):Double
  def getValue(projectedElem1:BDV[Double], elem2:MixedData):Double
  def getValue(elem1:MixedData, projectedElem2:BDV[Double]):Double
  def mapLeftElement(elem:MixedData):BDV[Double]
  def mapRightElement(elem:MixedData):BDV[Double]
  def writeToFile(filePrefix:String):Unit
}

object MixedVectorProjector
{
  /**
 * @param cat
 * @param catSizes
 * @param cont
 * @param projMatrix
 * @return Matrix x Vector
 */
  private def projectCategorical(elem:CategoricalMixedData, projMatrix:BDM[Double], addBias:Boolean=false):BDV[Double]=
  {
    val cat=elem.categoricalPart
    val catSizes=elem.numValuesForDiscretes
    var cont=elem.numericalPart
    
    if (addBias)
      cont=if (cont.isDefined) //Adding bias to first vector
             Some(BDV.vertcat(cont.get,BDV(1.0)))
           else
             Some(BDV(1.0))
    
    val numCatOHE=catSizes.sum
    val numNumerical=if (cont.isDefined)
                       cont.get.size
                     else
                       0
    
    assert(numCatOHE+numNumerical==projMatrix.cols)
    
    var result:BDV[Double]=BDV.zeros(projMatrix.rows)
    
    var offset=0
    for (i <- 0 until cat.size)
      {
        result+=projMatrix(::,offset+cat(i).toInt)
        offset+=catSizes(i)
      }
    
    if (cont.isDefined)
    {
      val c=cont.get
      val subMatrix=projMatrix(::,numCatOHE until projMatrix.cols)
      result+=subMatrix*c
    }
    
    return result
  }

  /**
   * @param bin
   * @param cont
   * @param projMatrix
   * @return
   */
  def project(elem:MixedData, projMatrix:BDM[Double], addBias:Boolean=false):BDV[Double]=
  {
    if (elem.isInstanceOf[CategoricalMixedData]) return projectCategorical(elem.asInstanceOf[CategoricalMixedData], projMatrix, addBias)
    val bin=elem.getBinaryPart()
    val optSignBin=elem.getOptionalSignedBinaryPart()
    var cont=elem.getNumericalPart()
    
    if (addBias)
      cont=if (cont.isDefined) //Adding bias to first vector
             Some(BDV.vertcat(cont.get,BDV(1.0)))
           else
             Some(BDV(1.0))
    
    val numBinary=if (bin.isDefined)
                    bin.get.size
                  else
                    0
    val numOptionalSignedBinary=if (optSignBin.isDefined)
                    optSignBin.get.size
                  else
                    0
    val numNumerical=if (cont.isDefined)
                       cont.get.size
                     else
                       0
    
    //println(s"$numBinary $numOptionalSignedBinary $numNumerical ${numBinary+numOptionalSignedBinary+numNumerical} ${projMatrix.cols}")
    assert(numBinary+numOptionalSignedBinary+numNumerical==projMatrix.cols)
    
    var result:BDV[Double]=BDV.zeros(projMatrix.rows)
    
    if (bin.isDefined)
    {
      val gBin=bin.get
      for (i <- 0 until gBin.size)
        if (gBin(i))
          result+=projMatrix(::,i)
    }
    
    if (optSignBin.isDefined)
    {
      val gOpt=optSignBin.get
      for (i <- 0 until gOpt.size)
        gOpt(i) match
        {
          case Positive => result+=projMatrix(::,numBinary+i)
          case Negative => result-=projMatrix(::,numBinary+i)
          case _ =>
        }
    }
    
    if (cont.isDefined)
    {
      val c=cont.get
      val subMatrix=projMatrix(::,(numBinary+numOptionalSignedBinary) until projMatrix.cols)
      result+=subMatrix*c
    }
    
    return result
  }

  /**
 * @param elem
 * @param v2
 * @return Vector x MixedVector
 */
  private def vectorProductCategorical(elem:CategoricalMixedData, v2:BDV[Double], addBias:Boolean=false):BDM[Double]= 
  {
    val cat1=elem.categoricalPart
    val catSizes1=elem.numValuesForDiscretes
    var cont1=elem.numericalPart
    
    if (addBias)
      cont1=if (cont1.isDefined) //Adding bias to first vector
             Some(BDV.vertcat(cont1.get,BDV(1.0)))
           else
             Some(BDV(1.0))
    
    val numCatOHE=catSizes1.sum
    val numNumerical=if (cont1.isDefined)
                       cont1.get.size
                     else
                       0
    
    val resultSize=numCatOHE+numNumerical
    var result:BDM[Double]=BDM.zeros(v2.size, resultSize)
    
    var offset=0
    for (i <- 0 until cat1.size)
    {
      result(::,offset+cat1(i).toInt)+=v2
      offset+=catSizes1(i)
    }
    
    if (cont1.isDefined)
    {
      val c1=cont1.get
      for (i <- 0 until c1.size)
        result(::,offset+i)+=v2 :* c1(i)
    }
    
    return result
  }
  
  /**
   * @param cat1
   * @param catSizes1
   * @param cont1
   * @param v2
   * @return Vector x MixedVector
   */
  def vectorProduct(elem:MixedData, v2:BDV[Double], addBias:Boolean=false):BDM[Double]= 
  {
    if (elem.isInstanceOf[CategoricalMixedData]) return vectorProductCategorical(elem.asInstanceOf[CategoricalMixedData], v2, addBias)
    val bin1=elem.getBinaryPart()
    val optSignBin=elem.getOptionalSignedBinaryPart()
    var cont1=elem.getNumericalPart()
    
    if (addBias)
      cont1=if (cont1.isDefined) //Adding bias to first vector
             Some(BDV.vertcat(cont1.get,BDV(1.0)))
           else
             Some(BDV(1.0))
    val numBinary=if (bin1.isDefined)
                    bin1.get.size
                  else
                    0
    val numOptionalSignedBinary=if (optSignBin.isDefined)
                    optSignBin.get.size
                  else
                    0
    val numNumerical=if (cont1.isDefined)
                       cont1.get.size
                     else
                       0
    
    val resultSize=numBinary+numOptionalSignedBinary+numNumerical
    var result:BDM[Double]=BDM.zeros(v2.size, resultSize)
    
    if (bin1.isDefined)
    {
      val gBin=bin1.get
      for (i <- 0 until gBin.size)
        if (gBin(i))
          result(::,i)+=v2
    }
    
    if (optSignBin.isDefined)
    {
      val gOpt=optSignBin.get
      for (i <- 0 until gOpt.size)
        gOpt(i) match
        {
          case Positive => result(::,numBinary+i)+=v2
          case Negative => result(::,numBinary+i)-=v2
          case _ =>
        }
    }
    
    if (cont1.isDefined)
    {
      val c1=cont1.get
      for (i <- 0 until c1.size)
        result(::,numBinary+numOptionalSignedBinary+i)+=v2 :* c1(i)
    }
    
    return result
  }
}

object LogisticUtilityFunction
{
  def readMatrixFromFile(br:BufferedReader):BDM[Double]=
  {
    var out=new ArrayList[Double]
    var line=br.readLine()
    var rows=0
    var cols=0
    while (line!=null)
    {
      rows+=1
      val parts=line.split(",")
      cols=parts.size
      for (p<-parts.map({case p=> p.toDouble}))
        out.add(p)
      line=br.readLine()
    }
    val outA=out.toArray().map(_.asInstanceOf[Double])
    new BDM(cols,outA,0).t
  }
  def readFromFile(filePrefix:String):LogisticUtilityFunction=
  {
    val LAMBDA=1.0
    
    val nameParts=filePrefix.split("_")
    val biasedLeft=nameParts(nameParts.size-2)=="true"
    val biasedRight=nameParts(nameParts.size-1)=="true"
    var v:BDM[Double]=null
    var w:BDM[Double]=null
    
    if (filePrefix.substring(0, 7).equals("hdfs://"))
    {
      val conf = new Configuration()
      val fs= FileSystem.get(conf)
      
      val inputW = fs.open(new Path(filePrefix+"_W.csv"))
      w=readMatrixFromFile(new BufferedReader(new InputStreamReader(inputW)))
      inputW.close()
      
      val inputV = fs.open(new Path(filePrefix+"_V.csv"))
      v=readMatrixFromFile(new BufferedReader(new InputStreamReader(inputV)))
      inputV.close()
    }
    else
    {
      w=csvread(new File(filePrefix+"_W.csv"))
      v=csvread(new File(filePrefix+"_V.csv"))
    }
    
    println(f"W is ${w.rows} x ${w.cols}")
    println(f"V is ${v.rows} x ${v.cols}")
    println(f"Left entity is ${if (biasedLeft) "" else "not"} biased")
    println(f"Right entity is ${if (biasedRight) "" else "not"} biased")
    return new LogisticUtilityFunction(w,v,LAMBDA,biasedLeft,biasedRight)
  }
}

class LogisticUtilityFunction(val W:BDM[Double], val V:BDM[Double], val lambda:Double, val biasedLeft:Boolean=false, val biasedRight:Boolean=false) extends UtilityFunction
{
  override def getValue(elem1:MixedData, elem2:MixedData):Double=getValue(MixedVectorProjector.project(elem1, W, biasedLeft),MixedVectorProjector.project(elem2, V, biasedRight))
  override def getValue(projectedElem1:BDV[Double], elem2:MixedData):Double=getValue(projectedElem1,MixedVectorProjector.project(elem2, V, biasedRight))
  override def getValue(elem1:MixedData, projectedElem2:BDV[Double]):Double=getValue(MixedVectorProjector.project(elem1, W, biasedLeft),projectedElem2)
  
  private def getValue(projectedElem1:BDV[Double], projectedElem2:BDV[Double]):Double=
  {
    val dotP=projectedElem1.dot(projectedElem2)
    return 1.0/(1.0+Math.exp(-dotP/lambda))
  }
  
  def writeToFile(filePrefix:String):Unit=
  {
    val biasesString=s"${biasedLeft.toString()}_${biasedRight.toString()}"
    if (filePrefix.substring(0, 7).equals("hdfs://"))
    {
      val conf = new Configuration()
      val fs= FileSystem.get(conf)
      for ((m,n) <- List((W,"W"),(V,"V")))
      {
        val output = fs.create(new Path(filePrefix+"_"+biasesString+"_"+n+".csv"))
        val writer = new PrintWriter(output);
        for (i <- 0 until m.rows)
        {
          val s=m(i,::).t.map(_.toString()).toArray.mkString(",")
          writer.println(s)
        }
        writer.close()
        output.close()
      }
      
    }
    else
    {
      csvwrite(new File(filePrefix+"_"+biasesString+"_W.csv"),W)
      csvwrite(new File(filePrefix+"_"+biasesString+"_V.csv"),V)
    }
  }
  
  def mapLeftElement(elem:MixedData):BDV[Double]=
  {
    return MixedVectorProjector.project(elem, W, biasedLeft)
  }
  
  def mapRightElement(elem:MixedData):BDV[Double]=MixedVectorProjector.project(elem, V, biasedRight)
}

object LogisticUtilityFunctionSGDLearner
{
  val DEFAULT_SUBSPACE_DIMENSION:Int=10
  val DEFAULT_REGULARIZATION_PARAMETER:Double=1
  val DEFAULT_LEARNING_RATE_START:Double=1.0
  //val DEFAULT_LEARNING_RATE_START:Double=0.1
  val DEFAULT_LEARNING_RATE_SPEED:Double=0.01
  val DEFAULT_FIRST_CONTINUOUS:Int=2
  val DEFAULT_MINIBATCH_SIZE:Int=100
  val DEFAULT_MAX_ITERATIONS:Int=20
  
  def learn(data:RDD[(MixedData,MixedData,Boolean)],
              subspaceDimension:Int=DEFAULT_SUBSPACE_DIMENSION,
              maxIterations:Int=DEFAULT_MAX_ITERATIONS,
              regParameter:Double=DEFAULT_REGULARIZATION_PARAMETER,
              minibatchSize:Double=DEFAULT_MINIBATCH_SIZE,
              learningRate0:Double=DEFAULT_LEARNING_RATE_START,
              learningRateSpeed:Double=DEFAULT_LEARNING_RATE_SPEED,
              dataSize:Option[Long]=None):Option[LogisticUtilityFunction]=
  {
    data.cache()
    val numElems=
      if (dataSize.isDefined)
        dataSize.get
      else
        data.count()
    var minibatchFraction=minibatchSize/numElems.toDouble
    if (minibatchFraction>1)
      minibatchFraction=1
      
    val LAMBDA=1.0
    val (elem1,elem2,l)=data.first()
    val e1BinSize=if (elem1.getBinaryPart().isDefined)
                    elem1.getBinaryPart().get.size
                  else
                    0
    val e2BinSize=if (elem2.getBinaryPart().isDefined)
                    elem2.getBinaryPart().get.size
                  else
                    0
    val e1OptSignBinSize=if (elem1.getOptionalSignedBinaryPart().isDefined)
                    elem1.getOptionalSignedBinaryPart().get.size
                  else
                    0
    val e2OptSignBinSize=if (elem2.getOptionalSignedBinaryPart().isDefined)
                    elem2.getOptionalSignedBinaryPart().get.size
                  else
                    0
    val e1ContSize=if (elem1.getNumericalPart().isDefined)
                     elem1.getNumericalPart().get.size
                   else
                     0
    val e2ContSize=if (elem2.getNumericalPart().isDefined)
                     elem2.getNumericalPart().get.size
                   else
                     0
                     
    var W:BDM[Double]=(BDM.rand(subspaceDimension,e1BinSize+e1OptSignBinSize+e1ContSize+1) :- 0.5)//Rand can be specified - Extra bias term added.
    var V:BDM[Double]=(BDM.rand(subspaceDimension,e2BinSize+e2OptSignBinSize+e2ContSize+1) :- 0.5)//Rand can be specified - Extra bias term added.
    var regParameterScaleW:Double=1/Math.sqrt(W.cols*W.rows)
    var regParameterScaleV:Double=1/Math.sqrt(V.cols*V.rows)
    
    val sc=data.sparkContext
    
    //SGD
    var consecutiveNoProgressSteps=0
    var i=1
    while ((i<maxIterations) && (consecutiveNoProgressSteps<10)) //Finishing condition: gradients are small for several iterations in a row
    {
      val learningRate=learningRate0/(1+learningRateSpeed*(i-1))
      
      //Use a minibatch instead of the whole dataset
      var minibatch=data.sample(false, minibatchFraction, java.lang.System.nanoTime().toInt) //Sample the original set
      while (minibatch.isEmpty())
        minibatch=data.sample(false, minibatchFraction, java.lang.System.nanoTime().toInt) //Sample the original set
      
      val bW=sc.broadcast(W)
      val bV=sc.broadcast(V)
      val (sumdW,sumdV,count)=
        minibatch.map({e => val (elem1,elem2,l)=e
                            val elem1Proj=MixedVectorProjector.project(elem1, bW.value, true) //Adding bias to first vector
                            val elem2Proj=MixedVectorProjector.project(elem2, bV.value, true)
                            val dotP=elem1Proj.dot(elem2Proj)
                            val z=if (l)
                                    1.0
                                  else
                                    -1.0
                            val s=1.0/(1.0+Math.exp(-z*dotP/LAMBDA))
                            val factor= -z*(1.0-s)/LAMBDA
                            
                            val dW=MixedVectorProjector.vectorProduct(elem1, elem2Proj, true) :* factor
                            val dV=MixedVectorProjector.vectorProduct(elem2, elem1Proj, true) :* factor
                              (dW,dV,1.0)
                            })
                 .reduce({case (t1,t2) => (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)})
      bW.destroy()
      bV.destroy()
      val gradientW=sumdW:/count
      val gradientV=sumdV:/count
      //val gradientProgress=Math.abs(sum(gradientWxy))
      val gradientProgress=sum(gradientW.map({ x => Math.abs(x) }))
      if (gradientProgress.isNaN() || gradientProgress.isInfinite())
      {
        println("Could not learn Utility Function because of numeric instability.")
        return None
      }
      if (gradientProgress<0.00001)
        consecutiveNoProgressSteps=consecutiveNoProgressSteps+1
      else
        consecutiveNoProgressSteps=0
        
        
      W=W-learningRate*(gradientW + 2*regParameter*regParameterScaleW*W)
      V=V-learningRate*(gradientV + 2*regParameter*regParameterScaleV*V)
      
      //DEBUG
      //val tempF=new LogisticUtilityFunction(W,V,LAMBDA)
      //tempF.writeToFile(MovieLensTransform.ROOT_PATH+s"debug/l0${learningRate0}-ls${learningRateSpeed}-step${i}_${maxIterations}")
      
      //Normalize cols - Should be done instead of norm regularization if few columns are affected at each step. Since this uses batches doing norm regularization is simpler.
      //W=_normalizeColumns(W)
      //V=_normalizeColumns(V)
      
      i=i+1
      minibatch.unpersist(true)
      //if (i%10==0)
      println(s"step:$i (max $maxIterations) | Gradient size:$gradientProgress | Steps with no progress:$consecutiveNoProgressSteps | W00:${gradientW(0,0)}")
      //println(W)
    }
    //if (consecutiveNoProgressSteps>=10)
    //  lastGradientLogs=LogisticModel.FULLY_CONVERGED_GRADIENTS
    data.unpersist(true)
    return Some(new LogisticUtilityFunction(W,V,LAMBDA,biasedLeft=true,biasedRight=true))
  }
  
  def learnUtilityTuned(rddData:RDD[(MixedData,MixedData,Boolean)],
                        ks:Array[Int]=Array[Int](10, 50, 100, 200),
                        regPs:Array[Double]=Array[Double](0.1, 1, 10),
                        l0s:Array[Double]=Array[Double](0.01,0.1,1.0),
                        lSs:Array[Double]=Array[Double](0.001,0.01,0.1),
                        batchSize:Int=DEFAULT_MINIBATCH_SIZE,
                        numIterations:Int=DEFAULT_MAX_ITERATIONS
                        ):(LogisticUtilityFunction,(Int,Double,Double))=
  {
    val sc=sparkContextSingleton.getInstance()
    
    val split=rddData.randomSplit(Array[Double](0.7,0.3), java.lang.System.nanoTime())
    val trainingData=split(0).repartition(rddData.getNumPartitions)
    trainingData.cache()
    val testData=split(1).repartition(rddData.getNumPartitions)
    testData.cache()
    val trainingDataSize=trainingData.count()
    
    var bestK=ks(0)
    var bestRegP=regPs(0)
    var bestHR=Double.MinValue
    var bestF:Option[LogisticUtilityFunction]=None
    
        
    for (k <- ks)
      for (r <- regPs)
        for (l0 <- l0s)
          for (ls <- lSs)
          {
            val f:Option[LogisticUtilityFunction]=LogisticUtilityFunctionSGDLearner.learn(trainingData, k, dataSize=Some(trainingDataSize), learningRate0=l0, learningRateSpeed=ls, minibatchSize=batchSize, maxIterations=numIterations)
            
            if (f.isDefined)
            {
              val bFunction=sc.broadcast(f.get)
              //Categorical
              /*val rmse=transformRDD(testData).map({case (xCat,xCont,yCat,yCont,v) => (xCat,xCont,None,yCont,v)})//Lose movieId
                                             .map(
                                                {
                                                  case (xCat,xCont,yCat,yCont,v) => val fxy=fv.getValue(xCat,catSizesUsers,xCont,yCat,catSizesMovies,yCont)
                                                                  math.pow(fxy-v,2)
                                                }).mean()*/
              def evaluatePrediction(user:MixedData, movie:MixedData, rating:Boolean, f:LogisticUtilityFunction):Double=
              {
                val fxy=f.getValue(user,movie)
                if (((fxy>0.5) && rating) ||
                    ((fxy<=0.5) && !rating))
                  1.0
                else
                  0.0
              }
              
              val hitRate=testData.map({case (user,movie,rating) => evaluatePrediction(user,movie,rating,bFunction.value)}).mean()
              val hitRateTrain=trainingData.sample(false, 0.01, java.lang.System.nanoTime()).map({case (user,movie,rating) => evaluatePrediction(user,movie,rating,bFunction.value)}).mean()
                                                
              println(s"TESTED - k:$k r:$r l0:$l0 ls:$ls HitRate:$hitRate TrainingHitRate:$hitRateTrain BEST:$bestHR")
              
              //DEBUG
              f.get.writeToFile(MovieLensTransform.ROOT_PATH+s"debug/utility_${k}_${r}_20iterations_${hitRate.toString().replace('.', '_')}HR")
                                                
              if (hitRate>bestHR)
              {
                bestF=f
                bestHR=hitRate
                bestK=k
                bestRegP=r
              }
            }
          }
      
    println(s"FINAL RESULT: k:$bestK r:$bestRegP BEST:$bestHR")
    
    
    return (bestF.get,(bestK,bestRegP,bestHR))
  }
  
  val NORMALIZING_R=1.0
  private def _normalizeColumns(m:BDM[Double]):BDM[Double]=
  {
    for (i <- 0 until m.cols)
    {
      var total=0.0
      for (j <- 0 until m.rows)
        total += m(j,i)*m(j,i)
      total=Math.sqrt(total)
      if (total>NORMALIZING_R)
        for (j <- 0 until m.rows)
          //m(j,i)=m(j,i)/total
          m(j,i)=m(j,i)*NORMALIZING_R/total
    }
    return m
  }
}