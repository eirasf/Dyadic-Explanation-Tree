package gal.udc

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import breeze.linalg.{ DenseVector => BDV }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.broadcast.Broadcast

object ClusteringHelper
{
  private def _clusterVectors(rddVectors:RDD[BDV[Double]], numClusters:Int=100, numIterations:Int=40):KMeansModel=KMeans.train(rddVectors.map({case x => Vectors.dense(x.toArray)}), numClusters, numIterations)
  
  private def _clusterAndCountItems(rddVectors:RDD[BDV[Double]], numClusters:Int=100, numIterations:Int=40):(KMeansModel,RDD[(Int,Int)])=
  {
    val model=_clusterVectors(rddVectors,numClusters,numIterations)
    val counts=_getClusterCounts(rddVectors,model)
    return (model, counts)
  }
  
  def clusterLeftEntity(rddData:RDD[MixedData],utilityFunction:Option[LogisticUtilityFunction], numClusters:Int=100, numIterations:Int=40):(KMeansModel,RDD[(Int,Int)])=
  {
    
    if (utilityFunction.isEmpty)
      return _clusterAndCountItems(rddData.map({case x => x.toDenseVector()}))
    else
    {
      val bUtility=rddData.sparkContext.broadcast(utilityFunction.get)
      val projectedDataRDD=rddData.map({case x =>
                                            val f=bUtility.value
                                            f.mapLeftElement(x)
                                       })
      return _clusterAndCountItems(projectedDataRDD)
    }
  }
  
  def clusterRightEntity(rddData:RDD[MixedData],utilityFunction:Option[LogisticUtilityFunction], numClusters:Int=100, numIterations:Int=40):(KMeansModel,RDD[(Int,Int)])=
  {
    
    if (utilityFunction.isEmpty)
      return _clusterAndCountItems(rddData.map({case x => x.toDenseVector()}))
    else
    {
      val bUtility=rddData.sparkContext.broadcast(utilityFunction.get)
      val projectedDataRDD=rddData.map({case x =>
                                            val f=bUtility.value
                                            f.mapRightElement(x)
                                       })
      return _clusterAndCountItems(projectedDataRDD)
    }
  }
  
  def _getClusterCounts(rddData:RDD[BDV[Double]], model:KMeansModel):RDD[(Int,Int)]=
  {
    val bModel=rddData.sparkContext.broadcast(model)
    val countsPerCluster=rddData.map({case (vector) =>
                                                  val m=bModel.value
                                                val prediction=m.predict(Vectors.dense(vector.toArray))
                                                (prediction,  1)})
                                        .reduceByKey(_+_)
                                                  
    countsPerCluster
  }
  
  def saveClusteringData(model:KMeansModel, countsAndAverages:RDD[(Int,Int)], prefix:String)=
  {
    model.save(sparkContextSingleton.getInstance(), prefix+"_model")
    countsAndAverages.coalesce(1).saveAsTextFile(prefix+"_counts.libsvm")
  }
}