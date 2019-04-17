package gal.udc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import PositiveMissingNegative.Missing
import PositiveMissingNegative.Negative
import PositiveMissingNegative.Positive
import PositiveMissingNegative.PositiveMissingNegative
import breeze.linalg.{ DenseVector => BDV }
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.mllib.clustering.KMeans




object Movie extends VariableNamer
{
  val GENRES=List[String]("(no genres listed)","Action","Adventure","Animation","Children","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","IMAX","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western")
  val MIN_YEAR=1891
  val MAX_YEAR=2015
  val LABELS_SIZE=1128
  
  private var _tagCache:Option[Map[Int,String]]=None
  
  def getTagName(tNum:Int):String=
  {
    if (_tagCache.isEmpty)
    {
      val sc=sparkContextSingleton.getInstance()
      val dataset=sc.textFile(MovieLensTransform.TAG_NAMES_FILE, MovieLensTransform.NUM_PARTITIONS)
      val transformedDataset:RDD[(Int,String)]=dataset.flatMap(
          {
            case "tagId,tag" => None
            case line =>
              val (n,r)=line.splitAt(line.indexOf(","))
              Some((n.toInt-1,r.substring(1)))
          })
      
      _tagCache=Some(transformedDataset.collect().toMap)
      sc.stop()
    }
    val tags=_tagCache.get
    if (!tags.contains(tNum))
      "WRONG TAG"
    return tags(tNum)
  }
  
  def getVariableName(varNum:Int):String=
  {
    varNum match
    {
      case x if 0 until GENRES.size contains x => GENRES(x)
      case x if GENRES.size until GENRES.size+LABELS_SIZE-1 contains x => Movie.getTagName(x-GENRES.size)
      case x if x==GENRES.size+LABELS_SIZE-1 => "Year"
      case default => "WRONG VARIABLE"
    }
  }
  
  def getSplitNames(varNum:Int, splitValue:Double):(String,String)=
  {
    varNum match
    {
      case x if 0 until GENRES.size contains x =>
        //val v=getVariableName(varNum)
        (s"👎",s"👍")
      case x if GENRES.size until GENRES.size+LABELS_SIZE-1 contains x =>
        //val v=getVariableName(varNum)
        //(f"$v%s < $splitValue%.4f",f"$v%s > $splitValue%.4f")
        (f"< $splitValue%.4f".replace(",", "."),f"> $splitValue%.4f".replace(",", "."))
      case x if x==GENRES.size+LABELS_SIZE-1 =>
        val year=splitValue*(Movie.MAX_YEAR-Movie.MIN_YEAR)+Movie.MIN_YEAR
        (f"< $year%.1f",f"> $year%.1f")
      case default => ("WRONG VARIABLE","WRONG VARIABLE")
    }
  }
}

class Movie(val id:Long, val year:Double, val genres:BDV[Boolean], val labels:Option[BDV[Double]], labelsPlusYear:BDV[Double]) extends MixedData(Some(labelsPlusYear),Some(genres),None)
{
  def this(id:Long, year: Double, genres:BDV[Boolean], labels:Option[BDV[Double]])=
  {
    this(id, year, genres, labels, if (labels.isEmpty)
                                     BDV(year)
                                   else
                                     BDV.vertcat(labels.get, BDV(year)))
  }
}

object User extends VariableNamer
{
  private var _movieNameCache:Option[Map[Int,String]]=None
  
  def getMovieName(movieId:Int):String=
  {
    if (_movieNameCache.isEmpty)
    {
      val sc=sparkContextSingleton.getInstance()
      val dataset=sc.textFile(MovieLensTransform.MOVIES_FILE, MovieLensTransform.NUM_PARTITIONS)
    
      val transformedDataset=dataset.flatMap(
          {
            case "movieId,title,genres" => None
            case line =>
              val parts=MovieLensTransform.getMoviesLineParts(line)
                Some((parts._1.toInt, parts._2))
          })
      val movieIdMap=sc.textFile(MovieLensTransform.ROOT_PATH+s"relevantMovieIds5000.txt")
                     .map({case x =>
                              var parts=x.substring(1, x.size-1).split(",")
                              (parts(0).toDouble.toInt,parts(1).toInt)})
      val mapRDD=transformedDataset.join(movieIdMap).map({case (movieId,(title,assignedId)) => (assignedId,title)})
      _movieNameCache=Some(mapRDD.collect().toMap)
    }
    _movieNameCache.get.getOrElse(movieId, "WRONG_VARIABLE")
  }
  def getVariableName(varNum:Int):String=
  {
    varNum match
    {
      case 0 => "NO_MOVIE"
      case default => getMovieName(varNum)
    }
  }
  
  def getSplitNames(varNum:Int, splitValue:Double):(String,String)=
  {
    varNum match
    {
      case default => val v=getMovieName(varNum)
                      if (splitValue<0)
                        //(s"👎",s"❓ | 👍")
                        (s"👎",s"👍")//With prediction
                      else
                        //(s"❓ | 👎",s"👍")
                        (s"👎",s"👍")//With prediction
    }
  }
}

class User(val id:Long, var popularMovieRatings:BDV[PositiveMissingNegative]) extends MixedData(None,None,Some(popularMovieRatings))
{
}

object MovieLensTransform
{
  val ROOT_PATH="/mnt/NTFS/owncloud/Datasets/MovieLens/ml-20m/"
  val NUM_PARTITIONS=7
  //val ROOT_PATH="hdfs:///user/ulccocef/movielens/"
  //val NUM_PARTITIONS=8192
  val MOVIES_FILE=ROOT_PATH+"movies.csv"
  val TAGS_FILE=ROOT_PATH+"genome-scores.csv"
  val TAG_NAMES_FILE=ROOT_PATH+"genome-tags.csv"
  val RATINGS_FILE=ROOT_PATH+"ratings.csv"
  
  //Positive if rating>=4.0
  private def isPositiveRating(r:Double):Boolean=r>=4.0
  
  def getMoviesLineParts(line:String):(String,String,String)=
  {
    val left=line.takeWhile(_!=',')
    val right=line.takeRight(line.length()-line.lastIndexOf(",")-1)
    val center=line.substring(left.length()+1,line.lastIndexOf(","))
    return (left,center,right)
  }
  
  private def userMovieRatingToLabeledPoint(user:User, movie:Movie, rating:Boolean):LabeledPoint=new LabeledPoint(if (rating) 1.0 else 0.0, Vectors.dense(BDV.vertcat(user.toDenseVector(),movie.toDenseVector()).toArray))
  
  private def getYearFromTitle(title:String):Option[Int]=
  {
    if (!title.contains("(") || !title.contains(")"))
      return None
    val yearString=title.substring(title.lastIndexOf("(")+1, title.lastIndexOf(")"))
    var year:Option[Int]=None
    try
    {
      year=Some(yearString.toInt)
    }
    catch
    {
      case e:Exception => year=None
    }
    return year
  }
  private def moviesLineToTuple(line:String):Option[(Long,Int,BDV[Boolean])]=
  {
    val fields=getMoviesLineParts(line)
    val genreVector=Array.fill[Boolean](Movie.GENRES.length)(false)
    for ((g, i) <- Movie.GENRES.zipWithIndex)
    {
      genreVector(i)=if (fields._3.contains(g))
                    true
                  else
                    false
    }
    val year=getYearFromTitle(fields._2)
    if (year.isDefined)
      return Some((fields._1.toLong, year.get, new BDV(genreVector)))
    else
      return None
  }
  private def printGenres(moviesFilePath:String)
  {
    val sc=sparkContextSingleton.getInstance()
    
    val dataset=sc.textFile(moviesFilePath, NUM_PARTITIONS)
    
    val transformedDataset=dataset.flatMap(
        {
          case "movieId,title,genres" => None
          case line => getMoviesLineParts(line)._3.split("\\|") //Check genres
        })
    
    transformedDataset.distinct().foreach(println) //Check genres
  }
  private def readMovies(withGenome:Boolean, onlyRated:Boolean=true):RDD[Movie]=
  {
    val sc=sparkContextSingleton.getInstance()
    
    val dataset=sc.textFile(MOVIES_FILE, NUM_PARTITIONS)
    
    val transformedDataset=dataset.flatMap(
        {
          case "movieId,title,genres" => None
          case line => moviesLineToTuple(line)
        })
        
    val (minYear,maxYear)=transformedDataset.map({case (id,year,genreVector) => (year,year)}).reduce({case ((min1,max1),(min2,max2)) => (math.min(min1,min2),math.max(max1,max2))})
    val yearRange=maxYear-minYear.toDouble
    val normalizedDataset=transformedDataset.map({case (id,year,genreVector) => (id,((year-minYear)/yearRange,genreVector))})
    
    val sourceDataset=if (onlyRated)
                        normalizedDataset.join(transformRatings().map(
                                                                    {
                                                                      case (userId,movieId,rating) => (movieId,1)
                                                                    })
                                                                  .distinct()
                                               )
                                         .map({case (id,((year,genres),temp)) => (id,(year,genres))})
                      else
                        normalizedDataset
    
    val moviesRDD:RDD[Movie]=if (withGenome)
                              {
                                val movieTagsRDD=readGenomeTags()
                                val bTags=sc.broadcast(movieTagsRDD.collect().toMap)
                                sourceDataset.flatMap({case (id,(year,genreVector)) =>
                                                          val tagMap=bTags.value
                                                          if (tagMap.contains(id))
                                                            Some(new Movie(id,year,genreVector,tagMap.get(id)))
                                                          else
                                                            None
                                                          })
                              }
                             else
                              sourceDataset.map({case (id,(year,genreVector)) => new Movie(id,year,genreVector,None)})
    
    return moviesRDD
  }
  private def transformRatings():RDD[(Long,Long,Boolean)]=
  {
    val sc=sparkContextSingleton.getInstance()
    
    val dataset=sc.textFile(RATINGS_FILE, NUM_PARTITIONS)
    
    val transformedDataset=dataset.flatMap(
        {
          case "userId,movieId,rating,timestamp" => None
          case line => val parts=line.split(",")
                       Some((parts(0).toLong,parts(1).toLong,parts(2).toDouble))
        })
    
    //FOR REGRESSION
    //val meanRating=transformedDataset.map(_._3).mean()
    //val ratingStd=transformedDataset.map(_._3).stdev()
    //val standardizedDataset=transformedDataset.map({case (userId,movieId,rating) => (userId,movieId,(rating-meanRating)/ratingStd)})
    //return standardizedDataset
        
    //FOR BINARY CLASSIFICATION
    return transformedDataset.map({case (userId,movieId,rating) => (userId,movieId,isPositiveRating(rating))})
  }
  private def readGenomeTags():RDD[(Long,BDV[Double])]=
  {
    val sc=sparkContextSingleton.getInstance()
    
    val dataset=sc.textFile(TAGS_FILE, NUM_PARTITIONS).filter(_!="movieId,tagId,relevance")
    val parts=dataset.map({case line => val parts=line.split(",")
                                          (parts(0).toLong,(parts(1).toInt,parts(2).toDouble))})
                                          
    val maxTagId=parts.map(_._2._1).max()
                                          
    return parts.groupByKey().map({case (movieId, tagValues) =>
                                      val tagArray=new Array[Double](maxTagId)
                                      for ((tagId,relevance) <- tagValues)
                                      {
                                        tagArray(tagId-1)=relevance
                                      }
                                      (movieId, BDV(tagArray))
                                      })
  }
  
  def getMovieLensRDD(minRatings:Int):RDD[(Boolean,User,Movie)]=
  {
    val sc=sparkContextSingleton.getInstance()
    
    val moviesRDD=readMovies(false) //No need to have the tags since we will use collect this RDD
    val bGenres=sc.broadcast(moviesRDD.map({case movie => (movie.id,movie)}).collect().toMap)
    //WITH GENOME
    val movieTagsRDD=readGenomeTags()
    val bTags=sc.broadcast(movieTagsRDD.collect().toMap)
    
    val ratingsRDD=transformRatings()
    val filteredRatingsRDD=ratingsRDD.flatMap(
                                  {
                                    case (userId,movieId,rating) =>
                                      val genreMap=bGenres.value
                                      val tagMap=bTags.value
                                      if (!genreMap.contains(movieId) || !tagMap.contains(movieId))
                                        None
                                      else
                                      {
                                        val movie=genreMap(movieId)
                                        val newMovie=new Movie(movieId,movie.year,movie.genres,tagMap.get(movieId))
                                        Some((userId,(newMovie,rating)))
                                      }
                                  }).partitionBy(new HashPartitioner(NUM_PARTITIONS))
                                  
    val userCodificationsRDD=getCodedUsersRDD(minRatings,None)
                              .map({case user => (user.id,user)})
                              .partitionBy(new HashPartitioner(NUM_PARTITIONS))
    return filteredRatingsRDD.join(userCodificationsRDD)
                             .map({case (userId,((movie,rating),user)) => (rating, user, movie)})
  }
  
  private def userRatingsToVector(ratings:Iterable[(Long,Boolean)],movieIdMap:Map[Long,Int]):BDV[PositiveMissingNegative]=
  {
    val v=Array.fill[PositiveMissingNegative](movieIdMap.size)(Missing)
    for ((m, r) <- ratings)
    {
      v(movieIdMap.get(m.toInt).get)=if (r) Positive else Negative //TODO There should be a -1 since the indices for the movies start at one.
    }
    new BDV(v)
  }
  
  def getRelevantMoviesMap(minRatings:Int):Map[Long,Int]=
  {
    val sc=sparkContextSingleton.getInstance()
    
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    if (fs.exists(new org.apache.hadoop.fs.Path(MovieLensTransform.ROOT_PATH+s"calculated/relevantMovieIds-$minRatings.txt")))
    {
      val movieIdMap=sc.textFile(MovieLensTransform.ROOT_PATH+s"calculated/relevantMovieIds-$minRatings.txt")
                     .map({case x =>
                              var parts=x.substring(1, x.size-1).split(",")
                              (parts(0).toDouble.toLong,parts(1).toInt)})
                     .collect().toMap
      return movieIdMap
    }
    
    val ratingsFile=ROOT_PATH+"ratings.csv"
    val ratingsRDD=transformRatings()
    val movieCountsRDD=ratingsRDD.map({case (userId,movieId,rating) => (movieId,1)})
                                 .reduceByKey(_+_)
                                 
    val sortedMovies=movieCountsRDD.sortBy(_._2, false, NUM_PARTITIONS)
    sortedMovies.cache()
    val relevantMovies=sortedMovies.filter(_._2>=minRatings).map(_._1).zipWithIndex().map({case (movieId,index) => (movieId,index.toInt)})
    //relevantMovies.foreach({case (movieId,count) => println(s"$movieId -> $count")})
    println(s"There are ${relevantMovies.count()} relevant movies with at least $minRatings ratings")
    //There are 8474 relevant movies with at least 100 ratings
    //There are 4482 relevant movies with at least 500 ratings
    //There are 3156 relevant movies with at least 1000 ratings
    //There are 1005 relevant movies with at least 5000 ratings
    //There are 462 relevant movies with at least 10000 ratings
    relevantMovies.repartition(1).saveAsTextFile(ROOT_PATH+s"calculated/relevantMovieIds-$minRatings.txt")
    return relevantMovies.collect().toMap
  }
  
  def getCodedUsersRDD(minRatings:Int, utilityFunction:Option[UtilityFunction]):RDD[User]=getCodedUsersRDD(getRelevantMoviesMap(minRatings), utilityFunction)
  
  def getCodedUsersRDD(relevantMovies:Map[Long,Int], utilityFunction:Option[UtilityFunction]):RDD[User]=
  {
    val sc=sparkContextSingleton.getInstance()
    val bMovieIdMap=sc.broadcast(relevantMovies)
    val ratingsRDD=transformRatings()
    val userCodifications=ratingsRDD.flatMap({case (userId,movieId,rating) => 
                                    if (bMovieIdMap.value.contains(movieId))
                                      Some((userId,(movieId,rating)))
                                    else
                                      None})
                          .groupByKey()
                          .map({case (userId,movieRatings) => new User(userId,userRatingsToVector(movieRatings,bMovieIdMap.value))})
    if (utilityFunction.isEmpty)
      return userCodifications
    else
    {
      val bUtility=sc.broadcast(utilityFunction.get)
      val popularMovieCodifications=getMovieCodificationRDD()
                                      .filter({case movie => bMovieIdMap.value.contains(movie.id)})
                                      .map({case movie => (movie.id,movie)})
                                      .collect()
                                      .toMap
      val bPMC=sc.broadcast(popularMovieCodifications)
      val movieUserPairs=userCodifications.flatMap({case user =>
                                            bMovieIdMap.value.keys.map({case movieId => (movieId,user)})})
      val userPredictedCodifications=movieUserPairs.map({case (movieId,user) =>
                                                             val m=bPMC.value
                                                             val f=bUtility.value
                                                             val prediction=f.getValue(user,m(movieId))>0.5
                                                             (user.id,(movieId, prediction))
                                                       })
                                                    .groupByKey()
                                                    .map({case (userId,movieRatings) => new User(userId,userRatingsToVector(movieRatings,bMovieIdMap.value))})
      
      return userPredictedCodifications
    }
  }
  
  def getMovieCodificationRDD():RDD[Movie]=
  {
    return readMovies(true, true)
  }
  
  def getMoviesRDDForExplicability(userCentroids:Array[BDV[Double]],f:LogisticUtilityFunction):RDD[(BDV[Double], BDV[Int])]=
  {
    //userCentroidsPath:String=ROOT_PATH+"calculated/userClusters-unmapped-5000ratings-centroids.libsvm"
    //userCentroids=MLUtils.loadLibSVMFile(sc, userCentroidsPath).map({case p => BDV(p.features.toArray)}).collect()
    //utilityFunctionPath:String=ROOT_PATH+"calculated/utility-raw_4_1.0_5000ratings"
    val sc=sparkContextSingleton.getInstance()
    val bCentroidList=sc.broadcast(userCentroids)
    val bUtilityFunction=sc.broadcast(f)
    
    val filteredRatingsRDD=getMovieCodificationRDD().map(
                                                         {
                                                           case movie =>
                                                              
                                                                  val centroids=bCentroidList.value
                                                                  val centroidRatings=BDV.zeros[Int](centroids.size)
                                                                  val f=bUtilityFunction.value
                                                                  for ((c,i) <- centroids.zipWithIndex)
                                                                  {
                                                                    if (f.getValue(c, movie)>0.5) //WARNING - f was trained with user as binary 
                                                                      centroidRatings(i)=1
                                                                    else
                                                                      centroidRatings(i)=0
                                                                  }
                                                                  (movie.toDenseVector(), centroidRatings)
                                                         })
    return filteredRatingsRDD
  }
  
  def getUsersRDDForExplicability(movieCentroids:Array[BDV[Double]],f:LogisticUtilityFunction,minRatings:Int,predict:Boolean):RDD[(BDV[Double], BDV[Int])]=
  {
    //movieCentroidsPath:String=ROOT_PATH+"calculated/movieClusters-unmapped",
    //movieCentroids=MLUtils.loadLibSVMFile(sc, movieCentroidsPath).map({case p => BDV(p.features.toArray)}).collect()
    //utilityFunctionPath:String=ROOT_PATH+"calculated/utility-raw_4_1.0_5000ratings",
    val sc=sparkContextSingleton.getInstance()
    val bCentroidList=sc.broadcast(movieCentroids)
    val bUtilityFunction=sc.broadcast(f)
    
    val usersRDD=getCodedUsersRDD(minRatings, if (predict) Some(f) else None)
                    .map({case user =>
                              val centroids=bCentroidList.value
                              val centroidRatings=BDV.zeros[Int](centroids.size)
                              val f=bUtilityFunction.value
                              for ((c,i) <- centroids.zipWithIndex)
                              {
                                if (f.getValue(user, c)>0.5) 
                                  centroidRatings(i)=1
                                else
                                  centroidRatings(i)=0
                              }
                              (user.toDenseVector(), centroidRatings)
                          })
    return usersRDD
  }
  
  def saveDyadicRDD(dataRDD:RDD[(MixedData,MixedData,Boolean)],path:String)=
  {
    val (e1,e2,r)=dataRDD.first()
    val binary1=if (e1.getBinaryPart().isDefined)
                  e1.getBinaryPart().get.size
                else
                  0
    val optSignBinary1=if (e1.getOptionalSignedBinaryPart().isDefined)
                      e1.getOptionalSignedBinaryPart().get.size
                    else
                      0
    val numerical1=if (e1.getNumericalPart().isDefined)
                      e1.getNumericalPart().get.size
                    else
                      0
    val binary2=if (e2.getBinaryPart().isDefined)
                  e2.getBinaryPart().get.size
                else
                  0
    val optSignBinary2=if (e2.getOptionalSignedBinaryPart().isDefined)
                      e2.getOptionalSignedBinaryPart().get.size
                    else
                      0
    val numerical2=if (e2.getNumericalPart().isDefined)
                      e2.getNumericalPart().get.size
                    else
                      0
    val fullPath=path+s"_${binary1}_${optSignBinary1}_${numerical1}_${binary2}_${optSignBinary2}_${numerical2}"
    val v=Vectors.dense(BDV.vertcat(e1.toDenseVector(), e2.toDenseVector()).toArray)
    println(s"Saving to $fullPath $numerical2 ${v.size} ${e1.toDenseVector().size} ${e2.toDenseVector().size}")
    val labeledPointRDD=dataRDD.map({case (e1, e2, r) => new LabeledPoint(if (r) 1.0 else 0.0, Vectors.dense(BDV.vertcat(e1.toDenseVector(), e2.toDenseVector()).toArray))})
    SparkSession.builder.getOrCreate().createDataFrame(labeledPointRDD).write.parquet(fullPath)
  }
  
  def readDyadicRDD(sc:SparkContext,path:String):RDD[(MixedData,MixedData,Boolean)]=
  {
    val pathParts=path.replace("_parquet", "").split("_")
    val binary1=pathParts(pathParts.size-6).toInt
    val optionalSignedBinary1=pathParts(pathParts.size-5).toInt
    val numerical1=pathParts(pathParts.size-4).toInt
    val optionalSignedBinary2=pathParts(pathParts.size-3).toInt
    val binary2=pathParts(pathParts.size-2).toInt
    val numerical2=pathParts(pathParts.size-1).toInt
    println(s"Reading from $path")
    //val rawData=MLUtils.loadLibSVMFile(sc, path)
    //SparkSession.builder.getOrCreate().read.parquet(path).printSchema()
    val rawData=SparkSession.builder.getOrCreate().read.parquet(path).rdd.map({case r=>LabeledPoint(r.getDouble(0),r.getAs("features"))}).repartition(NUM_PARTITIONS)
    //print(rawData.first())
    assert(rawData.first().features.size==binary1+optionalSignedBinary1+numerical1+binary2+optionalSignedBinary2+numerical2)
    return rawData.map({case l=> (MixedData.fromDenseVector(BDV(l.features.toArray.slice(0,binary1+optionalSignedBinary1+numerical1)), binary1, optionalSignedBinary1),
                                                    MixedData.fromDenseVector(BDV(l.features.toArray.slice(binary1+optionalSignedBinary1+numerical1,l.features.size)), binary2, optionalSignedBinary2),
                                                    l.label>0.0)})
  }
  
  def main(args: Array[String])
  {
    /*DEBUG*/
    //Stop annoying INFO messages
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    /**/
    
    //MLUtils.saveAsLibSVMFile(getMovieLensRDD(true), "/media/eirasf/NTFS2/movielens-20m-withgenome.libsvm")
    //MLUtils.saveAsLibSVMFile(getMovieLensRDD(false), "/home/eirasf/Escritorio/movielens-20m-nogenome.libsvm")
    /*val withGenomeRDD=getMovieLensRDD(true)
    println(s"Total ratings:${withGenomeRDD.count()}")
    //Total ratings:19800387
    println(s"Total movies:${withGenomeRDD.map(_.features(1)).distinct().count()}")
    //Total movies:10369
    println(s"Total users:${withGenomeRDD.map(_.features(0)).distinct().count()}")
    //Total users:138493*/
    val format = new SimpleDateFormat("yyyyMMdd-HHmmss")
    val startedAt=format.format(Calendar.getInstance().getTime())
    
    val DEFAULT_MIN_RATINGS=5000
    
    if (args.size<1)
      println("""Usage: MovieLensTransform method [params]
    exportDyadic [minRatings]
    toObject inputFile
    toParquet inputFile
    exportCodifications [minRatings]
    learnUtility [datasetPath|minRatings]
    cluster utilityFunctionPath [minRatings]
    kmeans dataset splitPoint
    entropy dataset splitPoint kmeansModel clusterCountsFile
    exportUsers utilityFunctionPath movieClusteringModelPath [minRatings predict]
    exportMovies utilityFunctionPath userClusteringModelPath""")
    else
    {
      args(0).toLowerCase() match
      {
        case "exportdyadic" =>
          if (args.size>2)
            println(s"""Usage: MovieLensTransform exportDyadic [minRatings]
    minRatings  Minimum number of ratings for a movie to be considered a popular movie (default: $DEFAULT_MIN_RATINGS)""")
          val minRatings=if (args.size==2) args(1).toInt else DEFAULT_MIN_RATINGS
          saveDyadicRDD(MovieLensTransform.getMovieLensRDD(minRatings).map({case (rating,user,movie) => (user.asInstanceOf[MixedData],movie.asInstanceOf[MixedData],rating)}), ROOT_PATH+"calculated/ml20m-dataset_")
        case "toobject" =>
          if (args.size>2)
            println(s"""Usage: MovieLensTransform toObject inputFile""")
          val inputFile=args(1)
          readDyadicRDD(sparkContextSingleton.getInstance(),inputFile).saveAsObjectFile(inputFile+"_obj")
        case "toparquet" =>
          if (args.size>2)
            println(s"""Usage: MovieLensTransform toParquet inputFile""")
          val inputFile=args(1)
          val spark=SparkSession.builder.getOrCreate()
          spark.createDataFrame(MLUtils.loadLibSVMFile(sparkContextSingleton.getInstance(), inputFile)).write.parquet(inputFile+"_parquet")
        case "exportcodifications" =>
          if (args.size>2)
            println(s"""Usage: MovieLensTransform exportCodifications [minRatings]
    minRatings  Minimum number of ratings for a movie to be considered a popular movie (default: $DEFAULT_MIN_RATINGS)""")
          val minRatings=if (args.size==2) args(1).toInt else DEFAULT_MIN_RATINGS
          val userCodification=getCodedUsersRDD(minRatings,None).map({case user => LabeledPoint(user.id,Vectors.dense(user.toDenseVector().toArray))})
          val spark=SparkSession.builder.getOrCreate()
          spark.createDataFrame(userCodification).write.parquet(ROOT_PATH+"calculated/userCodification")
          val movieCodification=getMovieCodificationRDD.map({case movie => LabeledPoint(movie.id,Vectors.dense(movie.toDenseVector().toArray))})
          spark.createDataFrame(movieCodification).write.parquet(ROOT_PATH+"calculated/movieCodification")
        case "learnutility" =>
          if (args.size>2)
            println(s"""Usage: MovieLensTransform learnUtility [datasetPath|minRatings]
    minRatings  Minimum number of ratings for a movie to be considered a popular movie (default: $DEFAULT_MIN_RATINGS)""")
          var minRatings=DEFAULT_MIN_RATINGS
          var datasetPath:Option[String]=None
          if (args.size==2)
          {
            if (args(1).forall(Character.isDigit(_)))
              minRatings=args(1).toInt
            else
              datasetPath=Some(args(1))
          }
          val rddData:RDD[(MixedData,MixedData,Boolean)]=
            if (datasetPath.isDefined)
              readDyadicRDD(sparkContextSingleton.getInstance(),datasetPath.get)
            else
              MovieLensTransform.getMovieLensRDD(minRatings).map({case (rating,user,movie) => (user.asInstanceOf[MixedData],movie.asInstanceOf[MixedData],rating)})
          val (bestF,(bestK,bestRegP,bestHR))=LogisticUtilityFunctionSGDLearner.learnUtilityTuned(rddData,l0s=Array[Double](0.1),lSs=Array[Double](0.1), ks=Array[Int](200), regPs=Array[Double](0.0000000000001), batchSize=20, numIterations=200)
          bestF.writeToFile(ROOT_PATH+s"output/utility-coded-raw_${bestK}_${bestRegP}_200iterations_${bestHR.toString().replace('.', '_')}HR")
        
        case "cluster" =>
          if (args.size>3)
            println(s"""Usage: MovieLensTransform cluster utilityFunctionPath [minRatings]
    minRatings  Minimum number of ratings for a movie to be considered a popular movie (default: $DEFAULT_MIN_RATINGS)""")
          val f=LogisticUtilityFunction.readFromFile(ROOT_PATH+args(1))
          val minRatings=if (args.size==3) args(2).toInt else DEFAULT_MIN_RATINGS
          
          val usersRDD=getCodedUsersRDD(minRatings,None).map(_.asInstanceOf[MixedData])
          println(s"Clustering ${usersRDD.count()} users....")
          usersRDD.cache()
          val (userClusters, userClusterCounts)=ClusteringHelper.clusterLeftEntity(usersRDD, Some(f))
          ClusteringHelper.saveClusteringData(userClusters, userClusterCounts, ROOT_PATH+s"output/userClusters")
          usersRDD.unpersist(false)
          
          val moviesRDD=getMovieCodificationRDD().map(_.asInstanceOf[MixedData])
          println(s"Clustering ${moviesRDD.count()} movies....")
          moviesRDD.cache()
          val (movieClusters, movieClusterCounts)=ClusteringHelper.clusterRightEntity(moviesRDD, Some(f))
          ClusteringHelper.saveClusteringData(movieClusters, movieClusterCounts, ROOT_PATH+s"output/movieClusters")
          moviesRDD.unpersist(false)
          
        case "kmeans" =>
          if (args.size!=3)
            println(s"""Usage: MovieLensTransform kmeans dataset splitPoint""")
          
          val datasetFile=args(1)
          val splitAt=args(2).toInt
          val numClusters=32
          val numIterations=40
          val sc=sparkContextSingleton.getInstance()
          val dataset=MLUtils.loadLibSVMFile(sc, datasetFile)
          val tupleData=dataset.map(
              {
                case x =>
                  val b=new BDV(x.features.toArray)
                  //(b(0 to splitAt-1),b(splitAt to -1).map(v => if (v>0) {1} else {0}))
                  b(0 to splitAt-1)
              })
              
          val model=KMeans.train(tupleData.map({case x => Vectors.dense(x.toArray)}), numClusters, numIterations)
          model.save(sc, datasetFile.replace(".libsvm", "_cluster"))
          
        case "entropy" =>
          if (args.size!=3)
            println(s"""Usage: MovieLensTransform entropy dataset splitPoint kmeansModel clusterCountsFile""")
          
          val datasetFile=args(1)
          val splitAt=args(2).toInt
          val clusteringModel=KMeansModel.load(sparkContextSingleton.getInstance(), args(3))
          val numClusters=100
          val numIterations=40
          val sc=sparkContextSingleton.getInstance()
          val itemClusterSizes=sc.textFile(args(4), 1).map({case s=> val parts=s.substring(1,s.size-1).split(",")
                                                                         (parts(0).toInt,parts(1).toInt)})
                                                          .sortBy(_._1, true)
                                                          .map(_._2)
                                                          .collect()
          val dataset=MLUtils.loadLibSVMFile(sc, datasetFile)
          val bModel=sc.broadcast(clusteringModel)
          val bSizes=sc.broadcast(itemClusterSizes)
          val (count,totalEntropyTimesCount)=dataset.map(
              {
                case x =>
                  val b=new BDV(x.features.toArray)
                  val m=bModel.value
                  //(b(0 to splitAt-1),b(splitAt to -1).map(v => if (v>0) {1} else {0}))
                  (m.predict(Vectors.dense(b(0 to splitAt-1).toArray)),(b(splitAt to -1).map(v => if (v>0) {1} else {0}),1))
              })
              .reduceByKey({case ((v1,c1),(v2,c2)) => (v1+v2,c1+c2)})
              .map({case (clId,(v,c)) => (c, c*DyadicExplanationTree.computeClusterWeightedEntropy(c, v, bSizes.value))})
              .reduce({case ((c1,e1),(c2,e2)) => (c1+c2,e1+e2)})
          println(s"Weighted Entropy for $datasetFile is ${totalEntropyTimesCount/count}")
        
        case "exportusers" =>
          if ((args.size<3) || (args.size>4))
            println(s"""Usage: MovieLensTransform exportUsers utilityFunctionPath movieClusteringModelPath [minRatings predict]
    minRatings  Minimum number of ratings for a movie to be considered a popular movie (default: $DEFAULT_MIN_RATINGS)
    predict     Indicates whether to predict the user's opinion regarding the most popular movies or leave """)
          val f=LogisticUtilityFunction.readFromFile(ROOT_PATH+args(1))
          val clusteringModel=KMeansModel.load(sparkContextSingleton.getInstance(), ROOT_PATH+args(2))
          val minRatings=if (args.size>=4) args(3).toInt else DEFAULT_MIN_RATINGS
          val predict=if (args.size>=4) args(4).toLowerCase()=="true" else false
          val movieCentroids=clusteringModel.clusterCenters.map({case x => BDV(x.toArray)})
          val rawUsersRDD=getUsersRDDForExplicability(movieCentroids,f,minRatings,predict)
          println(s"Exporting ${rawUsersRDD.count()} users....")
          val splitAt=rawUsersRDD.first()._1.size
          val usersRDD=rawUsersRDD.map({case (descriptiveVars,utilities) => new LabeledPoint(0.0, Vectors.dense(BDV.vertcat(descriptiveVars,utilities.map(_.toDouble)).toArray))})
          MLUtils.saveAsLibSVMFile(usersRDD.coalesce(1), ROOT_PATH+s"output/users-vs-movies${if (predict) "-with-prediction" else ""}-splitAt$splitAt-$startedAt.libsvm")
          
        case "exportmovies" =>
          if (args.size!=3)
            println(s"""Usage: MovieLensTransform exportMovies utilityFunctionPath userClusteringModelPath""")
          val f=LogisticUtilityFunction.readFromFile(ROOT_PATH+args(1))
          val clusteringModel=KMeansModel.load(sparkContextSingleton.getInstance(), ROOT_PATH+args(2))
          val userCentroids=clusteringModel.clusterCenters.map({case x => BDV(x.toArray)})
          val rawMoviesRDD=getMoviesRDDForExplicability(userCentroids,f)
          println(s"Exporting ${rawMoviesRDD.count()} movies....")
          val splitAt=rawMoviesRDD.first()._1.size
          val moviesRDD=rawMoviesRDD.map({case (descriptiveVars,utilities) => new LabeledPoint(0.0, Vectors.dense(BDV.vertcat(descriptiveVars,utilities.map(_.toDouble)).toArray))})
          MLUtils.saveAsLibSVMFile(moviesRDD.coalesce(1), ROOT_PATH+s"output/movies-vs-users-splitAt$splitAt-$startedAt.libsvm")
        
        case other => println(s"Unknown option: ${other}")
      }
      //Stop the Spark Context
      val sc=sparkContextSingleton.getInstance()
      sc.stop()
    }
  }
}