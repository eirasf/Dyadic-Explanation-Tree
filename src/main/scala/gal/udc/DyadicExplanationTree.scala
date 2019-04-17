package gal.udc

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.Array._

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.{ Vector => BV }
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
import java.io.File
import java.io.FileOutputStream
import scala.util.Random
import org.json4s.DefaultWriters.W
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.json4s.DefaultWriters.W
import org.apache.hadoop.fs.Path
import org.json4s.DefaultWriters.W
import org.apache.hadoop.fs.FSDataOutputStream


object sparkContextSingleton
{
  @transient private var instance: SparkContext = _
  private val conf : SparkConf = new SparkConf()
                                                //.setAppName("visualization-tree").setMaster("local[4]")
                                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                                .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
                                                .set("spark.kryoserializer.buffer.max", "512")
                                                //.set("spark.driver.maxResultSize", "2048")

  def getInstance(): SparkContext=
  {
    if (instance == null)
      instance = SparkContext.getOrCreate(conf)
    instance
  }  
}

object Log
{
  var filename:Option[String]=None
  def log(s:String)
  {
    if (filename.isEmpty)
      println(s)
    else
    {
      val pw = new PrintWriter(new FileOutputStream(new File(filename.get),true /* append = true */))
      pw.write(s+"\n")
      pw.close
    }
  }
}

class SplitCandidate(val splitValue:Double,
                      val leftWE:Double,
                      val rightWE:Double,
                      val leftRatio:Double,
                      val rightRatio:Double)
{
  //val heuristic= -leftWE-rightWE-Math.pow(leftRatio-rightRatio,2)
  //val heuristic= -leftWE-rightWE-leftRatio*leftRatio-rightRatio*rightRatio
  val weightedEntropy=leftWE*leftRatio+rightWE*rightRatio
  val heuristic= -weightedEntropy
  var quality:Option[Double]=None
  
  override def toString:String=
  {
    f"Split value: $splitValue%.4f ($leftWE%.2f (${leftRatio*100}%.2f%%) / $rightWE%.2f (${rightRatio*100}%.2f%%) ) -> $heuristic%.4f"
  }
}

class VariableFilter(val variable:Option[Int]=None,val minValue:Double=Double.NegativeInfinity,val maxValue:Double=Double.PositiveInfinity) extends Serializable
{
  def check(values:BDV[Double]):Boolean=
  {
    if (variable.isEmpty)
      return true
    return values(variable.get)>minValue && values(variable.get)<maxValue
  }
  override def toString():String=
    if (variable.isEmpty)
      "[]"
    else
      f"${variable.get}%d [$minValue%.4f - $maxValue%.4f]"
}

trait VariableNamer
{
  def getVariableName(varNum:Int):String
  def getSplitNames(varNum:Int, splitValue:Double):(String,String)
}

class DyadicExplanationTree(val filter:VariableFilter, dataEntropyP:Double=0.0, val relativeSize:Double=1.0)
{
  private var _dataEntropy=dataEntropyP
  def dataEntropy:Double=_dataEntropy
  
  private var _leftChild:Option[DyadicExplanationTree]=None
  def leftChild=_leftChild
  def leftChild_=(t:DyadicExplanationTree):Unit=
  {
    _resultingEntropy=None
    _leftChild=Some(t)
  }
  private var _rightChild:Option[DyadicExplanationTree]=None
  def rightChild=_rightChild
  def rightChild_=(t:DyadicExplanationTree):Unit=
  {
    _resultingEntropy=None
    _rightChild=Some(t)
  }
  private var _resultingEntropy:Option[Double]=None
  def resultingEntropy:Double=
  {
    if (leftChild.isDefined)
    {
      if (!_resultingEntropy.isDefined)
        _resultingEntropy=Some((leftChild.get.resultingEntropy*leftChild.get.relativeSize+rightChild.get.resultingEntropy*rightChild.get.relativeSize)/relativeSize)
      return _resultingEntropy.get
    }
    else
      return dataEntropy
  }
  
  def getNumLeaves():Int=
  {
    if (this._leftChild.isEmpty)
      return 1
    val numLeft=if (this._leftChild.isEmpty) 0 else this._leftChild.get.getNumLeaves()
    val numRight=if (this._rightChild.isEmpty) 0 else this._rightChild.get.getNumLeaves()
    return numLeft + numRight
  }
  
  def getNumVariables(level:Int):Int=
  {
    if (this._leftChild.isEmpty)
      return level
    return this._leftChild.get.getNumVariables(level+1) + this._rightChild.get.getNumVariables(level+1)
  }
  
  def quality(lambda:Double):Double=
  {
    return -this.resultingEntropy-lambda*this.getNumVariables(0)
  }
  
  override def toString():String=this.toPrefixedString("")
  
  def toString(namer:VariableNamer):String=this.toPrefixedString("", Some(namer))
  
  def toPrefixedString(prefix:String, namer:Option[VariableNamer]=None):String=
  {
    var stringSplit=if (this.filter.variable.isDefined)
                    {
                      val varName=if (namer.isDefined)
                                    namer.get.getVariableName(this.filter.variable.get)
                                  else
                                    f"${this.filter.variable.get}%d"
                      f" - Split at $varName%s (${this.filter.minValue}%.4f - ${this.filter.maxValue}%.4f)"
                    }
                    else
                      ""
    var s=f"$prefix%s${this.relativeSize*100}%.2f%% - ${this.dataEntropy}%.2f - ${this.resultingEntropy}%.2f${stringSplit}%s"
    if (this.leftChild.isDefined)
      s+="\n"+this.leftChild.get.toPrefixedString(prefix+"\t")
    if (this.rightChild.isDefined)
      s+="\n"+this.rightChild.get.toPrefixedString(prefix+"\t")
    return s
  }
  
  def exportToDot(fileName:String, simple:Boolean=false, namer:Option[VariableNamer]=None)=
  {
    val isHDFS=fileName.substring(0, 7).equals("hdfs://")
    var output:Option[FSDataOutputStream]=None
    
    if (isHDFS)
    {
      val conf = new Configuration()
      val fs= FileSystem.get(conf)
      output = Some(fs.create(new Path(fileName)))
    }
    val writer:PrintWriter=if (isHDFS)
                             new PrintWriter(output.get);
                           else
                             new PrintWriter(fileName);
    
    //WRITE CONTENTS
    writer.println("graph G {")
    _writeContentsToDot(writer, 0, simple, namer)
    writer.println("}")
    writer.close()
    if (isHDFS)
      output.get.close()
  }
  /**
   * @param writer
   * @param nodeId
   * @param simple
   * @return Returns the next available ID
   */
  private def _writeContentsToDot(writer:PrintWriter, nodeId:Int, simple:Boolean=false, namer:Option[VariableNamer]=None):Int=
  {
    val backgroundS=/*if (nodeId<=0)
                      "shape=box;color=\"#AAAAAA\";style=filled;fillcolor=\"#AAAAAA\";"
                    else*/
                      "shape=none;"
    val sVariable=if (this._leftChild.isDefined && this._leftChild.get.filter.variable.isDefined)
                    {
                      if (namer.isEmpty)
                        this._leftChild.get.filter.variable.get.toString()
                      else
                        namer.get.getVariableName(this._leftChild.get.filter.variable.get)
                    }
                    else
                      ""
    
    val strResultingEntropy=if (this._leftChild.isDefined) ("<font color="+"\"#00AA00\""+f"> - ${this.resultingEntropy}%.2f</font>".replace(",", ".")) else ""
                      
    if (!simple || (nodeId<=0))
      //fOut.write('\t%d [%slabel=<\n<font color="blue">%s%%%s</font><font color="#00AA00"> - %s</font><br/>%s>]\n'%(node[8],backgroundS,fFloat(node[6]*100,2),we,resultingWE,nextFilterV))
      //(node[8],backgroundS,fFloat(node[6]*100,2),we,resultingWE,nextFilterV)
      writer.println(s"\t$nodeId [${backgroundS}label=<\n<font color="+"\"blue\""+f">${this.relativeSize*100}%.2f".replace(",", ".")+"%"+f" - ${this.dataEntropy}%.2f</font>".replace(",", ".")+f"$strResultingEntropy%s<br/>$sVariable>]")  
    else
    {
      //fOut.write('\t%d [%slabel=<\n<font color="blue">%s%%</font><font color="#00AA00"> - %s</font><br/>%s>]\n'%(node[8],backgroundS,fFloat(node[6]*100,2),resultingWE,nextFilterV))
      writer.println(s"\t$nodeId [${backgroundS}label=<\n<font color="+"\"blue\""+f">${this.relativeSize*100}%.2f".replace(",", ".")+"%</font>"+f"$strResultingEntropy%s<br/>$sVariable>]")
    }
    
    var nextNodeId=nodeId+1  
    if (this._leftChild.isDefined)
    {
      val splitNames=if (namer.isEmpty)
                        (f"< ${this._leftChild.get.filter.maxValue}%.4f".replace(",", "."),f"> ${this._rightChild.get.filter.minValue}%.4f".replace(",", "."))
                      else
                        namer.get.getSplitNames(this._leftChild.get.filter.variable.get,this._leftChild.get.filter.maxValue)
      val leftNodeId=nextNodeId
      nextNodeId=this._leftChild.get._writeContentsToDot(writer, nextNodeId, simple, namer)
      val rightNodeId=nextNodeId
      nextNodeId=this._rightChild.get._writeContentsToDot(writer, nextNodeId, simple, namer)
      //fOut.write('\t%d -- %d [headlabel="< %s";fontsize=10;labeldistance=2.5;labelangle=40]\n'%(node[8],node[4][8],fFloat(nextFilterS)))
      //(node[8],node[4][8],fFloat(nextFilterS))
      writer.println(s"\t$nodeId -- $leftNodeId [headlabel="+"\""+splitNames._1.replace("\"", "")+"\""+";fontsize=10;labeldistance=2.5;labelangle=40]")
      //fOut.write('\t%d -- %d [label="> %s";fontsize=10]\n'%(node[8],node[5][8],fFloat(nextFilterS)))
      writer.println(s"\t$nodeId -- $rightNodeId [label="+"\""+splitNames._2.replace("\"", "")+"\""+";fontsize=10]")
    }
    nextNodeId
  }
  
  def getPrunedCopy(lambda:Double, level:Int=0):DyadicExplanationTree=
  {
    if (this._leftChild.isEmpty) return this
    val newTree=new DyadicExplanationTree(this.filter, this.dataEntropy, this.relativeSize)
    newTree.leftChild=this._leftChild.get.getPrunedCopy(lambda, level+1)
    newTree.rightChild=this._rightChild.get.getPrunedCopy(lambda, level+1)
    if (-this.relativeSize*(newTree.resultingEntropy-this.dataEntropy)-lambda*(newTree.getNumVariables(level)-level)>0)
      newTree
    else
      new DyadicExplanationTree(this.filter, this.dataEntropy, this.relativeSize) //Same tree but with all branches pruned.
  }
}

object DyadicExplanationTree
{
  var MAX_BINS:Int=10
  var PARTITIONS:Int=128
  val DEFAULT_NUM_CANDIDATES:Int=5
  var MIN_SPLIT_ENTROPY:Double=0.0
  val DEFAULT_MAX_LEVEL:Int=5
  
  def parseTreeFromString(s:String):Option[DyadicExplanationTree]=
  {
    return  _parseTreeFromString(s.split("\n").toList, "")._1
  }
  
  private def _parseTreeFromString(lines:List[String], prefix:String):(Option[DyadicExplanationTree],List[String])=
  {
    lines match
    {
      case Nil => (None,lines)
      case h :: rest =>
        if (h.startsWith(prefix))
        {
          val (left,newRest)=_parseTreeFromString(rest, prefix+"\t")
          val (right,finalRest)=_parseTreeFromString(newRest, prefix+"\t")
          val filter=if (h.contains("Split at"))
                     {
                       val filterPart=h.split("Split at ")(1)
                       val valNum=filterPart.substring(0, filterPart.indexOf(" ")).toInt
                       val range=filterPart.split("\\(")(1)
                       val rangeParts=range.substring(0, range.size-1).split(" - ").map(_.toFloat)
                       new VariableFilter(Some(valNum),rangeParts(0),rangeParts(1))
                     }
                     else
                       new VariableFilter()
          val relativeSize=h.split("%")(0).trim().toFloat/100
          val dataEntropy=h.split(" - ")(1).trim().toFloat
          val resultingEntropy=h.split(" - ")(2).trim().toDouble
          val node:DyadicExplanationTree=new DyadicExplanationTree(filter, dataEntropy, relativeSize)
          node._leftChild=left
          node._rightChild=right
          node._resultingEntropy=Some(resultingEntropy)
          (Some(node),finalRest)
        }
        else
          (None,lines)
    }
  }
  
  private def _showUsageAndExit()=
  {
    println("""Usage: DyadicExplanationTree dataset clusterCounts splitAt output [options]
    Dataset must be a libsvm file
Options:
    -l max level
    -c number of candidate trees to explore at each node""")
    System.exit(-1)
  }
  private def _parseParams(p:Array[String]):Map[String, Any]=
  {
    val m=scala.collection.mutable.Map[String, Any]("max_level" -> DEFAULT_MAX_LEVEL,
                                                    "num_candidates" -> DEFAULT_NUM_CANDIDATES)
    if (p.length<1)
      _showUsageAndExit()
    
    m("dataset")=p(0)
    m("clusterCounts")=p(1)
    m("splitAt")=p(2).toInt
    m("output")=p(3)
    
    var i=4
    while (i < p.length)
    {
      if ((i>=p.length-1) || (p(i).charAt(0)!='-'))
      {
        println("Unknown option: "+p(i))
        _showUsageAndExit()
      }
      val readOptionName=p(i).substring(1)
      val option=readOptionName match
        {
          case "l"   => "max_level"
          case "c"   => "num_candidates"
          case somethingElse => readOptionName
        }
      if (!m.keySet.exists(_==option) && option==readOptionName)
      {
        println("Unknown option:"+readOptionName)
        _showUsageAndExit()
      }
      
      m(option)=p(i+1).toInt
        
      i=i+2
    }
    return m.toMap
  }
  
  private def _getCurrentTimeString():String=(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()))
  
  private def _getSplits(data:RDD[(BDV[Double],BDV[Int])]):Array[List[Double]]=
  {
    val numVars=data.first()._1.length
    val numElems=data.count()
    val variableValuesRDD=data.flatMap({case (values,utilities) => values.toArray.zipWithIndex.map({case (value, variable) => ((value, variable), 1)})})
    var curTime=_getCurrentTimeString()
    Log.log(f"$curTime%s - Computing splits")
    val sortedValuesByVar=variableValuesRDD.reduceByKey(_+_)
                                           .map({case ((value,variable),count) => (variable,List((value,count)))})
                                           .reduceByKey(_++_)
                                           .map({case (variable, values) => (variable, values.sorted)})
    curTime=_getCurrentTimeString()
    val thresholdsByVar=sortedValuesByVar.map({case (variable,valueList) => (variable,_valueListToThresholds(valueList,numElems))})
    return thresholdsByVar.sortBy({case (variable, thresholds) => variable}, true)
                          .map({case (variable,thresholds) => thresholds})
                          .collect()
  }
  
  //Discretizes variable if needed and returns at most MAX_BINS thresholds to be used as split points.
  private def _valueListToThresholds(valueList:List[(Double,Int)],numElems:Double):List[Double]=
  {
    var thresholds:List[Double]=List[Double]()
    var elemCount=0
    if (valueList.size<=MAX_BINS)
    {
      var previous=valueList.head._1
      var current=valueList.tail
      val t=new ListBuffer[Double]()
      while(current.size>0)
      {
        t+=((previous+current.head._1)/2.0)
        previous=current.head._1
        current=current.tail
      }
      thresholds=t.toList
    }
    else
    {
      val desiredNumElems=numElems/MAX_BINS
      var binSpace=desiredNumElems
      var lastIntervalEnd:Option[Double]=None
      var previousValue:Option[Double]=None
      var currentIntervalStart:Option[Double]=None
      val t=new ListBuffer[Double]()
      for ((value,count) <- valueList)
      {
        if (currentIntervalStart.isEmpty)
          currentIntervalStart=Some(value)
        val exceeded=count>2*binSpace
        binSpace-=count
        if (binSpace<=0)
        {
          //Append a threshold at the start of the current interval
          //The threshold that closes this interval will be appended when another interval is completed
          if (lastIntervalEnd.isDefined)
            t.append((lastIntervalEnd.get+currentIntervalStart.get)/2.0)
          else
            t.append(currentIntervalStart.get)
          
          if (!exceeded)
          {
            binSpace=desiredNumElems
            currentIntervalStart=None
            lastIntervalEnd=Some(value)
          }
          else
          {
            if (count>desiredNumElems)//This value merits a bin for itself -> Append another threshold
            {
              if (previousValue.isDefined)
                t.append((previousValue.get+value)/2.0)
              binSpace=desiredNumElems
              currentIntervalStart=None
              lastIntervalEnd=Some(value)
            }
            else
            {
              binSpace=desiredNumElems-count
              currentIntervalStart=Some(value)
              lastIntervalEnd=Some(value)
            }
          }
        }
        previousValue=Some(value)
      }
      
      if (currentIntervalStart.isDefined)
        t.append((lastIntervalEnd.get+currentIntervalStart.get)/2.0)
      else
        t.append(lastIntervalEnd.get)
      thresholds=t.toList
    }
    
    return thresholds
  }
  
  def getTree(data:RDD[(BDV[Double],BDV[Int])], level:Int, numCandidates:Int, itemClusterSizes:Array[Int]):DyadicExplanationTree=
  {
    data.cache()
    val splitList=_getSplits(data)
    
    return _expandNode(data, splitList, List(new VariableFilter()), 1.0, 1.0/*TODO*/, level, numCandidates, itemClusterSizes=itemClusterSizes)
  }
  
  private def _expandNode(data:RDD[(BDV[Double],BDV[Int])], splitList:Array[List[Double]], filters:List[VariableFilter], relativeSize:Double, weightedEntropy:Double, level:Int, numCandidates:Int=DEFAULT_NUM_CANDIDATES, prefix:String="", itemClusterSizes:Array[Int]):DyadicExplanationTree=
  {
    val lastFilter:VariableFilter=if (filters.size>0)
                                    filters.head
                                  else
                                    new VariableFilter()
    
    val tree=new DyadicExplanationTree(lastFilter,weightedEntropy,relativeSize)
    
    if (level>0)
    {
      var nodeDataRDD=data.filter({case (variables, utilities) => _filterElem(filters,variables)})
      if (!nodeDataRDD.isEmpty())
      {
        nodeDataRDD=nodeDataRDD.repartition(PARTITIONS)
        
        //Remove splits that are outside the filters.
        val nodeSplitList=splitList.clone()
        for (f <- filters)
          if (f.variable.isDefined)
            nodeSplitList(f.variable.get)=nodeSplitList(f.variable.get).filter(x => x>f.minValue && x<f.maxValue) //TODO Check equalities
        
        val splitCounts=_getSplitCounts(nodeDataRDD,nodeSplitList)
        val sc=sparkContextSingleton.getInstance()
        val bClusterSizes=sc.broadcast(itemClusterSizes)
        val allCandidates=splitCounts.flatMap(
                                              {
                                                case (varNum,splitListCounts) =>
                                                  val cCounts=bClusterSizes.value
                                                  val best=_getBestQualitySplitPoint(splitListCounts, cCounts)
                                                  if (best.isEmpty)
                                                    None
                                                  else
                                                    Some(List((varNum,best.get)))
                                              })
        
        if (!allCandidates.isEmpty())
        {
          def mergeSortedNLists(l1:List[(Int,SplitCandidate)], l2:List[(Int,SplitCandidate)], maxElems:Int):List[(Int,SplitCandidate)]=
          {
            return (l1++l2).sortBy({case (varNum, splitCandidate) => -splitCandidate.heuristic}).take(maxElems)
          }
          
          val bestSplitCandidates=allCandidates.reduce({case (l1,l2) => mergeSortedNLists(l1,l2,numCandidates)})
          var bestSplit:Option[(DyadicExplanationTree,DyadicExplanationTree,Int,Double)]=None
          var bestWE:Double=Double.PositiveInfinity
          for (((varNum,splitCandidate),i) <- bestSplitCandidates.zipWithIndex)
          {
            Log.log(f"${_getCurrentTimeString()}%s - ## Exploring $prefix%s -> $i%d ($varNum%d ${splitCandidate.splitValue}%.4f - ${splitCandidate.heuristic}%.4f)")
            if (splitCandidate.weightedEntropy<weightedEntropy)//Don't explore splits with more entropy than the current node. Can there be such splits??
            {
              val newLeftFilter=new VariableFilter(Some(varNum),Double.NegativeInfinity,splitCandidate.splitValue)
              val leftFilters=newLeftFilter :: filters
              val newRightFilter=new VariableFilter(Some(varNum),splitCandidate.splitValue,Double.PositiveInfinity)
              val rightFilters=newRightFilter :: filters
              val leftChild:DyadicExplanationTree=if (splitCandidate.leftWE>MIN_SPLIT_ENTROPY)
                                          _expandNode(data, nodeSplitList, leftFilters, splitCandidate.leftRatio*relativeSize, splitCandidate.leftWE, level-1, numCandidates, prefix +f" -> $i%dL", itemClusterSizes=itemClusterSizes)
                                        else
                                          new DyadicExplanationTree(newLeftFilter, splitCandidate.leftWE, splitCandidate.leftRatio*relativeSize)
              val rightChild:DyadicExplanationTree=if (splitCandidate.rightWE>MIN_SPLIT_ENTROPY)
                                          _expandNode(data, nodeSplitList, rightFilters, splitCandidate.rightRatio*relativeSize, splitCandidate.rightWE, level-1, numCandidates, prefix +f" -> $i%dR", itemClusterSizes=itemClusterSizes)
                                        else
                                          new DyadicExplanationTree(newRightFilter, splitCandidate.rightWE, splitCandidate.rightRatio*relativeSize)
              val resultingSplitWE=leftChild.resultingEntropy*leftChild.relativeSize+rightChild.resultingEntropy*rightChild.relativeSize
              if (resultingSplitWE<bestWE)
              {
                bestWE=resultingSplitWE
                bestSplit=Some((leftChild,rightChild,varNum,splitCandidate.splitValue))
              }
            }
          }
          if (bestSplit.isDefined)
          {
            val (bestLeft,bestRight,bestVar,bestSplitNum)=bestSplit.get
            tree.leftChild=bestLeft
            tree.rightChild=bestRight
          }
          Log.log(f"${_getCurrentTimeString()}%s - ## Closing $prefix%s")
        }
      }
    }
    return tree
  }
  
  private def _filterElem(filters:List[VariableFilter], elem:BDV[Double]):Boolean=
  {
    filters.foreach({case f => if (!f.check(elem)) return false})
    return true
  }
  
  private def _getSplitCounts(data:RDD[(BDV[Double],BDV[Int])], splitList:Array[List[Double]]):RDD[(Int, List[(Int, Double, Int, BDV[Int])])]=
  {
    val sumsByVarSplitAndClass=data.flatMap(
                                      {
                                        case (variables,utilities) =>
                                          _getSplitNumbers(variables,splitList).map(
                                                                                {
                                                                                  case (varNum,splitNum,splitValue) =>
                                                                                    ((varNum,splitNum,splitValue),(1,utilities))
                                                                                })
                                      })
                                  .reduceByKey({case ((count1,utilities1),(count2,utilities2)) => (count1+count2,utilities1+utilities2)})
    val splitTotals=sumsByVarSplitAndClass.map({case ((varNum,splitNum,splitValue),(count,utilities)) => (varNum,(splitNum,splitValue,count,utilities))})
                                          .aggregateByKey(List[(Int,Double,Int,BDV[Int])]())({case (currentList,(splitNum,splitValue,count,utilities)) => currentList ++ List((splitNum,splitValue,count,utilities))},
                                                                                      {case (l1, l2) => l1++l2})
    return splitTotals
  }
  
  //Given a variable value v, returns the number of the variable and the number of the split where it belongs.
  //Same as above, but with the values and splits of many variables at once.
  // Example: getSplitNumbers([4.0,0],[[-2.0,1.0,2.0,3.0,5.0,20.0][0.5]]) -> [(0,4,5.0),(1,0,1.0)]
  private def _getSplitNumbers(variables:BDV[Double], splits:Array[List[Double]]):List[(Int,Int,Double)]=
  {
    return variables.iterator
                     .zip(splits.iterator)
                     .map(
                         {
                           case ((index,value),thresholds) => 
                             val previousLength=thresholds.length
                             val larger=thresholds.dropWhile(x => x<=value)
                             val newLength=larger.length
                             val topThreshold=if (newLength>0)
                                                larger.head
                                              else
                                                Double.PositiveInfinity
                             (index,previousLength-newLength,topThreshold)
                         }).toList
  }
  
  private def _getBestQualitySplitPoint(splitTotalsForVar:List[(Int, Double, Int, BDV[Int])], itemClusterSizes:Array[Int]):Option[SplitCandidate]=
  {
    def accumulate(t1:(Int,BDV[Int]),t2:(Int,BDV[Int])):(Int,BDV[Int])=
    {
      return (t1._1+t2._1,t1._2+t2._2)
    }
    
    val (totalNumElems,totalPositiveUtilities)=splitTotalsForVar.map({case (splitNum,splitValue,count,utilities) => (count,utilities)})
                                                                .reduceLeft(accumulate)
    var bestCandidate:Option[SplitCandidate]=None
    var leftNumElems:Int=0
    var leftPositiveUtilities:BDV[Int]=BDV.zeros[Int](totalPositiveUtilities.size)
    val sortedSplits=splitTotalsForVar.sortBy(_._1)
    for ((splitNum,splitValue,count,utilities) <- sortedSplits)
    {
      //println(f"$splitNum%d\t| $splitValue%.4f\t| $count%d")//DEBUG
      leftNumElems+=count
      leftPositiveUtilities+=utilities
      val leftRatio=leftNumElems.toDouble/totalNumElems
      val rightRatio=1.0-leftRatio
      if ((leftRatio>0) && (rightRatio>0))
      {
        val leftWE=computeClusterWeightedEntropy(leftNumElems,leftPositiveUtilities,itemClusterSizes)
        val rightWE=computeClusterWeightedEntropy(totalNumElems-leftNumElems,totalPositiveUtilities-leftPositiveUtilities,itemClusterSizes)
        val c=new SplitCandidate(splitValue,leftWE,rightWE,leftRatio,rightRatio)
        if (bestCandidate.isEmpty || bestCandidate.get.heuristic<c.heuristic)
        {
          bestCandidate=Some(c)
          if (splitValue>10000000)
            println(f"###########################\n\n####\n\n###########################\nWRONGLY SELECTED $splitNum%d") //DEBUG
        }
      }
    }
    return bestCandidate
  }
  
  def computeClusterWeightedEntropy(numElems:Int, positiveUtilityCounts:BDV[Int], itemClusterSizes:Array[Int]):Double=
  {
    if (numElems<=0)
      return 0.0
    
    def log(x:Double, base:Double):Double=
    {
        return (Math.log(x) / Math.log(base));
    }
      
    val totalItems=itemClusterSizes.sum
    return positiveUtilityCounts.iterator
                            .map(
                              {
                                case (i,v) =>
                                  val p=v.toDouble/numElems
                                  var h=if (p>0)
                                          -p*log(p,2)
                                        else
                                          0
                                  if (p<1)
                                    h+= -(1-p)*log(1-p,2)
                                  h*itemClusterSizes(i)/totalItems
                              })
                            .reduce(_+_)
  }
  
  def main(args: Array[String])
  {
    if (args.length <= 3)
    {
      _showUsageAndExit()
      return
    }
    
    val options=_parseParams(args)
    
    val datasetFile=options("dataset").asInstanceOf[String]
    val clusterCountsFile=options("clusterCounts").asInstanceOf[String]
    val splitAt=options("splitAt").asInstanceOf[Int]
    
    /*DEBUG*/
    //Stop annoying INFO messages
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    /**/
    
    val sc=sparkContextSingleton.getInstance()
    val itemClusterSizes=sc.textFile(clusterCountsFile, 1).map({case s=> val parts=s.substring(1,s.size-1).split(",")
                                                                         (parts(0).toInt,parts(1).toInt)})
                                                          .sortBy(_._1, true)
                                                          .map(_._2)
                                                          .collect()
    
    val dataset=MLUtils.loadLibSVMFile(sc, datasetFile)
                       //.sample(false, 0.001, 165731)//DEBUG
    
    Log.filename=Some(options("output").asInstanceOf[String])
    //val heuristicStr="-leftWE-rightWE-Math.pow(leftRatio-rightRatio,2)"
    //val heuristicStr="-leftWE-rightWE-leftRatio*leftRatio-rightRatio*rightRatio"
    val heuristicStr="-weightedEntropy"
    Log.log(f"${sc.applicationId}%s - Default parallelism:${sc.defaultParallelism}%d \nStarted at ${_getCurrentTimeString()}%s\nProcessing $datasetFile%s (${dataset.count()}%d elements)\n${options("max_level").asInstanceOf[Int]}%d levels\n${options("num_candidates").asInstanceOf[Int]}%d candidates\nHeuristic: $heuristicStr%s\n")
    
    val tupleData=dataset.map(
        {
          case x =>
            val b=new BDV(x.features.toArray)
            (b(0 to splitAt-1),b(splitAt to -1).map(v => if (v>0) {1} else {0}))
        })
    
    /*
    //DEBUG
    val (count,positiveUtilities)=tupleData.map({case (vars, utilities) => (1,utilities)}).reduce({case (a,b) => (a._1+b._1,a._2+b._2)})
    println(_computeClusterWeightedEntropy(count,positiveUtilities,itemClusterSizes))
    System.exit(0)
    val filters:List[VariableFilter]=List(new VariableFilter(Some(24),maxValue=0.5),new VariableFilter(Some(4),minValue=0.5),new VariableFilter(Some(13),maxValue=0.5))
    val data=tupleData
    val splitList=_getSplits(data)
    var nodeDataRDD=data.filter({case (variables, utilities) => _filterElem(filters,variables)})
    nodeDataRDD.cache()
    val splitCounts=_getSplitCounts(nodeDataRDD,splitList).filter(_._1==4).foreach(println)
    val bClusterSizes=sc.broadcast(itemClusterSizes)
    splitCounts.flatMap(
                        {
                          case (varNum,splitListCounts) =>
                            val cCounts=bClusterSizes.value
                            val best=_getBestQualitySplitPoint(splitListCounts, cCounts)
                            if (best.isEmpty)
                              None
                            else
                              Some(List((varNum,best.get)))
                        })
              .filter({case list => list(0)._1==4})
              .foreach(println)
    def mergeSortedNLists(l1:List[(Int,SplitCandidate)], l2:List[(Int,SplitCandidate)], maxElems:Int):List[(Int,SplitCandidate)]=
    {
      return (l1++l2).sortBy({case (varNum, splitCandidate) => -splitCandidate.heuristic}).take(maxElems)
    }
    
    val bestSplitCandidates=allCandidates.reduce({case (l1,l2) => mergeSortedNLists(l1,l2,5)})
    var bestSplit:Option[(DyadicExplanationTree,DyadicExplanationTree,Int,Double)]=None
    var bestWE:Double=Double.PositiveInfinity
    for (((varNum,splitCandidate),i) <- bestSplitCandidates.zipWithIndex)
      println(s"$varNum $splitCandidate")
    */  
        
    val treeString=getTree(tupleData, options("max_level").asInstanceOf[Int], options("num_candidates").asInstanceOf[Int],itemClusterSizes).toString()
    Log.log(f"Finished at ${_getCurrentTimeString()}%s\n\n$treeString%s")
    
    /**/
    //Stop the Spark Context
    sc.stop()
  }
}