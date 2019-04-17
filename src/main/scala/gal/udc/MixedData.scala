package gal.udc

import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg.DenseVector
import scala.util.control.Breaks._

object PositiveMissingNegative extends Enumeration
{
  type PositiveMissingNegative = Value
  val Positive, Missing, Negative = Value
}
import PositiveMissingNegative._

object MixedData
{
  def fromDenseVector(v:BDV[Double], numBinary:Int, numOptionalSignedBinary:Int):MixedData=
  {
    val b:Option[BDV[Boolean]]=if (numBinary<=0)
            None
          else
            Some(v(0 to numBinary-1).map(_>0.0))
    val o:Option[BDV[PositiveMissingNegative]]=if (numOptionalSignedBinary<=0)
            None
          else
            Some(v(numBinary to (numBinary+numOptionalSignedBinary-1)).map({case d => if (d==0.0) Missing else if (d==1.0) Positive else Negative}))
    val n:Option[BDV[Double]]=if (numBinary+numOptionalSignedBinary>=v.size)
            None
          else
            Some(v((numBinary+numOptionalSignedBinary) to v.size-1))
    new MixedData(n,b,o)
  }
}

class MixedData(private val numericalPart:Option[BDV[Double]], private val binaryPart:Option[BDV[Boolean]], private val optionalSignedBinaryPart:Option[BDV[PositiveMissingNegative]]) extends Serializable
{
  def getNumericalPart()=numericalPart
  def getBinaryPart()=binaryPart
  def getOptionalSignedBinaryPart()=optionalSignedBinaryPart
  def toDenseVector()=
  {
    val b=if (binaryPart.isEmpty)
            BDV(Array.emptyDoubleArray)
          else
            binaryPart.get.map({case v => if (v) 1.0 else 0.0})
            
    val o=if (optionalSignedBinaryPart.isEmpty)
            BDV(Array.emptyDoubleArray)
          else
            optionalSignedBinaryPart.get.map({case v => v match
                                                    {
                                                      case Positive => 1.0
                                                      case Missing => 0.0
                                                      case Negative => -1.0
                                                    }
                                             })
      
    BDV.vertcat(b,o,numericalPart.getOrElse(BDV(Array.emptyDoubleArray)))
  }
}

object CategoricalMixedData
{
  def fromLabeledPointRDD(lpRDD:RDD[LabeledPoint], firstNumerical:Int, normalizeNumerical:Boolean):RDD[(MixedData,Boolean)]=
  {
    if (!normalizeNumerical)
    {
      val maxs:Array[Int]=lpRDD.map({ x => x.features.toArray.slice(0, firstNumerical) })
                                  .reduce({ (x,y) =>x.zip(y).map({case (a,b) => Math.max(a,b)})})
                                  //.map({ x => if (x>1) x.toInt + 1 else 1})
                                  .map({ x => x.toInt})
      return lpRDD.map({ x => (new CategoricalMixedData(Some(BDV(x.features.toArray.slice(firstNumerical, x.features.size))),
                                                         BDV(x.features.toArray.slice(0, firstNumerical).map(_.toInt)),
                                                         maxs),
                               x.label==1) })
    }
    else
    {
      val maxs:(Array[Double],Array[Double],Int)=lpRDD.map({ x => var cont=x.features.toArray.slice(firstNumerical, x.features.size)
                                                                    (x.features.toArray.slice(0, firstNumerical), cont, 1) })
                                                         .reduce({ (x,y) => (x._1.zip(y._1).map({case (a,b) => Math.max(a,b)}),
                                                                             x._2.zip(y._2).map({case (a,b) => a+b}),
                                                                             x._3+y._3)})
      val maxsDisc:Array[Int]=maxs._1.map(_.toInt)
      val means=maxs._2.map({case x=>x/maxs._3})
      val stddevs=lpRDD.map({case x=>
                                var cont=x.features.toArray.slice(firstNumerical, x.features.size)
                                cont.zip(means).map({case (v,m) => val d=v-m
                                                                   d*d})
                                })
                       .reduce({case (x,y) => x.zip(y).map({case (a,b) => a+b})})
                       .map({case x => math.sqrt(x)})
      val gaussians=means.zip(stddevs)
      return lpRDD.map({ x => var cont=x.features.toArray.slice(firstNumerical, x.features.size)
                              cont=cont.zip(gaussians).map({case (x,(m,s)) =>
                                                              if (math.abs(s)>0)
                                                                (x-m)/s
                                                              else
                                                                0.0
                                                           })
                              (new CategoricalMixedData(Some(BDV(cont)),
                                                         BDV(x.features.toArray.slice(0, firstNumerical).map(_.toInt)),
                                                         maxsDisc),
                               x.label==1) })
    }
  }
}

class CategoricalMixedData(val numericalPart:Option[BDV[Double]], val categoricalPart:BDV[Int], val numValuesForDiscretes:Array[Int]) extends MixedData(numericalPart,None,None)
{
  private val nPart:Option[BDV[Double]]=numericalPart
  /*var dPart:BDV[Double]=numValuesForDiscretes match
                        {
                          case null => discretePart
                          case anything => OneHotEncode(discretePart, anything) 
                        }*/
  private var _cPart:BDV[Int]=categoricalPart
  var _modified=true
  def setCategoricalValue(index:Int, value:Int)=
    {
      _modified=true
      _cPart(index)=value
    }
  var isArtificialAnomaly:Boolean=false
  override def getBinaryPart()=if (_modified)
                               {
                                  _cachedBPart=OneHotEncode(_cPart, numValuesForDiscretes)
                                  _modified=false
                                  Some(_cachedBPart)
                               }
                               else
                                 Some(_cachedBPart)
  //Cache bPart
  var _cachedBPart:BDV[Boolean]=null
  
  val numDiscreteCombinations:Double=numValuesForDiscretes match
                        {
                          case null => scala.math.pow(2,(getBinaryPart().get.length-1))
                          case anything => var total:Double=1
                                            for (i <- 0 until anything.length)
                                              total = total * (anything(i)+1)
                                            total
                        }
  
  
  def OneHotEncode(d:BDV[Int], maxValues:Array[Int]):BDV[Boolean]=
  {
    val binarizedValues=Array.fill[Boolean](maxValues.sum + maxValues.length)(false)
    var i=0
    var current=0
    while (i<d.length)
    {
      //if (maxValues(i)>0)
      //{
        if (current+d(i).toInt>=binarizedValues.length)
          i=0;
        binarizedValues(current+d(i))=true
      //}
      //else
      //  binarizedValues(current)=d(i)
      current=current+(maxValues(i)+1)
      i=i+1
    }
    return BDV(binarizedValues)
  }
  
  override def toString():String=
  {
    "C:"+nPart.toString()+"##D:"+_cPart.toString()
  }
}