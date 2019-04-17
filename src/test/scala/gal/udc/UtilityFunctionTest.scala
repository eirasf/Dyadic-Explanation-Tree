package gal.udc

import org.scalatest.FunSuite
import breeze.linalg.{ DenseMatrix=> BDM }
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.norm
import java.io.File
import breeze.linalg.csvread
import breeze.linalg.csvwrite
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.InputStream
import java.io.FileReader

import PositiveMissingNegative._

class MixedVectorProjectorTest extends FunSuite
{
  test("MixedVectorProjector.project")
  {
    val xCont=BDV(0.1,0.2)
    val xCat=BDV(1,0)
    val catSizes=Array[Int](2,2)
    val M=BDM((0.1,0.2,0.3,0.4,0.5,0.6),(-0.1,-0.2,-0.3,-0.4,-0.5,-0.6))
    val r=MixedVectorProjector.project(new CategoricalMixedData(Some(xCont),xCat,catSizes),M)
    
    //assert((r-BDV(0.67,-0.67)).norm()<0.000001)
    assert(norm(r-BDV(0.67,-0.67))<0.000001)
  }
  
  test("MixedVectorProjector.projectBin")
  {
    val xCont=BDV(0.1,0.2)
    val xBin=BDV(false,true,true,false)
    val M=BDM((0.1,0.2,0.3,0.4,0.5,0.6),(-0.1,-0.2,-0.3,-0.4,-0.5,-0.6))
    val r=MixedVectorProjector.project(new MixedData(Some(xCont),Some(xBin),None),M)
    
    assert(norm(r-BDV(0.67,-0.67))<0.000001)
  }
  
  test("MixedVectorProjector.projectFull")
  {
    val xCont=BDV(0.1,0.2)
    val xBin=BDV(false,true,true,false)
    val xOptSignBin=BDV(Positive,Missing,Negative)
    val M=BDM((0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9),(-0.1,-0.2,-0.3,-0.4,-0.5,-0.6,-0.7,-0.8,-0.9))
    val r=MixedVectorProjector.project(new MixedData(Some(xCont),Some(xBin),Some(xOptSignBin)),M)
    
    assert(norm(r-BDV(0.56,-0.56))<0.000001)
  }
  
  test("MixedVectorProjector.vectorProduct")
  {
    val xCont1=BDV(0.1,0.2)
    val xCat1=BDV(1,0)
    val catSizes1=Array[Int](2,2)
    
    val v2=BDV(50.0,100.0)
    
    val r=MixedVectorProjector.vectorProduct(new CategoricalMixedData(Some(xCont1),xCat1,catSizes1),v2)
    val d=r-BDM((0.0,50.0,50.0,0.0,5.0,10.0),(0.0,100.0,100.0,0.0,10.0,20.0))
    
    var dSize=0.0
    
    for (i <- 0 until d.cols)
    {
      val col=d(::,i)
      dSize = dSize + norm(col)
    }
    
    assert(dSize<0.000001)
  }
  
  test("MixedVectorProjector.vectorProductBin")
  {
    val xCont1=BDV(0.1,0.2)
    val xBin=BDV(false,true,true,false)
    
    val v2=BDV(50.0,100.0)
    
    val r=MixedVectorProjector.vectorProduct(new MixedData(Some(xCont1),Some(xBin), None),v2)
    val d=r-BDM((0.0,50.0,50.0,0.0,5.0,10.0),(0.0,100.0,100.0,0.0,10.0,20.0))
    
    var dSize=0.0
    
    for (i <- 0 until d.cols)
    {
      val col=d(::,i)
      dSize = dSize + norm(col)
    }
    
    assert(dSize<0.000001)
  }
  
  test("MixedVectorProjector.vectorProductFull")
  {
    val xCont1=BDV(0.1,0.2)
    val xBin=BDV(false,true,true,false)
    val xOptSignBin=BDV(Positive,Missing,Negative)
    
    val v2=BDV(50.0,100.0)
    
    val r=MixedVectorProjector.vectorProduct(new MixedData(Some(xCont1),Some(xBin),Some(xOptSignBin)),v2)
    val d=r-BDM((0.0,50.0,50.0,0.0,50.0,0.0,-50.0,5.0,10.0),(0.0,100.0,100.0,0.0,100.0,0.0,-100.0,10.0,20.0))
    
    var dSize=0.0
    
    for (i <- 0 until d.cols)
    {
      val col=d(::,i)
      dSize = dSize + norm(col)
    }
    
    assert(dSize<0.000001)
  }
}

class LogisticUtilityFunctionTest extends FunSuite
{
  test("LogisticUtilityFunction.readMatrixFromFile")
  {
    val m=BDM((0.0,50.0,50.0,0.0,5.0,10.0),(0.0,100.0,100.0,0.0,10.0,20.0))
    
    var tempFile = File.createTempFile("prefix", "suffix");
    //println(tempFile.getAbsolutePath)
    csvwrite(tempFile,m)
    
    val r=LogisticUtilityFunction.readMatrixFromFile(new BufferedReader(new FileReader(tempFile)))
    
    //println(r)
    
    val d=m-r
    var dSize=0.0
    
    for (i <- 0 until d.cols)
    {
      val col=d(::,i)
      dSize = dSize + norm(col)
    }
    
    assert(dSize<0.000001)
  }
}
