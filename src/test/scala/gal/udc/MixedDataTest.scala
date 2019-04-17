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

class MixedDataTest extends FunSuite
{
  test("MixedDataTest.toAndFromDenseVector")
  {
    val b=Some(BDV(true,false))
    val o=Some(BDV(Positive,Missing,Negative,Positive))
    val n=Some(BDV(0.1,0.2))
    
    var mixed=new MixedData(n,b,o)
    var v=mixed.toDenseVector()
    assert(v.equals(MixedData.fromDenseVector(v, b.get.size, o.get.size).toDenseVector()))
    
    mixed=new MixedData(n,None,o)
    v=mixed.toDenseVector()
    assert(v.equals(MixedData.fromDenseVector(v, 0, o.get.size).toDenseVector()))
    
    mixed=new MixedData(n,None,None)
    v=mixed.toDenseVector()
    assert(v.equals(MixedData.fromDenseVector(v, 0, 0).toDenseVector()))
    
    mixed=new MixedData(None,b,o)
    v=mixed.toDenseVector()
    assert(v.equals(MixedData.fromDenseVector(v, b.get.size, o.get.size).toDenseVector()))
    
    mixed=new MixedData(n,b,None)
    v=mixed.toDenseVector()
    assert(v.equals(MixedData.fromDenseVector(v, b.get.size, 0).toDenseVector()))
    
    mixed=new MixedData(None,b,None)
    v=mixed.toDenseVector()
    assert(v.equals(MixedData.fromDenseVector(v, b.get.size, 0).toDenseVector()))
    
    mixed=new MixedData(None,None,o)
    v=mixed.toDenseVector()
    assert(v.equals(MixedData.fromDenseVector(v, 0, o.get.size).toDenseVector()))
  }
}
