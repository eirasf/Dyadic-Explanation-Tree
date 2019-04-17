package gal.udc

import org.apache.log4j.Level
import org.apache.log4j.Logger

import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }

import PositiveMissingNegative._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.KMeansModel

import vegas._
import vegas.render.WindowRenderer._
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File

object SpeedTests
{
  def savePlot(data:Seq[Map[String,Double]], xVarName:String, yVarName:String, filePath:String)=
  {
    val plot = Vegas("Temp plot")
                  .withData(data)
                  .encodeX(xVarName, Quant)
                  .encodeY(yVarName, Quant)
                  .mark(Line)
    
    implicit val renderer = vegas.render.ShowHTML(str => println(s"The HTML is $str"))
    
    
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(plot.html.pageHTML())
    bw.close()
  }
  
  def saveCSV(data:Seq[Map[String,Double]], filePath:String)=
  {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data(0).keys.map(_.toString()).mkString(";")+"\n")
    data.foreach({case m => bw.write(m.values.map(_.toString()).mkString(";")+"\n")})
    bw.close()
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
    
    /*val v=BDV(1,2,3,4)
    v.toArray.zipWithIndex.foreach(println)
    System.exit(0)*/
    
    /*
    //Plot example
    savePlot(Seq(
                      Map("country" -> 0.1, "population" -> 314.0),
                      Map("country" -> 0.2, "population" -> 64.0),
                      Map("country" -> 0.3, "population" -> 80.0)
                    ),
             "country",
             "population",
             "/home/eirasf/Escritorio/temp.html")
                  
    System.exit(0)
    */
    
    //OUTBRAIN TREE
    val outbrainS:String="""100.00% - 0.4780 - 0.2440
	48.62% - 0.4649 - 0.2759 - Split at 1 (-Infinity - 0.2750)
		26.36% - 0.3789 - 0.1629 - Split at 3 (-Infinity - 0.5000)
			13.18% - 0.3788 - 0.2535 - Split at 367 (-Infinity - 0.7150)
				6.59% - 0.3788 - 0.3718 - Split at 370 (-Infinity - 0.3750)
					3.12% - 0.3683 - 0.3683 - Split at 0 (-Infinity - 0.7250)
					3.47% - 0.3750 - 0.3750 - Split at 0 (0.7250 - Infinity)
				6.59% - 0.3788 - 0.1353 - Split at 370 (0.3750 - Infinity)
					3.13% - 0.1304 - 0.1304 - Split at 411 (-Infinity - 0.0350)
					3.46% - 0.1397 - 0.1397 - Split at 411 (0.0350 - Infinity)
			13.19% - 0.3789 - 0.0724 - Split at 367 (0.7150 - Infinity)
				6.57% - 0.1454 - 0.1201 - Split at 393 (-Infinity - 0.0050)
					2.68% - 0.0913 - 0.0913 - Split at 233 (-Infinity - 0.0050)
					3.89% - 0.1400 - 0.1400 - Split at 233 (0.0050 - Infinity)
				6.62% - 0.1455 - 0.0250 - Split at 393 (0.0050 - Infinity)
					4.41% - 0.0375 - 0.0375 - Split at 450 (-Infinity - 0.0050)
					2.21% - 0.0000 - 0.0000 - Split at 450 (0.0050 - Infinity)
		22.26% - 0.4972 - 0.4097 - Split at 3 (0.5000 - Infinity)
			9.90% - 0.4720 - 0.3006 - Split at 393 (-Infinity - 0.0050)
				4.95% - 0.4720 - 0.4673 - Split at 367 (-Infinity - 0.7150)
					2.32% - 0.4619 - 0.4619 - Split at 397 (-Infinity - 0.0050)
					2.62% - 0.4720 - 0.4720 - Split at 397 (0.0050 - Infinity)
				4.95% - 0.4720 - 0.1340 - Split at 367 (0.7150 - Infinity)
					1.78% - 0.0364 - 0.0364 - Split at 233 (-Infinity - 0.0050)
					3.17% - 0.1888 - 0.1888 - Split at 233 (0.0050 - Infinity)
			12.36% - 0.4972 - 0.4972 - Split at 393 (0.0050 - Infinity)
	51.38% - 0.4721 - 0.2138 - Split at 1 (0.2750 - Infinity)
		27.42% - 0.4044 - 0.1632 - Split at 3 (-Infinity - 0.5000)
			13.71% - 0.4043 - 0.1954 - Split at 367 (-Infinity - 0.7150)
				6.85% - 0.4043 - 0.3908 - Split at 281 (-Infinity - 0.6350)
					2.92% - 0.3727 - 0.3727 - Split at 1 (-Infinity - 0.2950)
					3.93% - 0.4043 - 0.4043 - Split at 1 (0.2950 - Infinity)
				6.85% - 0.4043 - 0.0000 - Split at 281 (0.6350 - Infinity)
					3.43% - 0.0000 - 0.0000 - Split at 587 (-Infinity - 0.0050)
					3.43% - 0.0000 - 0.0000 - Split at 587 (0.0050 - Infinity)
			13.72% - 0.4044 - 0.1311 - Split at 367 (0.7150 - Infinity)
				6.21% - 0.1792 - 0.1493 - Split at 547 (-Infinity - 0.0050)
					3.94% - 0.1742 - 0.1742 - Split at 142 (-Infinity - 0.5000)
					2.28% - 0.1062 - 0.1062 - Split at 142 (0.5000 - Infinity)
				7.50% - 0.1414 - 0.1160 - Split at 547 (0.0050 - Infinity)
					3.69% - 0.1167 - 0.1167 - Split at 564 (-Infinity - 0.0050)
					3.81% - 0.1154 - 0.1154 - Split at 564 (0.0050 - Infinity)
		23.95% - 0.5128 - 0.2718 - Split at 3 (0.5000 - Infinity)
			11.97% - 0.5128 - 0.5040 - Split at 370 (-Infinity - 0.3750)
				5.33% - 0.4982 - 0.4931 - Split at 393 (-Infinity - 0.0050)
					2.49% - 0.4873 - 0.4873 - Split at 397 (-Infinity - 0.0050)
					2.84% - 0.4982 - 0.4982 - Split at 397 (0.0050 - Infinity)
				6.64% - 0.5128 - 0.5128 - Split at 393 (0.0050 - Infinity)
			11.98% - 0.5128 - 0.0396 - Split at 370 (0.3750 - Infinity)
				5.68% - 0.0799 - 0.0661 - Split at 424 (-Infinity - 0.0050)
					2.44% - 0.0356 - 0.0356 - Split at 555 (-Infinity - 0.0050)
					3.25% - 0.0890 - 0.0890 - Split at 555 (0.0050 - Infinity)
				6.29% - 0.0157 - 0.0157 - Split at 424 (0.0050 - Infinity)"""
    
    
    //MOVIES TREE
    val moviesS:String="""100.00% - 0.8577234951448383 - 0.26
	59.93% - 0.33 - 0.17 - Split at 694 (-Infinity - 0.2139)
		50.79% - 0.20 - 0.12 - Split at 49 (-Infinity - 0.2134)
			39.70% - 0.11 - 0.08 - Split at 383 (-Infinity - 0.2110)
				12.18% - 0.18 - 0.16 - Split at 782 (-Infinity - 0.2214)
					7.73% - 0.11 - 0.11 - Split at 357 (-Infinity - 0.1325)
					4.45% - 0.24 - 0.24 - Split at 357 (0.1325 - Infinity)
				27.52% - 0.05 - 0.04 - Split at 782 (0.2214 - Infinity)
					10.91% - 0.06 - 0.06 - Split at 811 (-Infinity - 0.2766)
					16.62% - 0.03 - 0.03 - Split at 811 (0.2766 - Infinity)
			11.08% - 0.35 - 0.27 - Split at 383 (0.2110 - Infinity)
				6.51% - 0.20 - 0.18 - Split at 357 (-Infinity - 0.2026)
					3.06% - 0.11 - 0.11 - Split at 1058 (-Infinity - 0.0146)
					3.45% - 0.25 - 0.25 - Split at 1058 (0.0146 - Infinity)
				4.57% - 0.44 - 0.38 - Split at 357 (0.2026 - Infinity)
					2.98% - 0.30 - 0.30 - Split at 792 (-Infinity - 0.0626)
					1.59% - 0.53 - 0.53 - Split at 792 (0.0626 - Infinity)
		9.14% - 0.68 - 0.45 - Split at 49 (0.2134 - Infinity)
			5.66% - 0.52 - 0.40 - Split at 357 (-Infinity - 0.2026)
				3.94% - 0.38 - 0.32 - Split at 140 (-Infinity - 0.2668)
					3.08% - 0.22 - 0.22 - Split at 159 (-Infinity - 0.1003)
					0.87% - 0.69 - 0.69 - Split at 159 (0.1003 - Infinity)
				1.72% - 0.68 - 0.57 - Split at 140 (0.2668 - Infinity)
					1.06% - 0.44 - 0.44 - Split at 1011 (-Infinity - 0.3961)
					0.66% - 0.77 - 0.77 - Split at 1011 (0.3961 - Infinity)
			3.48% - 0.72 - 0.54 - Split at 357 (0.2026 - Infinity)
				1.17% - 0.65 - 0.56 - Split at 119 (-Infinity - 0.1321)
					0.50% - 0.71 - 0.71 - Split at 768 (-Infinity - 0.1118)
					0.67% - 0.44 - 0.44 - Split at 768 (0.1118 - Infinity)
				2.31% - 0.62 - 0.54 - Split at 119 (0.1321 - Infinity)
					0.67% - 0.29 - 0.29 - Split at 694 (-Infinity - 0.1297)
					1.65% - 0.64 - 0.64 - Split at 694 (0.1297 - Infinity)
	40.07% - 0.75 - 0.40 - Split at 694 (0.2139 - Infinity)
		21.17% - 0.83 - 0.60 - Split at 555 (-Infinity - 0.4411)
			7.48% - 0.72 - 0.56 - Split at 49 (-Infinity - 0.2134)
				3.16% - 0.46 - 0.40 - Split at 550 (-Infinity - 0.1916)
					1.55% - 0.24 - 0.24 - Split at 465 (-Infinity - 0.2119)
					1.61% - 0.55 - 0.55 - Split at 465 (0.2119 - Infinity)
				4.32% - 0.74 - 0.68 - Split at 550 (0.1916 - Infinity)
					1.87% - 0.62 - 0.62 - Split at 1011 (-Infinity - 0.3539)
					2.45% - 0.72 - 0.72 - Split at 1011 (0.3539 - Infinity)
			13.69% - 0.77 - 0.63 - Split at 49 (0.2134 - Infinity)
				3.80% - 0.85 - 0.77 - Split at 357 (-Infinity - 0.1625)
					1.97% - 0.68 - 0.68 - Split at 159 (-Infinity - 0.1003)
					1.83% - 0.87 - 0.87 - Split at 159 (0.1003 - Infinity)
				9.89% - 0.65 - 0.58 - Split at 357 (0.1625 - Infinity)
					6.06% - 0.51 - 0.51 - Split at 119 (-Infinity - 0.1321)
					3.83% - 0.69 - 0.69 - Split at 119 (0.1321 - Infinity)
		18.90% - 0.28 - 0.17 - Split at 555 (0.4411 - Infinity)
			9.94% - 0.10 - 0.07 - Split at 826 (-Infinity - 0.1606)
				2.16% - 0.16 - 0.12 - Split at 462 (-Infinity - 0.4117)
					0.02% - 0.26 - 0.26 - Split at 1022 (-Infinity - 0.0311)
					2.14% - 0.12 - 0.12 - Split at 1022 (0.0311 - Infinity)
				7.78% - 0.06 - 0.05 - Split at 462 (0.4117 - Infinity)
					5.00% - 0.03 - 0.03 - Split at 826 (-Infinity - 0.1234)
					2.78% - 0.08 - 0.08 - Split at 826 (0.1234 - Infinity)
			8.96% - 0.38 - 0.29 - Split at 826 (0.1606 - Infinity)
				5.16% - 0.47 - 0.42 - Split at 555 (-Infinity - 0.6899)
					0.76% - 0.71 - 0.71 - Split at 465 (-Infinity - 0.2703)
					4.40% - 0.37 - 0.37 - Split at 465 (0.2703 - Infinity)
				3.80% - 0.14 - 0.11 - Split at 555 (0.6899 - Infinity)
					3.66% - 0.10 - 0.10 - Split at 119 (-Infinity - 0.2686)
					0.14% - 0.38 - 0.38 - Split at 119 (0.2686 - Infinity)"""
    
    //USERS WITH PREDICTION TREE
    val usersWPS:String="""100.00% - 0.3753595959996144 - 0.02
	75.67% - 0.23 - 0.02 - Split at 631 (-Infinity - 0.0000)
		28.76% - 0.16 - 0.02 - Split at 134 (-Infinity - 0.0000)
			10.54% - 0.12 - 0.02 - Split at 853 (-Infinity - 0.0000)
				4.30% - 0.08 - 0.03 - Split at 799 (-Infinity - 0.0000)
					2.08% - 0.03 - 0.03 - Split at 223 (-Infinity - 0.0000)
					2.22% - 0.04 - 0.04 - Split at 223 (0.0000 - Infinity)
				6.24% - 0.03 - 0.01 - Split at 799 (0.0000 - Infinity)
					3.28% - 0.01 - 0.01 - Split at 657 (-Infinity - 0.0000)
					2.96% - 0.02 - 0.02 - Split at 657 (0.0000 - Infinity)
			18.22% - 0.07 - 0.02 - Split at 853 (0.0000 - Infinity)
				6.30% - 0.02 - 0.01 - Split at 74 (-Infinity - 0.0000)
					1.71% - 0.01 - 0.01 - Split at 681 (-Infinity - 0.0000)
					4.59% - 0.02 - 0.02 - Split at 681 (0.0000 - Infinity)
				11.92% - 0.04 - 0.02 - Split at 74 (0.0000 - Infinity)
					7.25% - 0.02 - 0.02 - Split at 462 (-Infinity - 0.0000)
					4.67% - 0.02 - 0.02 - Split at 462 (0.0000 - Infinity)
		46.92% - 0.10 - 0.01 - Split at 134 (0.0000 - Infinity)
			21.37% - 0.05 - 0.01 - Split at 733 (-Infinity - 0.0000)
				12.18% - 0.02 - 0.01 - Split at 849 (-Infinity - 0.0000)
					4.70% - 0.02 - 0.02 - Split at 527 (-Infinity - 0.0000)
					7.49% - 0.01 - 0.01 - Split at 527 (0.0000 - Infinity)
				9.19% - 0.03 - 0.02 - Split at 849 (0.0000 - Infinity)
					3.98% - 0.02 - 0.02 - Split at 335 (-Infinity - 0.0000)
					5.21% - 0.02 - 0.02 - Split at 335 (0.0000 - Infinity)
			25.55% - 0.06 - 0.01 - Split at 733 (0.0000 - Infinity)
				17.31% - 0.02 - 0.01 - Split at 128 (-Infinity - 0.0000)
					14.17% - 0.01 - 0.01 - Split at 900 (-Infinity - 0.0000)
					3.14% - 0.03 - 0.03 - Split at 900 (0.0000 - Infinity)
				8.24% - 0.03 - 0.02 - Split at 128 (0.0000 - Infinity)
					5.14% - 0.02 - 0.02 - Split at 686 (-Infinity - 0.0000)
					3.10% - 0.02 - 0.02 - Split at 686 (0.0000 - Infinity)
	24.33% - 0.48 - 0.03 - Split at 631 (0.0000 - Infinity)
		14.17% - 0.19 - 0.04 - Split at 594 (-Infinity - 0.0000)
			8.17% - 0.07 - 0.03 - Split at 947 (-Infinity - 0.0000)
				4.38% - 0.06 - 0.04 - Split at 399 (-Infinity - 0.0000)
					1.79% - 0.04 - 0.04 - Split at 798 (-Infinity - 0.0000)
					2.59% - 0.03 - 0.03 - Split at 798 (0.0000 - Infinity)
				3.79% - 0.03 - 0.02 - Split at 399 (0.0000 - Infinity)
					2.46% - 0.01 - 0.01 - Split at 230 (-Infinity - 0.0000)
					1.34% - 0.03 - 0.03 - Split at 230 (0.0000 - Infinity)
			6.00% - 0.12 - 0.04 - Split at 947 (0.0000 - Infinity)
				3.44% - 0.06 - 0.04 - Split at 929 (-Infinity - 0.0000)
					1.59% - 0.05 - 0.05 - Split at 455 (-Infinity - 0.0000)
					1.85% - 0.04 - 0.04 - Split at 455 (0.0000 - Infinity)
				2.55% - 0.08 - 0.05 - Split at 929 (0.0000 - Infinity)
					0.53% - 0.07 - 0.07 - Split at 271 (-Infinity - 0.0000)
					2.02% - 0.04 - 0.04 - Split at 271 (0.0000 - Infinity)
		10.16% - 0.27 - 0.03 - Split at 594 (0.0000 - Infinity)
			5.00% - 0.14 - 0.05 - Split at 765 (-Infinity - 0.0000)
				3.34% - 0.09 - 0.05 - Split at 505 (-Infinity - 0.0000)
					1.43% - 0.05 - 0.05 - Split at 403 (-Infinity - 0.0000)
					1.91% - 0.04 - 0.04 - Split at 403 (0.0000 - Infinity)
				1.66% - 0.07 - 0.04 - Split at 505 (0.0000 - Infinity)
					0.71% - 0.05 - 0.05 - Split at 39 (-Infinity - 0.0000)
					0.95% - 0.04 - 0.04 - Split at 39 (0.0000 - Infinity)
			5.16% - 0.08 - 0.02 - Split at 765 (0.0000 - Infinity)
				1.88% - 0.06 - 0.03 - Split at 793 (-Infinity - 0.0000)
					0.69% - 0.06 - 0.06 - Split at 927 (-Infinity - 0.0000)
					1.19% - 0.02 - 0.02 - Split at 927 (0.0000 - Infinity)
				3.28% - 0.02 - 0.01 - Split at 793 (0.0000 - Infinity)
					0.66% - 0.02 - 0.02 - Split at 815 (-Infinity - 0.0000)
					2.62% - 0.01 - 0.01 - Split at 815 (0.0000 - Infinity)"""
    
     //USERS TREE
     val usersS:String="""100.00% - 0.3753595959996144 - 0.29
	85.65% - 0.32 - 0.27 - Split at 13 (-Infinity - 0.5000)
		17.30% - 0.33 - 0.28 - Split at 4 (-Infinity - -0.5000)
			5.72% - 0.30 - 0.26 - Split at 1 (-Infinity - -0.5000)
				1.68% - 0.26 - 0.24 - Split at 3 (-Infinity - -0.5000)
					0.71% - 0.21 - 0.21 - Split at 27 (-Infinity - -0.5000)
					0.97% - 0.27 - 0.27 - Split at 27 (-0.5000 - Infinity)
				4.04% - 0.29 - 0.27 - Split at 3 (-0.5000 - Infinity)
					1.41% - 0.24 - 0.24 - Split at 11 (-Infinity - -0.5000)
					2.63% - 0.29 - 0.29 - Split at 11 (-0.5000 - Infinity)
			11.58% - 0.32 - 0.29 - Split at 1 (-0.5000 - Infinity)
				3.48% - 0.31 - 0.29 - Split at 36 (-Infinity - -0.5000)
					1.12% - 0.26 - 0.26 - Split at 12 (-Infinity - -0.5000)
					2.36% - 0.31 - 0.31 - Split at 12 (-0.5000 - Infinity)
				8.10% - 0.31 - 0.30 - Split at 36 (-0.5000 - Infinity)
					1.50% - 0.28 - 0.28 - Split at 11 (-Infinity - -0.5000)
					6.59% - 0.30 - 0.30 - Split at 11 (-0.5000 - Infinity)
		68.35% - 0.30 - 0.26 - Split at 4 (-0.5000 - Infinity)
			60.86% - 0.26 - 0.24 - Split at 24 (-Infinity - 0.5000)
				49.61% - 0.22 - 0.22 - Split at 4 (-Infinity - 0.5000)
					41.14% - 0.21 - 0.21 - Split at 1 (-Infinity - 0.5000)
					8.47% - 0.27 - 0.27 - Split at 1 (0.5000 - Infinity)
				11.24% - 0.37 - 0.35 - Split at 4 (0.5000 - Infinity)
					2.55% - 0.34 - 0.34 - Split at 38 (-Infinity - -0.5000)
					8.69% - 0.35 - 0.35 - Split at 38 (-0.5000 - Infinity)
			7.50% - 0.47 - 0.42 - Split at 24 (0.5000 - Infinity)
				6.06% - 0.41 - 0.39 - Split at 95 (-Infinity - 0.5000)
					1.21% - 0.34 - 0.34 - Split at 65 (-Infinity - -0.5000)
					4.86% - 0.41 - 0.41 - Split at 65 (-0.5000 - Infinity)
				1.43% - 0.55 - 0.52 - Split at 95 (0.5000 - Infinity)
					0.95% - 0.50 - 0.50 - Split at 46 (-Infinity - 0.5000)
					0.48% - 0.54 - 0.54 - Split at 46 (0.5000 - Infinity)
	14.35% - 0.54 - 0.44 - Split at 13 (0.5000 - Infinity)
		9.74% - 0.46 - 0.40 - Split at 24 (-Infinity - 0.5000)
			2.68% - 0.40 - 0.36 - Split at 49 (-Infinity - -0.5000)
				1.16% - 0.32 - 0.31 - Split at 34 (-Infinity - -0.5000)
					0.20% - 0.35 - 0.35 - Split at 11 (-Infinity - -0.5000)
					0.96% - 0.30 - 0.30 - Split at 11 (-0.5000 - Infinity)
				1.52% - 0.43 - 0.41 - Split at 34 (-0.5000 - Infinity)
					1.02% - 0.36 - 0.36 - Split at 36 (-Infinity - 0.5000)
					0.49% - 0.52 - 0.52 - Split at 36 (0.5000 - Infinity)
			7.06% - 0.46 - 0.42 - Split at 49 (-0.5000 - Infinity)
				5.98% - 0.41 - 0.39 - Split at 38 (-Infinity - 0.5000)
					4.20% - 0.35 - 0.35 - Split at 36 (-Infinity - 0.5000)
					1.78% - 0.49 - 0.49 - Split at 36 (0.5000 - Infinity)
				1.08% - 0.59 - 0.56 - Split at 38 (0.5000 - Infinity)
					0.23% - 0.53 - 0.53 - Split at 41 (-Infinity - -0.5000)
					0.85% - 0.57 - 0.57 - Split at 41 (-0.5000 - Infinity)
		4.61% - 0.61 - 0.53 - Split at 24 (0.5000 - Infinity)
			3.73% - 0.58 - 0.52 - Split at 95 (-Infinity - 0.5000)
				1.49% - 0.53 - 0.51 - Split at 46 (-Infinity - -0.5000)
					0.40% - 0.42 - 0.42 - Split at 65 (-Infinity - -0.5000)
					1.10% - 0.54 - 0.54 - Split at 65 (-0.5000 - Infinity)
				2.23% - 0.56 - 0.53 - Split at 46 (-0.5000 - Infinity)
					1.32% - 0.52 - 0.52 - Split at 34 (-Infinity - 0.5000)
					0.91% - 0.55 - 0.55 - Split at 34 (0.5000 - Infinity)
			0.88% - 0.63 - 0.56 - Split at 95 (0.5000 - Infinity)
				0.24% - 0.62 - 0.59 - Split at 92 (-Infinity - -0.5000)
					0.07% - 0.60 - 0.60 - Split at 85 (-Infinity - -0.5000)
					0.17% - 0.59 - 0.59 - Split at 85 (-0.5000 - Infinity)
				0.64% - 0.59 - 0.55 - Split at 92 (-0.5000 - Infinity)
					0.41% - 0.59 - 0.59 - Split at 46 (-Infinity - 0.5000)
					0.22% - 0.48 - 0.48 - Split at 46 (0.5000 - Infinity)"""
    
    val data=List((outbrainS,None,"OutbrainTree"),
                  (usersS,Some(User),"usersTree"),
                  (usersWPS,Some(User),"usersTree-withPrediction"),
                  (moviesS,Some(Movie),"moviesTree"))
    /*
    val t=DyadicExplanationTree.parseTreeFromString(outbrainS).get
    val lambda=30e-4
    val tPruned=t.getPrunedCopy(lambda)
    println(tPruned)
    println(s"O - Q:${t.quality(0)}")
    println(s"T - WE:${t.resultingEntropy} Q:${t.quality(lambda)}")
    println(s"P - WE:${tPruned.resultingEntropy} Q:${tPruned.quality(lambda)}")
    System.exit(0)
    */
    for ((s,namer,fName) <- data)
    {
      val t=DyadicExplanationTree.parseTreeFromString(s).get
      val FOLDER=s"/home/eirasf/Escritorio/$fName/"
      t.exportToDot(FOLDER+s"treeFULL.dot",namer=namer)
      
      val maxNV=5*32
      val numPoints=100
      val pruneValues=(0 until numPoints).map({case i =>
                                            val lambda=(1.0/maxNV)*i.toDouble/numPoints
                                            val tPruned=t.getPrunedCopy(lambda)
                                            Map("lambda" -> lambda, "quality" -> t.quality(lambda), "prunedQuality" -> tPruned.quality(lambda), "numGroups" -> tPruned.getNumLeaves().toDouble)})
      
      var firstChangeQuality=(-1)
      var firstChangeNumGroups=(-1)
      val previousQuality=pruneValues(0)("quality")
      val previousNumGroups=pruneValues(0)("numGroups")
      for ((v,i) <- pruneValues.zipWithIndex)
      {
        if ((firstChangeQuality<0) && (v("quality")!=previousQuality))
          firstChangeQuality=i
        if ((firstChangeNumGroups<0) && (v("numGroups")!=previousNumGroups))
          firstChangeNumGroups=i
      }
      
      saveCSV(pruneValues.seq/*.takeRight(pruneValues.size-firstChangeQuality+1)*/, FOLDER+"pruning.csv")
      
      savePlot(pruneValues.seq.takeRight(pruneValues.size-firstChangeQuality+1),
               "lambda",
               "quality",
               FOLDER+"quality.html")
               
          
      savePlot(pruneValues.seq.takeRight(pruneValues.size-firstChangeNumGroups+1),
               "lambda",
               "numGroups",
               FOLDER+"numGroups.html")
      
      val bestValue=pruneValues.map({case m => (m("quality"),m("lambda"))}).max
      val bestLambda=bestValue._2
      val tPruned=t.getPrunedCopy(bestLambda)
      tPruned.exportToDot(FOLDER+s"tree-${bestLambda.toString().replace(".", "-")}_${bestValue._1.toString().replace(".", "-")}.dot",namer=namer)
      val tPruned2=t.getPrunedCopy(0.001)
      tPruned2.exportToDot(FOLDER+s"tree-${0.001.toString().replace(".", "-")}_${tPruned2.quality(0.001).toString().replace(".", "-")}.dot",simple=true,namer=namer)
      println(s"$fName - ${tPruned2.resultingEntropy} - ${tPruned2.quality(0.001)}")
    }
    System.exit(0)
    
    val sc=sparkContextSingleton.getInstance()
    
    val clusteringModel=KMeansModel.load(sparkContextSingleton.getInstance(), "/home/eirasf/Escritorio/userClusters_model")
    val movieCentroids=clusteringModel.clusterCenters.map({case x => BDV(x.toArray)})
    movieCentroids.foreach({case x=>println(x.size)})
    sc.stop()
    System.exit(0)
    
    val rddFullRaw=MLUtils.loadLibSVMFile(sc, MovieLensTransform.ROOT_PATH+"calculated/ml20m-dataset_0_1005_0_20_0")
    val f=rddFullRaw.first()
    println(f.features.size)
    println(f)
    sc.stop()
    System.exit(0)
    
    val rddFull=MovieLensTransform.readDyadicRDD(sparkContextSingleton.getInstance(),MovieLensTransform.ROOT_PATH+"calculated/ml20m-dataset_0_1005_0_20_0")
    val movie=rddFull.first()._2
    
    val nBin=if (movie.getBinaryPart().isEmpty)
              0
             else
              movie.getBinaryPart().get.size
    val nOpt=if (movie.getOptionalSignedBinaryPart().isEmpty)
              0
             else
              movie.getOptionalSignedBinaryPart().get.size
    val nNum=if (movie.getNumericalPart().isEmpty)
              0
             else
              movie.getNumericalPart().get.size
    println(s"MOVIE B:$nBin O:$nOpt N:$nNum")
    sc.stop()
    System.exit(0)
    
    val subspaceDimension=100
    val LAMBDA=1.0
    val e1BinSize=0
    val e1OptSignBinSize=1000
    val e1ContSize=0
    val e2BinSize=0
    val e2OptSignBinSize=1000
    val e2ContSize=0
    
    
    var W:BDM[Double]=(BDM.rand(subspaceDimension,e1BinSize+e1OptSignBinSize+e1ContSize+1) :- 0.5)//Rand can be specified - Extra bias term added.
    var V:BDM[Double]=(BDM.rand(subspaceDimension,e2BinSize+e2OptSignBinSize+e2ContSize+1) :- 0.5)//Rand can be specified - Extra bias term added.
    
    val rddData=sc.parallelize(Array.fill[Int](10000)(0), 1000).map({case x=>
                                                                        val e1=new MixedData(if (e1ContSize>0) Some(BDV.rand(e1ContSize)) else None,
                                                                                              if (e1BinSize>0) Some(BDV.rand(e1BinSize).map(_>0.5)) else None,
                                                                                              if (e1OptSignBinSize>0) Some(BDV.rand(e1OptSignBinSize).map({case x=> if (x<=0.33) Negative else if (x>=0.67) Positive else Missing})) else None)
                                                                        val e2=new MixedData(if (e2ContSize>0) Some(BDV.rand(e2ContSize)) else None,
                                                                                              if (e2BinSize>0) Some(BDV.rand(e2BinSize).map(_>0.5)) else None,
                                                                                              if (e2OptSignBinSize>0) Some(BDV.rand(e2OptSignBinSize).map({case x=> if (x<=0.33) Negative else if (x>=0.67) Positive else Missing})) else None)
                                                                        (e1,e2,true)})
    
                                                                        
    rddData.cache()
    val timeStart=System.currentTimeMillis()
    val bW=sc.broadcast(W)
    val bV=sc.broadcast(V)
    val (sumdW,sumdV,count)=
      rddData.map({e => val (elem1,elem2,l)=e
                          //val elem1Proj=MixedVectorProjector.project(elem1, bW.value, true) //Adding bias to first vector
                          val elem1Proj=MixedVectorProjector.project(elem1, W, true) //Adding bias to first vector
                          //val elem2Proj=MixedVectorProjector.project(elem2, bV.value, true)
                          val elem2Proj=MixedVectorProjector.project(elem2, V, true)
                          val dotP=elem1Proj.dot(elem2Proj)
                          val z=if (l)
                                  1.0
                                else
                                  -1.0
                          val s=1.0/(1.0+Math.exp(-z*dotP/LAMBDA))
                          val factor= -z*(1.0-s)/LAMBDA
                          
                          val dW=MixedVectorProjector.vectorProduct(elem1, elem2Proj, true) :* factor
                          //(elem1.toDenseVector.asDenseMatrix.t*elem2Proj.asDenseMatrix).t
                          val dV=MixedVectorProjector.vectorProduct(elem2, elem1Proj, true) :* factor
                          //factor*(elem2.toDenseVector.asDenseMatrix.t*elem1Proj.asDenseMatrix).t
                          
                          /*
                          println("--------------------")
                          println(s"$elem1 -> $elem1Proj")
                          println(s"$elem2 -> $elem2Proj")
                          println(s"dotP:$dotP")
                          println(s"s:$s")
                          println(s"dW:$dW")
                          println("####################")
                          */
                            (dW,dV,1.0)
                          })
               .reduce({case (t1,t2) => (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)})
    bW.destroy()
    bV.destroy()
    println(sumdW.cols)
    println(System.currentTimeMillis()-timeStart)
    
    //Stop the Spark Context
    sc.stop()
  }
}