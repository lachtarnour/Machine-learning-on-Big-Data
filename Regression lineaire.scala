// Databricks notebook source
val v1 =Array(1.0,2.0)
val v2 =Array(3.0,4.0)

// COMMAND ----------

def sum(a: Array[Double], b: Array[Double]): Array[Double] = {
  a.zip(b).map { case (x, y) => x + y }
}

// COMMAND ----------

def prod(v1: Array[Double], v2: Array[Double]):Double = {
  val v = (v1 zip v2)
  v.map { case (x, y) => x * y }.reduce((a,b)=>a+b)
}

// COMMAND ----------

def soustraction(a: Array[Double], b: Array[Double]): Array[Double] = {
  a.zip(b).map { case (x, y) => x - y }
}

// COMMAND ----------

def prod_par_scal(a: Array[Double], b:Double): Array[Double] = {
  a.map { case x => x * b }
}

// COMMAND ----------

def sigma(dtrain:Array[(Array[Double],Double)],init_w:Array[Double]):Array[Double]={
  dtrain.map{case (x,y) => prod_par_scal(x,2*(prod(init_w,x)-y))}.reduce((a,b)=>sum(a,b))
}
def batchGD(dtrain : Array[(Array[Double],Double)],init_w : Array[Double], nb_of_epochs:  Int, stepsize:Double): Array[Double] = 
{
val m = dtrain.size // |Dtrain|
var w = init_w
for (i <- 1 to nb_of_epochs){
    val s = sigma(dtrain, w) ;
    w = soustraction(w,prod_par_scal(s,(stepsize/m)))
    } 
  w  
}




// COMMAND ----------

val X=(1 to 2000 ).by(2).toArray
val dtrain=X.map(x=>(Array(x.toDouble,1.0),(1*x+2).toDouble))
//dtrain=[((1.0,1.0),3.0),((3.0,1.0),0.5)....]



val sizefeat=2 

var winit= Array.fill(sizefeat)(0.0)
val stepsize = 0.00000001


val w1=batchGD(dtrain, winit, 100, stepsize) 


// COMMAND ----------

println(w1(0), w1(1))

// COMMAND ----------

//batch GD with RDDs 
def sigma_RDD(train : RDD[(Array[Double],Double)],current_w : Array[Double]): Array[Double] = {
  train.map{case (x,y)=>prod_par_scal(x,2*(prod(current_w,x)-y))}.reduce((a,b)=>sum(a,b))
}
 
def batchGD_RDD(dtrain : RDD[(Array[Double],Double )],init_w : Array[Double],  sizefeat : Int , nb_of_epochs: Int, stepsize:Double): Array[Double] =
{
val m = dtrain.count()
var w = init_w
dtrain.persist()
for (i <- 1 to nb_of_epochs){
    val s = sigma_RDD(dtrain, w) ;
    w = soustraction(w,prod_par_scal(s,(stepsize/m)))
    } 
  w  
}


// COMMAND ----------

val X=(1 to 2000 ).by(2).toArray
var dtrain=X.map(x=>(x.toDouble-1)/(2000-1)).map(x=>(Array(x.toDouble,1.0),(1*x+2).toDouble))

val dtrainrdd = sc.parallelize(dtrain)

val sizefeat=2 

//val res = gradlocal(dtrain, Array.fill(sizefeat)(0.0), sizefeat)

var w= Array.fill(sizefeat)(0.0)
val stepsize = 0.1
val w1=batchGD_RDD(dtrainrdd, w, sizefeat, 100,  stepsize) 
println(w1(0), w1(1))


// COMMAND ----------

import scala.util.Random
def SGD(dtrain : Array[(Array[Double],Double)],init_w : Array[Double], nb_of_epochs:  Int, stepsize:Double): Array[Double] =
{
val m = dtrain.size
var w = init_w
for (i <- 1 to nb_of_epochs){
  var (x,y)=Random.shuffle(dtrain.toList).head;
  var s=prod_par_scal(x,2*(prod(w,x)-y));
  w = soustraction(w,prod_par_scal(s,(stepsize/m)))
  } 
w  
}
  

// COMMAND ----------

val X=(1 to 2000 ).by(2).toArray
var dtrain=X.map(x=>(x.toDouble-1)/(2000-1)).map(x=>(Array(x.toDouble,1.0),(1*x+2).toDouble))

val dtrainrdd = sc.parallelize(dtrain)

val sizefeat=2 

//val res = gradlocal(dtrain, Array.fill(sizefeat)(0.0), sizefeat)

var w= Array.fill(sizefeat)(0.0)
val stepsize = 0.1
val w1=SGD(dtrain, w, 10000,  stepsize) 
println(w1(0), w1(1))

// COMMAND ----------

// deuxieme solution sans echantillionnage :
def SGD(dtrain : Array[(Array[Double],Double)], init_w : Array[Double] , stepsize: Double): Array[Double] =
{
  var w = init_w ;
  for ((x,y)<-dtrain){ w= soustraction(w, prod_scal(x,stepsize*2.0*(prod(w,x)-y)))} ;
  w
}
val X = (1 to 2000).by(2).toArray

val dtrain = X.map(x=>(Array(x,1.0),(1*x+2).toDouble))

var w = Array.fill(sizefeat)(0.0)
val stepsize = 0.0000001
val w1 = SGD(dtrain, w, stepsize)
println(w1(0), w1(1))


// COMMAND ----------

def SGD_RDD(dtrain : Array[(Array[Double],Double )],  sizefeat : Int , nb_of_epochs: Int, stepsize:Double): Array[Double] =
{
  var w= Array.fill(sizefeat)(0.0)
  val dtrainrdd = sc.parallelize(dtrain).glom().zipWithIndex().persist()
  val np=dtrainrdd.count().toInt
  
  for (i <- 0 to np-1){
    w=dtrainrdd.flatMap{case(p,j)=>if (i==j) {Array[Double](SGD(p, w, stepsize))} else {Array[Double]()}}.collect()
  }
  

}

// COMMAND ----------

def SGD_RDD_AVG(train : RDD[(Array[Double],Double)], stepsize : Double): Array[Double] = { 
  val w = Array.fill(sizefeat)(0.0)
  val traingi = train.glom().persist() 
  val wRDD = traingi.map{ case(p) => SGD(p,w,stepsize)} 
  val np = traingi.count().toInt
  wRDD.reduce(sum).map(e => e/np)
}




// COMMAND ----------

val X=(1 to 2000 ).by(2).toArray
var dtrain=X.map(x=>(x.toDouble-1)/(2000-1)).map(x=>(Array(x.toDouble,1.0),(1*x+2).toDouble))

val dtrainrdd = sc.parallelize(dtrain)

val stepsize = 0.01

val sizefeat=2 

val w1 = SGD_RDD_AVG(dtrainrdd, stepsize)
println(w1(0), w1(1))

// COMMAND ----------

import scala.math.sqrt

def dot(x:Array[Double], y:Array[Double]):Array[Double] =
 (for(z<-0 to x.size - 1) yield x(z)*y(z)).toArray[Double]


val stepsize =0.1
val epsilon = 0.00001
val w = Array.fill(sizefeat)(0.0)

def SGD_AdaGrad(dtrain : Array[(Double, Array[Double])], init_w : Array[Double] , init_gsquare : Array[Double] , stepsize: Double, epsilon : Double): Array[Double] =
{
  var w = init_w 
  ;
  var gsquare = init_gsquare ;
  
  for (d<-dtrain){
    val y = d._1; 
    val x = d._2;
    val g = prodbyscal(stepsize*2.0*(prods(w,x)-y),x) ;
    val gg =  &(g,g) ;
    gsquare = sum(gsquare, gg);&
    val adapted = gsquare.map(x => stepsize/sqrt(epsilon + x));
    w= subtr(w, dot(adapted, g))
  } ;
  w
}

val w1 = SGD_AdaGrad(dtrain, w, w,  stepsize, epsilon)
println(w1(0), w1(1))

