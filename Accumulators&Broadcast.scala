// Databricks notebook source
//Accumulators, a simple example
val accum = sc.longAccumulator("My Accumulator")
 
sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
accum.value

// COMMAND ----------

val textLines = Array("Michael, 29", "", "", "Andy, 30", "", "Justin, 19")
val tl_rdd = sc.parallelize(textLines)


// COMMAND ----------

tl_rdd.count
tl_rdd.collect()

// COMMAND ----------

//creer un accumulateur 
val emptyLines = sc.longAccumulator("Empty Lines Counter")
 

def f(x:String):Array[String]={
  //si chaine vide retourne array vide et incremente emptyLines par 1
  if (x=="") {emptyLines.add(1) ; Array[String]()}
  
  //sinon retourne la liste des mots de la chaine 
  else x.split(" ")
}

// COMMAND ----------

emptyLines.setValue(0)
val words=tl_rdd.flatMap(f)
emptyLines.value

// COMMAND ----------

//probleme : 
//la valeur de emptyLines n'a pas changé 

//cause 
//il n'y a pas un job qui a ete lancé 

//solution 
//lancer un job arbitraire 

// COMMAND ----------

//par exemple count
words.count

// COMMAND ----------

emptyLines.value

// COMMAND ----------

//reset 
emptyLines.reset
emptyLines.value
//set value
emptyLines.setValue(5)
emptyLines.value

// COMMAND ----------

// MAGIC %md
// MAGIC **braodcast**
// MAGIC broadcast en Scala est une fonctionnalité qui permet de distribuer une variable à travers tous les nœuds d'un cluster Spark, afin de la rendre accessible à tous les processus sur ces nœuds. 
// MAGIC 
// MAGIC la fonction broadcast en Scala est utile lorsque vous avez une variable qui est utilisée plusieurs fois dans votre code Spark et qui peut être stockée en mémoire
// MAGIC => cela peut améliorer les performances lorsque vous avez une variable qui est utilisée dans de nombreuses opérations de transformation et d'action sur RDD ou DataFrame
// MAGIC 
// MAGIC 
// MAGIC **persiste**
// MAGIC La fonction persist en Scala est utilisée pour stocker un RDD ou un DataFrame en mémoire ou sur un disque
// MAGIC Cependant, cela ne garantit pas que les variables associées seront disponibles localement sur chaque nœud du cluster
// MAGIC 
// MAGIC la fonction persist est utile pour stocker les données RDD ou DataFrame en mémoire ou sur un disque pour éviter de recalculer les transformations à chaque fois qu'elles sont appelées.

// COMMAND ----------

// MAGIC %md
// MAGIC #application sur les jointure 

// COMMAND ----------

// en scala les dictionnaire sont des Map "hashmap" (clé,valeur)


val rand = new scala.util.Random
val bigCollection=for (x<-1 to 100 ) yield(1+rand.nextInt(3),rand.nextInt(100)) 
val smallCollection=Map(1->"a",2->"b",3->"c")

// COMMAND ----------



// COMMAND ----------

// parallelize accepte en parametre des type sequence 
val small_RDD=sc.parallelize(smallCollection.toSeq)
val big_RDD=sc.parallelize(bigCollection.toSeq)

// COMMAND ----------

println("small RDD count=",small_RDD.count)
println("big RDD count=",big_RDD.count)

// COMMAND ----------

small_RDD.join(big_RDD).take(10)
//resultat les clés sont tous des 1 ce qui prouve qu'il y a eu un shuffle au cours de la jointure et qui est tres couteux 
//solution broadcast

// COMMAND ----------

//creer un braodcast (envoyer smallCollection aux differents executeurs)
val broadcastSc=sc.broadcast(smallCollection)


// COMMAND ----------

broadcastSc.value

// COMMAND ----------

val mapJoined=big_RDD.map(x=>{
  val dict=broadcastSc.value;
  (x._1,(dict(x._1),x._2))

})

// COMMAND ----------

mapJoined.take(10)
//maintenant il y a pas eu un shuffle 

// COMMAND ----------

//exercice kmeans on utilisant le broacaste pour eliminer le castesian 
//etudier l'impact 

// COMMAND ----------

//somme de deux vecteurs 
def sum(arr1:Array[Double], arr2:Array[Double]):Array[Double] = {
  val som = arr1.zip(arr2).map({case (a, b) => a + b})
  som 
}

//definir la distance euclidienne 
def euclidian_dist(arr1:Array[Double],arr2:Array[Double]):Double={
  val sum=arr1.zip(arr2).map({case (a,b)=> Math.pow(a-b,2)}).reduce((a,b)=>a+b)
  Math.pow(sum,0.5)
}


val lines= sc.textFile("FileStore/tables/iris_data.txt")
val data = lines.map(line=>line.split(","))

val unique_labels=data.map(record=>record.last).distinct
unique_labels.count()


//initialiser centroides 
val arraycentroides=data.takeSample(false,unique_labels.count.toInt,System.nanoTime).map(x=>x.slice(0,4))

val dataIndexed = data.zipWithIndex().map{case(xi,index)=>(index,xi)}
val xi=dataIndexed.map{case(index,xi)=>(index,xi.slice(0,4).map(x=>x.toDouble))}.persist()


// COMMAND ----------

// MAGIC %md
// MAGIC ## persist(-) coalesce(-) broadcast(-)

// COMMAND ----------

var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()
val epsilon = 0.001
var doNotStop: Long = 1
val numIterations=10
var temps_execution_job0: Array[Double] = Array.empty
var i=0 

// COMMAND ----------

while (i< numIterations && doNotStop>0){
  //initialiser le temps 
  val t0 = System.nanoTime()
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}

  //nombre de centroides qui ont changés de coordonnées
  doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()

  centroids=newcentroids 
  i=i+1
  println(3-doNotStop)
  
  //fin de l'iteration
  val t1=System.nanoTime()
  temps_execution_job0 = temps_execution_job0 :+ (t1 - t0).toDouble
}
val temps_execution_job_double0 = temps_execution_job0.asInstanceOf[Array[Double]]



// COMMAND ----------

// MAGIC %md
// MAGIC ##persist(+) coalesce(-) broadcast(-)

// COMMAND ----------

var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()
val epsilon = 0.001
var doNotStop: Long = 1
val numIterations=10
var temps_execution_job1: Array[Double] = Array.empty

// COMMAND ----------

var i=0 
while (i< numIterations && doNotStop>0){
  //initialiser le temps 
  val t0 = System.nanoTime()
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}.persist()

  //nombre de centroides qui ont changés de coordonnées
  doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()

  centroids=newcentroids 
  i=i+1
  println(3-doNotStop)
  
  //fin de l'iteration
  val t1=System.nanoTime()
  temps_execution_job1 = temps_execution_job1 :+ (t1 - t0).toDouble
}
val temps_execution_job_double1 = temps_execution_job1.asInstanceOf[Array[Double]]



// COMMAND ----------

// MAGIC %md
// MAGIC ## persist(-) coalesce(+) broadcast(-)

// COMMAND ----------

var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()
val epsilon = 0.001
var doNotStop: Long = 1
val numIterations=10
var temps_execution_job2: Array[Double] = Array.empty

// COMMAND ----------

var i=0 
while (i< numIterations && doNotStop>0){
  //initialiser le temps 
  val t0 = System.nanoTime()
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}.coalesce(8)

  //nombre de centroides qui ont changés de coordonnées
  doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()

  centroids=newcentroids 
  i=i+1
  println(3-doNotStop)
  
  //fin de l'iteration
  val t1=System.nanoTime()
  temps_execution_job2 = temps_execution_job2 :+ (t1 - t0).toDouble
}
val temps_execution_job_double2 = temps_execution_job2.asInstanceOf[Array[Double]]



// COMMAND ----------

// MAGIC %md
// MAGIC ## persist(+) coalesce(+) broadcast(-)

// COMMAND ----------

var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()
val epsilon = 0.001
var doNotStop: Long = 1
val numIterations=10
var temps_execution_job3: Array[Double] = Array.empty

// COMMAND ----------

var i=0 
while (i< numIterations && doNotStop>0){
  //initialiser le temps 
  val t0 = System.nanoTime()
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}.coalesce(8).persist()

  //nombre de centroides qui ont changés de coordonnées
  doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()

  centroids=newcentroids 
  i=i+1
  println(3-doNotStop)
  
  //fin de l'iteration
  val t1=System.nanoTime()
  temps_execution_job3 = temps_execution_job3 :+ (t1 - t0).toDouble
}
val temps_execution_job_double3 = temps_execution_job3.asInstanceOf[Array[Double]]



// COMMAND ----------

println("persist(-) coalesce(-) broadcast(-)")
println("temps d'execusion Total==>",temps_execution_job_double0.sum)
print("temps d'execution pour chaque iteration")
for (i <- temps_execution_job_double0) {
  print(i + " ")
}
println()
println("_____________________________________________________________________________________________________________________________________________")

println("persist(+) coalesce(-) broadcast(-)")
println("temps d'execusion Total==>",temps_execution_job_double1.sum)
print("temps d'execution pour chaque iteration")
for (i <- temps_execution_job_double1) {
  print(i + " ")
}
println()
println("_____________________________________________________________________________________________________________________________________________")

println("persist(-) coalesce(+) broadcast(-)")
println("temps d'execusion Total==>",temps_execution_job_double2.sum)
print("temps d'execution pour chaque iteration")
for (i <- temps_execution_job_double2) {
  print(i + " ")
}
println()
println("_____________________________________________________________________________________________________________________________________________")

println("persist(+) coalesce(+) broadcast(-)")
println("temps d'execusion Total==>",temps_execution_job_double3.sum)
print("temps d'execution pour chaque iteration")
for (i <- temps_execution_job_double3) {
  print(i + " ")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## persist(-) coalesce(-) broadcast(+)

// COMMAND ----------

//avec broadcast + elimination de cartisian
val broadCenroids=sc.broadcast(centroids)
val zik=xi.map{case (i,x)=>{val listCentr=}
