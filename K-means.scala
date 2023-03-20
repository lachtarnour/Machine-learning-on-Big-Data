// Databricks notebook source
// MAGIC %sh
// MAGIC wget https://www.dropbox.com/s/9kits2euwawcsj0/iris.data.txt

// COMMAND ----------

// MAGIC %sh 
// MAGIC pwd

// COMMAND ----------

//definir la distance euclidienne 
def euclidian_dist(arr1:Array[Double],arr2:Array[Double]):Double={
  val sum=arr1.zip(arr2).map({case (a,b)=> Math.pow(a-b,2)}).reduce((a,b)=>a+b)
  Math.pow(sum,0.5)
}

//



// COMMAND ----------

def sum(arr1:Array[Double], arr2:Array[Double]):Array[Double] = {
  val som = arr1.zip(arr2).map({case (a, b) => a + b})
  som 
}

// COMMAND ----------

val lines= sc.textFile("FileStore/tables/iris_data.txt")

// COMMAND ----------

lines.count()

// COMMAND ----------

lines.take(3)

// COMMAND ----------

Array("5.1,3.5,1.4,0.2,Iris-setosa", "4.9,3.0,1.4,0.2,Iris-setosa", "4.7,3.2,1.3,0.2,Iris-setosa").last

// COMMAND ----------

System.nanoTime

// COMMAND ----------

val data = lines.map(line=>line.split(","))
val unique_labels=data.map(record=>record.last).distinct
unique_labels.count()

// COMMAND ----------

unique_labels.collect

// COMMAND ----------

//takeSample(withReplacement, num, seed)
//slice function extract elements starting from ‘first-index’ (Inclusive) to ‘until-index’ (exclusive)
//persist une technique d'optimisation qui enregistre le résultat de l'évaluation des RDD dans la mémoire cache.


//initialisation de centroides 
val arraycentroides=data.takeSample(false,unique_labels.count.toInt,System.nanoTime).map(x=>x.slice(0,4))

val centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()


// COMMAND ----------

centroids.collect()

// COMMAND ----------

// on s'occupe maintenat des x_i

val dataIndexed = data.zipWithIndex().map{case(xi,index)=>(index,xi)}
val xi=dataIndexed.map{case(index,xi)=>(index,xi.slice(0,4).map(x=>x.toDouble))}.persist()

// COMMAND ----------

xi.take(1)

// COMMAND ----------

val combinations=xi.cartesian(centroids)
combinations.take(2)

// COMMAND ----------

val distances=combinations.map{
    case ( (indice_point,point),(indice_centroid,centroid) )=>
         ( indice_point,( indice_centroid,euclidian_dist(point,centroid) ) )
}

// COMMAND ----------

//(i,(j,distance(point(i),centroide(j)))
distances.take(1)

// COMMAND ----------

val dist_point_centroides=distances.groupByKey().mapValues(x=>x.toArray)

// COMMAND ----------

//[(i,[j,distance(xi,cj) pour tout  centroid j]) pour tout point i] 
//indice de point associer a ses distance par apport au differents centroides


dist_point_centroides.take(2)

// COMMAND ----------

val zik=distances.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if (a._2<b._2) a else b))

// COMMAND ----------

//[i,(j,distance(xi,ci) avec cj le centroide le plus proche)]
zik.take(1)

// COMMAND ----------

xi.take(1)

// COMMAND ----------

zik.join(xi).count()

// COMMAND ----------

//[i,((j,distance(xi,cj)),xi)]
zik.join(xi).take(1)

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
var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()

val dataIndexed = data.zipWithIndex().map{case(xi,index)=>(index,xi)}
val xi=dataIndexed.map{case(index,xi)=>(index,xi.slice(0,4).map(x=>x.toDouble))}.persist()


// COMMAND ----------

val epsilon = 0.001
val doNotStop = 1
val numIterations=5

var i=0 
while (i< numIterations && doNotStop>0){
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}

  //nombre de centroides qui ont changés de coordonnées
  val doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()



  centroids=newcentroids 

  i=i+1
  
  println(3-doNotStop)
  
}

// COMMAND ----------

println("nombre de centroids non stabilisé",3-doNotStop)

// COMMAND ----------

// à chaque iteration on est en train de creer des nouvelles instance pour zik et centroides 


//probleme grave:
//on remarque que le temps d'execution de l'iteration i+1 est plus long que le temps de calcule de l'iteration i:

//explication
// cela est du au faite que l'operation i+1 depends de la relation i donc pour effecturer l'iteration i+1 on doit executé tout les operations dans l'iteration i ainsi que tous les operations dans l'ieration i
//cad a l'iteration i on doit executer tous les transformations qui exisitent dans les iterations precedentes

// COMMAND ----------

//solution 1 : 
//persiste pour les differents variables 
// *pour xi c'est resolut il a deja .persist() et il reste sur la ram donc on n'a pas besoin de le transferer a chaque fois du disque vers la ram 
// *pour les centroides le probleme est aussi resolut
// *pour les newcentroides il faut la mettre dans ce ligne de code 
//val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}.persist()

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
var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()

val dataIndexed = data.zipWithIndex().map{case(xi,index)=>(index,xi)}
val xi=dataIndexed.map{case(index,xi)=>(index,xi.slice(0,4).map(x=>x.toDouble))}.persist()


// COMMAND ----------

val epsilon = 0.001
val doNotStop = 1
val numIterations=10

var i=0 
while (i< numIterations && doNotStop>0){
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}.persist()

   
  //nombre de centroides qui ont changés de coordonnées
  val doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()



  centroids=newcentroids 

  i=i+1
  
  
  print("n. partitions centroids",centroids.getNumPartitions)
  println(3-doNotStop)
}

// COMMAND ----------

println(3-doNotStop)

// COMMAND ----------

//pour 5 iterations pas de convergence 
//pour 10 iterations  trop du temps

// COMMAND ----------

//rapelle
//val a=b.cartesian(c)
// nbr partition de a = (nbr de partition de b) x (nbr de partition de c)

//a=b.join(c) 
//nbr partition de a = (nbr de partition de b) + (nbr de partition de c)

//nbr de partition de combinaition= nbr de partition xi * nbr de partition de centroid
//nbr de partie de centroids=nbr de partie de newcentroide 
//nbr partition de newcentorids = (nbr de partition de zik) + (nbr de partition de xi)
//nbr de partition de zik = nbr de partie de distance = nbr de partie de combinaition

//le probleme
// a chaque fois donc le nbr de partition de centroids augmente 

//solution 
//ajouter au newcentroide .coalesce(8) pour baisser le nombre de partie a 8 qui est plus importantes que le persiste 

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
var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()

val dataIndexed = data.zipWithIndex().map{case(xi,index)=>(index,xi)}
val xi=dataIndexed.map{case(index,xi)=>(index,xi.slice(0,4).map(x=>x.toDouble))}.persist()


// COMMAND ----------

val epsilon = 0.001
var doNotStop = 1
val numIterations=5

var i=0 
while (i< numIterations && doNotStop>0){
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}.coalesce(8).persist()

   
  //nombre de centroides qui ont changés de coordonnées
  var doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()



  centroids=newcentroids 

  i=i+1
  
  
  print("n. partitions centroids",centroids.getNumPartitions)
  println(3-doNotStop)
}

// COMMAND ----------

//pour evaluer la qualité de clustering on a besoin des labels, on reviens au RDD dataIntexed 

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
var centroids =sc.parallelize(arraycentroides).zipWithIndex().map{case (c,index)=>(index,c.map(x=>x.toDouble))}.persist()

val dataIndexed = data.zipWithIndex().map{case(xi,index)=>(index,xi)}
val xi=dataIndexed.map{case(index,xi)=>(index,xi.slice(0,4).map(x=>x.toDouble))}.persist()


// COMMAND ----------

val epsilon = 0.001
val doNotStop = 1
val numIterations=30

var i=0 
while (i< numIterations && doNotStop>0){
  // calcul des newCentroids
  val combinaison=xi.cartesian(centroids)
  
  val distance=combinaison.map{case((i,x),(j,c))=>(i,(j,euclidian_dist(x,c)))}
  
  val  zik=distance.groupByKey().mapValues(x=>x.toArray.reduce((a,b)=>if(a._2<b._1) a else b))
  
  val  newcentroids=zik.join(xi).map{case (i,((j,d),xi))=>(j,(xi,1))}.reduceByKey((a,b)=>(sum(a._1,b._1),a._2+b._2)).map{case (j,(sum_xi,count_xi))=>(j,sum_xi.map(e=>e/count_xi))}.coalesce(8).persist()

   
  //nombre de centroides qui ont changés de coordonnées
  val doNotStop=newcentroids.join(centroids).map{case(k,(newc,oldc))=>euclidian_dist(newc,oldc)}.filter(x=> x > epsilon ).count()



  centroids=newcentroids 

  i=i+1
  
  
  println("nbr de cluster stabilisé", 3-doNotStop)
  
  
  
  //zik=[(i,(j,distance(xi,cj) avec cj le cluster le plus proche ))]
  //dataIndexed=[(i,(xi,label_i))]
  //v=[x1,x2,x3,x4,label]
  //on va chercher les classes presentes dans les 3 clusters 
  val check_clustering=zik.join(dataIndexed).map{case (i,((j,d),v))=>(j,v.last)}.groupByKey().mapValues(x=>x.toArray.distinct).collect()
  for (e<-check_clustering)
    for (j<-e._2) println(e._1,j)
    
  
}

// COMMAND ----------

//exercice faire une fonction avec des parametres 
