

import java.lang.Math._

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{graphx, SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

/**
 * Created by lenovo on 2016/3/29.
 */
object chengLayout {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
    //校验输入参数

   if(args.length!=5){
     printf("input parameters <vertex path> <edges path> <output path> <deep> <time> ")
     return
   }
    val time=args(4).toInt
    val vertexPath=args(0)
    val edgesPath=args(1)
    val outputPath=args(2)
    val deep:Int=args(3).toInt
    val high:Double=5000D
    val wide:Double=5000D
    val maxLabel:Int=1000
    val vertexEachStep=1000000

    //加载顶点集边集
    val articles: RDD[String] = sc.textFile(vertexPath)
    val links: RDD[String] = sc.textFile(edgesPath)
/*
    val vertices = articles.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(1))
    }*/
    //临时的
    val vertices = articles.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(0))
    }
    //因为是当做无向图计算，所以只保留一条边
    val edges = links.map { line =>
      val fields = line.split("\t")
      val vid1=fields(0).toLong
      val vid2=fields(1).toLong
      if(vid1>vid2){
        Edge(vid2, vid1, 1F)
      }
      else{
        Edge(vid1,vid2,1F)
      }
    }.distinct()

    val graph = Graph(vertices, edges).persist()

    val degreeMap:Map[Int,Int]=graph.degrees.map(x=>(x._2,1)).reduceByKey((A,B)=>A+B).collect().toMap
    //对度分层
    var sum:Int=0
    var layer:Set[Int]=Set()
    var i=degreeMap.keySet.max
    while(i>0){
      if(degreeMap.contains(i)&&sum+degreeMap(i)>vertexEachStep){
        layer+=i
        sum=degreeMap(i)
      }
      else if(degreeMap.contains(i)){
        sum+=degreeMap(i)
      }
      i-=1
    }
    print(layer)
    //对顶点进行预处理
    val graph2 = preprocessor(graph, deep, maxLabel, layer,sc)
    //graph2.vertices.saveAsTextFile(outputPath)
    //graph2.edges.saveAsTextFile("/pagerank/edges")

/*    if(isDirected){
      //graph2=directedPreprocessor(graph,deep)
    }
    else{
      graph2=preprocessor(graph,deep)
    }*/



    //layout(graph2,20,2500)

   // layout(graph2,20,2500).vertices.saveAsHadoopFile(outputPath,classOf[VertexId],classOf[(Set[VertexId],(Double,Double),Float)],classOf[RDDMultipleTextOutputFormat])
    var graph3=layout(graph2,time,2500).persist()
    output(graph3)
  }
  //应对边数进行限制，防止某个顶点有太多的边
 // def preprocessor[ED:ClassTag](graph:Graph[String,ED],maxDeep:Int):Graph[Set[VertexId],Long]={




  def preprocessor(graph:Graph[String,Float],maxDeep:Int,maxLabel:Int,layer:Set[Int],sc:SparkContext)={
    //初始化图顶点的属性（即LPA的标签），开始时每个顶点的标签为顶点ID
    val lpaGraph=graph.mapVertices{case (vid,_)=>Map(0->Set(vid))}.persist()
    val iterTime=sc.broadcast(1)
    //向源顶点发送目标顶点的attr（dst顶点的label），向目标顶点发送源顶点的attr（src的label）
    //你可以这么理解，我们通过两个顶点和EdgeAttr去确定一条边，那么一条边的两个顶点直接就互为邻居



     def sendMessage(e:EdgeTriplet[Map[Int,Set[VertexId]],Float])={
       val keySizeSrc:Int=e.srcAttr.keySet.size
       val keySizeDst:Int=e.dstAttr.keySet.size
       val dstKeySetSize = {
         var count = 0
         val it = e.dstAttr.iterator
         while (it.hasNext) {
           count += it.next()._2.size
         }
         count
       }
       val srcKeySetSize={
         var count=0
         val it=e.srcAttr.iterator
         while(it.hasNext){
           count+=it.next()._2.size
         }
         count
       }
   //    LoggerFactory.getLogger("chengLayout").warn(e.srcAttr.keySet.toString()+"$$$$$$$$$$$$$$$$$$$$"+e.dstAttr.keySet.toString()+"#####################"+"id="+e.dstId)

       if(srcKeySetSize<=maxLabel&&dstKeySetSize<=maxLabel){
        // LoggerFactory.getLogger("chengLayout").warn(e.srcId+"$$$$$$$$$$$$$$$$$$$$"+e.srcAttr(keySizeSrc-1).size+"-----"+e.dstId+"#####################"+e.dstAttr(keySizeDst-1).size)
         //###################!!!!!!!!!!!!!!!!!!!!!!!!!!!由于有重复边，所以源顶点发送给目的顶点消息，目的顶点发送给源顶点消息会重复发送，增加负担，出现bug,可以设置源顶点id>目的顶点id时才发送

         if((e.srcAttr(keySizeSrc-1).size+dstKeySetSize)<maxLabel&&(e.dstAttr(keySizeDst-1).size+srcKeySetSize)<maxLabel){
           Iterator((e.dstId,e.srcAttr(keySizeSrc-1)-e.dstId),(e.srcId,e.dstAttr(keySizeDst-1)-e.srcId))
         }
         else if((e.srcAttr(keySizeSrc-1).size+dstKeySetSize)<maxLabel){
           Iterator((e.dstId,e.srcAttr(keySizeSrc-1)-e.dstId))
         }
         else if((e.dstAttr(keySizeDst-1).size+srcKeySetSize)<maxLabel){
           Iterator((e.srcId,e.dstAttr(keySizeDst-1)-e.srcId))
         }
         else{
           Iterator()
         }
       }
       else{
         Iterator()
       }
       var newSetSrc=Set()
       if(srcKeySetSize>maxLabel){
         var i=0
         while(i<maxLabel){

           i+=1
         }
       }
    }

    //合并消息的函数
    //对发送来的消息进行合并
    //原理：就是对发送过来的Mao的Key进行合并，并对相同的Key的值进行累加的操作，这样顶点函数就可以取出邻居
    //数量最多的label了
      def mergeMessage(msg1:Set[VertexId],msg2:Set[VertexId]):Set[VertexId]={
        if(msg1.contains(-1)||msg2.contains(-1)){
          Set(-1)
        }
        else if(msg1.size>maxLabel||msg2.size>maxLabel){
          Set(-1)
        }
        else {
          val set=msg1 ++ msg2
          if(set.size>maxLabel){
            Set(-1)
          }
          else{
            set
          }
        }
      }
    def mergeMessageFirst(msg1:Set[VertexId],msg2:Set[VertexId]):Set[VertexId]={
        msg1 ++ msg2
    }
    //顶点函数
    //以为初始化的消息为empty，所以返回原来节点的属性值，否则取出数量最多的标签
      def vertexProgram(vid:VertexId,attr:Map[Int,Set[VertexId]],message:Set[VertexId])={
        //LoggerFactory.getLogger("chengLayout").warn("$$$$"+vid+"  "+attr+"  "+message+"  "+(message++attr))
/*      if(vid==146271392968588L){
        print("!!!!!!!!!"+message.size)
      }*/
        val iterTime=attr.keySet.size
        val labelCount={
          var count=0
          val it=attr.iterator
          while(it.hasNext){
            count+=it.next()._2.size
          }
          count
        }
        if(message.contains(-1)){
          attr+((iterTime)->Set().asInstanceOf[Set[VertexId]])
        }
        else if(labelCount+message.size>maxLabel){
          attr+((iterTime)->Set().asInstanceOf[Set[VertexId]])
        }
        else{
          val it=message.iterator
          var newSet:Set[VertexId]=Set()
          while(it.hasNext) {
            val elem=it.next()
            val it2 = attr.iterator
            var has=false
            while (it2.hasNext) {
              val set: Set[VertexId] = it2.next()._2
              if (set.contains(elem)){
                has=true
              }
            }
            if(!has){
              newSet+=elem
            }
          }
          attr+(iterTime ->newSet)
        }

    }
    //第一次迭代的顶点函数

    def vertexProgramFrist(vid:VertexId,attr:Map[Int,Set[VertexId]],message:Set[VertexId])={
      attr+(0->message)
    }

    //pregel模型
    var g = lpaGraph
    // compute the messages

    var messages = g.mapReduceTriplets(sendMessage, mergeMessageFirst)
    var activeMessages = messages.count()

    // Loop
    var prevG: Graph[Map[Int,Set[VertexId]],Float] = null
    var i = 0
    var isFirst=true
    while (activeMessages > 0 && i < maxDeep) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      var newVerts:VertexRDD[Map[Int,Set[VertexId]]]=null
      if(isFirst){
        newVerts = g.vertices.innerJoin(messages)( vertexProgramFrist).cache()
        isFirst=false
      }
      else{
        newVerts = g.vertices.innerJoin(messages)( vertexProgram).cache()
      }
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.cache()

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMessage, mergeMessage, Some((newVerts, EdgeDirection.Either))).cache()
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      newVerts.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1

    }

    val graph2 = g.persist()
    g.vertices.saveAsTextFile("/pagerank/vertex")


 //   graph2.edges.saveAsTextFile("/pagerank/888")
 //   graph2.vertices.saveAsTextFile("/pagerank/777")
  //  val newEdges=graph2.vertices.flatMap{case(id,set)=>set.map(x=>Edge(id.toLong,x,1F))}
 val newEdges=graph2.vertices.flatMap{case (id,map)=>map.flatMap{case (index,set)=> set.map(x=>Edge(id, x.toLong, 1F))}}
/*    val newEdges=graph2.vertices.flatMap{case (id,map)=>map.flatMap{case (index,set)=> set.map(x=>
 {
   val vid1=id
   val vid2=x.toLong
   if(vid1>vid2){
     Edge(vid2, vid1, 1F)
   }
   else{
     Edge(vid1,vid2,1F)
   }
 }
 ) }}.distinct()*/
    val newVertexs=graph2.collectNeighborIds(EdgeDirection.Either)
  //  newEdges.saveAsTextFile("/pagerank/999")
  //  newVertexs.saveAsTextFile("/pagerank/888")
    val newGraph=Graph(newVertexs,newEdges).mapVertices((id,array)=>array.toSet).persist()
    newGraph.vertices.count()
    newGraph.edges.count()
   // newEdges.unpersist()
    //newVertexs.unpersist()
    graph2.unpersist(blocking = true)
  //  newGraph.vertices.saveAsTextFile("/pagerank/vertex")
  //  newGraph.edges.saveAsTextFile("/pagerank/edge")
    newGraph

  }
  //有向图还没调好
  def directedPreprocessor[ED:ClassTag](graph:Graph[String,ED],maxDeep:Int):Graph[Set[VertexId],ED]={
    //初始化图顶点的属性（即LPA的标签），开始时每个顶点的标签为顶点ID
    val lpaGraph=graph.mapVertices{case (vid,_)=>Set(vid)}
    //向源顶点发送目标顶点的attr（dst顶点的label），向目标顶点发送源顶点的attr（src的label）
    //你可以这么理解，我们通过两个顶点和EdgeAttr去确定一条边，那么一条边的两个顶点直接就互为邻居

    def sendMessage(e:EdgeTriplet[Set[VertexId],ED])={
      Iterator((e.dstId,e.srcAttr))
    }

    //合并消息的函数
    //对发送来的消息进行合并
    //原理：就是对发送过来的Mao的Key进行合并，并对相同的Key的值进行累加的操作，这样顶点函数就可以取出邻居
    //数量最多的label了
    def mergeMessage(msg1:Set[VertexId],msg2:Set[VertexId]):Set[VertexId]={
      msg1++msg2
    }
    //顶点函数
    //以为初始化的消息为empty，所以返回原来节点的属性值，否则取出数量最多的标签
    def vertexProgram(vid:VertexId,attr:Set[VertexId],message:Set[VertexId])={
      if(message.isEmpty) attr else message
    }
    //初始化消息
    val initialMessage=Set[VertexId]()

    //maxSteps为迭代的次数
    Pregel(lpaGraph,initialMessage,maxIterations=maxDeep,activeDirection=EdgeDirection.Out)(
      vprog=vertexProgram,
      sendMsg=sendMessage,
      mergeMsg=mergeMessage

    )
  }
  def layout[ED:ClassTag](graph:Graph[Set[VertexId],ED],iterations:Int,temperature:Float):Graph[(Set[VertexId],(Double,Double),Float),ED] ={
    val high:Double=5000D
    val wide:Double=5000D
    val area:Double=high*wide
    val k=sqrt(area / graph.vertices.count())
    //##0.85是退火系数
    val temperature2=(1F/0.85F)*temperature
    //随机生成坐标
    val randomGraph=graph.mapVertices((id,set)=>(set,(random()*high,random()*wide),temperature )  )
    graph.unpersist(blocking = true)
    //##调试代码
/*    val randomGraph=graph.mapVertices((id,set)=>(set,{if(id==1){(3193.1953644410614,456.943409213304)}
      else if(id==4){(1537.7309068258644,2373.2497251986797)}
        else(random()*high,random()*wide)},temperature2 )  )*/

    randomGraph.persist()
    //randomGraph.triplets.saveAsTextFile("/pagerank/666")

    //randomGraph.vertices.saveAsTextFile("/pagerank/222")
    //计算距离
    def dist(location1:(Double,Double),location2:(Double,Double)):Double={
      sqrt(pow((location1._1-location2._1),2)+pow((location1._2-location2._2),2))
    }
    //计算引力
    def attractiveForce(distance:Double):Double={
      pow(distance,2)/k
    }
    //计算斥力
    def repulsiveForce(distance:Double):Double={
      pow(k,2)/distance
    }

    //#############a对b的引力即是，b对a的引力，斥力也一样，但是问题是，一个度很大的顶点不一定有有二街邻居，而度小的顶点的二阶邻居可能会包含这个度很大的顶点，度很大的店计算斥力只要计算其与一届邻居的斥力，而其他度小的要计算二阶邻居的斥力，这造成了互为邻居的点的力的计算不相等，但是对度小的顶点的力的计算已经完成，计算度大的顶点的力时是否可以考虑也计算二阶邻居的斥力？？
    def sendMessage(e:EdgeTriplet[(Set[VertexId],(Double,Double),Float),ED])={
print("1")
      val distance=dist(e.srcAttr._2,e.dstAttr._2)
      var attract=0D
      var attractX=0D
      var attractY=0D
      //只计算相邻节点间引力
      if(e.srcAttr._1.contains(e.dstId)) {
        attract = attractiveForce(distance)
        //防止顶点重合
        if(e.srcAttr._2._1==e.dstAttr._2._1&&e.srcAttr._2._2==e.dstAttr._2._2) {
          attractX=0D
          attractY=0D
        }
        else{
          attractX=(-1)*(e.srcAttr._2._1-e.dstAttr._2._1)/(distance+0.00001D)*attract
          attractY=(-1)*(e.srcAttr._2._2-e.dstAttr._2._2)/(distance+0.00001D)*attract
        }
      }
      //计算关联紧密顶点间的斥力
      val repulsive=repulsiveForce(distance)
      var repulsiveX=0D
      var repulsiveY=0D
      //防止顶点重合
      if(e.srcAttr._2._1==e.dstAttr._2._1&&e.srcAttr._2._2==e.dstAttr._2._2) {
        if (e.srcId < e.dstId) {
          repulsiveX=(e.srcAttr._2._1-e.dstAttr._2._1+0.00001D)/(distance+0.00001D)*repulsive
          repulsiveY=(e.srcAttr._2._2-e.dstAttr._2._2+0.00001D)/(distance+0.00001D)*repulsive
        }
        else{
          repulsiveX=(e.srcAttr._2._1-e.dstAttr._2._1-0.00001D)/(distance+0.00001D)*repulsive
          repulsiveY=(e.srcAttr._2._2-e.dstAttr._2._2-0.00001D)/(distance+0.00001D)*repulsive
        }
      }
      else{
        repulsiveX=(e.srcAttr._2._1-e.dstAttr._2._1)/distance*repulsive
        repulsiveY=(e.srcAttr._2._2-e.dstAttr._2._2)/distance*repulsive
      }


      //计算合力
      val forceX = attractX + repulsiveX
      val forceY = attractY + repulsiveY
      //调试代码
      /*        println("("+e.srcAttr._2._1+"-"+e.dstAttr._2._1+")/"+"("+distance+"+"+0.00001D+")*"+attract)
              println(repulsive)
              println(e.srcId+"$$"+e.dstId)
              println("attractX="+attractX+"   repulsiveX"+repulsiveX)
              println("attractY="+attractY+"   repulsiveY"+repulsiveY)*/
      if(forceX.isNaN){
        println( attractX + "  :  "+repulsiveX)
      }
      if(attractX.isNaN)
        println((e.srcAttr._2._1-e.dstAttr._2._1)+"/"+distance+"*"+attract)
      if(repulsiveX.isNaN)
        println((e.srcAttr._2._1-e.dstAttr._2._1)+"/"+distance+"*"+repulsive)
      (e.srcId,(forceX,forceY))
      //(e.dstId,e.srcAttr._2)
      Iterator((e.srcId,(forceX,forceY)))
    }
    val initialMessage=(1D,1D)

    //更新顶点位置
    def vertexProgram(vid:VertexId,attr:(Set[VertexId],(Double,Double),Float),message:(Double,Double))={
      println("2       "+vid)
      var dispX=message._1
      val dispXX=dispX
      var dispY=message._2
      val disp=sqrt(dispX*dispX+dispY*dispY)
      val temp=attr._3
      if(disp>temp) {
        if (dispX.isInfinity && dispY.isInfinity) {
          dispX = 1.414D * temp
          dispY = 1.414D * temp
        }
        else {
          if (dispX.isInfinity)
            dispX = temp
          else
            dispX = dispX / disp * temp
          if (dispY.isInfinity)
            dispY = temp
          else
            dispY = dispY / disp * temp
        }
      }
      var newX=attr._2._1+dispX
      var newY=attr._2._2+dispY
      if(newX<0){
       newX=0
      }
      if(newX>wide){
        newX=wide
      }
      if(newY<0){
        newY=0
      }
      if(newY>high){
        newY=high
      }
      //调试代码
      if(newX.isNaN)
      println("attr="+attr._2._1+"  dispx="+dispX+"  dispXX="+dispXX+"   disp"+disp)

      //0.85是退火算法的冷却系数
      (attr._1,(newX,newY),temp*0.85F)
      //(attr._1,message,temp*0.85F)
    }
    def mergeMessage(msg1:(Double,Double),msg2:(Double,Double)):(Double,Double)={
      print("3")
      var x=msg1._1+msg2._1
      var y=msg1._2+msg2._2
      if((msg1._1.isPosInfinity&&msg2._1.isNegInfinity)||(msg1._1.isNegInfinity&&msg2._1.isPosInfinity))
        x=random()
      if((msg1._2.isPosInfinity&&msg2._2.isNegInfinity)||(msg1._2.isNegInfinity&&msg2._2.isPosInfinity))
        y=random()
      (x,y)
    }

    Pregel(randomGraph,initialMessage,maxIterations=iterations,activeDirection=EdgeDirection.Out)(
      vprog=vertexProgram,
      sendMsg=sendMessage,
      mergeMsg=mergeMessage
    )
  }
  //有向图没调好
  def directedlayout[ED:ClassTag](graph:Graph[Set[VertexId],ED],iterations:Int,temperature:Float):Graph[(Set[VertexId],(Double,Double),Float),ED] ={
    val high:Double=5000D
    val wide:Double=5000D
    val area:Double=high*wide
    val k=sqrt(area / graph.vertices.count())
    //##0.85是退火系数
    val temperature2=(1F/0.85F)*temperature
    //随机生成坐标
    val randomGraph=graph.mapVertices((id,set)=>(set,(random()*high,random()*wide),temperature )  )
    graph.unpersist(blocking = true)
    //##调试代码
    /*    val randomGraph=graph.mapVertices((id,set)=>(set,{if(id==1){(3193.1953644410614,456.943409213304)}
          else if(id==4){(1537.7309068258644,2373.2497251986797)}
            else(random()*high,random()*wide)},temperature2 )  )*/

    randomGraph.persist()
    //randomGraph.triplets.saveAsTextFile("/pagerank/666")

    //randomGraph.vertices.saveAsTextFile("/pagerank/222")
    //计算距离
    def dist(location1:(Double,Double),location2:(Double,Double)):Double={
      sqrt(pow((location1._1-location2._1),2)+pow((location1._2-location2._2),2))
    }
    //计算引力
    def attractiveForce(distance:Double):Double={
      pow(distance,2)/k
    }
    //计算斥力
    def repulsiveForce(distance:Double):Double={
      pow(k,2)/distance
    }

    def sendMessage(e:EdgeTriplet[(Set[VertexId],(Double,Double),Float),ED])={
      Iterator({
        val distance=dist(e.srcAttr._2,e.dstAttr._2)
        var attract=0D
        var attractX=0D
        var attractY=0D
        //只计算相邻节点间引力
        if(e.srcAttr._1.contains(e.dstId)) {
          attract = attractiveForce(distance)
          //防止顶点重合
          if(e.srcAttr._2._1==e.dstAttr._2._1&&e.srcAttr._2._2==e.dstAttr._2._2) {
            attractX=0D
            attractY=0D
          }
          else{
            attractX=(-1)*(e.srcAttr._2._1-e.dstAttr._2._1)/(distance+0.00001D)*attract
            attractY=(-1)*(e.srcAttr._2._2-e.dstAttr._2._2)/(distance+0.00001D)*attract
          }
        }
        //计算关联紧密顶点间的斥力
        val repulsive=repulsiveForce(distance)
        var repulsiveX=0D
        var repulsiveY=0D
        //防止顶点重合
        if(e.srcAttr._2._1==e.dstAttr._2._1&&e.srcAttr._2._2==e.dstAttr._2._2) {
          if (e.srcId < e.dstId) {
            repulsiveX=(e.srcAttr._2._1-e.dstAttr._2._1+0.00001D)/(distance+0.00001D)*repulsive
            repulsiveY=(e.srcAttr._2._2-e.dstAttr._2._2+0.00001D)/(distance+0.00001D)*repulsive
          }
          else{
            repulsiveX=(e.srcAttr._2._1-e.dstAttr._2._1-0.00001D)/(distance+0.00001D)*repulsive
            repulsiveY=(e.srcAttr._2._2-e.dstAttr._2._2-0.00001D)/(distance+0.00001D)*repulsive
          }
        }
        else{
          repulsiveX=(e.srcAttr._2._1-e.dstAttr._2._1)/distance*repulsive
          repulsiveY=(e.srcAttr._2._2-e.dstAttr._2._2)/distance*repulsive
        }


        //计算合力
        val forceX = attractX + repulsiveX
        val forceY = attractY + repulsiveY
        //调试代码
        /*        println("("+e.srcAttr._2._1+"-"+e.dstAttr._2._1+")/"+"("+distance+"+"+0.00001D+")*"+attract)
                println(repulsive)
                println(e.srcId+"$$"+e.dstId)
                println("attractX="+attractX+"   repulsiveX"+repulsiveX)
                println("attractY="+attractY+"   repulsiveY"+repulsiveY)*/
        if(forceX.isNaN){
          println( attractX + "  :  "+repulsiveX)
        }
        if(attractX.isNaN)
          println((e.srcAttr._2._1-e.dstAttr._2._1)+"/"+distance+"*"+attract)
        if(repulsiveX.isNaN)
          println((e.srcAttr._2._1-e.dstAttr._2._1)+"/"+distance+"*"+repulsive)
        (e.srcId,(forceX,forceY))
        //(e.dstId,e.srcAttr._2)
      })
    }
    val initialMessage=(1D,1D)

    //更新顶点位置
    def vertexProgram(vid:VertexId,attr:(Set[VertexId],(Double,Double),Float),message:(Double,Double))={
      var dispX=message._1
      val dispXX=dispX
      var dispY=message._2
      val disp=sqrt(dispX*dispX+dispY*dispY)
      val temp=attr._3
      if(disp>temp) {
        if (dispX.isInfinity && dispY.isInfinity) {
          dispX = 1.414D * temp
          dispY = 1.414D * temp
        }
        else {
          if (dispX.isInfinity)
            dispX = temp
          else
            dispX = dispX / disp * temp
          if (dispY.isInfinity)
            dispY = temp
          else
            dispY = dispY / disp * temp
        }
      }
      var newX=attr._2._1+dispX
      var newY=attr._2._2+dispY
      if(newX<0){
        newX=0
      }
      if(newX>wide){
        newX=wide
      }
      if(newY<0){
        newY=0
      }
      if(newY>high){
        newY=high
      }
      //调试代码
      if(newX.isNaN)
        println("attr="+attr._2._1+"  dispx="+dispX+"  dispXX="+dispXX+"   disp"+disp)

      //0.85是退火算法的冷却系数
      (attr._1,(newX,newY),temp*0.85F)
      //(attr._1,message,temp*0.85F)
    }
    def mergeMessage(msg1:(Double,Double),msg2:(Double,Double)):(Double,Double)={
      var x=msg1._1+msg2._1
      var y=msg1._2+msg2._2
      if((msg1._1.isPosInfinity&&msg2._1.isNegInfinity)||(msg1._1.isNegInfinity&&msg2._1.isPosInfinity))
        x=random()
      if((msg1._2.isPosInfinity&&msg2._2.isNegInfinity)||(msg1._2.isNegInfinity&&msg2._2.isPosInfinity))
        y=random()
      (x,y)
    }

    Pregel(randomGraph,initialMessage,maxIterations=iterations,activeDirection=EdgeDirection.Out)(
      vprog=vertexProgram,
      sendMsg=sendMessage,
      mergeMsg=mergeMessage
    )
  }
  def output[ED:ClassTag](graph: Graph[(Set[VertexId],(Double,Double),Float),ED]): Unit ={
    class VertexMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
        val v=value.asInstanceOf[(Double,Double)]
        val x=v._1
        val y=v._2
        val rowNum:Int=(x/50).asInstanceOf[Int]
        val culNum:Int=(y/50).asInstanceOf[Int]
        rowNum.toString+"_"+culNum.toString
      }
    }

    class EdgeMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
        val x=key.asInstanceOf[(Double,Double)]
        val rowNum:Int=(x._1/50).asInstanceOf[Int]
        val culNum:Int=(x._2/50).asInstanceOf[Int]
        rowNum.toString+"_"+culNum.toString
      }
    }
    val vertexs=graph.vertices.map(x=>(x._1,(x._2._2._1,x._2._2._2)))
    val edges=graph.triplets.map(x=>(x.srcAttr._2,x.dstAttr._2))
    vertexs.saveAsHadoopFile("/pagerank/outputVertexs",classOf[String],classOf[(Double,Double)],classOf[VertexMultipleTextOutputFormat])
    edges.saveAsHadoopFile("/pagerank/outputEdges",classOf[String],classOf[(Double,Double)],classOf[ EdgeMultipleTextOutputFormat])
  }
}
