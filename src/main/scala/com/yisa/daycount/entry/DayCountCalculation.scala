package com.yisa.daycount.entry

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Put
import java.util.ArrayList
import org.apache.hadoop.hbase.util.Bytes
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get 
import scala.collection.mutable.HashMap

object DayCountCalculation {

  def main(args: Array[String]) = {

    if (args.length < 4) {
      println("please input appname  sparkMaster zookeeper hdfsHostname")
      System.exit(0)
    }
 
    var zkquorum = args(2)
    var hdfspath = "hdfs://" + args(3) + ":8020"

    val daydatedir = getdate(-1)
    val path = hdfspath.concat("/").concat("/wifidate/").concat(daydatedir).concat("/");
    println("处理的路径为：" + path)

    val sprakConf = new SparkConf().
    setAppName(args(0));
    val sc = new SparkContext(sprakConf)
    val scc = sc.sequenceFile[String, String](path)

    //    val data = List(
    //        ("a23231212","鲁A1234","440232343434","700000323","20160523140222","20160523140225"),
    //      
    //    val scc = sc.parallelize(data)

    //写入格式 start by liliwei  20160602 
    //String value = wifiInfo.getMac() + "," + wifiInfo.getPlateNumber() + "," + wifiInfo.getLocationId() + ","
    //    + wifiInfo.getEquId() + "," + wifiInfo.getKakoucaptureTime() + "," + wifiInfo.getWifiCaptureTime();
    //写入格式 end
    //以 mac plateNumber locationid equid kakoucapturetime过滤数据，将重复数据去掉
    val filteredData = scc.map(y =>
      {
        val x = y._2.split(",")
        //     	 val x = y.split(",") 
        (
          x(0).concat("_").concat(x(1)).concat("_").concat(x(2)).concat(x(3)).concat("_").concat(x(4)),
          x(0).concat(",").concat(x(1)).concat(",").concat(x(2)).concat(",").concat(x(3)).concat(",").concat(x(4)).concat(",").concat(x(5)))
     
      
      
      }).reduceByKey((x, y) => (x))
      .cache()

    //mac_plateNumber+locationid(equid_kakaoucapturetime_wificaptime,1)

    //这一步先按天将数据分组（当前会出现非一天的数据）
    val date_Data = filteredData.map(y =>
      {
        val x = y._2.split(",")
        val daytime = getdate2(x(4));
        (
          //mac_PlateNumber_LocationId_daytime
          x(0).concat("_").concat(x(1)).concat("_").concat(x(2)).concat("_").concat(daytime),
          (
            //EquId_KakoucaptureTime_WifiCaptureTime   1 
            x(3).concat("_").concat(x(4)).concat("_").concat(x(5)), 1))
      })
      //key:wifiInfo.getMac() + "_" + wifiInfo.getPlateNumber() + "_" + wifiInfo.getLocationId()+ "_"+daytime
      //value:[getEquId() + "_" + wifiInfo.getKakoucaptureTime() + "_" + wifiInfo.getWifiCaptureTime(),1]
      .reduceByKey((x, y) =>
        //mac_plateNumber+locationid(equid_kakaoucapturetime_wificaptime-equid_kakaoucapturetime_wificaptime,1+1)
        (x._1.concat("-").concat(y._1), x._2 + y._2) //key :mac_plateNumber_location
        //value:[equid_kakoucapturetime_wificaturetime-equid_kakoucapturetime_wificaturetime,1 +1]  
        //          x._1-y._1                                                       x._2+y._2
        )
    //        .cache()

    //locationrdd       
    //key :mac_plateNumber_location_daytime
    //value:[equid_kakoucapturetime_wificaturetime-equid_kakoucapturetime_wificaturetime,1 +1]  
    //          x._1-y._1                                                       x._2+y._2
    //value 同行信息 ，相同mac platenumber 在相同Location下的同行次数
    val countRdd = date_Data.map(x => {

      val rd1 = x._1.split("_")
      // x:mac_plateNumber_location_daytime
      //rd1:[mac,plateNumber,location,daytime]
      // mac_plateNumber,[1,[在相同Location下的同行次数,
      //                     location,equid_kakoucapturetime_wificaturetime-equid_kakoucapturetime_wificaturetime,
      //                     plateNumber:在相同Location下的同行次数]]    
      (rd1(0).concat("_").concat(rd1(1)).concat("_").concat(rd1(3)), //mac_plateNumber_daytime
        (1, //mac_platenumber同行locationid数，每天

          (x._2._2, //[在相同Location下的同行次数
            rd1(2).concat(",").concat(x._2._1), // location,equid_kakoucapturetime_wificaturetime-equid_kakoucapturetime_wificaturetime
            rd1(2).concat(":").concat(x._2._2.toString()) // location:在相同Location下的同行次数
            )
        )
      )
    })
    //key :mac_plateNumber_daytime
    //value :
    /**
     * countRdd: RDD[(String, (Int, (Int, String, String)))]
     * key:String,mac_plateNumber_daytime
     * value :(Int, (Int, String, String))
     * Int:mac_platenumber同行的locationid数量（每天）如：某人开着车，带着手机，一天同行了5个locationid（卡口数）
     * Int：mac_platenumber在相同locationid下，同行了几次。如：某人开着车，带着手机，在某一个locationid下过了3次
     * String:同相同locationid下面的同行详细信息：location,equid_kakoucapturetime_wificaturetime-equid_kakoucapturetime_wificaturetime
     * Stirng：相同loationid下面的同行次数   location:在相同Location下的同行次数   这个用于显示每个卡口的同行信息1112:1,2222:2
     *
     */
    .reduceByKey((x, y) => {

      //卡口数量
      val locationCount = x._1 + y._1

      //同行数量
      val tongxingCount = x._2._1 + y._2._1

      //同行的详细信息
      val tongxingInfo = x._2._2.concat("+").concat(y._2._2)
      //所有Location以及通过的同行的统计
      val locationTongXing = x._2._3.concat("_").concat(y._2._3)
      //      val scores = locationCount*7 + tongxingCount*3
      (locationCount, (tongxingCount, tongxingInfo, locationTongXing))
    })
    //      .cache()

    //   countRdd.foreach(println)

    countRdd.foreachPartition(data => {
      var config = HBaseConfiguration.create()
      //        		  config.set("hbase.zookeeper.quorum", "zhuhai7,zhuhai5,zhuhai9")
      config.set("hbase.zookeeper.quorum", zkquorum)
      config.set("hbase.zookeeper.property.clientPort", "2181")
      //    val conn =new HTable(myConf, TableName.valueOf(tableName))
      //mac表
      val TABLENAMEMAC = "mac_count";
      val TABLENAMEPLATE = "plate_count";
      val hMacTable = new HTable(config, TableName.valueOf(TABLENAMEMAC))
      val hPlateTable = new HTable(config, TableName.valueOf(TABLENAMEPLATE))
      //    val hMacTable = conn.getTable(TableName.valueOf(TABLENAMEMAC))
      //    val hPlateTable = conn.getTable(TableName.valueOf(TABLENAMEPLATE))

      //         var listPutMac = new ArrayList[Put]()
      //        var listPutPlate = new ArrayList[Put]()
      hMacTable.setAutoFlush(false, false) //关键点1
      hMacTable.setWriteBufferSize(3 * 1024 * 1024) //关键点2

      hPlateTable.setAutoFlush(false, false) //关键点1
      hPlateTable.setWriteBufferSize(3 * 1024 * 1024) //关键点2

      data.foreach { x =>
        {

          val x1 = x._1.split("_");
          val mac = x1(0)
          val plateNumber = x1(1)
          val daytime = x1(2)
          val locationCount = x._2._1
          val tongxingCount = x._2._2._1
          //详细info
          //440232343435,700000323_20160523140222_20160523140225
          //+440232343434,700000323_20160523140222_20160523140225-700000323_20160523140222_20160523140225
          //+440232343436,700000323_20160523140222_20160523140225-700000323_20160523140222_20160523140225
          val info = x._2._2._2
          val locationTX = x._2._2._3
          val hbase_column_name = "info"
          //mac_plateNumber为rowKey
          var rowKey_mac = mac.concat("_").concat(plateNumber)

          var rowKey_plate = plateNumber.concat("_").concat(mac)

          var put_plate = new Put(Bytes.toBytes(rowKey_plate))
          var put_mac = new Put(Bytes.toBytes(rowKey_mac))
          var get = new Get(Bytes.toBytes(rowKey_mac));

          var result = hMacTable.get(get);
          //如果原先数据不存在，不需要进行处理。
          if (result == null || result.isEmpty() || result.size() <= 0) {
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("locationcount"), Bytes.toBytes(locationCount))
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("tongxingcount"), Bytes.toBytes(tongxingCount))
            //这个用于显示每个卡口的同行信息
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("kakouinfo"), Bytes.toBytes(locationTX))
            //这个用于显示同行的详细信息
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("txinfo"), Bytes.toBytes(info))
            //同行天数，需要hbase的协处理器累加
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("daycount"), Bytes.toBytes(1))
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("begin"), Bytes.toBytes(daytime))
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("end"), Bytes.toBytes(daytime))
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("score"), Bytes.toBytes(0))

            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("locationcount"), Bytes.toBytes(locationCount))
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("tongxingcount"), Bytes.toBytes(tongxingCount))
            //这个用于显示每个卡口的同行信息
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("kakouinfo"), Bytes.toBytes(locationTX))
            //这个用于显示同行的详细信息
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("txinfo"), Bytes.toBytes(info))
            //同行天数，需要hbase的协处理器累加
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("daycount"), Bytes.toBytes(1))
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("begin"), Bytes.toBytes(daytime))
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("end"), Bytes.toBytes(daytime))
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("score"), Bytes.toBytes(0))

          } else {
            var reslocationCount = Bytes.toInt(result.getValue(Bytes.toBytes(hbase_column_name), Bytes.toBytes("locationcount")));
            var restongxingCount = Bytes.toInt(result.getValue(Bytes.toBytes(hbase_column_name), Bytes.toBytes("tongxingcount")));
            var reBegin = Bytes.toString(result.getValue(Bytes.toBytes(hbase_column_name), Bytes.toBytes("begin")));
            //这个用于显示每个卡口的同行信息
            //1112:1,2222:2
            var reskakouInfo = Bytes.toString(result.getValue(Bytes.toBytes(hbase_column_name), Bytes.toBytes("kakouinfo")));

            var reskakou = reskakouInfo.split("_", -1);
            //将location放入map集合用于location计算
            var tmpMap = new HashMap[String, Integer];

            for (tmp <- reskakou) {
              var t = tmp.split(":", -1);
              tmpMap.put(t(0), Integer.parseInt(t(1)));
            }

            var restxInfo = Bytes.toString(result.getValue(Bytes.toBytes(hbase_column_name), Bytes.toBytes("txinfo")));
            var resDayCount = Bytes.toInt(result.getValue(Bytes.toBytes(hbase_column_name), Bytes.toBytes("daycount")));
            var resScore = Bytes.toInt(result.getValue(Bytes.toBytes(hbase_column_name), Bytes.toBytes("score")));

            //			List<Cell> locationCount = put.get(Bytes.toBytes(hbase_column_name), Bytes.toBytes("locationcount"));
            var txCount = tongxingCount;
            var kakouInfo = locationTX
            var txInfo = info
            var dayCount = 1;
            var begin = daytime
            var end = daytime
            var score = 0;
            var dayCountNew = resDayCount + dayCount;
            var txCountNew = restongxingCount + txCount;
            var txInfoNew = txInfo + "+" + restxInfo;

            //卡口通过信息
            var locationInfoNew = "";
            var tmpCount = 0;
            var kakou = kakouInfo.split("_", -1);
            for (tmp <- kakou) {
              var t = tmp.split(":", -1);
              if (tmpMap.contains(t(0))) {
                var tCount = tmpMap.get(t(0)).get + t(1).toInt;
                tmpMap.put(t(0), tCount)

              } else {
                tmpMap.put(t(0), t(1).toInt)
              }
            }

            //遍历新的kakouinfo 生成新的locationInfoNew，
            var haslocationInfo = false;

            tmpMap.keys.foreach { i =>

              if (!haslocationInfo) {

                locationInfoNew = i.concat(":").concat(tmpMap.get(i).get.toString())
              } else {
                locationInfoNew += "_" + i.concat(":").concat(tmpMap.get(i).get.toString())
              }
              haslocationInfo = true;
              tmpCount = tmpCount + 1

            }

            //通过的location数
            var locationCountNew = tmpCount;
            var scoreNew = txCountNew * 3 + locationCountNew * 7;

            //这里是新添加的内容

            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("begin"), Bytes.toBytes(reBegin));
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("end"), Bytes.toBytes(end));
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("locationcount"), Bytes.toBytes(locationCountNew))
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("tongxingcount"), Bytes.toBytes(txCountNew))
            //这个用于显示每个卡口的同行信息
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("kakouinfo"), Bytes.toBytes(locationInfoNew))
            //这个用于显示同行的详细信息
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("txinfo"), Bytes.toBytes(txInfoNew))
            //同行天数，需要hbase的协处理器累加
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("daycount"), Bytes.toBytes(dayCountNew))
            put_mac.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("score"), Bytes.toBytes(scoreNew))

            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("begin"), Bytes.toBytes(reBegin));
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("end"), Bytes.toBytes(end));
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("locationcount"), Bytes.toBytes(locationCountNew))
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("tongxingcount"), Bytes.toBytes(txCountNew))
            //这个用于显示每个卡口的同行信息
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("kakouinfo"), Bytes.toBytes(locationInfoNew))
            //这个用于显示同行的详细信息
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("txinfo"), Bytes.toBytes(txInfoNew))
            //同行天数，需要hbase的协处理器累加
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("daycount"), Bytes.toBytes(dayCountNew))
            put_plate.addColumn(Bytes.toBytes(hbase_column_name), Bytes.toBytes("score"), Bytes.toBytes(scoreNew))

          }

          hMacTable.put(put_mac)
          hPlateTable.put(put_plate)

          //              listPutMac.add(put_mac)
          //              listPutPlate.add(put_plate)

          //               if(listPutMac.size() >= 10000 || listPutPlate.size() >= 10000 ){

          //                    hMacTable.put(listPutMac)
          //                    hPlateTable.put(listPutPlate)
          //                    listPutMac.clear()
          //                    listPutPlate.clear()
          //                }
        }
      }

      hMacTable.flushCommits()
      hPlateTable.flushCommits()
      hMacTable.close();
      hPlateTable.close();
    })
  }

  def getdate(x: Int): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, x)
    dateFormat.format(cal.getTime)
  }

  def getdate2(x: String): String = {
    //    20160523140222
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    var date2: Date = dateFormat.parse(x);

    var dateFormat2: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var dateString: String = dateFormat2.format(date2);

    return dateString;
  }

}