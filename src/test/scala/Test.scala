
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date


object Test {
    def main(args: Array[String]) {
//        val path = new File("E://id_rsa.pub");
//        println("我们要处理的路径为："+path)
//        
//        if(path.exists()){
//          print(path.getPath);
//        }
 
      print( getdate2("20160523140222"));
        
      
    }
    
    
    
     def getdate2(x:String):String = {
//    20160523140222
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    var date2:Date =  dateFormat.parse(x);
    
     var dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
     var dateString:String=dateFormat2.format(date2);
     
     return dateString;
  }
    
    
         def getdate(x:Int):String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, x)
    dateFormat.format(cal.getTime)
  }
}