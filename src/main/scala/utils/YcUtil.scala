package utils

import org.apache.spark.sql.Row


object YcUtil {
  /*
  将ip地址转为长整形
   */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("\\.") //  1.1.1.1
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L // |    <<   优先级
      //                           00000001
      //   ipNum<<8L       0000000000000000     |
      //                    0000000000000010000000001
    }
    ipNum
  }

  /*
  到给定的ip规则( ipRules列表) 中查找  ip，返回下标
   */
  def binarySearch(ipRules: Array[Row], ip: Long): Int = {   //     structed data
    var low = 0
    var high = ipRules.length - 1
    while (low <= high) {
      var middle = (low + high) / 2
      if ((ip >= ipRules(middle).getAs("startNum").asInstanceOf[Long]) && (ip <= ipRules(middle).getAs("endNum").asInstanceOf[Long])) {
        return middle
      } else if (ip < ipRules(middle).getAs("startNum").asInstanceOf[Long]) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1
  }

}

case class IpRule(startNum: Long, endNum: Long, province: String)
