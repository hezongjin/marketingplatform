import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

import com.dadiyunwu.util.{CommHelper, SparkHelper}

import scala.collection.mutable
import scala.io.Source

object test {
  def main(args: Array[String]): Unit = {

    val pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val time = LocalDateTime.ofInstant(new Date(System.currentTimeMillis()).toInstant, ZoneId.systemDefault())
    val time2 = time.format(pattern)


    val predicates =
      Array(
        "2015-09-16" -> "2015-09-30",
        "2015-10-01" -> "2015-10-15",
        "2015-10-16" -> "2015-10-31",
        "2015-11-01" -> "2015-11-14",
        "2015-11-15" -> "2015-11-30",
        "2015-12-01" -> "2015-12-15"
      ).map {
        case (start, end) =>
          s"cast(modified_time as date) >= date '$start' " + s"AND cast(modified_time as date) <= date '$end'"
      }


    val industryMap = CommHelper.readFile2Map4String(test.getClass, "industry.txt")
    //    println(industryMap.mkString(","))

    println(Byte.MaxValue)
    println(Byte.MinValue)


  }
}
