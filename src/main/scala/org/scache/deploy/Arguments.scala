package org.scache.deploy

import org.scache.rpc.RpcAddress
import org.scache.util.{Logging, ScacheConf, Utils}

/**
 * Created by frankfzw on 16-9-13.
 */
private[deploy] trait Arguments extends Logging {
  def parse(args: List[String]): Unit
}

private[deploy] class MasterArguments(args: Array[String], conf: ScacheConf) extends Arguments {
  var host = Utils.findLocalInetAddress().getHostAddress
  var port = 6388
  var isLocal = true

  // Check for settings in environment variables
  host = conf.getString("scache.master.ip", host)
  port = conf.getInt("scache.master.port", port)

  parse(args.toList)

  override def parse(args: List[String]): Unit = args match {
    case ("--ip" | "-i") :: value :: tail =>
      // Utils.checkHost(value, "ip is invalid " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: value :: tail =>
      port = value.toInt
      parse(tail)

    case ("--local" | "-l") :: value :: tail =>
      isLocal = value.toBoolean
      parse(tail)

    case Nil => // end here

    case _ =>
      // scalastyle:off println
      System.err.println(
        "Usage: Master [options]\n" +
        "\n" +
        "Options:\n" +
        "  -i HOST, --ip HOST     Hostname to listen on \n" +
        "  -p PORT, --port PORT   Port to listen on (default: 6388)\n" +
        "                         Default is conf/scache.conf.")
      // scalastyle:on println
      System.exit(1)
  }
}

private[deploy] class ClientArguments(args: Array[String], conf: ScacheConf) extends Arguments {
  var host = Utils.findLocalInetAddress().getHostAddress
  var masterIp = host
  var isLocal = true
  var port = 5678
  var masterPort = 6388

  masterIp = conf.getString("scache.master.ip", masterIp)
  port = conf.getInt("scache.clent.port", port)
  masterPort = conf.getInt("scache.master.port", masterPort)


  parse(args.toList)


  override def parse(args: List[String]): Unit = args match {
    case ("--master" | "-m") :: value :: tail =>
      // Utils.checkHost(value, "ip is invalid" + value)
      masterIp = value
      parse(tail)
    case ("--ip" | "-i") :: value :: tail =>
      // Utils.checkHost(value, "ip is invalid " + value)
      host = value
      parse(tail)
    case ("--local" | "-l") :: value :: tail =>
      isLocal = value.toBoolean
      parse(tail)
    case ("--port" | "-p") :: value :: tail =>
      port = value.toInt
      parse(tail)

    case ("--masterPort" | "-mp") :: value :: tail =>
      masterPort = value.toInt
      parse(tail)
    case Nil => // end here

    case _ =>
      // scalastyle:off println
      System.err.println(
        "Usage: Client [options]\n" +
        "\n" +
        "Options:\n" +
        "  -i HOST, --ip HOST     Hostname to listen on \n" +
        "  -p PORT, --port PORT   Port to listen on (default: 5678)\n" +
        "  -m HOST, --master HOST     Hostname of SCache Master\n" +
        "                         Default is conf/spark-defaults.conf.")
      // scalastyle:on println
      System.exit(1)

  }

}
