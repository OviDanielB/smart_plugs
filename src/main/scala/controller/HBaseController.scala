package controller

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}


object HBaseController {

  private[this] lazy val conf : Configuration = HBaseConfiguration.create
  conf.set("hbase.zookeeper.quorum", "localhost")
  //  conf.set("hbase.zookeeper.znode.parent", "localhost")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.master", "localhost:60000")

  private[this] lazy val connection : Connection = ConnectionFactory.createConnection(conf)

  def getDefaultConnection: Connection = {
    this.connection
  }

  def getDefaultConfiguration : Configuration = {
    this.conf
  }
}
