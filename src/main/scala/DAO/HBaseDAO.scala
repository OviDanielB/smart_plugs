package DAO

import controller.HBaseController
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object HBaseDAO {

  private val connection : Connection = HBaseController.getDefaultConnection

  private def b(s : String) : Array[Byte] = Bytes.toBytes(s)

  def createTable(name : String, columnFamilies: Array[String]) : Boolean = {

    val admin : Admin = connection.getAdmin

    val tableDescriptor : HTableDescriptor = new HTableDescriptor(TableName.valueOf(name))

    for (columnFamily <- columnFamilies){
      tableDescriptor.addFamily(new HColumnDescriptor(columnFamily))
    }

    admin.createTable(tableDescriptor)
    true
  }

  def listTables() : ListBuffer[String] = {
    var list = new ListBuffer[String]()

    val admin : Admin = connection.getAdmin
    val tableDescriptors : Array[HTableDescriptor] = admin.listTables

    for (i <- tableDescriptors) {
      val tableName : String = i.getNameAsString
      list += tableName
    }
    list
  }

  def describeTable(table: String) : String = {

    val admin : Admin = connection.getAdmin

    val tableName : TableName = TableName.valueOf(table)
    val tableDescriptor : HTableDescriptor = admin.getTableDescriptor(tableName)

    var columnFamilies : String = ""
    val hColumnDescriptor : Array[HColumnDescriptor] = tableDescriptor.getColumnFamilies

    for (j <- hColumnDescriptor) {
      columnFamilies += j.getNameAsString
      columnFamilies += " "
    }

    tableName + ": " + columnFamilies
  }

  def exists(table: String) : Boolean = {
    val admin : Admin = connection.getAdmin
    val tableName : TableName = TableName.valueOf(table)

    admin.tableExists(tableName)
  }

  def dropTable(table : String) : Unit = {
    val admin : Admin = connection.getAdmin
    val tableName : TableName = TableName.valueOf(table)


    // To delete a table or change its settings, you need to first disable the table
    admin.disableTable(tableName)

    // Delete the table
    admin.deleteTable(tableName)
  }

  def truncateTable(table: String, preserveSplits: Boolean) : Unit = {
    val admin : Admin = connection.getAdmin
    val tableName : TableName = TableName.valueOf(table)

    // To delete a table or change its settings, you need to first disable the table
    admin.disableTable(tableName)

    // Truncate the table
    admin.truncateTable(tableName, preserveSplits)
  }

  def put(table: String, rowKey: String, columns: Array[String]) : Unit = {
    val hTable : Table = connection.getTable(TableName.valueOf(table));

    val p : Put = new Put(Bytes.toBytes(rowKey))

    for (i <- 0 to columns.length/3) {

      val columnFamily : String = columns(i*3)
      val column : String = columns(i*3 + 1)
      val value : String = columns(i*3 + 2)

      p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))

    }

    // Saving the put Instance to the HTable.
    hTable.put(p)

    // closing HTable
    hTable.close
  }

  def get(table : String, rowKey: String, columnFamily: String, column : String) : Unit = {
    // Instantiating HTable class
    val hTable : Table = connection.getTable(TableName.valueOf(table))

    val g : Get = new Get(Bytes.toBytes(rowKey))

    // Narrowing the scope
    // g.addFamily(b(columnFamily));
    // g.addColumn(b(columnFamily), b(column));

    // Reading the data
    val result : Result = hTable.get(g)

    val value : Array[Byte] = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
    Bytes.toString(value)
  }

  def delete(table : String, rowKey : String, columnFamily: String, column : String) : Boolean = {

    // Instantiating HTable class
    val hTable : Table = connection.getTable(TableName.valueOf(table))

    // Instantiating Delete class
    val delete : Delete = new Delete(Bytes.toBytes(rowKey))

    if (columnFamily != null && column != null)
      delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column))

    else if (columnFamily != null)
      delete.addFamily(Bytes.toBytes(columnFamily))

      // deleting the data
    hTable.delete(delete)

      // closing the HTable object
    hTable.close
    true
  }

  def scanTable(table : String, columnFamily: String, column : String) {

    val items: Table = connection.getTable(TableName.valueOf(table))

    val scan: Scan = new Scan

    if (columnFamily != null && column != null)
      scan.addColumn(b(columnFamily), b(column));

    else if (columnFamily != null)
      scan.addFamily(b(columnFamily))

    val scanner: ResultScanner = items.getScanner(scan)

    var r = scanner.next
    // Reading values from scan result
    while (r != null) {
      println("Found row : " + r)
      r = scanner.next()
    }

    scanner.close()
  }

  def printRow(result : Result) = {
    val cells = result.rawCells();
    print( Bytes.toString(result.getRow) + " : " )
    for(cell <- cells){
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      val col_value = Bytes.toString(CellUtil.cloneValue(cell))
      print("(%s,%s) ".format(col_name, col_value))
    }
    println()
  }


  def main(args: Array[String]) = {

    HBaseAdmin.checkHBaseAvailable(conf)
    println("HBase is running!")

    if (exists("test_table")) {
            dropTable("test_table")
    }

    createTable("test_table", Array("test_family"))

//     Put example
    val table : Table = connection.getTable(TableName.valueOf("test_table"))

    val put = new Put(Bytes.toBytes("row1"))

    put.addColumn(Bytes.toBytes("test_family"), Bytes.toBytes("test_column_name"), Bytes.toBytes("test_value"))
    put.addColumn(Bytes.toBytes("test_family"), Bytes.toBytes("test_column_name2"), Bytes.toBytes("test_value2"))

    table.put(put)

    // Get example
    println("Get Example:")
    var get = new Get(Bytes.toBytes("row1"))
    var result = table.get(get)
    printRow(result)

    //Scan example
    println("\nScan Example:")
    var scan = table.getScanner(new Scan())
    scan.asScala.foreach(result => {
      printRow(result)
    })

    table.close()
    connection.close()
  }
}