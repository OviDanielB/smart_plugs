package model

class PlugData(mId: Long, mTimestamp: Long, mValue: Float, mProperty: Boolean, mPlug: Int, mHousehold: Int, mHouse: Int ) extends Serializable {

  def this() = this(0l,0l,0f,false,0,0,0)

  var id : Long = mId
  var timestamp : Long = mTimestamp
  var value : Float = mValue
  var property : Boolean = mProperty
  var plug_id : Int = mPlug
  var household_id : Int = mHousehold
  var house_id : Int = mHouse

  def isWorkMeasurement() : Boolean = {
    !this.property
  }

  def isLoadMeasurement() : Boolean = {
    this.property
  }

  def fullPlugID : String = {
    val houseString = String.valueOf(this.house_id)
    val householdString = String.valueOf(this.household_id)
    val plugString = String.valueOf(this.plug_id)

    //houseString + "_" + householdString + "_" + plugString
    houseString + "_" + plugString
  }
}
