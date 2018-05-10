package model

class PlugData(mId: Long, mTimestamp: Long, mValue: Float, mProperty: Boolean, mPlug: Int, mHousehold: Int, mHouse: Int ) extends Serializable {

  var id : Long = mId
  var timestamp : Long = mTimestamp
  var value : Float = mValue
  var property : Boolean = mProperty
  var plug_id : Int = mPlug
  var household_id : Int = mHousehold
  var house_id : Int = mHouse
}
