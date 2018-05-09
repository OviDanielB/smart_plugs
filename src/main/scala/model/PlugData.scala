package model

class PlugData(mId: Int, mTimestamp: Int, mValue: Float, mProperty: Boolean, mPlug: Int, mHousehold: Int, mHouse: Int ) {

  var id : Int = mId
  var timestamp : Int = mTimestamp
  var value : Float = mValue
  var property : Boolean = mProperty
  var plug_id : Int = mPlug
  var household_id : Int = mHousehold
  var house_id : Int = mHouse
}
