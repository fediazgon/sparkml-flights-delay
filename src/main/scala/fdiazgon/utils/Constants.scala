package fdiazgon.utils

object Constants {

  val NEW_COL_CRS_DEP_TIME_MIN = "CRSDepTimeMin"
  val NEW_COL_DEP_TIME_MIN = "DepTimeMin"
  val NEW_COL_OVERDELAY = "Overdelay"
  val NEW_COL_WEEKEND = "Weekend"
  val NEW_COL_ROUTE = "Route"

  val INDEX_INPUT_COLS = Array("UniqueCarrier", "Origin", "Dest", NEW_COL_ROUTE)
  val INDEXED_COL_SUFFIX = "_index"

  val FEATURES_INPUT_COLS = Array("Distance", "TaxiOut", "DepDelay", NEW_COL_CRS_DEP_TIME_MIN, NEW_COL_DEP_TIME_MIN)
  val FEATURES_OUTPUT_COL = "features"

  val LABEL_COL = "ArrDelay"
  val PREDICTION_COL = "Predicted"

  val METRIC_NAME = "mae"

}
