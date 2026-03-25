package com.simpplr.sql

trait CommonSql {
  def MAX_ID_OF_TABLE = "SELECT NVL(MAX(id),0) from %s"

  def MAX_ID_OF_TWO_TABLES = "SELECT max_id from (SELECT (SELECT NVL(MAX(id),0) from %s) as t1_MAX,   (SELECT NVL(MAX(id),0) from %s) as t2_MAX,   GREATEST(t1_MAX, t2_MAX) as max_id)"

  def DATA_SOURCE_ID_FOR_ZEUS_SQL = "SELECT IFNULL(id,-1) as data_source_id from udl.ref_data_source where code ='DS002' and active_flag = true "

  def DATA_SOURCE_ID_FOR_CORE_SQL = "SELECT IFNULL(id,-1) as data_source_id from udl.ref_data_source where code ='DS001' and active_flag = true "

  def DATA_SOURCE_ID_FOR_SHARED_SQL = "SELECT IFNULL(id,-1) as data_source_id from udl.ref_data_source where code ='DS006' and active_flag = true "

  def DATA_SOURCE_ID_FOR_LS_SQL = "SELECT IFNULL(id,-1) as data_source_id from udl.ref_data_source where code ='DS005' and active_flag = true "
}
