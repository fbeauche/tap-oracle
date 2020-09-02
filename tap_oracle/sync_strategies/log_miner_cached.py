#!/usr/bin/env python3
import singer
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import tap_oracle.db as orc_db
import tap_oracle.sync_strategies.common as common
import tap_oracle.sync_strategies.log_miner as log_miner
import singer.metrics as metrics
import copy
import pdb
import time
import decimal
import cx_Oracle

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000


def sync_table(conn_config, stream, state, desired_columns):
   connection = orc_db.open_connection(conn_config)
   connection.outputtypehandler = common.OutputTypeHandler

   stream = log_miner.add_automatic_properties(stream)

   cur = connection.cursor()
   cur.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
   cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS."00+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
   time_extracted = utils.now()

   common.send_schema_message(stream, ['scn'])

   stream_version = singer.get_bookmark(state, stream.tap_stream_id, 'version')
   # If there was no bookmark for stream_version, it is the first time
   # this table is being sync'd, so get a new version, write to
   # state
   if stream_version is None:
      stream_version = int(time.time() * 1000)
      state = singer.write_bookmark(state,
                                    stream.tap_stream_id,
                                    'version',
                                    stream_version)
      singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

   activate_version_message = singer.ActivateVersionMessage(
      stream=stream.stream,
      version=stream_version)
   singer.write_message(activate_version_message)

   md = metadata.to_map(stream.metadata)
   schema_name = md.get(()).get('schema-name')

   escaped_columns = map(lambda c: common.prepare_columns_sql(stream, c,"t"), desired_columns)
   escaped_schema  = schema_name
   escaped_table   = stream.table
   cache_table = "VOOBANSTITCH.V_SCN_ROW_PER_TABLE"

   bookmarked_scn = singer.get_bookmark(state, stream.tap_stream_id, 'scn')
   
   with metrics.record_counter(None) as counter:
      LOGGER.info("Resuming Incremental replication from scn = %s",  bookmarked_scn)

      select_sql      = f"""SELECT {','.join(escaped_columns)}, v.cscn as scn
                              FROM {escaped_schema}.{escaped_table} t
                              INNER JOIN  {cache_table} v
                                 ON t.rowid = v.row_id AND v.SEG_OWNER = '{escaped_schema}' AND v.TABLE_NAME = '{escaped_table}'
                              WHERE v.cscn >= {bookmarked_scn}
                              ORDER BY v.cscn ASC
                              """
 
      rows_saved = 0
      LOGGER.info("select %s", select_sql)
      columns_for_record = desired_columns + ['scn', '_sdc_deleted_at']
      for row in cur.execute(select_sql):
         rowWithNullDeletedAt = row + (None,)
         record_message = common.row_to_singer_message(stream,
                                                       rowWithNullDeletedAt,
                                                       stream_version,
                                                       columns_for_record,
                                                       time_extracted)

         singer.write_message(record_message)
         rows_saved = rows_saved + 1

         state = singer.write_bookmark(state,
                                       stream.tap_stream_id,
                                       'scn',
                                       record_message.record['scn'])

         if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
             singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

         counter.increment()
 
   # TODO : Get deleted rows?
   cur.close()
   connection.close()
   return state

# Local Variables:
# python-indent-offset: 3
# End:
