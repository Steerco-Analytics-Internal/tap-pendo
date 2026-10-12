import json

import humps
import singer
from singer import Transformer, metadata, metrics
from singer.transform import strptime_to_utc
from singer.utils import strftime
from tap_pendo.streams import is_limit_reached

LOGGER = singer.get_logger()


def sync_stream(state, start_date, instance):
    stream = instance.stream

    # Get bookmark from state or start date for the stream
    bookmark_date = instance.get_bookmark(state, instance.name, start_date,
                                          instance.replication_key)
    bookmark_dttm = strptime_to_utc(bookmark_date)
    new_bookmark = bookmark_dttm
    max_records = instance.max_records

    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer(
            integer_datetime_fmt="unix-milliseconds-integer-datetime-parsing"
    ) as transformer:
        loop_for_records = True
        # A limited stream stops once it has emitted its records
        while loop_for_records and not is_limit_reached(counter.value, max_records):
            # Get records for the stream
            (stream, records), loop_for_records = instance.sync(state, new_bookmark)
            for record in records:
                if is_limit_reached(counter.value, max_records):
                    break
                schema_dict = stream.schema.to_dict()
                stream_metadata = metadata.to_map(stream.metadata)

                transformed_record = instance.transform(record)

                # Transform record as per field selection in metadata
                try:
                    transformed_record = transformer.transform(
                        transformed_record, schema_dict, stream_metadata)
                except Exception as err:
                    LOGGER.error('Error: %s', err)
                    LOGGER.error(' for schema: %s',
                                 json.dumps(schema_dict, sort_keys=True, indent=2))
                    LOGGER.error('Transform failed for %s', record)
                    raise err

                # Check for replication_value from record and if value found then use it for updating bookmark
                replication_value = transformed_record.get(
                    humps.decamelize(instance.replication_key))
                if replication_value:
                    record_timestamp = strptime_to_utc(replication_value)
                    new_bookmark = max(new_bookmark, record_timestamp)

                    if record_timestamp >= bookmark_dttm:
                        singer.write_record(stream.tap_stream_id, transformed_record)
                        counter.increment()

                else: # No replication_value found then write record without considering for bookmark
                    singer.write_record(stream.tap_stream_id, transformed_record)
                    LOGGER.info('Replication Value NULL for tap_stream_id: %s', stream.tap_stream_id)
                    counter.increment()

                # preserve the last processed record which will be useful if stream supports pagination
                instance.last_synced_record = record

            # Update bookmark and write state for the stream with new_bookmark
            instance.update_bookmark(state, instance.name, strftime(new_bookmark),
                                     instance.replication_key)
            singer.write_state(state)

            # Stop paging the parent once its limited child has its records and the parent needs no more
            if (instance.limited_child_is_done()
                    and (max_records is None or is_limit_reached(counter.value, max_records))):
                break

        return counter.value


def sync_full_table(state, instance):
    stream = instance.stream

    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer(
            integer_datetime_fmt="unix-milliseconds-integer-datetime-parsing"
    ) as transformer:
        # A stream limited to 0 records makes no request
        if is_limit_reached(0, instance.max_records):
            return counter.value
        # Get records for the stream
        (stream, records) = instance.sync(state)
        for record in records:
            # A limited stream stops once it has emitted its records
            if is_limit_reached(counter.value, instance.max_records):
                break
            schema_dict = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            transformed_record = instance.transform(record)

            # Transform record as per field selection in metadata
            try:
                transformed_record = transformer.transform(
                    transformed_record, schema_dict, stream_metadata)
            except Exception as err:
                LOGGER.error('Error: %s', err)
                LOGGER.error(' for schema: %s',
                             json.dumps(schema_dict, sort_keys=True, indent=2))
                LOGGER.error('Transform failed for %s', record)
                raise err

            singer.write_record(stream.tap_stream_id, transformed_record)
            counter.increment()
        # return the count of records synced
        return counter.value
