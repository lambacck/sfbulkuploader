from __future__ import absolute_import

import logging

from celery import shared_task

from salesforce_bulk import SalesforceBulk, CsvDictsAdapter

from sqlalchemy import create_engine
from sqlalchemy.sql import text

log = logging.getLogger(__name__)


def field_modifier(value):
    if hasattr(value, 'isoformat'):
        return value.isoformat()

    if value is None:
        return ''

    return unicode(value).encode('utf-8')


def row_modifier(row):
    return [field_modifier(x) for x in row]


@shared_task
def upload_table(sessionId, hostname, tablename, connection_string):
    schema, table = tablename.split('.')

    log.debug('%s, %s, %s, %s, %s, %s', sessionId, hostname, tablename, connection_string, schema, table)

    bulk = SalesforceBulk(
        sessionId=sessionId,
        host=hostname)

    engine = create_engine(connection_string)

    result = engine.execute(text('select column_name from information_schema.columns where table_name = :table and table_schema = :schema'), {'table': table, 'schema': schema})
    exclude = ['sfid', 'id', 'systemmodstamp', 'isdeleted']
    columns = [x[0] for x in result if not x[0].startswith('_') and x[0].lower() not in exclude]

    log.debug('columns: %s', columns)
    column_select = ','.join('"%s"' % x for x in columns)

    result = engine.execute('select %s from %s' % (column_select, tablename))

    dict_iter = (dict(zip(columns, row_modifier(row))) for row in result)
    dict_iter = list(dict_iter)
    log.debug('Sending rows: %s', [x['name'] for x in dict_iter])
    csv_iter = CsvDictsAdapter(iter(dict_iter))

    job = bulk.create_insert_job(table.capitalize(), contentType='CSV')
    batch = bulk.post_bulk_batch(job, csv_iter)

    bulk.wait_for_batch(job, batch)

    bulk_result = []

    def save_results(rows, failed, remaining):
        bulk_result[:] = [rows, failed, remaining]

    flag = bulk.get_upload_results(job, batch, callback=save_results)

    bulk.close_job(job)

    log.debug('results: %s, %s', flag, bulk_result)

    return bulk_result
