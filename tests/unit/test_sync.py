import copy
import unittest
from unittest.mock import patch

import singer
import tap_postgres


class TestMixedSync(unittest.TestCase):
    def test_new_and_resumed_snapshots_survive_cdc(self):
        def stream(name, selected=True):
            return {
                'tap_stream_id': name, 'stream': name, 'table_name': name,
                'schema': {'properties': {'id': {'type': 'integer'}}},
                'metadata': [
                    {'breadcrumb': [], 'metadata': {
                        'selected': selected, 'database-name': 'postgres',
                        'schema-name': 'public', 'replication-method': 'LOG_BASED'}},
                    {'breadcrumb': ['properties', 'id'], 'metadata': {'selected': True}},
                ],
            }

        def bookmark(lsn, version, xmin=None):
            return {'last_replication_method': 'LOG_BASED', 'lsn': lsn,
                    'version': version, 'xmin': xmin}

        def snapshot(conn, selected, current, columns, metadata):
            # Successful full-table completion preserves a resumed snapshot LSN.
            entry = current['bookmarks'][selected['tap_stream_id']]
            entry.setdefault('version', 40)
            entry['xmin'] = None
            return current

        def cdc(conn, selected, current, end_lsn, state_file):
            # Model WAL filtering and per-table progress. The resumed snapshot
            # needs WAL older than the existing table's bookmark.
            start = min(current['bookmarks'][s['tap_stream_id']]['lsn'] for s in selected)
            for selected_stream in selected:
                name = selected_stream['tap_stream_id']
                if name == 'resumed' and start <= 150:
                    singer.write_message(singer.RecordMessage(stream=name, record={'id': 7}))
                current['bookmarks'][name]['lsn'] = 300
            singer.write_message(singer.StateMessage(value=copy.deepcopy(current)))
            return current

        for mixed in (True, False):
            with self.subTest(mixed=mixed):
                state = {'currently_syncing': 'resumed', 'bookmarks': {
                    'resumed': bookmark(100, 20, 42),
                }}
                catalog = {'streams': [stream('new'), stream('resumed')]}
                if mixed:
                    state['bookmarks'].update({
                        'existing': bookmark(200, 10), 'removed': bookmark(50, 30),
                    })
                    catalog['streams'].extend([stream('existing'), stream('removed', False)])
                emitted = []
                with patch.object(tap_postgres, 'refresh_streams_schema'), \
                        patch.object(tap_postgres, 'register_type_adapters'), \
                        patch.object(tap_postgres.sync_common, 'send_schema_message'), \
                        patch.object(tap_postgres.logical_replication, 'fetch_current_lsn', return_value=300), \
                        patch.object(tap_postgres.full_table, 'sync_table', side_effect=snapshot), \
                        patch.object(tap_postgres.logical_replication, 'sync_tables', side_effect=cdc), \
                        patch.object(singer, 'write_message', side_effect=emitted.append):
                    result = tap_postgres.do_sync({}, catalog, 'LOG_BASED', state)

                expected = {'new': bookmark(300, 40),
                            'resumed': bookmark(300 if mixed else 100, 20)}
                if mixed:
                    expected['existing'] = bookmark(300, 10)
                self.assertEqual(result, {'currently_syncing': None, 'bookmarks': expected})
                records = [message for message in emitted if isinstance(message, singer.RecordMessage)]
                self.assertEqual([(message.stream, message.record) for message in records],
                                 [('resumed', {'id': 7})] if mixed else [])
                self.assertEqual(emitted[-1].value, result)
