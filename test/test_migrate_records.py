from unittest import TestCase
from srcRow import SrcRow
from sinkRow import SinkRow

class TestSrc_row(TestCase):
    def test_initial_row(self):
        sr = SrcRow()
        self.assertEqual(sr.id, None)
        self.assertEqual(sr.time, None)
        self.assertEqual(sr.date, None)
        self.assertEqual(sr.source_address, None)
        self.assertEqual(sr.destination_address, None)
        self.assertEqual(sr.data, None)
        self.assertEqual(sr.cemi, None)

    def test_inital_row_failing(self):
        sr2 = SrcRow()
        self.assertFalse(sr2.id, 5)

class TestSink_row(TestCase):
    def test_inital_sink_row(self):
        sink = SinkRow()
        self.assertEqual(sink.sequence_number, None)
        self.assertEqual(sink.timestamp, None)
        self.assertEqual(sink.source_addr, None)
        self.assertEqual(sink.destination_addr, None)
        self.assertEqual(sink.apci, None)
        self.assertEqual(sink.priority, None)
        self.assertEqual(sink.flag_communication, None)
        self.assertEqual(sink.flag_read, None)
        self.assertEqual(sink.flag_write, None)
        self.assertEqual(sink.flag_transmit, None)
        self.assertEqual(sink.flag_refresh, None)
        self.assertEqual(sink.flag_read_at_init, None)
        self.assertEqual(sink.repeated, None)
        self.assertEqual(sink.hop_count, None)
        self.assertEqual(sink.payload, None)
        self.assertEqual(sink.payload_length, None)
        self.assertEqual(sink.raw_package, None)
        self.assertEqual(sink.is_manipulated, None)
        self.assertEqual(sink.attack_type_id, None)

class TestMigrate_records(TestCase):
    def test_migrate_records(self):
        self.assertEqual(2,2)
        #self.fail()

if __name__ == '__main__':
    unittest.main()