from mock import MagicMock, call

from eevee.ingestion.feeders import IngestionFeeder


class TestFeeder(IngestionFeeder):

    def __init__(self, version, test_records):
        super(TestFeeder, self).__init__(version)
        self.test_records = test_records

    @property
    def source(self):
        return 'testsource'

    def records(self):
        return self.test_records


def test_feeder():
    test_records = ['1', 'beans', 'a', '00000000']
    feeder = TestFeeder(10, test_records)

    mock_monitor = MagicMock()
    feeder.register_monitor(mock_monitor)
    assert mock_monitor in feeder.monitors

    read_records = list(feeder.read())
    assert read_records == test_records
    assert mock_monitor.call_args_list == list(map(call, test_records))


def test_feeder_empty():
    test_records = []
    feeder = TestFeeder(10, test_records)

    mock_monitor = MagicMock()
    feeder.register_monitor(mock_monitor)
    assert mock_monitor in feeder.monitors

    read_records = list(feeder.read())
    assert read_records == test_records
    assert not mock_monitor.called
