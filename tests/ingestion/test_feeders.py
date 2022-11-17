#!/usr/bin/env python
# encoding: utf-8

from mock import MagicMock, call

from splitgill.ingestion.feeders import IngestionFeeder


class ExampleFeederForTests(IngestionFeeder):
    def __init__(self, version, test_records):
        super(ExampleFeederForTests, self).__init__(version)
        self.test_records = test_records

    @property
    def source(self):
        return u'testsource'

    def records(self):
        return self.test_records


def test_feeder():
    test_records = [u'1', u'beans', u'a', u'00000000']
    feeder = ExampleFeederForTests(10, test_records)
    read_records = list(feeder.read())
    assert read_records == test_records


def test_feeder_signals():
    test_records = [u'1', u'beans', u'a', u'00000000']
    feeder = ExampleFeederForTests(10, test_records)

    mock_reader_monitor = MagicMock(spec=lambda *args, **kwargs: None)
    feeder.read_signal.connect(mock_reader_monitor)

    read_records = list(feeder.read())
    assert read_records == test_records
    assert mock_reader_monitor.call_args_list == [
        call(feeder, number=1, record=u'1'),
        call(feeder, number=2, record=u'beans'),
        call(feeder, number=3, record=u'a'),
        call(feeder, number=4, record=u'00000000'),
    ]


def test_feeder_empty():
    test_records = []
    feeder = ExampleFeederForTests(10, test_records)

    mock_monitor = MagicMock(spec=lambda *args, **kwargs: None)
    feeder.read_signal.connect(mock_monitor)

    read_records = list(feeder.read())
    assert read_records == test_records
    assert not mock_monitor.called
