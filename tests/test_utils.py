from datetime import datetime, timedelta, timezone, date

from freezegun import freeze_time

from splitgill.utils import to_timestamp, parse_to_timestamp, now, partition


class TestToTimestamp:
    def test_no_tz(self):
        assert to_timestamp(datetime(2012, 1, 14, 12, 0, 1)) == 1326542401000

    def test_with_utc_tz(self):
        assert (
            to_timestamp(datetime(2012, 1, 14, 12, 0, 1, tzinfo=timezone.utc))
            == 1326542401000
        )

    def test_with_other_tz(self):
        four_hours_ahead = timezone(timedelta(hours=4))
        assert (
            to_timestamp(datetime(2012, 1, 14, 12, 0, 1, tzinfo=four_hours_ahead))
            == 1326528001000
        )

    def test_no_rounding(self):
        # check that the 9 doesn't round the 3 up, it just gets cut off
        assert (
            to_timestamp(datetime(2012, 1, 14, 12, 0, 1, 333999)) == 1326542401000 + 333
        )

    def test_date(self):
        assert to_timestamp(date(2012, 1, 14)) == 1326499200000


class TestParseTimestamp:
    def test_default_no_tz(self):
        assert parse_to_timestamp("2012-01-14", "%Y-%m-%d") == 1326499200000

    def test_default_no_tz_is_utc(self):
        no_tz = parse_to_timestamp("2012-01-14", "%Y-%m-%d")
        with_utc_tz = parse_to_timestamp("2012-01-14", "%Y-%m-%d", timezone.utc)
        assert no_tz == with_utc_tz

    def test_different_tz(self):
        five_hours_behind = timezone(timedelta(hours=-5))
        assert (
            parse_to_timestamp(
                "2012-01-14 15:30:54", "%Y-%m-%d %H:%M:%S", five_hours_behind
            )
            == 1326573054000
        )

    def test_when_format_has_tz(self):
        # if UTC was used instead of the tz in the formatted string, we'd expect to get
        # 1326555054000 as the result
        assert (
            parse_to_timestamp("2012-01-14 15:30:54 +0300", "%Y-%m-%d %H:%M:%S %z")
            == 1326544254000
        )

    def test_when_format_has_tz_and_we_give_tz(self):
        ten_hours_ahead = timezone(timedelta(hours=10))
        # if the +10 tz was applied here then we'd expect 1326519054000 to come out, but
        # it is ignored because the timezone is specified in the formatted string
        assert (
            parse_to_timestamp(
                "2012-01-14 15:30:54 +0300", "%Y-%m-%d %H:%M:%S %z", ten_hours_ahead
            )
            == 1326544254000
        )


@freeze_time("2012-01-14 12:00:01")
def test_now():
    assert now() == 1326542401000


class TestPartition:
    def test_empty(self):
        assert list(partition([], 10)) == []

    def test_even(self):
        assert list(partition([1, 2, 3, 4, 5, 6], 2)) == [[1, 2], [3, 4], [5, 6]]

    def test_uneven(self):
        assert list(partition([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]

    def test_more(self):
        assert list(partition([1, 2, 3, 4, 5], 100)) == [[1, 2, 3, 4, 5]]
