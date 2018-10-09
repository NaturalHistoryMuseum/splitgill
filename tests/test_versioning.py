#!/usr/bin/env python
# encoding: utf-8

from datetime import datetime, timedelta

import pytest

from eevee.utils import to_timestamp
from eevee.versioning import Versioned


def test_version_matches():
    reference_time = datetime.now()
    versioning = Versioned(to_timestamp(reference_time))

    day_before = to_timestamp(reference_time - timedelta(1))
    day_after = to_timestamp(reference_time + timedelta(1))

    assert versioning.version_matches(day_before)
    assert not versioning.version_matches(day_after)
    assert versioning.version_matches(day_before, day_after)
    assert not versioning.version_matches(day_after, day_before)
    assert not versioning.version_matches(day_before, day_before)


def test_version_enforces_int():
    with pytest.raises(AssertionError):
        Versioned(str(to_timestamp(datetime.now())))

    with pytest.raises(AssertionError):
        Versioned(u'banana')

    with pytest.raises(AssertionError):
        Versioned({u'a': 4})

    Versioned(1531440000000)
