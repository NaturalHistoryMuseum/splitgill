#!/usr/bin/env python3
# encoding: utf-8

from datetime import datetime, timedelta

from eevee.versioning import Versioned


def test_version_matches():
    reference_time = datetime.now()
    versioning = Versioned(reference_time)

    day_before = reference_time - timedelta(1)
    day_after = reference_time + timedelta(1)

    assert versioning.version_matches(day_before)
    assert not versioning.version_matches(day_after)
    assert versioning.version_matches(day_before, day_after)
    assert not versioning.version_matches(day_after, day_before)
    assert not versioning.version_matches(day_before, day_before)
