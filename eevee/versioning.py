#!/usr/bin/env python
# encoding: utf-8


class Versioned(object):

    def __init__(self, version):
        # has to be an int representing an epoch time in milliseconds, this assert makes sure the
        # developer is doing it right
        assert isinstance(version, int), u'the version must be an integer number of milliseconds' \
                                         u'since the UNIX epoch'
        self.version = version

    def version_matches(self, lower, upper=None):
        """
        Checks whether the version lies within the lower and upper timestamp bounds specified. If no
        upper bound is specified the version just has to be later than the lower timestamp.

        :param lower: the lower timestamp (inclusive) as an integer number of milliseconds since the
                      UNIX epoch
        :param upper: the upper timestamp (exclusive) as an integer number of milliseconds since the
                      UNIX epoch or None
        :return: True if the version sits within the limits, False if not
        """
        return lower <= self.version and (upper is None or self.version < upper)
