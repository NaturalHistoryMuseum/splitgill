#!/usr/bin/env python3
# encoding: utf-8


class Versioned(object):

    def __init__(self, version):
        self.version = version

    def version_matches(self, lower, upper=None):
        """
        Checks whether the version lies within the lower and upper timestamp bounds specified. If no upper bound is
        specified the version just has to be later than the lower timestamp.

        :param lower: the lower timestamp (inclusive)
        :param upper: the upper timestamp (exclusive), can be None
        :return: True if the version sits within the limits, False if not
        """
        return lower <= self.version and (upper is None or self.version < upper)
