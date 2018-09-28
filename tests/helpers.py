#!/usr/bin/env python3
# encoding: utf-8


# h/t http://code.activestate.com/recipes/52308-the-simple-but-handy-collector-of-a-bunch-of-named/
class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
