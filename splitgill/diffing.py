import abc
import marshal

import dictdiffer
import six


def format_diff(differ, diff):
    """
    Formats the given differ and diff for mongo storage.

    :param differ: the differ object
    :param diff: the diff
    :return: a dict for storage
    """
    return {u'id': differ.differ_id, u'd': diff}


def extract_diff(raw_diff):
    """
    Given a diff from mongo storage, return the differ object used and the diff itself.

    :param raw_diff: the diff from mongo
    :return: a 2-tuple of the differ object used to create the diff and the diff itself
    """
    return differs[raw_diff[u'id']], raw_diff[u'd']


@six.add_metaclass(abc.ABCMeta)
class Differ(object):
    """
    Abstract class defining the Differ interface methods.
    """

    def __init__(self, differ_id):
        """
        :param differ_id: the id of the differ, this will be stored alongside the diffs produced
        """
        self.differ_id = differ_id

    @abc.abstractmethod
    def can_diff(self, data):
        """
        Whether this differ can diff the given data.

        :param data: the data to check
        :return: True or False
        """
        pass

    @abc.abstractmethod
    def diff(self, old, new, ignore=None):
        """
        Produce a diff that when provided to the patch function can modify the old data
        state to the new data state. If ignore is provided then the keys within it will
        be ignored during the diff.

        :param old: the old data
        :param new: the new data
        :param ignore: the keys to ignore. This should be a list or a set.
        :return: the diff
        """
        pass

    @abc.abstractmethod
    def patch(self, diff_result, old, in_place=False):
        """
        Given the return from the diff function and some data, apply the diff to patch
        the old data. If the in_place parameter is True then the patch will be applied
        in place and the old data passed in will be returned. If in_place is False (the
        default) then the old data is copied before applying the patch.

        :param diff_result: the diff to apply
        :param old: the old data
        :param in_place: whether to update the old data in place or not (default: False)
        :return: the updated data
        """
        pass


class DictDifferDiffer(Differ):
    """
    A Differ that uses the dictdiffer lib to diff the dicts.

    The ID used for this differ is 'dd'.
    """

    def __init__(self):
        super(DictDifferDiffer, self).__init__(u'dd')

    def can_diff(self, data):
        """
        We can diff any dict! Wee!

        :param data: the data to check
        :return: True
        """
        return True

    def diff(self, old, new, ignore=None):
        """
        Diffs the two data dicts using dictdiffer and returns the diff as a list. The
        ignore parameter is passed straight through to dictdiffer.diff so refer to that
        doc for information on how it should be provided.

        :param old: the old data
        :param new: the new data
        :param ignore: the keys to ignore
        :return: the diff as a list
        """
        return list(dictdiffer.diff(old, new, ignore=ignore))

    def patch(self, diff_result, old, in_place=False):
        """
        Given a dictdiffer diff result and some data, apply the diff to patch the old
        data. If the in_place parameter is True then the patch will be applied in place
        and the old data passed in will be returned. If in_place is False (the default)
        then the old data is copied before applying the patch. The copy is done using
        marshall rather than copy.deepcopy (as it is in the dictdiffer lib) as it is the
        fastest way to copy an object.

        :param diff_result: the diff to apply
        :param old: the old data
        :param in_place: whether to update the old data in place or not (default: False)
        :return: the updated data
        """

        if not in_place:
            old = marshal.loads(marshal.dumps(old))
        return dictdiffer.patch(diff_result, old, in_place=True)


class ShallowDiffer(Differ):
    """
    A Differ that only works on dicts that don't have nested dicts.

    Assuming this allows it to use dict.update to patch the old data dict to the new
    which is really quick! The ID used for this differ is 'sd'.
    """

    def __init__(self):
        super(ShallowDiffer, self).__init__(u'sd')

    def can_diff(self, data):
        """
        We can only diff the data if it doesn't contain any nested dicts.

        :param data: the data to check
        :return: True if the data dict passed contains no nested dicts
        """
        return all(not isinstance(value, dict) for value in data.values())

    def diff(self, old, new, ignore=None):
        """
        Diffs the two data dicts and returns the diff as a dict containing two keys:

            - 'r':  a list of keys that were removed
            - 'c': a dict of changes made

        Any keys present in the ignored parameter are ignored in the diff.

        :param old: the old data
        :param new: the new data
        :param ignore: the keys to ignore
        """
        diff = {}
        if ignore is None:
            ignore = []
        new_keys = set(new.keys()) - set(ignore)

        removes = set(old.keys()) - new_keys
        if removes:
            diff[u'r'] = list(removes)

        changes = {}
        for key in new_keys:
            if key in old:
                if old[key] != new[key]:
                    # a value has changed
                    changes[key] = new[key]
            else:
                # a new value has been added
                changes[key] = new[key]
        if changes:
            diff[u'c'] = changes

        return diff

    def patch(self, diff_result, old, in_place=False):
        """
        Given a diff result from this differs diff function and some data, apply the
        diff to patch the old data using the dict.update function and `del` to remove
        the removed keys.

        If the in_place parameter is True then the patch will be applied in place and the old data
        passed in will be returned. If in_place is False (the default) then the old data is copied
        before applying the patch. The copy is done using marshall rather for speed.

        :param diff_result: the diff to apply
        :param old: the old data
        :param in_place: whether to update the old data in place or not (default: False)
        :return: the updated data
        """
        if not in_place:
            old = marshal.loads(marshal.dumps(old))
        for key in diff_result.get(u'r', []):
            del old[key]
        old.update(diff_result.get(u'c', {}))
        return old


# the differs, instantiated globally for ease of use
SHALLOW_DIFFER = ShallowDiffer()
DICT_DIFFER_DIFFER = DictDifferDiffer()

# a dict of all the differs, instantiated and keyed by their ids
differs = {differ.differ_id: differ for differ in [SHALLOW_DIFFER, DICT_DIFFER_DIFFER]}
