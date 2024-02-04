import abc
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from itertools import zip_longest
from typing import (
    Iterable,
    Tuple,
    Any,
    Union,
    NamedTuple,
    Dict,
    Deque,
    List,
    TypeVar,
    Generic,
    Optional,
    Collection,
)

import regex as rx

# this regex matches invalid characters which we would like to remove from all string
# values as they are ingested into the system. It matches unicode control characters
# (i.e. category C*) but not \n, \r, or \t.
invalid_char_regex = rx.compile(r"[^\P{C}\n\r\t]")


def prepare_data(value: Any) -> Union[str, dict, list, int, float, bool, None]:
    """
    Prepares the given value for storage in MongoDB. Conversions are completed like so:

        - None values are just returned as is
        - str values have invalid characters removed and are then returned. The
          characters are currently all unicode control characters except \n, \r, and \t.
        - int, float, bool, and None values are returned with no changes made
        - datetime values are converted to isoformat strings
        - dict values are returned as a new dict instance, with all the keys converted
          to strings and all the values recursively prepared using this function.
        - lists, sets, and tuples are converted to lists with each element of the value
          prepared by this function.

    :param value: the value to be stored in MongoDB
    :return: None, str, int, float, bool, tuple, or dict depending on the input value
    """
    if value is None:
        return None
    if isinstance(value, str):
        # replace any invalid characters in the string with the empty string
        return invalid_char_regex.sub("", value)
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, dict):
        # mongodb doesn't allow non-string keys so we need to convert them here
        return {str(key): prepare_data(value) for key, value in value.items()}
    if isinstance(value, (list, set, tuple)):
        return list(map(prepare_data, value))
    if isinstance(value, datetime):
        # stringifying this ensures the tz info is recorded and won't change going
        # in/out mongo
        return value.isoformat()
    # fallback
    return str(value)


class DiffOp(NamedTuple):
    """
    A namedtuple describing the differences found by the diff function.
    """

    # the path where the changes should be applied at in the root dict
    path: Tuple[Union[str, int], ...]
    # a dict of the changes made at that path
    ops: Dict[str, Any]


_T = TypeVar("_T")


@dataclass
class Comparison(abc.ABC, Generic[_T]):
    """
    A comparison between two objects of the same type.
    """

    path: Tuple[Union[str, int], ...]
    left: _T
    right: _T

    @abc.abstractmethod
    def compare(self) -> Tuple[Optional[DiffOp], List["Comparison"]]:
        """
        Compare the two objects and return a 2-tuple containing a DiffOp and a list of
        further comparisons which need to be handled. If no differences are found, then
        the first element of the returned 2-tuple will be None.

        :return: A 2-tuple containing a DiffOp and a list of further Comparison objects
        """
        pass


class DictComparison(Comparison[dict]):
    """
    A comparison between two dicts.
    """

    def compare(self) -> Tuple[Optional[DiffOp], List["Comparison"]]:
        """
        Compares the two dicts and return a 2-tuple containing a DiffOp and a list of
        further comparisons which need to be handled. If no differences are found, then
        the first element of the returned 2-tuple will be None.

        :return: A 2-tuple containing a DiffOp and a list of further Comparison objects
        """
        missing = object()
        ops = {}
        further_comparisons = []

        new_values = {
            key: value for key, value in self.right.items() if key not in self.left
        }
        if new_values:
            ops["dn"] = new_values

        deleted_keys = [key for key in self.left if key not in self.right]
        if deleted_keys:
            ops["dd"] = deleted_keys

        changes = {}
        for key, left_value in self.left.items():
            right_value = self.right.get(key, missing)

            # deletion or equality, nothing to do
            if right_value is missing or left_value == right_value:
                continue

            # check for nested container objects and add Comparison objects to the list
            # if any are found of the same types
            if isinstance(left_value, dict) and isinstance(right_value, dict):
                further_comparisons.append(
                    DictComparison((*self.path, key), left_value, right_value)
                )
            elif isinstance(left_value, list) and isinstance(right_value, list):
                further_comparisons.append(
                    ListComparison((*self.path, key), left_value, right_value)
                )
            else:
                changes[key] = right_value
        if changes:
            ops["dc"] = changes

        return DiffOp(self.path, ops) if ops else None, further_comparisons


@dataclass
class ListComparison(Comparison[list]):
    """
    A comparison between two lists.
    """

    def compare(self) -> Tuple[DiffOp, List["Comparison"]]:
        """
        Compares the two lists and return a 2-tuple containing a DiffOp and a list of
        further comparisons which need to be handled. If no differences are found, then
        the first element of the returned 2-tuple will be None.

        :return: A 2-tuple containing a DiffOp and a list of further Comparison objects
        """
        missing = object()
        ops = {}
        further_comparisons = []

        changes = []
        for index, (left_value, right_value) in enumerate(
            zip_longest(self.left, self.right, fillvalue=missing)
        ):
            if left_value == right_value:
                continue

            if left_value is missing:
                # the right list is longer, so store all the new values so that they can
                # just be added to the left list to patch it, and stop
                ops["ln"] = self.right[index:]
                break
            elif right_value is missing:
                # the left value is longer, so store the index from which elements in
                # the left list will be deleted to shorten it to the length of the right
                # list, and stop
                ops["ld"] = index
                break
            else:
                # a change in the values at this index in each list, check for nested
                # container objects and add Comparison objects to the list if any are
                # found of the same types
                if isinstance(left_value, dict) and isinstance(right_value, dict):
                    further_comparisons.append(
                        DictComparison((*self.path, index), left_value, right_value)
                    )
                elif isinstance(left_value, list) and isinstance(right_value, list):
                    further_comparisons.append(
                        ListComparison((*self.path, index), left_value, right_value)
                    )
                else:
                    changes.append((index, right_value))
        if changes:
            ops["lc"] = changes

        return DiffOp(self.path, ops) if ops else None, further_comparisons


class DiffingTypeComparisonException(Exception):
    """
    Exception raised if the base type and the new type passed to the diff function below
    are not both dicts.
    """

    pass


def diff(base: dict, new: dict) -> Iterable[DiffOp]:
    """
    Finds the differences between the two dicts, yielding DiffOps. Each DiffOp describes
    specific differences between the base dict and the new dict. By applying them all
    using the patch function below, the new dict can be recreated from the base dict.

    For efficiency, the DiffOps represent all the changes at a container level (e.g. a
    dict or list) not each specific change to every version at a specific key or index.
    This saves not only database space, but also allows for a faster patch function as
    changes can be applied en masse instead of individually.

    :param base: the base dict
    :param new: the new version of the base dict
    :return: yields DiffOps (if any changes are found)
    """
    if base == new:
        return

    if not isinstance(base, dict) or not isinstance(new, dict):
        raise DiffingTypeComparisonException("Both base and new must be dicts")

    # TODO: we could write a shortcut when one of the dicts is empty

    queue: Deque[Comparison] = deque([DictComparison(tuple(), base, new)])
    while queue:
        comparison: Comparison = queue.popleft()
        diff_op, further_comparisons = comparison.compare()
        if diff_op:
            yield diff_op
        if further_comparisons:
            queue.extend(further_comparisons)


# # dynamically figure out the typing of the DiffOp based on it's annotations
# _diff_op_typing = get_type_hints(DiffOp)
# _DiffOpType = Tuple[_diff_op_typing["path"], _diff_op_typing["ops"]]


def patch(base: dict, ops: Collection[DiffOp]) -> dict:
    """
    Applies the operations in the ops iterable to the base dict, returning a new dict.
    If there are no operations to apply, the base dict is returned unchanged.

    Note that although the returned dict is new, the nested container values in it may
    or may not be new and could reference the same exact object as in the passed base
    dict. A nested container will be copied to avoid modifying the same container
    referenced in the base dict if there are any modifications made to it directly, or
    to any nested containers below it at any depth. If the nested container contains no
    changes to itself or its nested containers, it is not copied and the original
    reference to it from the base dict is used.

    :param base: the starting dict
    :param ops: the DiffOps to apply to the base dict (can be pure tuples, doesn't have
                to be DiffOp namedtuples)
    :return: a new dict with the changes applied
    """
    # nothing to do
    if len(ops) == 0:
        return base

    # create a copy of the base dict so that we don't modify it and can return a new one
    new = base.copy()

    for path, op in ops:
        # loop through the path finding the target of the operations we're going to
        # perform. At every point in the path, replace the container with a copy to
        # ensure we don't modify the container object from the base.
        target = new
        for key_or_index in path:
            target_copy = target[key_or_index].copy()
            target[key_or_index] = target_copy
            target = target_copy

        # dict ops
        if "dc" in op:
            target.update(op["dc"])
        if "dn" in op:
            target.update(op["dn"])
        if "dd" in op:
            for key in op["dd"]:
                del target[key]

        # list ops
        if "lc" in op:
            for index, value in op["lc"]:
                target[index] = value
        if "ln" in op:
            target.extend(op["ln"])
        if "ld" in op:
            del target[op["ld"] :]

    return new
