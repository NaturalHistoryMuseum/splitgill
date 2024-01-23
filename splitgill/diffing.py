from collections import deque
from datetime import datetime
from itertools import chain, zip_longest
from typing import Iterable, Tuple, Any, Union, NamedTuple, Dict, Deque

import regex as rx
from cytoolz import get_in

# this regex matches invalid characters which we would like to remove from all string
# values as they are ingested into the system. It matches unicode control characters
# (i.e. category C*) but not \n, \r, or \t.
invalid_char_regex = rx.compile(r"[^\P{C}\n\r\t]")


def prepare_data(value: Any) -> Union[str, dict, tuple, int, float, bool, None]:
    """
    Prepares the given value for storage in MongoDB. Conversions are completed like so:

        - None values are just returned as is
        - str values have invalid characters removed and are then returned. The
          characters are currently all unicode control characters except \n, \r, and \t.
        - int, float, bool, and None values are returned with no changes made
        - datetime values are converted to isoformat strings
        - dict values are returned as a new dict instance, with all the keys converted
          to strings and all the values recursively prepared using this function.
        - lists, sets, and tuples are converted to tuples with each element of the value
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
        return tuple(map(prepare_data, value))
    if isinstance(value, datetime):
        # stringifying this ensures the tz info is recorded and won't change going
        # in/out mongo
        return value.isoformat()
    # fallback
    return str(value)


# a namedtuple describing the differences found by the diff function below. Each DiffOp
# includes a tuple path and a dict of the changes made at that path.
DiffOp = NamedTuple("DiffOp", path=Tuple[Union[str, int], ...], ops=Dict[str, Any])


class DiffingTypeComparisonException(Exception):
    pass


# internal type for the diff function below
_QueueItem = Tuple[Tuple[Union[str, int], ...], Union[dict, tuple], Union[dict, tuple]]


def diff(base: dict, new: dict) -> Iterable[DiffOp]:
    """
    Finds the differences between the two dicts, yielding DiffOps. The DiffOps describe
    how to get the new dict from the base dict.

    This function only deals with dicts, tuples, and strs. Use the prepare function
    above before passing the dicts in.

    :param base: this dict is treated as the current version
    :param new: this dict is treated as the new version
    :return: yields DiffOps (if any changes are found)
    """
    if base == new:
        return

    if not isinstance(base, dict) or not isinstance(new, dict):
        raise DiffingTypeComparisonException("Both base and new must be dicts")

    # TODO: we could write a shortcut when one of the dicts is empty

    missing = object()
    queue: Deque[_QueueItem] = deque([(tuple(), base, new)])

    while queue:
        path, left, right = queue.popleft()
        ops = {}

        if isinstance(left, dict) and isinstance(right, dict):
            new_values = {key: value for key, value in right.items() if key not in left}
            if new_values:
                ops["dn"] = new_values

            deleted_keys = [key for key in left if key not in right]
            if deleted_keys:
                ops["dd"] = deleted_keys

            changes = {}
            for key, left_value in left.items():
                right_value = right.get(key, missing)
                # deletion or equality, nothing to do
                if right_value is missing or left_value == right_value:
                    continue

                if (isinstance(left_value, dict) and isinstance(right_value, dict)) or (
                    isinstance(left_value, tuple) and isinstance(right_value, tuple)
                ):
                    queue.append(((*path, key), left_value, right_value))
                else:
                    changes[key] = right_value

            if changes:
                ops["dc"] = changes
        else:
            # otherwise, they're both tuples
            changes = []
            for index, (left_value, right_value) in enumerate(
                zip_longest(left, right, fillvalue=missing)
            ):
                if left_value == right_value:
                    continue

                if left_value is missing:
                    # add
                    ops["tn"] = right[index:]
                    break
                elif right_value is missing:
                    # delete
                    ops["td"] = index
                    break
                else:
                    # change
                    if isinstance(left_value, dict) and isinstance(right_value, dict):
                        # only queue dicts, if we go into tuples we can't (easily)
                        # reconstruct
                        queue.append(((*path, index), left_value, right_value))
                    else:
                        changes.append((index, right_value))

            if changes:
                ops["tc"] = changes

        if ops:
            yield DiffOp(path, ops)


def patch(base: dict, ops: Iterable[Union[Tuple[str, dict], DiffOp]]) -> dict:
    """
    Applies the operations in the ops iterable to the base dict, returning a new dict.
    This function always returns a new dict, even if there are no ops to perform.

    :param base: the starting dict
    :param ops: the DiffOps to apply to the base dict (can be pure tuples, doesn't have
                to be DiffOp objects)
    :return: a new dict with the changes applied
    """
    base = base.copy()

    for path, op in ops:
        # dict ops
        if "dc" in op:
            get_in(path, base).update(op["dc"])
        if "dn" in op:
            get_in(path, base).update(op["dn"])
        if "dd" in op:
            target = get_in(path, base)
            for key in op["dd"]:
                del target[key]

        # tuple ops
        if "tc" in op:
            # turn into a list so that we can manipulate it
            target = list(get_in(path, base))
            for index, value in op["tc"]:
                target[index] = value
            parent = get_in(path[:-1], base)
            parent[path[-1]] = tuple(target)
        if "tn" in op:
            parent = get_in(path[:-1], base)
            # in case we get a tuple and a list, chain instead of concat
            parent[path[-1]] = tuple(chain(parent[path[-1]], op["tn"]))
        if "td" in op:
            parent = get_in(path[:-1], base)
            parent[path[-1]] = parent[path[-1]][: op["td"]]

    return base
