import json
import typing as t
from uuid import uuid4


class AttrObject:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def mk_rpc_bundle(method: str, params, dump=False):
    bundle = {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': uuid4().hex
    }
    if not dump:
        return bundle
    return json.dumps(bundle)


class NestedSample:
    def __init__(self):
        self.base = 100

    def find_truth(self, *args, **kwargs):
        return all(args) and all(kwargs.values())

    def add_many(self, a, b, *args, **kwargs):
        return self.base + a + b + sum(args) + sum(kwargs.values())


class SampleRpcObject:
    def __init__(self, base: int):
        self.base = base
        self.nested = NestedSample()

    def add(self, a: int, b: t.Union[int, float], *, make_negative: bool = True) -> int:
        """
        Adds a to b and makes in negative.
        :param a: an integer
        :param b: an integer or float
        :param make_negative: a flag
        :return: result
        """
        return self.base + int(a + b) * (-1 * int(make_negative))

    def add_many(self, a, b, *args, **kwargs):
        return self.base + a + b + sum(args) + sum(kwargs.values())

    def kwonly(self, a, b, *, trash: bool = True):
        pass

    def pos_or_kw__var_pos__kw_only__kwargs(self, key, *get_patterns, by=None, **kwargs):
        return [key, get_patterns, by, kwargs]
