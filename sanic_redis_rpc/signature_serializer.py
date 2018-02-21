import typing as t
from collections import OrderedDict
from inspect import Signature, _empty, Parameter


class SignatureSerializer:
    def __init__(
            self,
            instance,
            private: bool = False,
            magic: bool = False,
    ):
        self.instance = instance
        self.type_ = type(instance)

        self.private = private
        self.magic = magic

    @property
    def callables(self) -> t.Tuple[str, t.Callable]:
        for name in self.instance.__dir__():
            if not self.magic and name[-2:] == name[:2] == '__':
                continue
            if not self.private and name[0] == '_':
                continue

            class_entity = getattr(self.type_, name)
            if isinstance(class_entity, property):
                continue

            entity = getattr(self.instance, name)
            if not callable(entity):
                continue

            yield name, entity

    def _serialize_parameter_default(self, default):
        if default is _empty:
            default = None
        if type(default) not in [int, str, bool, list, dict, set, type(None), None]:
            default = str(default)
        return default

    def _serialize_annotation(self, annotation):
        if annotation is _empty:
            return None
        return annotation.__name__

    def inspect_entity(self, entity: t.Union[str, t.Callable]) -> t.Dict[str, t.Any]:
        if isinstance(entity, str):
            entity = getattr(self.instance, entity)

        sig = Signature.from_callable(entity)

        inspected = {
            'return': self._serialize_annotation(sig.return_annotation),
            'doc': entity.__doc__,
        }

        _parameters = OrderedDict()
        for name, parameter in sig.parameters.items():
            parameter: Parameter = parameter
            _parameters[name] = {
                'kind': parameter.kind.name,
                'default': self._serialize_parameter_default(parameter.default),
                'type': self._serialize_annotation(parameter.annotation),
            }

        inspected['parameters'] = _parameters

        return inspected

    def to_dict(self) -> t.Dict[str, t.Dict[str, t.Any]]:
        res = OrderedDict()
        for method_name, method in self.callables:
            res[method_name] = self.inspect_entity(method)

        return res
