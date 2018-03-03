import aioredis
import pytest
from aioredis.util import _NOTSET
from sanic_redis_rpc.signature_serializer import SignatureSerializer


# noinspection PyMethodMayBeStatic
class SignatureSerializerTest:
    pytestmark = [pytest.mark.signatures]

    async def test__callables(self):
        ser = SignatureSerializer(aioredis.Redis('fake'))
        for callable_ in ser.callables:
            assert callable_

    def test__inspect_entity(self):
        class Sample:
            def sample1(self, bla: int = 1) -> int:
                """
                Sample docstring
                :param bla: something
                :return: integer plus one
                """
                return bla + 1

            def sample2(self, *args, **kwargs):
                return 'hate crew deathroll'

        ser = SignatureSerializer(Sample())
        inspected = ser.inspect_entity('sample1')
        assert inspected
        assert inspected['return_type'] == 'int'
        assert inspected['doc']

        inspected = ser.inspect_entity('sample2')
        assert inspected
        assert len(inspected['parameters']) == 2

    async def test__to_dict(self):
        ser = SignatureSerializer(aioredis.Redis('fake'))
        assert ser.to_dict()

    async def test__redis_default_parameter_NOTSET_casted_to_None(self):
        ser = SignatureSerializer(aioredis.Redis('fake'))
        spec = ser.inspect_entity('echo')
        # encoding=_NOTSET is a last parameter
        assert spec['parameters'][-1]['default'] is None
