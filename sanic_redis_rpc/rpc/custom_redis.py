import aioredis


class CustomRedis(aioredis.Redis):

    def memory_usage(self, key: str, *, samples: int=5):
        """
        The MEMORY USAGE command reports the number of bytes that a key and its value require to be stored in RAM.
        The reported usage is the total of memory allocations for data and administrative overheads that a key its
        value require.

        For nested data types, the optional SAMPLES option can be provided, where count is the number of sampled nested
        values. By default, this option is set to 5. To sample the all of the nested values, use SAMPLES 0.
        :param key: key
        :param samples: the number of sampled nested values.
        :return:
        """

        return self.execute(b'MEMORY', b'USAGE', key, b'SAMPLES', samples)
