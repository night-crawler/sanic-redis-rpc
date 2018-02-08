from sanic.exceptions import SanicException

JSON_RPC_VERSION = '2.0'


class RpcError(SanicException):
    MESSAGE = ''
    ERROR_CODE = -32000

    def __init__(self, id=None, data=None, message=None):
        super(RpcError, self).__init__(message or self.MESSAGE, status_code=200)
        self.data = data
        self.id = id

    def __str__(self):
        return self.MESSAGE

    def as_dict(self):
        error = {'message': self.MESSAGE, 'code': self.ERROR_CODE}
        if self.data:
            error['data'] = self.data

        return {
            'id': self.id,
            'jsonrpc': JSON_RPC_VERSION,
            'error': error
        }


class RpcInvalidRequestError(RpcError):
    ERROR_CODE = -32600
    MESSAGE = 'Invalid request'


class RpcMethodNotFoundError(RpcError):
    ERROR_CODE = -32601
    MESSAGE = 'Method not found'


class RpcInternalError(RpcError):
    ERROR_CODE = -32603
    MESSAGE = 'Internal Error'


class RpcInvalidParamsError(RpcError):
    ERROR_CODE = -32602
    MESSAGE = 'Invalid params'


class RpcParseError(RpcError):
    ERROR_CODE = -32700
    MESSAGE = 'Invalid JSON was received'
