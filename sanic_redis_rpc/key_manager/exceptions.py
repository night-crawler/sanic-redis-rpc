from sanic.exceptions import SanicException, InvalidUsage, NotFound


class SearchIdNotFoundError(SanicException):
    MESSAGE = 'Search identifier `{search_id}` is not present in service redis'

    def __init__(self, search_id: str):
        super().__init__(self.MESSAGE.format(search_id=search_id), status_code=404)


class WrongPageSizeError(InvalidUsage):
    MESSAGE = 'Page size must be a positive integer but you passed {page_size}'

    def __init__(self, page_size):
        super().__init__(self.MESSAGE.format(page_size=page_size))


class WrongNumberError(InvalidUsage):
    MESSAGE = 'Page number must be an integer >= 1 and <= count but you passed {page_num}'

    def __init__(self, page_num):
        super().__init__(self.MESSAGE.format(page_num=page_num))


class PageNotFoundError(NotFound):
    pass
