class ECMWFRequestException(Exception):
    def __init__(self, message, *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class ECMWFRetrieveException(Exception):
    def __init__(self, message, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
