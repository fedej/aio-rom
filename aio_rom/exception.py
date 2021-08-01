class ORMException(RuntimeError):
    pass


class ModelNotFoundException(ORMException):
    pass


class ModelNotLoadedException(ORMException):
    pass
