
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class ArgumentError(Error):
    """Exception raised for argument errors.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, arg, opts):
        opts = ', '.join(opts)
        Exception.__init__(self,"Invalid '{}' provided, possible options: {}".format(arg, opts)) 