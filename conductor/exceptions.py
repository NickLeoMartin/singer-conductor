class RunCommandException(Exception):
    """
    Custom exception to raise when run command fails
    """

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)
