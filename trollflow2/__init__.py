class AbortProcessing(Exception):
    def __init__(self, message, errors):
        super(AbortProcessing, self).__init__(message)
