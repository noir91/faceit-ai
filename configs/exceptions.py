class BatchError(Exception):

    def __init__(self, message):
        super().__init__(message)
    
    def __str__(self):
        return f"Error: {self.args[0]}, code: {self.code}"
    

class SkippingMatch(Exception):
    pass

class NoCheckpoint(Exception):
    pass