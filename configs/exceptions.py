class BatchError(Exception):

    def __init__(self, message):
        super().__init__(message)
    
    def __str__(self):
        return f"Error: {self.args[0]}"
    

class SkippingMatch(Exception):
    pass

class NoCheckpoint(Exception):
    pass

class SoftRateLimit(Exception):
    
    def __init__(self, message):
        super().__init__(message)
    
    def __str__(self):
        return f"You've been soft rate limited!"
    
class EmptyData(Exception):
    pass