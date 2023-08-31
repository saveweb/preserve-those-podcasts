class FeedTooLargeError(Exception):
    def __init__(self, message='too large'):
        self.message = message
    
    def __str__(self):
        return self.message
