class ChatHistory:
    def __init__(self):
        self.history = []

    def add(self, user, bot):
        self.history.append({"user": user, "bot": bot})

    def get(self):
        return self.history

    def clear(self):
        self.history = []