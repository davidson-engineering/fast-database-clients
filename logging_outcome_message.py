from dataclasses import dataclass

SUCCESS = "SUCCESS"
FAILED = "FAILED"



@dataclass
class LoggingActionOutcome:
    action: str
    outcome: str = None

    def message(self):
        return f"{self.action} -> {self.outcome}"

    def __call__(self, action=None, outcome=None):
        if action:
            self.action = action
        if outcome:
            self.outcome = outcome
        return self.message()