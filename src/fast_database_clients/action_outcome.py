#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------


# Enum type for success and failure of action->outcome messaging pair
from dataclasses import dataclass
from enum import Enum

class ActionOutcome(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self):
        return self.value.upper()


@dataclass
class ActionOutcomeMessage:
    """
    A class to generate messages representing an action -> outcome

    Attributes:
        action (str): The action.
        action_verbose (str): The verbose action.
        outcome (str): The outcome.

    Methods:
        message: Get the message representing the action -> outcome.
        message_verbose: Get the verbose message representing the action -> outcome.
        __call__: Update the action, outcome, or action_verbose attributes and return a log record.

    Examples:
        >>> log_action_outcome = ActionOutcomeMessage(
                action="Sending metric to influxdb",
                action_verbose=f"Sending metric:{metric.name} to bucket:'{bucket}' on influxdb at {self.url}"
            )
        >>> log_action_outcome(outcome=ActionOutcome.SUCCESS)
        {
            'msg': 'Sending metric to influxdb -> success',
            'extra': {'details': "Sending metric:py_metric1 to bucket:'metrics2' on influxdb at http://localhost:8086"}
        }
        >>> log_action_outcome(outcome=ActionOutcome.FAILED)
        {
            'msg': 'Sending metric to influxdb -> failed',
            'extra': {'details': "Sending metric:py_metric1 to bucket:'metrics2' on influxdb at http://localhost:8086"}
        }
    """

    action: str
    action_verbose: str = None
    outcome: str = None

    @property
    def message(self):
        """
        Get the message representing the action -> outcome.

        Returns:
            str: The message representing the action -> outcome.
        """
        return f"{self.action} -> {self.outcome}"

    @property
    def message_verbose(self):
        """
        Get the verbose message representing the action -> outcome.

        Returns:
            str: The verbose message representing the action -> outcome.
        """
        return f"{self.action_verbose} -> {self.outcome}"

    def __call__(self, action=None, outcome=None, action_verbose=None):
        """
        Update the action, outcome, or action_verbose attributes and return a log record.

        Args:
            action (str): The action to update.
            outcome (str): The outcome to update.
            action_verbose (str): The verbose action to update.

        Returns:
            dict: A log record containing the message and details.
        """
        if action:
            self.action = action
        if outcome:
            self.outcome = outcome
        if action_verbose:
            self.action_verbose = action_verbose

        log_record = dict(msg=self.message, extra=dict(details=self.message_verbose))
        return log_record

