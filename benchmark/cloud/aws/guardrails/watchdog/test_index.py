"""Table tests for the Stage B lifetime watchdog.

boto3 is a Lambda runtime dependency and is not installed on the workstation, so
the tests inject a stub client before importing the handler. The behaviour under
test is which instances the watchdog considers expired and which tags it scopes
itself to, because that logic is what bounds the cost of an abandoned fleet.

Run with:
  python3 -m unittest discover -s benchmark/cloud/aws/guardrails/watchdog -p 'test_*.py'
"""

import datetime
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

INSTANCE_ID = "i-0123456789abcdef0"


class FakeEC2:
    def __init__(self, instances):
        self.instances = instances
        self.filters = None
        self.terminated = []

    def get_paginator(self, name):
        assert name == "describe_instances"
        return self

    def paginate(self, Filters):
        self.filters = Filters
        return [{"Reservations": [{"Instances": self.instances}]}]

    def terminate_instances(self, InstanceIds):
        self.terminated = list(InstanceIds)
        return {
            "TerminatingInstances": [
                {
                    "InstanceId": instance_id,
                    "PreviousState": {"Name": "running"},
                    "CurrentState": {"Name": "shutting-down"},
                }
                for instance_id in InstanceIds
            ]
        }


def load_handler(fake):
    """Import the handler against a stub boto3 client."""
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda service: fake)
    sys.modules.pop("index", None)
    import index

    return index


class WatchdogExpiryTest(unittest.TestCase):
    def setUp(self):
        os.environ["MAX_LIFETIME_HOURS"] = "36"
        os.environ["GRACE_MINUTES"] = "15"
        os.environ["TAG_FILTERS"] = "application=lighthouse,experiment=015,stage=b"

    def run_watchdog(self, age_hours):
        now = datetime.datetime.now(datetime.timezone.utc)
        fake = FakeEC2([{
            "InstanceId": INSTANCE_ID,
            "LaunchTime": now - datetime.timedelta(hours=age_hours),
        }])
        summary = load_handler(fake).handler({}, None)
        return fake, summary

    def test_expiry_boundary(self):
        # 36 hours of lifetime plus the 15 minute grace period is the cutoff, so
        # a node is only ever terminated after its own shutdown timer had its
        # chance to fire first.
        cases = [
            ("well inside lifetime", 1.0, []),
            ("inside the grace period", 36.1, []),
            ("past lifetime and grace", 36.5, [INSTANCE_ID]),
            ("far past lifetime", 72.0, [INSTANCE_ID]),
        ]
        for name, age_hours, expected in cases:
            with self.subTest(name):
                fake, summary = self.run_watchdog(age_hours)
                self.assertEqual(fake.terminated, expected)
                self.assertEqual(summary["expired"], expected)

    def test_termination_is_reported(self):
        fake, summary = self.run_watchdog(72.0)
        self.assertEqual(
            summary["terminated"],
            [{
                "instance_id": INSTANCE_ID,
                "previous_state": "running",
                "current_state": "shutting-down",
            }],
        )

    def test_only_running_and_pending_instances_are_considered(self):
        fake, _ = self.run_watchdog(1.0)
        self.assertIn(
            {"Name": "instance-state-name", "Values": ["pending", "running"]},
            fake.filters,
        )

    def test_tag_filters_match_the_topology_default_tags(self):
        fake, _ = self.run_watchdog(1.0)
        for key, value in [
            ("application", "lighthouse"),
            ("experiment", "015"),
            ("stage", "b"),
        ]:
            self.assertIn({"Name": "tag:{}".format(key), "Values": [value]}, fake.filters)

    def test_nothing_is_terminated_when_nothing_expired(self):
        fake, summary = self.run_watchdog(0.5)
        self.assertEqual(fake.terminated, [])
        self.assertEqual(summary["terminated"], [])


if __name__ == "__main__":
    unittest.main()
