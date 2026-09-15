"""Terminate Stage B benchmark instances that outlive their declared lifetime.

EC2 has no provider-enforced maximum run duration, unlike the GCP
`max_run_duration` the accepted one-shard baseline used, so the lifetime ceiling
is enforced from outside the fleet instead of by the provider.

Every node also schedules its own `shutdown -h` from user data, and the topology
sets `instance_initiated_shutdown_behavior = "terminate"`. This watchdog
therefore waits out a grace period and only ever acts on a node that could not
terminate itself, which keeps the documented teardown path the normal one.
"""

import datetime
import json
import os

import boto3

ec2 = boto3.client("ec2")


def _expired_instances(cutoff):
    filters = [{"Name": "instance-state-name", "Values": ["pending", "running"]}]
    for pair in os.environ["TAG_FILTERS"].split(","):
        key, _, value = pair.partition("=")
        filters.append({"Name": "tag:{}".format(key), "Values": [value]})

    expired = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(Filters=filters):
        for reservation in page["Reservations"]:
            for instance in reservation["Instances"]:
                if instance["LaunchTime"] <= cutoff:
                    expired.append(instance["InstanceId"])
    return expired


def handler(event, context):
    max_lifetime = datetime.timedelta(hours=float(os.environ["MAX_LIFETIME_HOURS"]))
    grace = datetime.timedelta(minutes=float(os.environ.get("GRACE_MINUTES", "15")))
    cutoff = datetime.datetime.now(datetime.timezone.utc) - max_lifetime - grace

    expired = _expired_instances(cutoff)

    terminated = []
    if expired:
        response = ec2.terminate_instances(InstanceIds=expired)
        terminated = [
            {
                "instance_id": state["InstanceId"],
                "previous_state": state["PreviousState"]["Name"],
                "current_state": state["CurrentState"]["Name"],
            }
            for state in response["TerminatingInstances"]
        ]

    summary = {
        "cutoff": cutoff.isoformat(),
        "expired": expired,
        "terminated": terminated,
    }
    print(json.dumps(summary))
    return summary
