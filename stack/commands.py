import botocore.exceptions
import datetime
import pytz
import time
import tzlocal


MIN_TIME = pytz.utc.localize(datetime.datetime.min)


def create_stack(stack, args, aws):
  cfn = aws.client("cloudformation")

  cfn.create_stack(
    StackName=stack.name,
    TemplateBody=stack.template.to_json(),
    Tags=[
      dict(Key=key, Value=value) for (key, value) in stack.tags.iteritems()
    ]
  )

  for event in _wait_for_stack(cfn, stack.name, MIN_TIME):
    _log_stack_event(event)

  return event["ResourceStatus"] == "CREATE_COMPLETE"


def update_stack(stack, args, aws):
  cfn = aws.client("cloudformation")

  # Determine the time of the latest event for the stack, so that we don't
  # log any events before that time.
  last_event = _get_stack_events(cfn, stack.name).next()
  tm = last_event["Timestamp"]

  try:
    cfn.update_stack(
      StackName=stack.name,
      TemplateBody=stack.template.to_json(),
      Tags=[
        dict(Key=key, Value=value) for (key, value) in stack.tags.iteritems()
      ]
    )
  except botocore.exceptions.ClientError, e:
    if e.response["Error"]["Message"] == "No updates are to be performed.":
      return True

    raise

  for event in _wait_for_stack(cfn, stack.name, tm):
    _log_stack_event(event)

  return event["ResourceStatus"] == "UPDATE_COMPLETE"


def remove_stack(stack, args, aws):
  cfn = aws.client("cloudformation")

  # Because we're about to delete the stack, we need to watch its events using
  # its arn instead of it's name.
  try:
    stacks = cfn.describe_stacks(StackName=stack.name)["Stacks"]
    arn = stacks[0]["StackId"]
  except botocore.exceptions.ClientError, e:
    message = e.response["Error"]["Message"]
    if message == "Stack with id " + stack.name + " does not exist":
      return True

    raise

  # Determine the time of the latest event for the stack, so that we don't
  # log any events before that time.
  last_event = _get_stack_events(cfn, arn).next()
  tm = last_event["Timestamp"]

  cfn.delete_stack(
    StackName=stack.name
  )

  for event in _wait_for_stack(cfn, arn, tm):
    _log_stack_event(event)

  return event["ResourceStatus"] == "DELETE_COMPLETE"


def print_stack(stack, args, aws):
  print stack.template.to_json(indent=args.indent)
  return True


def diff_stack(stack, args, aws):
  pass


TERMINAL_STATES = {
  "CREATE_COMPLETE",
  "CREATE_FAILED",
  "DELETE_COMPLETE",
  "DELETE_FAILED",
  "ROLLBACK_COMPLETE",
  "ROLLBACK_FAILED",
  "UPDATE_COMPLETE",
  "UPDATE_ROLLBACK_COMPLETE",
  "UPDATE_ROLLBACK_FAILED",
}


def _wait_for_stack(cfn, stack_name, oldest_event_timestamp):
  r"""
  Block until the stack transitions into a terminal state.

  :param cfn: The CloudFormation client to use to retrieve the stack's events.
  :param stack_name: The name of the CloudFormation stack.
  :param oldest_event_timestamp: The oldest event timestamp to ignore.
  :return: The event that corresponds ot the terminal state of the stack.
  """
  seen_event_ids = set()

  while True:
    events = [
      event for event in _get_stack_events(cfn, stack_name)
      if event["EventId"] not in seen_event_ids and
      event["Timestamp"] > oldest_event_timestamp
    ]

    for event in reversed(events):
      # Remember this event for later so that it's never processed again
      seen_event_ids.add(event["EventId"])

      yield event

    # The event variable is defined by the above loop.  The value of the
    # variable after the loop exits is the last element of the iterator.
    if (event["ResourceType"] == "AWS::CloudFormation::Stack" and
            event["ResourceStatus"] in TERMINAL_STATES):
      break

    time.sleep(15)


def _get_stack_events(cfn, stack_name):
  r"""
  Retrieve all of the events for a given stack.

  The order of the events will be the same as how they're returned by the
  CloudFormation API which is in reverse chronological order.

  :param cfn: The CloudFormation client to use to retrieve the stack's events.
  :param stack_name: The name of the CloudFormation stack.
  :return: A generator of all of the stack's events.
  """
  kwargs = {}

  while True:
    response = cfn.describe_stack_events(StackName=stack_name, **kwargs)

    for event in response["StackEvents"]:
      yield event

    next_token = response.get("NextToken")
    if next_token:
      kwargs["NextToken"] = next_token
      continue

    break


def _log_stack_event(event, tz=tzlocal.get_localzone()):
  print event["Timestamp"].astimezone(tz).strftime("%Y-%m-%dT%H:%M:%S"), \
    event["ResourceStatus"], \
    event["ResourceType"], \
    event["LogicalResourceId"], \
    event.get("ResourceStatusReason", "")
