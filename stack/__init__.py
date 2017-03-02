import attr


@attr.s(frozen=True)
class Stack(object):
  filename = attr.ib()
  module = attr.ib()
  name = attr.ib()
  tags = attr.ib()
  template = attr.ib()
