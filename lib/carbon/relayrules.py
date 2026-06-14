import re
from os.path import exists, getmtime
from twisted.internet.task import LoopingCall
from carbon import log
from carbon.conf import OrderedConfigParser
from carbon.util import parseDestinations
from carbon.exceptions import CarbonConfigException


class RelayRule:
  def __init__(self, condition, destinations, continue_matching=False):
    self.condition = condition
    self.destinations = destinations
    self.continue_matching = continue_matching

  def matches(self, metric):
    return bool(self.condition(metric))


def loadRelayRules(path):
  rules = []
  parser = OrderedConfigParser()

  if not parser.read(path):
    raise CarbonConfigException("Could not read rules file %s" % path)

  defaultRule = None
  for section in parser.sections():
    if not parser.has_option(section, 'destinations'):
      raise CarbonConfigException("Rules file %s section %s does not define a "
                                  "'destinations' list" % (path, section))

    destination_strings = parser.get(section, 'destinations').split(',')
    destinations = parseDestinations(destination_strings)

    if parser.has_option(section, 'pattern'):
      if parser.has_option(section, 'default'):
        raise CarbonConfigException("Section %s contains both 'pattern' and "
                                    "'default'. You must use one or the other." % section)
      pattern = parser.get(section, 'pattern')
      regex = re.compile(pattern, re.I)

      continue_matching = False
      if parser.has_option(section, 'continue'):
        continue_matching = parser.getboolean(section, 'continue')
      rule = RelayRule(
        condition=regex.search, destinations=destinations, continue_matching=continue_matching)
      rules.append(rule)
      continue

    if parser.has_option(section, 'default'):
      if not parser.getboolean(section, 'default'):
        continue  # just ignore default = false
      if defaultRule:
        raise CarbonConfigException("Only one default rule can be specified")
      defaultRule = RelayRule(condition=lambda metric: True,
                              destinations=destinations)

  if not defaultRule:
    raise CarbonConfigException("No default rule defined. You must specify exactly one "
                                "rule with 'default = true' instead of a pattern.")

  rules.append(defaultRule)
  return rules


class RelayRulesManager(object):
  """Loads relay rules and reloads them when the rules file changes.

  The initial load is strict: an unreadable or invalid file raises, so a
  misconfigured relay refuses to start (matching the original one-shot
  loader). Subsequent reloads are tolerant -- if the file is missing or the
  new config is invalid, the previously loaded (working) rules are kept and
  the relay keeps routing.
  """

  def __init__(self):
    self.rules = []
    self.rules_file = None
    self.read_task = LoopingCall(self.read_rules)
    self.rules_last_read = 0.0

  def read_from(self, rules_file):
    self.rules_file = rules_file
    # Strict initial load: propagate errors so a bad config stops startup.
    self.rules = loadRelayRules(rules_file)
    try:
      self.rules_last_read = getmtime(rules_file)
    except (OSError, IOError):
      self.rules_last_read = 0.0
    # Poll for changes so edits take effect without restarting the relay.
    if not self.read_task.running:
      self.read_task.start(10, now=False)

  def read_rules(self):
    """Reload the rules if the file changed, keeping previous rules on error."""
    if not exists(self.rules_file):
      # The file may be missing or mid-edit; keep the rules we already have.
      return

    # Only read if the rules file has been modified.
    try:
      mtime = getmtime(self.rules_file)
    except (OSError, IOError):
      log.err("Failed to get mtime of %s" % self.rules_file)
      return
    if mtime <= self.rules_last_read:
      return

    try:
      new_rules = loadRelayRules(self.rules_file)
    except Exception as e:
      log.err("Failed to load relay rules from %s, "
              "keeping previous rules: %s" % (self.rules_file, e))
      # Remember this version so the same broken file isn't re-read every tick;
      # a corrected file will have a newer mtime and be picked up.
      self.rules_last_read = mtime
      return

    log.relay("Loaded %d relay rules from %s" % (len(new_rules), self.rules_file))
    self.rules = new_rules
    self.rules_last_read = mtime
