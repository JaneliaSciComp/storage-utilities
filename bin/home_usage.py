''' home_usage.py
    Find users using more than a set amout of disk space in their home
    directories and send them an email warning them. This uses the Starfish API
    and required a token (see https://starfish.int.janelia.org/doc/api).
    Starfish is updated at 01:00, 13:00, and 21:00, so this program is best run
    2-3 hours after one (or more) of those times.
'''

__version__ = '1.2.0'

import argparse
from datetime import datetime, timedelta
from html import escape
from operator import attrgetter
import os
import sys
from colorama import Fore, Style
import requests
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Groups
ALLOWED_GROUPS = ['flyem', 'flylight', 'jayaraman', 'karpovap', 'mousebrainmicro', 'projtechres',
                  'quantitativegenomics', 'rubin', 'scicomp', 'svoboda']
# Time
DAY = 3600 * 24
# Database
DB = {}
# Email
SENDER = 'donotreply@hhmi.org'
DEVELOPER = 'svirskasr@hhmi.org'
WIKI_URL = 'https://hhmi.atlassian.net/wiki/spaces/SCSW/pages/156055444/Network+Storage'

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # pylint: disable=broad-exception-caught
    if 'STARFISH_JWT' not in os.environ:
        terminate_program("Environment variable STARFISH_JWT is not defined")
    if ARG.DEBUG:
        details = call_responder('starfish', f"auth/{os.environ['STARFISH_JWT'].split(':')[1]}")
        LOGGER.warning(f"Token is valid until {details['valid_until_hum']}")
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    # Database
    for source in ("storage",):
        dbo = attrgetter(f"{source}.dev.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'dev', dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def call_responder(server, endpoint):
    ''' Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
        Returns:
          JSON response
    '''
    url = attrgetter(f"{server}.url")(REST) + endpoint
    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer " + os.environ["STARFISH_JWT"]}
    try:
        req = requests.get(url, headers=headers, timeout=30)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    if req.status_code == 404:
        return None
    if req.status_code == 400:
        LOGGER.error(req.content)
    terminate_program(f"Status: {str(req.status_code)}")
    return None



def generate_email(userid, work, consumed):
    ''' Generate and send an email
        Keyword arguments:
          userid: user ID
          work: record from Workday
          consumed: consumed space in human-readable form
        Returns:
          None
    '''
    recipient = work['email']
    subject = "Disk space warning"
    banner = ''
    if ARG.TEST:
        banner = f"<p><b>[TEST] This message would have been sent to {escape(recipient)}</b></p>"
        recipient = DEVELOPER
        subject = "[TEST] " + subject
    msg = f'''{banner}<p>Hi {escape(work['first'])},</p>
<p>You are using {escape(consumed)} in your home directory. When the home share fills up,
it causes problems for everyone. Please help us by decreasing your disk usage to
{ARG.LIMIT}TiB or less. For more information on where your data can be stored,
please see <a href="{WIKI_URL}">the wiki</a>.</p>
<p>Regards,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Management</p>
'''
    try:
        LOGGER.info(f"Sending email to {recipient}")
        JRC.send_email(msg, SENDER, [recipient], subject, mime='html')
    except Exception as err:
        LOGGER.error(err)
        return
    if ARG.TEST:
        # Don't record the notification, or real users would be suppressed for 24 hours
        return
    payload = {'userId': userid,
               'size': consumed,
               'notified': datetime.now()
              }
    coll = DB['storage'].overage
    try:
        coll.update_one({'userId': userid}, {"$set": payload}, upsert=True)
    except Exception as err:
        terminate_program(err)


def notify_allowed(userid, work):
    ''' Determine if this user can be notified
        Keyword arguments:
          userid: user ID
          work: record from Workday
        Returns:
          None
    '''
    # Is the user in Workday?
    if not work or 'config' not in work:
        LOGGER.warning(f"{userid} was not found in Workday")
        return False
    # Is the user active?
    if work['config'].get('active') != 'Y':
        LOGGER.warning(f"{userid} is not active in Workday")
        return False
    if ARG.TEST:
        # Testing: ignore the 24 hour limit so the developer can run repeatedly
        return True
    coll = DB['storage'].overage
    try:
        result = coll.find_one({'userId': userid})
    except Exception as err:
        terminate_program(err)
    if not result:
        return True
    delta = datetime.now() - result['notified']
    if delta.days < 1:
        LOGGER.warning(f"{userid} can be notified in " \
                       + f"{str(timedelta(seconds=DAY - delta.seconds))}")
    return bool(delta.days >= 1)


def process_usage():
    ''' Retrieve and process disk usage stats
        Keyword arguments:
          None
        Returns:
          None
    '''
    resp = call_responder('starfish', attrgetter(f"starfish.query.{ARG.GROUP}")(REST))
    if resp is None:
        terminate_program(f"No usage data returned for group {ARG.GROUP}")
    for usr in resp:
        if usr['rec_aggrs']['size'] > ARG.LIMIT * (1024 ** 4):
            # Returns None if the user isn't in Workday (handled in notify_allowed)
            data = call_responder("config", "config/workday/" + usr['fn'])
            if not notify_allowed(usr['fn'], data):
                print(f"{Fore.YELLOW}{usr['fn']:<16}  {usr['rec_aggrs']['size_hum']}" \
                      + Style.RESET_ALL)
                continue
            print(f"{Fore.RED}{usr['fn']:<16}  {usr['rec_aggrs']['size_hum']}{Style.RESET_ALL}")
            if ARG.WRITE or ARG.TEST:
                generate_email(usr['fn'], data['config'], usr['rec_aggrs']['size_hum'])
        else:
            print(f"{Fore.GREEN}{usr['fn']:<16}  {usr['rec_aggrs']['size_hum']}{Style.RESET_ALL}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Warn users if they're using too much disk space")
    PARSER.add_argument('--limit', dest='LIMIT', action='store',
                        type=float, default=.5, help='Threshold in TiB')
    PARSER.add_argument('--group', dest='GROUP', action='store',
                        default='scicomp', choices=ALLOWED_GROUPS,
                        help='Group to check')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Send email')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help=f"Send email only to {DEVELOPER} "
                        + "(ignores 24 hour limit, doesn't record notifications)")
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        REST = JRC.get_config("rest_services")
    except Exception as gerr:
        terminate_program(gerr)
    initialize_program()
    process_usage()
    terminate_program()
