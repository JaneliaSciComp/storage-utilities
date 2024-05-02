''' home_usage.py
    Find users using more than a set amout of disk space in their home
    directories and send them an email warning them. This uses the Starfish API
    and required a token (see https://starfish.int.janelia.org/doc/api).
    Starfish is updated at 01:00, 13:00, and 21:00, so this program is best run
    2-3 hours after one (or more) of those times.
'''

__version__ = '0.0.1'

import argparse
from datetime import datetime, timedelta
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
    msg = f"{work['first']};\n" \
          + f"You are using {consumed} in your home directory. Please help "\
          + "Scientific Computing Software by decreasing your disk usage to " \
          + f"{ARG.LIMIT}TB or less. Thanks for your cooperation.\n" \
          + "Regards,\n    Some annoying program"
    try:
        LOGGER.info(f"Sending email to {work['email']}")
        JRC.send_email(msg, SENDER, [work['email']], "Disk space warning")
    except Exception as err:
        LOGGER.error(err)
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
        return False
    # Is the user active?
    if work['config']['active'] != 'Y':
        return False
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
    for usr in resp:
        if usr['rec_aggrs']['size'] > ARG.LIMIT * (1024 ** 4):
            try:
                data = call_responder("config", "config/workday/" + usr['fn'])
            except requests.HTTPError:
                terminate_program(f"User {usr['fn']} was not found in Workday")
            if not notify_allowed(usr['fn'], data):
                print(f"{Fore.YELLOW}{usr['fn']:<16}  {usr['rec_aggrs']['size_hum']}" \
                      + Style.RESET_ALL)
                continue
            print(f"{Fore.RED}{usr['fn']:<16}  {usr['rec_aggrs']['size_hum']}{Style.RESET_ALL}")
            if ARG.WRITE:
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
