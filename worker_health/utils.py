import json
import logging
import pprint
import shutil
import subprocess
from urllib.request import urlopen

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)

USER_AGENT_STRING = "Python (https://github.com/mozilla-platform-ops/android-tools/tree/master/worker_health)"


def run_cmd(cmd):
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).strip().decode()


def bitbar_systemd_service_present(warn=False, error=False):
    try:
        run_cmd("systemctl status bitbar > /dev/null 2>&1")
    except subprocess.CalledProcessError:
        if warn:
            logger.warn("this should be run on the primary devicepool host for maximum data.")
        if error:
            logger.error("this must be run on the primary devicepool host!")
        return False
    return True


def list_intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))


# handles continuationToken
def get_jsonc(an_url, verbosity=0):
    output_dict = {}
    headers = {"User-Agent": USER_AGENT_STRING}
    retries_allowed = 2
    retries_left = retries_allowed

    while retries_left >= 0:
        if verbosity > 2:
            print(an_url)
        response = requests.get(an_url, headers=headers)
        result = response.text
        try:
            output = json.loads(result)
            # will only break on good decode
            break
        except json.decoder.JSONDecodeError as e:
            logger.warning("get_jsonc: '%s': json decode error. input: %s" % (an_url, result))
            logger.warning(e)
            if retries_left == 0:
                logger.error("get_jsonc: '%s': failed %s times, returning empty" % (an_url, retries_allowed + 1))
                return output_dict
        retries_left -= 1
    output_dict = output

    if verbosity > 2:
        pprint.pprint(output_dict)

    while "continuationToken" in output:
        payload = {"continuationToken": output["continuationToken"]}
        if verbosity > 2:
            print("CONT %s, %s" % (an_url, output["continuationToken"]))
        response = requests.get(an_url, headers=headers, params=payload)
        result = response.text
        # TODO: handle exceptions here also
        output = json.loads(result)
        # tc messes with us and sends back and empty workers array
        if "workers" in output and len(output["workers"]):
            # THIS IS SQUASHING STUFF
            output_dict["workers"].extend(output["workers"])

    if verbosity > 2:
        pprint.pprint(output_dict)
    return output_dict


def consecutive_non_ones_from_end(an_array):
    an_array.reverse()
    counter = 0
    for item in an_array:
        if item != 1:
            counter += 1
        else:
            break
    return counter


def graph_percentage(value, show_label=False, round_value=False):
    return_string = ""
    if round_value:
        value = round(value, 1)
    if show_label:
        return_string += "%s: "
    return_string += "["
    for i in range(1, 11):
        if value >= i * 0.1:
            return_string += "="
        else:
            return_string += " "
    return_string += "]"
    return return_string


# returns (url, response, exception)
def fetch_url(url):
    try:
        response = urlopen(url)
        return url, response.read(), None
    except Exception as e:
        return url, None, e


def pformat_term(a_string):
    cols, _width = shutil.get_terminal_size(fallback=(120, 50))
    return pprint.pformat(a_string, width=(cols - 2), compact=True)


# https://www.peterbe.com/plog/best-practice-with-retries-with-requests
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# returns (url, response, exception)
def get_jsonc2(an_url, verbosity=0):
    headers = {"User-Agent": USER_AGENT_STRING}
    retries_left = 2

    while retries_left >= 0:
        if verbosity > 2:
            print(an_url)
        response = requests_retry_session().get(an_url, headers=headers)
        result = response.text
        try:
            output = json.loads(result)
            # will only break on good decode
            break
        except json.decoder.JSONDecodeError as e:
            logger.warning("json decode error. input: %s" % result)
            if retries_left == 0:
                return an_url, None, e
        print("request failure, manual retry")
        retries_left -= 1

    while "continuationToken" in output:
        payload = {"continuationToken": output["continuationToken"]}
        if verbosity > 2:
            print("%s, %s" % (an_url, output["continuationToken"]))
        response = requests_retry_session().get(an_url, headers=headers, params=payload)
        result = response.text
        output = json.loads(result)
    return an_url, output, None


def mkdir_p(dir_to_create):
    subprocess.run(f"mkdir -p {dir_to_create}", shell=True)


# given an array, find <item> in array, and return item and <number_of_followers> after item
def arr_get_followers(array, item, number_of_followers, raise_on_errors=False):
    if len(array) == 0:
        if raise_on_errors:
            raise Exception("empty array")
        return []

    if item not in array:
        if raise_on_errors:
            raise Exception("item not in array!")
        return []

    if number_of_followers > len(array):
        raise Exception("too many followers for length of array!")

    counter = 0
    for a_item in array:
        if a_item == item:
            end_index = counter + number_of_followers + 1
            return array[counter:end_index]
        counter += 1


def arr_get_slice_from_item(array, item):
    if item not in array:
        raise Exception("item not in array!")

    counter = 0
    for a_item in array:
        if a_item == item:
            return array[counter:]
        counter += 1
