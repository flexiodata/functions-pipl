
# ---
# name: pipl-enrich-people
# deployed: true
# title: Pipl People Enrichment
# description: Return a person's profile information based on their email address.
# params:
#   - name: email
#     type: string
#     description: The email address of the person you wish you find.
#     required: true
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
# returns:
# examples:
#   - '"tcook@apple.com"'
#   - '"bill.gates@microsoft.com"'
# ---

import json
import urllib
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import itertools
from datetime import *
from cerberus import Validator
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    # get the api key from the variable input
    auth_token = dict(flex.vars).get('pipl_api_key')
    if auth_token is None:
        raise ValueError

    # get the input
    input = flex.input.read()
    try:
        input = json.loads(input)
        if not isinstance(input, list): raise ValueError
    except ValueError:
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['email'] = {'required': True, 'type': 'string'}
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    # if the input is valid return an error
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    # see here for more info:
    # https://docs.pipl.com/reference
    # https://docs.pipl.com/reference#configuration-parameters
    # https://docs.pipl.com/docs/top_match-configuration-parameter
    # https://docs.pipl.com/reference#errors
    url_query_params = {
        'email': input['email'].lower().strip(),
        'key': auth_token.strip()
    }
    url_query_str = urllib.parse.urlencode(url_query_params)
    url = 'https://api.pipl.com/search/?' + url_query_str

    # get the response data as a JSON object
    response = requests_retry_session().get(url)

    # sometimes results are pending; for these, return text indicating
    # the result is pending so the user can refresh later to look for
    # the completed result
    status_code = response.status_code
    if status_code == 202:
        flex.output.content_type = "application/json"
        flex.output.write([['Result Pending...']])
        return

    # if a result can't be found or wasn't formatted properly,
    # return a blank (equivalent to not finding a bad email address)
    if status_code == 400 or status_code == 404 or status_code == 422:
        flex.output.content_type = "application/json"
        flex.output.write([['']])
        return

    # return an error for any other non-200 result
    response.raise_for_status()

    # limit the results to the requested properties
    content = response.json()
    content = get_item_info(content)

    properties = [p.lower().strip() for p in input['properties']]
    if len(properties) == 1 and (properties[0] == '' or properties[0] == '*'):
        properties = content
    else:
        properties = [content.get(p) or '' for p in properties]

    # return the results
    result = [properties]
    result = json.dumps(result, default=to_string)
    flex.output.content_type = "application/json"
    flex.output.write(result)

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(429, 500, 502, 503, 504),
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
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None

def get_item_info(item):

    info = OrderedDict()
    return info
