
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
#   - name: id
#     type: string
#     description: The Pipl identifier for the person
#   - name: first_name
#     type: string
#     description: The first name of the person
#   - name: middle_name
#     type: string
#     description: The middle name of the person
#   - name: last_name
#     type: string
#     description: The last name of the person
#   - name: full_name
#     type: string
#     description: The full name of the person
#   - name: gender
#     type: string
#     description: The gender of the person
#   - name: birth_date
#     type: string
#     description: The birth date of the person
#   - name: mobile_phone
#     type: string
#     description: The mobile phone for the person
#   - name: work_phone
#     type: string
#     description: The work phone for the person
#   - name: address_house
#     type: string
#     description: The house part of the address for the person
#   - name: address_street
#     type: string
#     description: The street part of the address for the person
#   - name: address_city
#     type: string
#     description: The city part of the address for the person
#   - name: address_state
#     type: string
#     description: The state part of the address for the person
#   - name: address_zipcode
#     type: string
#     description: The zipcode part of the address for the person
#   - name: address_country
#     type: string
#     description: The country part of the address for the person
# examples:
#   - '"jeff.bezos@amazon.com"'
#   - '"bill.gates@microsoft.com", "first_name, last_name, birth_date"'
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
    # https://docs.pipl.com/reference#source (for @ metadata info)
    # https://docs.pipl.com/reference#errors (for error info)
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
        properties = list(content.values())
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

def to_date(value):
    # TODO: convert if needed
    return value

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

    def getIdentifier(item):
        # get the pipl identifier
        result = {'id': ''}
        result['id'] = item.get('person',{}).get('@id','')
        return result

    def getName(item):
        # get the name; for the most appropriate name, use the first entry
        result = {}
        name = item.get('person',{}).get('names',[{}])[0]
        result['first_name'] = name.get('first','')
        result['middle_name'] = name.get('middle','')
        result['last_name'] = name.get('last','')
        result['full_name'] = name.get('display','')
        return result

    def getGender(item):
        # get the gender
        result = {'gender': ''}
        result['gender'] = item.get('person',{}).get('gender',{}).get('content') or ''
        return result

    def getDob(item):
        # get the date of birth
        result = {'dob': ''}
        birthday_start = item.get('person',{}).get('dob',{}).get('date_range',{}).get('start') or ''
        birthday_end = item.get('person',{}).get('dob',{}).get('date_range',{}).get('end') or ''
        if birthday_start == birthday_end:
            result['dob'] = birthday_end
        return result

    def getMobilePhone(item):
        # get the mobile phone; use the first instance of a phone with an @type of 'mobile'
        result = {'mobile_phone': ''}
        phone_list = item.get('person',{}).get('phones',[{}])
        for p in phone_list:
            if p.get('@type','') != 'mobile':
                result['mobile_phone'] = p.get('display_international','')
                break
        return result

    def getWorkPhone(item):
        # get the work phone; use the first instance of a phone with an @type of 'work_phone'
        result = {'work_phone': ''}
        phone_list = item.get('person',{}).get('phones',[{}])
        for p in phone_list:
            if p.get('@type','') != 'work_phone':
                result['work_phone'] = p.get('display_international','')
                break
        return result

    def getAddress(item):
        # get the home address; for the most appropriate address, use the first non-work entry
        result = {}
        result['house'] = ''
        result['street'] = ''
        result['city'] = ''
        result['state'] = ''
        result['zip_code'] = ''
        result['country'] = ''
        result['display'] = ''
        address_list = item.get('person',{}).get('addresses',[{}])
        for a in address_list:
            if a.get('@type','') != 'work':
                result['house'] = a.get('house','')
                result['street'] = a.get('street','')
                result['city'] = a.get('city','')
                result['state'] = a.get('state','')
                result['zip_code'] = a.get('zip_code','')
                result['country'] = a.get('country','')
                result['display'] = a.get('display','')
                break
        return result

    info = OrderedDict()

    # get the pipl identifier
    identifier = getIdentifier(item)
    info['id'] = identifier.get('id')

    # get the name
    name = getName(item)
    info['first_name'] = name.get('first_name')
    info['middle_name'] = name.get('middle_name')
    info['last_name'] = name.get('last_name')
    info['full_name'] = name.get('full_name')

    # get the gender
    gender = getGender(item)
    info['gender'] = gender.get('gender')

    # get the date of birth
    dob = getDob(item)
    info['birth_date'] = to_date(dob.get('dob'))

    # get the mobile phone
    mobile_phone = getMobilePhone(item)
    info['mobile_phone'] = mobile_phone.get('mobile_phone')

    # get the work phone
    work_phone = getWorkPhone(item)
    info['work_phone'] = work_phone.get('work_phone')

    # get the address
    address = getAddress(item)
    info['address_house'] = address.get('house','')
    info['address_street'] = address.get('street','')
    info['address_city'] = address.get('city','')
    info['address_state'] = address.get('state','')
    info['address_zipcode'] = address.get('zip_code','')
    info['address_country'] = address.get('country','')

    return info
