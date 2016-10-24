import sys
import json
from datetime import datetime
from getpass import getpass
import requests
import pandas
from collections import Counter
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

class ClevertaPy:
    def __init__(self, account_id, passcode):
        self.url = 'https://api.clevertap.com/1/'
        self.headers = {
            'X-CleverTap-Account-Id': account_id,
            'X-CleverTap-Passcode': passcode,
            'content_type': 'application/json'
        }

    def upload_profiles(self, csv_path):
        profiles_csv = pandas.read_csv(csv_path)
        profiles = json.loads(profiles_csv.to_json(orient='records'))
        profile_chunks = []
        read_profiles = len(profiles)
        for i in range(0, read_profiles, 1000):
            profile_chunks.append(profiles[i:i + 1000])
        uploaded_profiles = 0
        for profile_chunk in profile_chunks:
            profiles_json = [
                {
                    'identity': profile['Email'],
                    'type': 'profile',
                    'ts': datetime.utcnow().strftime('%s'),
                    'profileData': {
                        'Name': profile['Name']
                        'Email': profile['Email'],
                    }
                } for profile in profile_chunk
            ]

            upload_url = self.url + 'upload'
            payload = {
                'd': profiles_json
            }
            response = requests.post(upload_url, json=payload, headers=self.headers)
            response_data = json.loads(response.content)
            if(response_data['status'] == 'success'):
                uploaded_profiles = uploaded_profiles + response_data['processed']
                print '{0} / {1} profiles uploaded.'.format(uploaded_profiles, read_profiles)
            else:
                raise Warning(response_data['error'])

        return [uploaded_profiles, read_profiles]

    def get_records(self, cursor, records=[], i=0):
        records_url = self.url + 'profiles.json?cursor=' + cursor

        response = requests.get(records_url, headers=self.headers)
        response_data = json.loads(response.content)
        if(response_data['status'] == 'success'):
            next_cursor = response_data.get('next_cursor')
            if(next_cursor):
                i = i + len(response_data['records'])
                print '{0} profiles fetched.'.format(i)
                records = self.get_records(next_cursor, records, i)
            for record in response_data['records']:
                record_json = {'Email': record['email']}
                records.append(record_json)
        else:
            raise Warning(response_data['error'])

        return records

    def download_profiles(self, property, operator, value):
        batch_url = self.url + 'profiles.json?batch_size=5000'
        payload = {
            'common_profile_properties': {
                'profile_fields': [
                    {
                        'name': property,
                        'operator': operator,
                        'value': value
                    }
                ]
            }
        }

        response = requests.post(batch_url, json=payload, headers=self.headers)
        response_data = json.loads(response.content)
        if(response_data['status'] == 'success'):
            profiles = self.get_records(response_data['cursor'])
        else:
            raise Warning(response_data['error'])

        return profiles

    def get_diff(self, csv_path, property, operator, value):
        csv_profiles = pandas.read_csv(csv_path, usecols=['Email'])
        profiles = json.loads(csv_profiles.to_json(orient='records'))
        clevertap_profiles = self.download_profiles(property, operator, value)

        return profiles, clevertap_profiles

if(__name__ == '__main__'):
    print 'Control + C to exit.'
    command = raw_input('1. Upload\n2. Diff\n')
    if(command not in['1', '2']):
        print 'Exiting ...'
        sys.exit()
    clevertap_account_id = raw_input('Clevertap Account ID:')
    clevertap_passcode = getpass('Clevertap Passcode:')
    csv_path = raw_input('Profiles CSV full path:\n')
    clevertap_client = ClevertaPy(clevertap_account_id, clevertap_passcode)

    if(command == '1'):
        print 'Uploading ...'
        print '{0} profiles successfully uploaded out of {1}.'.format(*clevertap_client.upload_profiles(csv_path))
    elif(command == '2'):
        property = raw_input('Clevertap common profile property:')
        operator = raw_input('1. Equals\n')
        if(operator != '1'):
            print 'Exiting ...'
            sys.exit()
        else:
            operator = 'equals'
        value = raw_input('Value:')
        print 'Fetching ...'
        csv_profiles, clevertap_profiles = clevertap_client.get_diff(csv_path, property, operator, value)
        print 'There are {0} profiles in CSV and {1} on Clevertap.'.format(len(csv_profiles), len(clevertap_profiles))
