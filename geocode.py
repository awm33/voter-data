import os

import requests
import psycopg2
import psycopg2.extras
import backoff
from ratelimit import *

def fatal_code(e):
    if e.response and e.response.status_code:
        return 400 <= e.response.status_code < 500

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException,
                       requests.exceptions.Timeout,
                       requests.exceptions.ConnectionError),
                      max_tries=5,
                      giveup=fatal_code,
                      factor=2)
@rate_limited(8)
def geocode_registration(settings, cur, reg):
    raw_query_components = [
        reg['house_number'],
        reg['street_name'],
        reg['apartment_number'],
        reg['address_line_2'],
        reg['city'],
        reg['state'],
        reg['zip']
    ]

    query_components = filter(lambda x: x != None and x != '', raw_query_components)

    query = ','.join(query_components)

    url = 'https://api.mapbox.com/geocoding/v5/mapbox.places/' + query + '.json'
    response = requests.get(
        url,
        params={
            'access_token': settings['mapbox_api_key'],
            'country': 'US',
            'types': 'address'
        })

    if response.status_code != 200:
        print('=' * 50)
        print(query)
        print(response.status_code)
        print(response.text)
        return

    data = response.json()

    if 'features' not in data or len(data['features']) == 0:
        print('=' * 50)
        print(query)
        print(response.status_code)
        print(response.text)
        return

    cur.execute(
        'UPDATE registration SET longitude = %s, latitude = %s, raw_geocode_data = %s WHERE id = %s;',
        (data['features'][0]['geometry']['coordinates'][0],
         data['features'][0]['geometry']['coordinates'][1],
         psycopg2.extras.Json(data),
         reg['id']))

def get_settings():
    return {
        'database': {
            'database': os.getenv('VOTER_DATABASE_NAME', 'voting'),
            'user': os.getenv('VOTER_DATABASE_USER', 'amadonna'),
            'password': os.getenv('VOTER_DATABASE_PASSWORD', None),
            'host': os.getenv('VOTER_DATABASE_HOST', 'localhost'),
            'port': os.getenv('VOTER_DATABASE_PORT', '5432')
        },
        'mapbox_api_key': os.getenv('MAPBOX_API_KEY')
    }

def main():
    settings = get_settings()

    with psycopg2.connect(**settings['database']) as conn1:
        with conn1.cursor('geocode_cursor', cursor_factory=psycopg2.extras.DictCursor) as select_cur:
            select_cur.execute('SELECT * FROM registration WHERE raw_geocode_data IS NULL;')

            with psycopg2.connect(**settings['database']) as conn2:
                with conn2.cursor() as update_cur:
                    count = 0
                    for row in select_cur:
                        count += 1
                        geocode_registration(settings, update_cur, row)

                        if count % 100 == 0:
                            print(count)
                            conn2.commit()

if __name__ == "__main__":
    main()
