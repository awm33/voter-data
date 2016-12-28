import os
import io
import re
import csv

import yaml
import psycopg2
import arrow

export_filename_regex = r"^(?P<county>[A-Z]+)\s(?P<file_type>Zone\sTypes|Zone\sCodes|FVE|Election\sMap)\s(?P<file_date>[0-9]{8})\.txt$"

def load_models():
    models = {}
    for (dirpath, dirnames, filenames) in os.walk(os.path.join(os.getcwd(), 'models')):
        for filename in filenames:
            print('Loading model file: ' + filename)
            with open(os.path.join(dirpath, filename)) as file:
                model = yaml.load(file.read())
                models[filename.replace('.yml','')] = model
    return models

def generate_field_sql(field_def):
    if 'postgresType' in field_def:
        sql_type = field_def['postgresType']
    elif field_def['type'] == 'integer':
        sql_type = 'integer'
    elif field_def['type'] == 'number':
        sql_type = 'double precision'
    elif field_def['type'] == 'object':
        sql_type = 'jsonb'
    elif field_def['type'] == 'date':
        sql_type = 'timestamp'
    elif field_def['type'] == 'string':
        sql_type = 'text'

    return '"' + field_def['name'] + '" ' + sql_type + ','

def generate_primary_key_sql(primary_key):
    if isinstance(primary_key, list):
        primary_key = '","'.join(primary_key)

    return ' PRIMARY KEY("' + primary_key + '")'

def create_table(cur, model_name, model):
    print('Creating table for: ' + model_name)

    fields = model['fields']
    sql = 'CREATE TABLE "' + model_name + '" ('
    for field in fields:
        sql += generate_field_sql(field)
    if 'primaryKey' in model:
        sql += generate_primary_key_sql(model['primaryKey'])
    else:
        sql = sql[:-1]
    sql += ');'
    
    print(sql)

    cur.execute(sql)

def create_tables(cur, models):
    for model_name in models:
        create_table(cur, model_name, models[model_name])

def drop_tables(cur, models):
    for model_name in models:
        print('Dropping table if exists: ' + model_name)
        result = cur.execute('DROP TABLE IF EXISTS "' + model_name + '"')

def format_registration_dates(row, model):
    for i in range(0, len(model['fields'])):
        field = model['fields'][i]
        if field['name'] == 'snapshot_date':
            continue
        if field['type'] == 'date':
            position = i - 1 # id field is 0
            if row[position] != '':
                row[position] = arrow.get(row[position],'MM/DD/YYYY').format('YYYY-MM-DD HH:mm:ss')

def load_file(cur, model_name, model, file, snapshot_date):
    with io.StringIO() as output:
        snapshot_date_formatted = arrow.get(snapshot_date,'YYYYMMDD').format('YYYY-MM-DD HH:mm:ss')
        input_tsv = csv.reader(file, delimiter='\t')
        output_csv = csv.writer(output)
        for row in input_tsv:
            if model['exportFileType'] == 'FVE':
                format_registration_dates(row, model)
            row.append(snapshot_date_formatted)
            output_csv.writerow(row)
        output.seek(0)

        cols = ','.join(list(
                map(lambda x: x['name'],
                    filter(lambda x: 'importSkip' not in x or x['importSkip'] != True,
                        model['fields']))))
        cur.copy_expert('COPY ' + model_name + ' (' + cols + ') FROM STDIN WITH CSV', output)

def get_model_by_file_type(models, file_type):
    for model_name in models:
        if 'exportFileType' in models[model_name] and models[model_name]['exportFileType'] == file_type:
            return model_name, models[model_name]

def load_data(cur, models, data_path):
    for (dirpath, dirnames, filenames) in os.walk(data_path):
        for filename in filenames:
            print(filename)
            match = re.match(export_filename_regex, filename)
            if match:
                with open(os.path.join(dirpath, filename)) as file:
                    file_metadata = match.groupdict()
                    model_name, model = get_model_by_file_type(models, file_metadata['file_type'])
                    load_file(cur, model_name, model, file, file_metadata['file_date'])

def load_political_parties(cur, models, political_party_data_file):
    with open(political_party_data_file, encoding='ISO-8859-1') as file:
        model = models['political_party']
        with io.StringIO() as output:
            input_tsv = csv.reader(file, delimiter='\t')
            rows = list(input_tsv)

            snapshot_date_formatted = arrow.get(rows[-1][0],'M/D/YYYY').format('YYYY-MM-DD HH:mm:ss')

            output_csv = csv.writer(output)
            for row in rows[1:-2]: ## skip header and datetime signature at the end of the file
                row.append(snapshot_date_formatted)
                output_csv.writerow(row)
            output.seek(0)
            cur.copy_expert('COPY political_party FROM STDIN WITH CSV', output)

def get_settings():
    return {
        'database': {
            'database': os.getenv('VOTER_DATABASE_NAME', 'voting'),
            'user': os.getenv('VOTER_DATABASE_USER', 'amadonna'),
            'password': os.getenv('VOTER_DATABASE_PASSWORD', None),
            'host': os.getenv('VOTER_DATABASE_HOST', 'localhost'),
            'port': os.getenv('VOTER_DATABASE_PORT', '5432')
        },
        'data_path': os.getenv('VOTER_DATA_PATH', '/users/amadonna/Downloads/Statewide'),
        'political_party_data_file': os.getenv('VOTER_POLITICAL_PARTY_DATA_FILE', '/users/amadonna/Downloads/Political'),
    }

def main():
    settings = get_settings()
    models = load_models()

    with psycopg2.connect(**settings['database']) as conn:
        with conn.cursor() as cur:
            drop_tables(cur, models)
            create_tables(cur, models)
            load_political_parties(cur, models, settings['political_party_data_file'])
            load_data(cur, models, settings['data_path'])

if __name__ == "__main__":
    main()
