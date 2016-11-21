import os
import io
import re

import yaml
import psycopg2

export_filename_regex = r"^(?P<county>[A-Z]+)\s(?P<file_type>Zone\sTypes|Zone\sCodes|FVE|Election\sMap)\s(?P<file_date>[0-9]{8})\.txt$"

def load_models():
    models = {}
    for (dirpath, dirnames, filenames) in os.walk(os.path.join(os.getcwd(), 'models')):
        for filename in filenames:
            print('Loading model file: ' + filename)
            file = open(os.path.join(dirpath, filename))
            model = yaml.load(file.read())
            models[filename.replace('.yml','')] = model
    return models

def generate_field_sql(field_name, field_def):
    if field_def['type'] == 'integer':
        sql_type = 'integer'
    elif field_def['type'] == 'string' and 'format' not in field_def:
        sql_type = 'text'
    elif field_def['type'] == 'string' and field_def['format'] == 'date-time':
        sql_type = 'timestamp'

    return '"' + field_name + '" ' + sql_type + ','

def generate_primary_key_sql(primary_key):
    if isinstance(primary_key, list):
        primary_key = '","'.join(primary_key)

    return ' PRIMARY KEY("' + primary_key + '")'

def create_table(cur, model_name, model):
    print('Creating table for: ' + model_name)

    properties = model['properties']
    ordered_property_keys = sorted(model['properties'], key = lambda x: properties[x]['order'])
    sql = 'CREATE TABLE "' + model_name + '" ('
    for field_name in ordered_property_keys:
        sql += generate_field_sql(field_name, properties[field_name])
    sql += generate_primary_key_sql(model['primaryKey'])
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

def load_file(cur, model, file, snapshot_date):
    # output = io.StringIO()
    # output.write('First line.\n')

def get_model_by_file_type(models, file_type):
    for model_name in models:
        if models[model_name]['exportFileType'] == file_type:
            return models[model_name]

def load_data(cur, models, data_path):
    for (dirpath, dirnames, filenames) in os.walk(data_path):
        for filename in filenames:
            match = re.match(export_filename_regex, filename)

            if match:
                file = open(os.path.join(dirpath, filename))
                file_metadata = match.groupdict()
                model = get_model_by_file_type(models, file_metadata['file_type'])
                load_file(cur, model, file, file_metadata['file_date'])

def get_settings():
    return {
        'database': {
            'database': os.getenv('VOTER_DATABASE_NAME', 'voting'),
            'user': os.getenv('VOTER_DATABASE_USER', 'amadonna'),
            'password': os.getenv('VOTER_DATABASE_PASSWORD', None),
            'host': os.getenv('VOTER_DATABASE_HOST', 'localhost'),
            'port': os.getenv('VOTER_DATABASE_PORT', '5432')
        },
        'data_path': os.getenv('VOTER_DATA_PATH', '~/amadonna/Downloads/Statewide')
    }

def main():
    settings = get_settings()
    models = load_models()

    with psycopg2.connect(**settings['database']) as conn:
        with conn.cursor() as cur:
            drop_tables(cur, models)
            create_tables(cur, models)
            load_data(cur, models, settings['data_path'])

if __name__ == "__main__":
    main()
