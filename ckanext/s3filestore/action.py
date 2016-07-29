mport logging
import json
import urlparse
import datetime

import pylons
import requests

import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins as p

log = logging.getLogger(__name__)
_get_or_bust = logic.get_or_bust
_validate = ckan.lib.navl.dictization_functions.validate


def s3upload(context, data_dict):

           data_dict, errors = _validate(data_dict, schema, context)
           if errors:
               raise p.toolkit.ValidationError(errors)

           res_id = data_dict['resource_id']

           BASE_PATH = config.get('ckan.storage_path')
           SQLALCHEMY_URL = config.get('sqlalchemy.url')
           AWS_ACCESS_KEY_ID = config.get('ckanext.s3filestore.aws_access_key_id')
           AWS_SECRET_ACCESS_KEY = config.get('ckanext.s3filestore.aws_secret_access_key')
           AWS_BUCKET_NAME = config.get('ckanext.s3filestore.aws_bucket_name')
           AWS_STORAGE_PATH = config.get('ckanext.s3filestore.aws_storage_path')

           for root, dirs, files in os.walk(BASE_PATH):
              if files:
                 resource_id2 = str(root.split('/')[-2]) + str(root.split('/')[-1]) + str(files[0])
                 resource_ids_and_paths[resource_id2] = os.path.join(root, files[0])

           s3_connection = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
           bucket = s3_connection.get_bucket(AWS_BUCKET_NAME)
           k = Key(bucket)

           engine = create_engine(SQLALCHEMY_URL)
           connection = engine.connect()

           resource = connection.execute(text('''
               SELECT id, url, url_type
               FROM resource
               WHERE id = :id
           '''), id=resource_id)
           if resource.rowcount:
               _id, url, _type = resource.first()
               if _type == 'upload' and url:
                   file_name = url.split('/')[-1] if '/' in url else url

           connection.close()
           engine.dispose()

           k.key = 'resources/' + resource_id + '/' +file_name

           if AWS_STORAGE_PATH:
              k.key = AWS_STORAGE_PATH + '/' + k.key

           if os.path.isfile('http://gspf-resource.s3.amazonaws.com/' +  k.key)==False:

              uploaded_resources = []

              k.set_contents_from_filename(resource_ids_and_paths[resource_id])
              k.make_public()
              uploaded_resources.append(resource_id)

            return True

