from routes.mapper import SubMapper

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import ckan.model as model
import ckan.plugins as p
import ckan.lib.helpers as h
import ckan.logic as logic
import ckan.lib.base as base
import ckan.lib.search as search
import ckanext.s3filestore.logic as action

import os
from ckan.common import _
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from pylons import config

from ckan.lib.cli import ManageDb,SearchIndexCommand

import multiprocessing as mp
from pylons import config
import ckan.lib.cli as cli
import sqlalchemy as sa
from ckan.lib.search import clear

redirect = base.redirect

class ResourceDataController(base.BaseController):

    def resource_s3upload(self, id, resource_id):

        resource_ids_and_paths = {}
        rootpaths = {}

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
              rootpaths[resource_id2] = root

        s3_connection = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        bucket = s3_connection.get_bucket(AWS_BUCKET_NAME)
        k = Key(bucket)

        engine = create_engine(SQLALCHEMY_URL)
        connection = engine.connect()

        dset = connection.execute(text('''
            SELECT state
            FROM package
            WHERE id = :id and state = 'draft'
        '''), id=id)

        if dset.rowcount:

            connection.execute(text('''
               UPDATE package
               SET state = 'active'
               WHERE id = :id
            '''), id=id)


#            SQLALCHEMY_URL = config.get('sqlalchemy.url')

#            engine = sa.create_engine(SQLALCHEMY_URL)
#            connection = engine.connect()

            package_ids = []
            result = connection.execute("select id from package where state = 'active';")
            for row in result:
               package_ids.append(row[0])


            def start(ids):
               ## load actual enviroment for each subprocess, so each have thier own
               ## sa session
               from ckan.lib.search import rebuild, commit
               rebuild(package_ids=ids)
               commit()

            def chunks(l, n):
               """ Yield n successive chunks from l.
               """
               newn = int(len(l) / n)
               for i in xrange(0, n-1):
                  yield l[i*newn:i*newn+newn]
               yield l[n*newn-newn:]

            processes = []

            for chunk in chunks(package_ids, mp.cpu_count()):
               process = mp.Process(target=start, args=(chunk,))
               processes.append(process)
               process.daemon = True
               process.start()

            for process in processes:
               process.join()

            result = engine.execute("select id from package where state = 'deleted';")
            for row in result:
               clear(row[0])


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
           k.key = AWS_STORAGE_PATH +'/' + k.key

        uploaded_resources = []

        k.set_contents_from_filename(resource_ids_and_paths[resource_id])
        k.make_public()
        uploaded_resources.append(resource_id)
        os.remove(resource_ids_and_paths[resource_id])
        redirect(h.url_for(controller='package', action='resource_read', id=id, resource_id=resource_id))

    def organization_s3upload(self, id):

        ids_and_paths = {}
        rootpaths = {}
        orgfile = ""

        BASE_PATH = config.get('ckan.storage_path')
        SQLALCHEMY_URL = config.get('sqlalchemy.url')
        AWS_ACCESS_KEY_ID = config.get('ckanext.s3filestore.aws_access_key_id')
        AWS_SECRET_ACCESS_KEY = config.get('ckanext.s3filestore.aws_secret_access_key')
        AWS_BUCKET_NAME = config.get('ckanext.s3filestore.aws_bucket_name')
        AWS_STORAGE_PATH = config.get('ckanext.s3filestore.aws_storage_path')

        s3_connection = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        bucket = s3_connection.get_bucket(AWS_BUCKET_NAME)
        k = Key(bucket)

        engine = create_engine(SQLALCHEMY_URL)
        connection = engine.connect()
        file_name = ""
        image_url="http"
        resource = connection.execute(text('''
            SELECT id, image_url
            FROM "group"
            WHERE name = :id
        '''), id=id)
        if resource.rowcount:
            _id, image_url = resource.first()
            file_name = image_url.split('/')[-1] if '/' in image_url else image_url
      
            
        if 'http' in image_url:
 
            file_name = image_url.split('/')[-1]

        else:
        
            k.key = AWS_STORAGE_PATH +'/storage/uploads/group/' + file_name
            orgfile = '/home/ckan/storage/uploads/group/' + file_name

            connection.execute(text('''
            UPDATE "group"
            SET image_url = :key
            WHERE name = :id
            '''), id=id,key='http://gspf-resource.s3.amazonaws.com/' + k.key)

            connection.close()
            engine.dispose()

            uploaded_resources = []

            k.set_contents_from_filename(orgfile)
            k.make_public()
            uploaded_resources.append(id)

        redirect(h.url_for(controller='organization', action='read', id=id))

class S3FileStorePlugin(plugins.SingletonPlugin):

    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)

    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(p.IResourceController, inherit=True)
    plugins.implements(p.IResourceView, inherit=True)
    plugins.implements(p.IOrganizationController, inherit=True)

    legacy_mode = False
    resource_show_action = None

    def get_actions(self):
      return {
        'resource_create': action.resource_create,
        'resource_update': action.resource_update,
        'group_create': action.group_create,
        'group_update': action.group_update,
        'organization_create': action.organization_create,
        'organization_update': action.organization_update,
      }

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')

    # IConfigurable

    def configure(self, config):
        # Certain config options must exists for the plugin to work. Raise an
        # exception if they're missing.
        missing_config = "{0} is not configured. Please amend your .ini file."
        config_options = (
            'ckanext.s3filestore.aws_access_key_id',
            'ckanext.s3filestore.aws_secret_access_key',
            'ckanext.s3filestore.aws_bucket_name'
        )
        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))

        # Check that options actually work, if not exceptions will be raised
#        if toolkit.asbool(
#                config.get('ckanext.s3filestore.check_access_on_startup',
#                           True)):
#            ckanext.s3filestore.uploader.BaseS3Uploader().get_s3_bucket(
#                config.get('ckanext.s3filestore.aws_bucket_name'))

#    def after_update(self, context, resource):

#        redirect(h.url_for(controller='ckanext.s3filestore.plugin:ResourceDataController', action='resource_s3upload',id=resource['package_id'],  resource_id=resource['id']))

#    def after_create(self, context, resource):

#        redirect(h.url_for(controller='ckanext.s3filestore.plugin:ResourceDataController', action='resource_s3upload',id=resource['package_id'],  resource_id=resource['id']))

    def before_show(self, resource_dict):

        BASE_PATH = config.get('ckan.storage_path')
        SQLALCHEMY_URL = config.get('sqlalchemy.url')
        AWS_ACCESS_KEY_ID = config.get('ckanext.s3filestore.aws_access_key_id')
        AWS_SECRET_ACCESS_KEY = config.get('ckanext.s3filestore.aws_secret_access_key')
        AWS_BUCKET_NAME = config.get('ckanext.s3filestore.aws_bucket_name')
        AWS_STORAGE_PATH = config.get('ckanext.s3filestore.aws_storage_path')

        s3_connection = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        bucket = s3_connection.get_bucket(AWS_BUCKET_NAME)
        k = Key(bucket)

        id=resource_dict['package_id']
        resource_id=resource_dict['id']
        url = resource_dict['url']

        if resource_dict['url_type'] == 'upload':

           k.key = 'resources/' + resource_id + '/' +str(url.split('/')[-1])

           if AWS_STORAGE_PATH:
              k.key = AWS_STORAGE_PATH + '/' + k.key

           tpath = BASE_PATH +'/resources/'+str(resource_id[0:3])+'/'+str(resource_id[3:6])+'/'+str(resource_id[6:])
           tdir  = BASE_PATH +'/resources/'+str(resource_id[0:3])+'/'+str(resource_id[3:6])

           if os.path.isdir(tdir): 
              if os.path.isfile(tpath)==False:

                 if k.exists():
                    k.get_contents_to_filename(tpath)

           else:
              if k.exists():
                 os.makedirs(tdir)
                 k.get_contents_to_filename(tpath)

        return resource_dict

    # IRoutes

    def before_map(self, map):
        # add 160415 aac
 #       map.connect('resource_s3upload',
 #                 '/dataset/{id}/resource/{resource_id}/upload',
 #                controller='ckanext.s3filestore.plugin:ResourceDataController', 
 #                action='resource_s3upload')

 #       map.connect('organization_s3upload',
 #                 '/organization/{id}/upload',
 #                controller='ckanext.s3filestore.plugin:ResourceDataController',
 #                action='organization_s3upload')

        return map

    # IUploader

    def get_resource_uploader(self, data_dict):
        '''Return an uploader object used to upload resource files.'''
        return ckanext.s3filestore.uploader.S3ResourceUploader(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        '''Return an uploader object used to upload general files.'''
        return ckanext.s3filestore.uploader.S3Uploader(upload_to, old_filename)
