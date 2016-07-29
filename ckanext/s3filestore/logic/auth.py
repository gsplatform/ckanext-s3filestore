import ckanext.s3filestore.logic.auth as auth


def s3upload(context, data_dict):
    return auth.s3filestore_auth(context, data_dict)

