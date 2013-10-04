# -*- coding: utf-8 -*-

"""
    eve.methods.put
    ~~~~~~~~~~~~~~~

    This module imlements the PUT method.

    :copyright: (c) 2013 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

from datetime import datetime
from eve.auth import requires_auth
from flask import current_app as app, abort, request
from eve.utils import document_etag, document_link, config, debug_error_message
from eve.methods.common import get_document, parse, payload as payload_, \
    ratelimit
from eve.methods.common import validate_document, failure_resp_item, success_resp_item

@ratelimit()
@requires_auth('item')
def put(resource, **lookup):
    """Perform a document replacement. Updates are first validated against
    the resource schema. If validation passes, the document is repalced and
    an OK status update is returned. If validation fails a set of validation
    issues is returned.

    :param resource: the name of the resource to which the document belongs.
    :param **lookup: document lookup query.

    .. versionchanged:: 0.1.1
        auth.request_auth_value is now used to store the auth_field value.

    .. versionadded:: 0.1.0
    """
    resource_def = app.config['DOMAIN'][resource]
    schema = resource_def['schema']
    validator = app.validator(schema, resource)

    original = get_document(resource, **lookup)
    if not original:
        # not found
        abort(404)

    last_modified = None
    etag = None
    object_id = original[config.ID_FIELD]

    payload = payload_()
    document = payload

    document, issues = validate_document(document, validator, resource,
                                             resource_def, original=original)
    if len(issues) == 0:
        last_modified = document[config.LAST_UPDATED]

        # notify callbacks
        getattr(app, "on_insert")(resource, [document])
        getattr(app, "on_insert_%s" % resource)([document])

        # single replacement
        app.data.replace(resource, object_id, document)

    response_item = {}
    if len(issues):
        response_item = failure_resp_item(issues)
    else:
        response_item = success_resp_item(object_id, document, resource,
                                          resource_def)
    response = response_item

    return response, last_modified, etag, 200
