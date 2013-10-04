# -*- coding: utf-8 -*-

"""
    eve.methods.post
    ~~~~~~~~~~~~~~~~

    This module imlements the POST method, supported by the resources
    endopints.

    :copyright: (c) 2013 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

from flask import current_app as app, request
from eve.utils import document_link, config, document_etag
from eve.auth import requires_auth
from eve.methods.common import parse, payload, ratelimit
from eve.methods.common import validate_document, failure_resp_item, success_resp_item


@ratelimit()
@requires_auth('resource')
def post(resource, payl=None):
    """ Adds one or more documents to a resource. Each document is validated
    against the domain schema. If validation passes the document is inserted
    and ID_FIELD, LAST_UPDATED and DATE_CREATED along with a link to the
    document are returned. If validation fails, a list of validation issues
    is returned.

    :param resource: name of the resource involved.
    :param payl: alternative payload. When calling post() from your own code
                 you can provide an alternative payload This can be useful, for
                 example, when you have a callback function hooked to a certain
                 endpoint, and want to perform additional post() calls from
                 there.

                 Please be advised that in order to successfully use this
                 option, a request context must be available.

                 See https://github.com/nicolaiarocci/eve/issues/74 for a
                 discussion, and a typical use case.

    .. versionchanged:: 0.1.1
        auth.request_auth_value is now used to store the auth_field value.

    .. versionchanged:: 0.1.0
       More robust handling of auth_field.
       Support for optional HATEOAS.

    .. versionchanged: 0.0.9
       Event hooks renamed to be more robuts and consistent: 'on_posting'
       renamed to 'on_insert'.
       You can now pass a pre-defined custom payload to the funcion.

    .. versionchanged:: 0.0.9
       Storing self.app.auth.userid in auth_field when 'user-restricted
       resource access' is enabled.

    .. versionchanged: 0.0.7
       Support for Rate-Limiting.
       Support for 'extra_response_fields'.

       'on_posting' and 'on_posting_<resource>' events are raised before the
       documents are inserted into the database. This allows callback functions
       to arbitrarily edit/update the documents being stored.

    .. versionchanged:: 0.0.6
       Support for bulk inserts.

       Please note: validation constraints are checked against the database,
       and not between the payload documents themselves. This causes an
       interesting corner case: in the event of a multiple documents payload
       where two or more documents carry the same value for a field where the
       'unique' constraint is set, the payload will validate successfully, as
       there are no duplicates in the database (yet). If this is an issue, the
       client can always send the documents once at a time for insertion, or
       validate locally before submitting the payload to the API.

    .. versionchanged:: 0.0.5
       Support for 'application/json' Content-Type .
       Support for 'user-restricted resource access'.

    .. versionchanged:: 0.0.4
       Added the ``requires_auth`` decorator.

    .. versionchanged:: 0.0.3
       JSON links. Superflous ``response`` container removed.
    """

    resource_def = app.config['DOMAIN'][resource]
    schema = resource_def['schema']
    validator = app.validator(schema, resource)
    documents = []
    issues = []
    singular_inserts = app.config['SINGULAR_INSERTS']

    # validation, and additional fields
    if payl is None:
        payl = payload()
    
    if singular_inserts:
        payl_items = [('item', payl)]
    else:
        payl_items = payl.items()
    
    for key, value in payl_items:
        document, doc_issues = validate_document(value, validator, resource,
                                                 resource_def)
        issues.append(doc_issues)
        if len(doc_issues) == 0:
            documents.append(document)

    if len(documents):
        # notify callbacks
        getattr(app, "on_insert")(resource, documents)
        getattr(app, "on_insert_%s" % resource)(documents)

        # bulk insert
        ids = app.data.insert(resource, documents)

    # build response payload
    response = {}
    response_items = []

    if singular_inserts:
        response_data = zip(['item'], issues)
    else:
        response_data = zip(payl.keys(), issues)

    for key, doc_issues in response_data:
        if len(doc_issues):
            response_item = failure_resp_item(doc_issues)
        else:
            response_item = success_resp_item(ids.pop(0), documents.pop(0),
                                              resource, resource_def)
        response_items.append((key, response_item))

    if singular_inserts:
        response = response_items[0]
    else:
        for key, response_item in response_items:
            response[key] = response_item

    return response, None, None, 200
