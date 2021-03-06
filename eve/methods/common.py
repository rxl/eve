# -*- coding: utf-8 -*-

"""
    eve.methods.common
    ~~~~~~~~~~~~~~~~~~

    Utility functions for API methods implementations.

    :copyright: (c) 2013 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

import traceback
import time
from datetime import datetime
from flask import current_app as app, request, abort, g, Response
import simplejson as json
from ..utils import str_to_date, parse_request, document_etag, config, \
    request_method, debug_error_message
from functools import wraps
from werkzeug.exceptions import BadRequestKeyError, InternalServerError
from eve.validation import ValidationError

def get_document(resource, **lookup):
    """ Retrieves and return a single document. Since this function is used by
    the editing methods (POST, PATCH, DELETE), we make sure that the client
    request references the current representation of the document before
    returning it.

    :param resource: the name of the resource to which the document belongs to.
    :param **lookup: document lookup query

    .. versionchanged:: 0.0.9
       More informative error messages.

    .. versionchanged:: 0.0.5
      Pass current resource to ``parse_request``, allowing for proper
      processing of new configuration settings: `filters`, `sorting`, `paging`.
    """
    req = parse_request(resource)
    document = app.data.find_one(resource, **lookup)
    if document:

        if not req.if_match:
            # we don't allow editing unless the client provides an etag
            # for the document
            abort(403, description=debug_error_message(
                'An etag must be provided to edit a document'
            ))

        # ensure the retrieved document has LAST_UPDATED and DATE_CREATED,
        # eventually with same default values as in GET.
        document[config.LAST_UPDATED] = last_updated(document)
        document[config.DATE_CREATED] = date_created(document)

        if req.if_match != document_etag(document):
            # client and server etags must match, or we don't allow editing
            # (ensures that client's version of the document is up to date)
            abort(412, description=debug_error_message(
                'Client and server etags don\'t match'
            ))

    return document


def parse(value, resource):
    """ Safely evaluates a string containing a Python expression. We are
    receiving json and returning a dict.

    :param value: the string to be evaluated.
    :param resource: name of the involved resource.

    .. versionchanged:: 0.1.0
       Support for PUT method.

    .. versionchanged:: 0.0.5
       Support for 'application/json' Content-Type.

    .. versionchanged:: 0.0.4
       When parsing POST requests, eventual default values are injected in
       parsed documents.
    """

    try:
        # assume it's not decoded to json yet (request Content-Type = form)
        document = json.loads(value)
    except:
        # already a json
        document = value

    # By design, dates are expressed as RFC-1123 strings. We convert them
    # to proper datetimes.
    dates = app.config['DOMAIN'][resource]['dates']
    document_dates = dates.intersection(set(document.keys()))
    for date_field in document_dates:
        document[date_field] = str_to_date(document[date_field])

    # update the document with eventual default values
    if request_method() in ('POST', 'PUT'):
        defaults = app.config['DOMAIN'][resource]['defaults']
        missing_defaults = defaults.difference(set(document.keys()))
        schema = config.DOMAIN[resource]['schema']
        for missing_field in missing_defaults:
            document[missing_field] = schema[missing_field]['default']

    return document


def payload():
    """ Performs sanity checks or decoding depending on the Content-Type,
    then returns the request payload as a dict. If request Content-Type is
    unsupported, aborts with a 400 (Bad Request).

    .. versionchanged:: 0.0.9
       More informative error messages.
       request.get_json() replaces the now deprecated request.json


    .. versionchanged:: 0.0.7
       Native Flask request.json preferred over json.loads.

    .. versionadded: 0.0.5
    """
    content_type = request.headers['Content-Type'].split(';')[0]

    if content_type == 'application/json':
        return request.get_json()
    elif content_type == 'application/x-www-form-urlencoded':
        return request.form if len(request.form) else \
            abort(400, description=debug_error_message(
                'No form-urlencoded data supplied'
            ))
    else:
        abort(400, description=debug_error_message(
            'Unknown or no Content-Type header supplied'))


class RateLimit(object):
    """ Implements the Rate-Limiting logic using Redis as a backend.

    :param key_prefix: the key used to uniquely identify a client.
    :param limit: requests limit, per period.
    :param period: limit validity period
    :param send_x_headers: True if response headers are supposed to include
                           special 'X-RateLimit' headers

    .. versionadded:: 0.0.7
    """
    # We give the key extra expiration_window seconds time to expire in redis
    # so that badly synchronized clocks between the workers and the redis
    # server do not cause problems
    expiration_window = 10

    def __init__(self, key_prefix, limit, period, send_x_headers=True):
        self.reset = (int(time.time()) // period) * period + period
        self.key = key_prefix + str(self.reset)
        self.limit = limit
        self.period = period
        self.send_x_headers = send_x_headers
        p = app.redis.pipeline()
        p.incr(self.key)
        p.expireat(self.key, self.reset + self.expiration_window)
        self.current = min(p.execute()[0], limit + 1)

    remaining = property(lambda x: x.limit - x.current)
    over_limit = property(lambda x: x.current > x.limit)


def get_rate_limit():
    """ If available, returns a RateLimit instance which is valid for the
    current request-response.

    .. versionadded:: 0.0.7
    """
    return getattr(g, '_rate_limit', None)


def ratelimit():
    """ Enables support for Rate-Limits on API methods
    The key is constructed by default from the remote address or the
    authorization.username if authentication is being used. On
    a authentication-only API, this will impose a ratelimit even on
    non-authenticated users, reducing exposure to DDoS attacks.

    Before the function is executed it increments the rate limit with the help
    of the RateLimit class and stores an instance on g as g._rate_limit. Also
    if the client is indeed over limit, we return a 429, see
    http://tools.ietf.org/html/draft-nottingham-http-new-status-04#section-4

    .. versionadded:: 0.0.7
    """
    def decorator(f):
        @wraps(f)
        def rate_limited(*args, **kwargs):
            method_limit = app.config.get('RATE_LIMIT_' + request_method())
            if method_limit and app.redis:
                limit = method_limit[0]
                period = method_limit[1]
                # If authorization is being used the key is 'username'.
                # Else, fallback to client IP.
                key = 'rate-limit/%s' % (request.authorization.username
                                         if request.authorization else
                                         request.remote_addr)
                rlimit = RateLimit(key, limit, period, True)
                if rlimit.over_limit:
                    return Response('Rate limit exceeded', 429)
                # store the rate limit for further processing by
                # send_response
                g._rate_limit = rlimit
            else:
                g._rate_limit = None
            return f(*args, **kwargs)
        return rate_limited
    return decorator


def last_updated(document):
    """Fixes document's LAST_UPDATED field value. Flask-PyMongo returns
    timezone-aware values while stdlib datetime values are timezone-naive.
    Comparisions between the two would fail.

    If LAST_UPDATE is missing we assume that it has been created outside of the
    API context and inject a default value, to allow for proper computing of
    Last-Modified header tag. By design all documents return a LAST_UPDATED
    (and we don't want to break existing clients).

    :param document: the document to be processed.

    .. versionchanged:: 0.1.0
       Moved to common.py and renamed as public, so it can also be used by edit
       methods (via get_document()).

    .. versionadded:: 0.0.5
    """
    if config.LAST_UPDATED in document:
        return document[config.LAST_UPDATED].replace(tzinfo=None)
    else:
        return epoch()


def date_created(document):
    """If DATE_CREATED is missing we assume that it has been created outside of
    the API context and inject a default value. By design all documents
    return a DATE_CREATED (and we dont' want to break existing clients).

    :param document: the document to be processed.

    .. versionchanged:: 0.1.0
       Moved to common.py and renamed as public, so it can also be used by edit
       methods (via get_document()).

    .. versionadded:: 0.0.5
    """
    return document[config.DATE_CREATED] if config.DATE_CREATED in document \
        else epoch()


def epoch():
    """ A datetime.min alternative which won't crash on us.

    .. versionchanged:: 0.1.0
       Moved to common.py and renamed as public, so it can also be used by edit
       methods (via get_document()).

    .. versionadded:: 0.0.5
    """
    return datetime(1970, 1, 1)

def validate_document(document, validator, resource, resource_def, original=None):
    doc_issues = []

    try:
        document = parse(document, resource)

        if original:
            # document is being replaced (as with a PUT request)
            object_id = original[config.ID_FIELD]
            validation = validator.validate_replace(document, object_id)
        else:
            # document is being inserted (as with a POST request)
            validation = validator.validate(document)

        if validation:
            # validation is successful
            document[config.LAST_UPDATED] = datetime.utcnow().replace(microsecond=0)
            if original:
                document[config.ID_FIELD] = object_id
                document[config.DATE_CREATED] = original[config.DATE_CREATED]
            else:
                document[config.DATE_CREATED] = document[config.LAST_UPDATED]
            
            # if 'user-restricted resource access' is enabled
            # and there's an Auth request active,
            # inject the auth_field into the document
            auth_field = resource_def['auth_field']
            if auth_field:
                request_auth_value = app.auth.request_auth_value
                if request_auth_value and request.authorization:
                    document[auth_field] = request_auth_value
        else:
            # validation errors added to list of document issues
            doc_issues.extend(validator.errors)
    except ValidationError as e:
        raise e
    except InternalServerError as e:
        raise e
    except Exception as e:
        traceback.print_exc()
        # most likely a problem with the incoming payload, report back to
        # the client as if it was a validation issue
        doc_issues.append(str(e))

    return document, doc_issues

def failure_resp_item(doc_issues):
    return {
        'status': config.STATUS_ERR,
        'issues': doc_issues
    }

def success_resp_item(id, document, resource, resource_def):
    response_item = {}
    response_item['status'] = config.STATUS_OK
    response_item[config.ID_FIELD] = id
    document = document
    response_item[config.LAST_UPDATED] = document[config.LAST_UPDATED]

    # add in etag of posted doc
    lookup = { config.ID_FIELD: response_item[config.ID_FIELD] }
    posted_doc = app.data.find_one(resource, **lookup)
    response_item['etag'] = document_etag(posted_doc)

    # add in hateoas links
    if resource_def['hateoas']:
        response_item['_links'] = \
            {'self': document_link(resource,
                                   response_item[config.ID_FIELD])}

    # add any additional field that might be needed
    allowed_fields = [x for x in resource_def['extra_response_fields']
                      if x in document.keys()]
    for field in allowed_fields:
        response_item[field] = document[field]

    return response_item
