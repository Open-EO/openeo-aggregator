from openeo_driver.errors import OpenEOApiException


class BackendLookupFailureException(OpenEOApiException):
    status_code = 400
    code = "BackendLookupFailure"
    message = "Failed to determine back-end to use."
    _description = None
