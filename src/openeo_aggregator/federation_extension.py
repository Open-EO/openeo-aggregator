"""
Constants and logic related to openEO Federation Extension
https://github.com/Open-EO/openeo-api/tree/draft/extensions/federation

"""

FED_EXT_CONFORMANCE_CLASS = "https://api.openeo.org/extensions/federation/0.1.0"


# Discoverable resources can explicitly list the subset of back-ends
# that support or host the exposed resource or functionality
# with the property federation:backends.
FED_EXT_BACKENDS = "federation:backends"


# Resources and back-ends can be temporarily or unintentionally unavailable.
# It is especially important to communicate to users missing resources
# when compiling a list of resources across multiple back-ends.
# Clients will assume that all lists of resources are a combination
# of all back-ends listed under federation in GET /.
# Federated APIs can expose if any of the back-ends was not available
# when building the resource listing response
# with the property federation:missing.
FED_EXT_MISSING = "federation:missing"
