
# The openEO Aggregator: federated openEO processing

The openEO Aggregator is a software component to group multiple openEO back-ends together
into a unified, federated openEO processing platform.


```{mermaid}
flowchart LR

U("👤 User") --> A("openEO Aggregator")

subgraph federation ["Federated openEO Processing"]

    A --> B1("openEO Back-end 1")
    A --> B2("openEO Back-end 2")
    A --> B3("openEO Back-end 3")
end
```


## Core openEO API

The [openEO API](https://openeo.org/) is an open, standardized API for Earth Observation data processing,
connecting openEO-capable clients at the user side with openEO-capable back-ends at the (cloud) processing side.
Not only does it decouple the clients requirements from the technology stack of the back-ends,
it also allows the user or client to switch between back-ends with minimal or even no code changes.
Multiple openEO back-end implementations have been developed and are available today,
each based on different processing technologies and each providing a different set of data collections.

The freedom to choose a back-end and avoiding lock-in is one of the key features of openEO,
but it also implies that the user is required to make a choice, as there is no default back-end.
Moreover, the user might want to combine data or processing functionality from different back-ends,
which is not directly supported by openEO's core API.
Note that Earth Observation data is fast-growing and diverse,
making it unsafe to assume that a single provider will be able to host all EO data.

## Federated openEO processing

The "openEO Aggregator" project aims to address this problem through a proxy-like component to
build a federated openEO processing platform.
The openEO Aggregator allows to group multiple openEO back-ends together
and to and expose their combined power as a single, openEO-compliant API endpoint to the user,
including, but not limited to:

- merging and unification of general resource metadata such as data collections and openEO processes
- unified listing of batch jobs of a user across multiple back-ends
- dispatching of simple processing requests (both for synchronous processing and batch jobs) to the appropriate back-end
- handling of more complex processing requests that require data from multiple back-ends



```{toctree}
:hidden:
pages/installation.md
pages/configuration.md
pages/usage.md
```
