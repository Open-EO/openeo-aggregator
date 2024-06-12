project = "openEO Aggregator"

extensions = [
    "myst_parser",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]
myst_enable_extensions = ["colon_fence"]

root_doc = "index"
html_theme = "furo"


exclude_patterns = [
    "_build",
    "build",
    ".venv",
    "venv",
]
