import datetime

# General information about the project.
project = "openEO Aggregator"
author = "Stefaan Lippens"
copyright = f"2021 - {datetime.datetime.now():%Y}"

# Sphinx extensions
extensions = [
    "myst_parser",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]
myst_enable_extensions = [
    "colon_fence",
]

# Doc discovery settings
root_doc = "index"
exclude_patterns = [
    "_build",
    "build",
    ".venv",
    "venv",
]

# HTML theme settings
html_theme = "furo"
html_title = "openEO Aggregator"

html_theme_options = {
    "source_repository": "https://github.com/Open-EO/openeo-aggregator",
    "source_branch": "master",
    "source_directory": "docs/",
}
