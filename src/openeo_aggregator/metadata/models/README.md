# Metadata models

The python models in this directory were first generated with the
[openapi-generator](https://github.com/openapitools/openapi-generator).
After that, additional validation rules and merging logic was added manually.

You can follow these steps to generate the models yourself:
1. Install the openapi-generator: `pip install openapi-generator-cli`
2. Convert the [openEO API openapi.yaml file](https://github.com/Open-EO/openeo-api/blob/master/openapi.yaml)
   to json.

        sudo openapi-generator-cli generate -g openapi -i openapi.yaml -o .

3. Generate models from openapi.json

        openapi-python-client generate --path openapi.json

Note that some models use extensions such as the datacube stac-extension.
In such cases the typing was added manually using the [specification](https://github.com/stac-extensions/datacube/blob/main/json-schema/schema.json).
An example is the CubeDimension class, where the openapi file does not specify an extent or values property.

[Quicktype](https://github.com/quicktype/quicktype) can be used to also generate python models for the extensions.
But the generated models for e.g. the datacube extension were too extensive to be useful.
