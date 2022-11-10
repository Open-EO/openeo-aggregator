from openeo_aggregator.metadata.validator import compare_get_processes


def test_compare_get_processes_parameter_mismatch(requests_mock, capsys):
    requests_mock.get(
        "https://oeo1.test/processes",
        json={
            "processes": [
                {
                    "id": "cos",
                    "parameters": [{"name": "x", "schema": {"type": "number"}}],
                },
            ]
        },
    )
    requests_mock.get(
        "https://oeo2.test/processes",
        json={
            "processes": [
                {
                    "id": "cos",
                    "parameters": [{"name": "x", "schema": {"type": "string"}}],
                },
            ]
        },
    )
    compare_get_processes(["https://oeo1.test", "https://oeo2.test"])

    captured = capsys.readouterr()
    stdout = captured.out
    assert "\n- [ ] **https://oeo2.test : cos** (merging.py" in stdout
    assert "Parameter 'x' field 'schema' value differs from merged." in stdout
    assert "\n    - merged `{'type': 'number'}`\n" in stdout
    assert "\n    - value `{'type': 'string'}`\n" in stdout
    assert '        -  "type": "number"\n' in stdout
    assert '        +  "type": "string"\n' in stdout
