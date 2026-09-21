def test_declared_field_descriptions_reach_the_profile() -> None:
    from graflo.architecture.onto_sample import ResourceSample, profile_sample

    sample = ResourceSample(
        resource_name="server",
        docs=[{"serial": "S-1", "plain": "x"}],
        description="Servers as recorded",
        field_descriptions={"serial": "Hardware serial"},
    )
    profile = profile_sample(sample)
    by_path = {field.path: field for field in profile.fields}
    assert by_path["serial"].description == "Hardware serial"
    assert by_path["plain"].description is None
    assert profile.description == "Servers as recorded"
