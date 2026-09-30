def test_declared_field_descriptions_reach_the_profile() -> None:
    from graflo.architecture.onto_sample import ResourceSample, profile_sample

    sample = ResourceSample(
        resource_name="machine",
        docs=[{"serial": "S-1", "plain": "x"}],
        description="Machines as recorded",
        field_descriptions={"serial": "Nameplate serial"},
    )
    profile = profile_sample(sample)
    by_path = {field.path: field for field in profile.fields}
    assert by_path["serial"].description == "Nameplate serial"
    assert by_path["plain"].description is None
    assert profile.description == "Machines as recorded"
