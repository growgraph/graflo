import pathlib

from graflo.util.onto import Patterns, FilePattern


def test_patterns():
    # Create Patterns with FilePattern instances
    patterns = Patterns()

    # Add file patterns directly
    pattern_a = FilePattern(
        regex=".*", sub_path=pathlib.Path("dir_a/dir_b"), resource_name="a"
    )
    pattern_b = FilePattern(
        regex="^asd", sub_path=pathlib.Path("./"), resource_name="b"
    )

    patterns.add_file_pattern("a", pattern_a)
    patterns.add_file_pattern("b", pattern_b)

    # Test that patterns work correctly
    assert isinstance(patterns.patterns["a"].sub_path / "a", pathlib.Path)
    assert str(patterns.patterns["b"].sub_path / "a") == "a"

    # Test that patterns can be accessed by name
    assert "a" in patterns.patterns
    assert "b" in patterns.patterns
    assert patterns.get_resource_type("a") == "file"
    assert patterns.get_resource_type("b") == "file"
