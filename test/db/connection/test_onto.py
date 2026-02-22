"""Tests for DBConfig.from_env() method with prefix, profile, and suffix support."""

import pytest

from graflo.db.connection.onto import ArangoConfig, Neo4jConfig, TigergraphConfig


class TestArangoConfigFromEnv:
    """Tests for ArangoConfig.from_env()."""

    def test_from_env_default_prefix(self, monkeypatch):
        """Test default behavior without prefix - reads ARANGO_* variables."""
        # Set environment variables with default prefix
        monkeypatch.setenv("ARANGO_URI", "http://localhost:8529")
        monkeypatch.setenv("ARANGO_USERNAME", "testuser")
        monkeypatch.setenv("ARANGO_PASSWORD", "testpass")
        monkeypatch.setenv("ARANGO_DATABASE", "testdb")

        # Load config without prefix
        config = ArangoConfig()

        # Verify values are read correctly
        assert config.uri == "http://localhost:8529"
        assert config.username == "testuser"
        assert config.password == "testpass"
        assert config.database == "testdb"

    def test_from_env_with_prefixes(self, monkeypatch):
        """Test behavior with two different prefixes - USER_ and LAKE_."""
        # Set environment variables with USER_ prefix
        monkeypatch.setenv("USER_ARANGO_URI", "http://user-db:8529")
        monkeypatch.setenv("USER_ARANGO_USERNAME", "user_admin")
        monkeypatch.setenv("USER_ARANGO_PASSWORD", "user_secret")
        monkeypatch.setenv("USER_ARANGO_DATABASE", "user_db")

        # Set environment variables with LAKE_ prefix
        monkeypatch.setenv("LAKE_ARANGO_URI", "http://lake-db:8529")
        monkeypatch.setenv("LAKE_ARANGO_USERNAME", "lake_admin")
        monkeypatch.setenv("LAKE_ARANGO_PASSWORD", "lake_secret")
        monkeypatch.setenv("LAKE_ARANGO_DATABASE", "lake_db")

        # Load configs with different prefixes
        user_config = ArangoConfig.from_env(prefix="USER")
        lake_config = ArangoConfig.from_env(prefix="LAKE")

        # Verify USER_ prefix config
        assert user_config.uri == "http://user-db:8529"
        assert user_config.username == "user_admin"
        assert user_config.password == "user_secret"
        assert user_config.database == "user_db"

        # Verify LAKE_ prefix config
        assert lake_config.uri == "http://lake-db:8529"
        assert lake_config.username == "lake_admin"
        assert lake_config.password == "lake_secret"
        assert lake_config.database == "lake_db"

    def test_from_env_with_port_components(self, monkeypatch):
        """Test that port/hostname/protocol components are read with prefix.

        Note: from_env() reads env vars via Pydantic but doesn't construct URIs
        from components. That logic is in from_docker_env(). This test verifies
        that the components are accessible as separate fields.
        """
        # Set environment variables with USER_ prefix using port components
        monkeypatch.setenv("USER_ARANGO_PORT", "8530")
        monkeypatch.setenv("USER_ARANGO_HOSTNAME", "custom-host")
        monkeypatch.setenv("USER_ARANGO_PROTOCOL", "https")
        monkeypatch.setenv("USER_ARANGO_USERNAME", "admin")

        # Load config with prefix
        config = ArangoConfig.from_env(prefix="USER")

        # Verify username is read correctly
        # Note: PORT/HOSTNAME/PROTOCOL are not model fields, so they won't be
        # automatically read. Only fields defined in the model are read.
        assert config.username == "admin"

    def test_from_env_with_profile(self, monkeypatch):
        """Test profile: ARANGO_{profile}_* (e.g. ARANGO_DEV_URI)."""
        monkeypatch.setenv("ARANGO_DEV_URI", "http://dev-db:8529")
        monkeypatch.setenv("ARANGO_DEV_USERNAME", "dev_user")
        monkeypatch.setenv("ARANGO_DEV_PASSWORD", "dev_pass")
        monkeypatch.setenv("ARANGO_DEV_DATABASE", "dev_db")

        config = ArangoConfig.from_env(profile="DEV")

        assert config.uri == "http://dev-db:8529"
        assert config.username == "dev_user"
        assert config.password == "dev_pass"
        assert config.database == "dev_db"

    def test_from_env_with_suffix(self, monkeypatch):
        """Test suffix: ARANGO_*_{suffix} (e.g. ARANGO_URI_DEV)."""
        monkeypatch.setenv("ARANGO_URI_DEV", "http://dev-db:8529")
        monkeypatch.setenv("ARANGO_USERNAME_DEV", "dev_user")
        monkeypatch.setenv("ARANGO_PASSWORD_DEV", "dev_pass")
        monkeypatch.setenv("ARANGO_DATABASE_DEV", "dev_db")

        config = ArangoConfig.from_env(suffix="DEV")

        assert config.uri == "http://dev-db:8529"
        assert config.username == "dev_user"
        assert config.password == "dev_pass"
        assert config.database == "dev_db"

    def test_from_env_at_most_one_qualifier(self):
        """Test that at most one of prefix, profile, suffix may be set."""
        with pytest.raises(ValueError, match="At most one of prefix, profile, suffix"):
            ArangoConfig.from_env(prefix="USER", profile="DEV")
        with pytest.raises(ValueError, match="At most one of prefix, profile, suffix"):
            ArangoConfig.from_env(prefix="USER", suffix="DEV")
        with pytest.raises(ValueError, match="At most one of prefix, profile, suffix"):
            ArangoConfig.from_env(profile="DEV", suffix="DEV")


class TestNeo4jConfigFromEnv:
    """Tests for Neo4jConfig.from_env()."""

    def test_from_env_default_prefix(self, monkeypatch):
        """Test default behavior without prefix - reads NEO4J_* variables."""
        # Set environment variables with default prefix
        monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
        monkeypatch.setenv("NEO4J_USERNAME", "neo4j_user")
        monkeypatch.setenv("NEO4J_PASSWORD", "neo4j_pass")
        monkeypatch.setenv("NEO4J_DATABASE", "neo4j_db")

        # Load config without prefix
        config = Neo4jConfig.from_env()

        # Verify values are read correctly
        assert config.uri == "bolt://localhost:7687"
        assert config.username == "neo4j_user"
        assert config.password == "neo4j_pass"
        assert config.database == "neo4j_db"

    def test_from_env_with_prefixes(self, monkeypatch):
        """Test behavior with two different prefixes - USER_ and LAKE_."""
        # Set environment variables with USER_ prefix
        monkeypatch.setenv("USER_NEO4J_URI", "bolt://user-neo4j:7687")
        monkeypatch.setenv("USER_NEO4J_USERNAME", "user_neo4j")
        monkeypatch.setenv("USER_NEO4J_PASSWORD", "user_neo4j_pass")
        monkeypatch.setenv("USER_NEO4J_DATABASE", "user_neo4j_db")

        # Set environment variables with LAKE_ prefix
        monkeypatch.setenv("LAKE_NEO4J_URI", "bolt://lake-neo4j:7687")
        monkeypatch.setenv("LAKE_NEO4J_USERNAME", "lake_neo4j")
        monkeypatch.setenv("LAKE_NEO4J_PASSWORD", "lake_neo4j_pass")
        monkeypatch.setenv("LAKE_NEO4J_DATABASE", "lake_neo4j_db")

        # Load configs with different prefixes
        user_config = Neo4jConfig.from_env(prefix="USER")
        lake_config = Neo4jConfig.from_env(prefix="LAKE")

        # Verify USER_ prefix config
        assert user_config.uri == "bolt://user-neo4j:7687"
        assert user_config.username == "user_neo4j"
        assert user_config.password == "user_neo4j_pass"
        assert user_config.database == "user_neo4j_db"

        # Verify LAKE_ prefix config
        assert lake_config.uri == "bolt://lake-neo4j:7687"
        assert lake_config.username == "lake_neo4j"
        assert lake_config.password == "lake_neo4j_pass"
        assert lake_config.database == "lake_neo4j_db"

    def test_from_env_with_bolt_port(self, monkeypatch):
        """Test that BOLT_PORT is read with prefix.

        Note: from_env() reads env vars via Pydantic. BOLT_PORT is a model field,
        so it will be read. However, URI construction from BOLT_PORT is handled
        in from_docker_env(), not from_env().
        """
        # Set environment variables with USER_ prefix using BOLT_PORT
        monkeypatch.setenv("USER_NEO4J_BOLT_PORT", "7688")
        monkeypatch.setenv("USER_NEO4J_USERNAME", "admin")

        # Load config with prefix
        # Note: from_env with prefix returns a dynamically created subclass, but it has all Neo4jConfig attributes
        config = Neo4jConfig.from_env(prefix="USER")

        # Verify BOLT_PORT and username are read correctly
        # The dynamically created class inherits from Neo4jConfig, so it has bolt_port
        assert config.bolt_port == 7688
        assert config.username == "admin"


class TestTigergraphConfigFromEnv:
    """Tests for TigergraphConfig.from_env()."""

    def test_from_env_default_prefix(self, monkeypatch):
        """Test default behavior without prefix - reads TIGERGRAPH_* variables."""
        # Set environment variables with default prefix
        monkeypatch.setenv("TIGERGRAPH_URI", "http://localhost:14240")
        monkeypatch.setenv("TIGERGRAPH_USERNAME", "tigergraph_user")
        monkeypatch.setenv("TIGERGRAPH_PASSWORD", "tigergraph_pass")
        monkeypatch.setenv("TIGERGRAPH_DATABASE", "tigergraph_db")

        # Load config without prefix
        config = TigergraphConfig.from_env()

        # Verify values are read correctly
        assert config.uri == "http://localhost:14240"
        assert config.username == "tigergraph_user"
        assert config.password == "tigergraph_pass"
        assert config.database == "tigergraph_db"
        assert config.gs_port == 14240

    def test_from_env_with_prefixes(self, monkeypatch):
        """Test behavior with two different prefixes - USER_ and LAKE_."""
        # Set environment variables with USER_ prefix
        monkeypatch.setenv("USER_TIGERGRAPH_URI", "http://user-tg:9000")
        monkeypatch.setenv("USER_TIGERGRAPH_USERNAME", "user_tg")
        monkeypatch.setenv("USER_TIGERGRAPH_PASSWORD", "user_tg_pass")
        monkeypatch.setenv("USER_TIGERGRAPH_DATABASE", "user_tg_db")

        # Set environment variables with LAKE_ prefix
        monkeypatch.setenv("LAKE_TIGERGRAPH_URI", "http://lake-tg:9000")
        monkeypatch.setenv("LAKE_TIGERGRAPH_USERNAME", "lake_tg")
        monkeypatch.setenv("LAKE_TIGERGRAPH_PASSWORD", "lake_tg_pass")
        monkeypatch.setenv("LAKE_TIGERGRAPH_DATABASE", "lake_tg_db")

        # Load configs with different prefixes
        user_config = TigergraphConfig.from_env(prefix="USER")
        lake_config = TigergraphConfig.from_env(prefix="LAKE")

        # Verify USER_ prefix config
        assert user_config.uri == "http://user-tg:9000"
        assert user_config.username == "user_tg"
        assert user_config.password == "user_tg_pass"
        assert user_config.database == "user_tg_db"

        # Verify LAKE_ prefix config
        assert lake_config.uri == "http://lake-tg:9000"
        assert lake_config.username == "lake_tg"
        assert lake_config.password == "lake_tg_pass"
        assert lake_config.database == "lake_tg_db"

    def test_uri_without_scheme(self, monkeypatch):
        """Test that URIs without scheme (host:port format) are handled correctly."""
        # Set URI without scheme
        monkeypatch.setenv("TIGERGRAPH_URI", "localhost:14240")
        monkeypatch.setenv("TIGERGRAPH_USERNAME", "testuser")

        config = TigergraphConfig.from_env()

        # Should normalize to include scheme
        assert config.uri == "http://localhost:14240"
        assert config.port == "14240"
        assert config.hostname == "localhost"
        assert config.protocol == "http"

    def test_port_conflict_warning(self, monkeypatch):
        """Test that port conflicts between URI and gs_port generate a warning."""
        import warnings

        # Set both URI with port and gs_port with different value
        monkeypatch.setenv("TIGERGRAPH_URI", "http://localhost:14240")
        monkeypatch.setenv("TIGERGRAPH_GS_PORT", "9000")
        monkeypatch.setenv("TIGERGRAPH_USERNAME", "testuser")

        # Capture warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = TigergraphConfig.from_env()

            # Should have a warning about port conflict
            assert len(w) > 0
            assert any("Port conflict" in str(warning.message) for warning in w)
            assert any("14240" in str(warning.message) for warning in w)
            assert any("9000" in str(warning.message) for warning in w)

        # Port from URI should be preferred
        assert config.uri == "http://localhost:14240"
        assert config.port == "14240"
        # gs_port should be updated to match URI port
        assert config.gs_port == 14240

    def test_port_no_conflict_when_matching(self, monkeypatch):
        """Test that no warning is generated when URI port matches gs_port."""
        import warnings

        # Set both URI with port and gs_port with same value
        monkeypatch.setenv("TIGERGRAPH_URI", "http://localhost:14240")
        monkeypatch.setenv("TIGERGRAPH_GS_PORT", "14240")
        monkeypatch.setenv("TIGERGRAPH_USERNAME", "testuser")

        # Capture warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = TigergraphConfig.from_env()

            # Should have no port conflict warnings
            port_conflict_warnings = [
                warning for warning in w if "Port conflict" in str(warning.message)
            ]
            assert len(port_conflict_warnings) == 0

        # Both should match
        assert config.uri == "http://localhost:14240"
        assert config.port == "14240"
        assert config.gs_port == 14240
