# Example 1: Multiple Tabular Sources


Suppose we have a table that represents people:


{{ read_csv('data/people.csv') }}

and a table that represents their roles in a company:

{{ read_csv('data/departments.csv') }}

We want to define vertices `Person` and `Department` and set up the rules of how to map tables to vertex key-value pairs.

Let's define vertices as

```yaml
 vertices:
 -   name: person
     fields:
     -   id
     -   name
     -   age
     indexes:
     -   fields:
         -   id
 -   name: department
     fields:
     -   name
     indexes:
     -   fields:
         -   name
```

and edges as 

```yaml
 edges:
 -   source: person
     target: department
```

The graph structure is quite simple:

![People Resource Image](../assets/1-ingest-csv/figs/hr_vc2vc.png){ width="200" }


Let's define the mappings: we want to rename the fields `person`, `person_id` and `department` and specify explicitly `target_vertex` to avoid the collision, since both `Person` and `Department` have a field called `name`.  

```yaml
-   resource_name: people
    apply:
    -   vertex: person
-   resource_name: departments
    apply:
    -   map:
            person: name
            person_id: id
    -   target_vertex: department
        map:
            department: name
```

Department Resource

![Department Resource Image](../assets/1-ingest-csv/figs/hr.resource-departments.png){ width="700" }

People Resource

![People Resource Image](../assets/1-ingest-csv/figs/hr.resource-people.png){ width="200" }


Transforming the data and ingesting it into an ArangoDB takes a few lines of code:

```python
from suthing import FileHandle
from graflo import Caster, Patterns, Schema
from graflo.db.connection.onto import ArangoConfig

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

# Option 1: Load config from docker/arango/.env (recommended)
conn_conf = ArangoConfig.from_docker_env()

# Option 2: Load from environment variables
# Set: ARANGO_URI, ARANGO_USERNAME, ARANGO_PASSWORD, ARANGO_DATABASE
# conn_conf = ArangoConfig.from_env()

# Option 3: Create config directly
# conn_conf = ArangoConfig(
#     uri="http://localhost:8535",
#     username="root",
#     password="123",
#     database="mygraph",  # For ArangoDB, 'database' maps to schema/graph
# )

# Create Patterns with file patterns
from graflo.util.onto import FilePattern
import pathlib

patterns = Patterns()
patterns.add_file_pattern(
    "people",
    FilePattern(regex="^people.*\.csv$", sub_path=pathlib.Path("."), resource_name="people")
)
patterns.add_file_pattern(
    "departments",
    FilePattern(regex="^dep.*\.csv$", sub_path=pathlib.Path("."), resource_name="departments")
)

# Or use resource_mapping for simpler initialization
# patterns = Patterns(
#     _resource_mapping={
#         "people": "./people.csv",
#         "departments": "./departments.csv",
#     }
# )

from graflo.caster import IngestionParams

caster = Caster(schema)

ingestion_params = IngestionParams(
    clean_start=False,  # Set to True to wipe existing database
    # max_items=1000,  # Optional: limit number of items to process
)

caster.ingest(
    output_config=conn_conf,  # Target database config
    patterns=patterns,  # Source data patterns
    ingestion_params=ingestion_params,
)

```

Please refer to [examples](https://github.com/growgraph/graflo/tree/main/examples/1-ingest-csv)

For more examples and detailed explanations, refer to the [API Reference](../reference/index.md). 