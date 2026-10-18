"""The IRIs the aspects ground their types in.

Two conventions, stated here so a reader does not have to infer them:

**Time is grounded in PROV-O and SOSA, not OWL-Time.** ``time:hasBeginning``
ranges over a ``time:Instant``, not a literal, so grounding a ``DATETIME``
column in it is a claim that becomes false the moment the schema is projected to
OWL. ``prov:generatedAtTime``, ``prov:invalidatedAtTime`` and ``sosa:resultTime``
are literal-ranged and say the same thing truthfully. OWL-Time stays in
``exact_match`` at the concept level, where it is about the *type*.

**Units are UCUM tokens, carried per row.** An abstract observation type serves
temperature and pressure alike, so no single unit in its schema would be
correct; ``result_unit`` is grounded in ``qudt:ucumCode`` and travels with
the measurement. UCUM has no currency, so currency falls back to ISO-4217 alpha
codes (``USD``, ``EUR``).
"""

from __future__ import annotations

PROV = "http://www.w3.org/ns/prov#"
SOSA = "http://www.w3.org/ns/sosa/"
SSN = "http://www.w3.org/ns/ssn/"
TIME = "http://www.w3.org/2006/time#"
QUDT = "http://qudt.org/schema/qudt/"
DCTERMS = "http://purl.org/dc/terms/"

PROV_ENTITY = f"{PROV}Entity"
PROV_ACTIVITY = f"{PROV}Activity"
PROV_AGENT = f"{PROV}Agent"
PROV_GENERATED_AT = f"{PROV}generatedAtTime"
PROV_INVALIDATED_AT = f"{PROV}invalidatedAtTime"
PROV_SPECIALIZATION_OF = f"{PROV}specializationOf"
PROV_WAS_DERIVED_FROM = f"{PROV}wasDerivedFrom"
PROV_WAS_ATTRIBUTED_TO = f"{PROV}wasAttributedTo"
PROV_WAS_INFLUENCED_BY = f"{PROV}wasInfluencedBy"

SOSA_OBSERVATION = f"{SOSA}Observation"
SOSA_SENSOR = f"{SOSA}Sensor"
SOSA_FEATURE_OF_INTEREST = f"{SOSA}FeatureOfInterest"
SOSA_HAS_FEATURE_OF_INTEREST = f"{SOSA}hasFeatureOfInterest"
SOSA_OBSERVED_PROPERTY = f"{SOSA}observedProperty"
SOSA_HAS_SIMPLE_RESULT = f"{SOSA}hasSimpleResult"
SOSA_RESULT_TIME = f"{SOSA}resultTime"

SSN_PROPERTY = f"{SSN}Property"
TIME_PROPER_INTERVAL = f"{TIME}ProperInterval"
QUDT_UCUM_CODE = f"{QUDT}ucumCode"
DCTERMS_IS_PART_OF = f"{DCTERMS}isPartOf"

__all__ = [
    "DCTERMS",
    "DCTERMS_IS_PART_OF",
    "PROV",
    "PROV_ACTIVITY",
    "PROV_AGENT",
    "PROV_ENTITY",
    "PROV_GENERATED_AT",
    "PROV_INVALIDATED_AT",
    "PROV_SPECIALIZATION_OF",
    "PROV_WAS_ATTRIBUTED_TO",
    "PROV_WAS_DERIVED_FROM",
    "PROV_WAS_INFLUENCED_BY",
    "QUDT",
    "QUDT_UCUM_CODE",
    "SOSA",
    "SOSA_FEATURE_OF_INTEREST",
    "SOSA_HAS_FEATURE_OF_INTEREST",
    "SOSA_HAS_SIMPLE_RESULT",
    "SOSA_OBSERVATION",
    "SOSA_OBSERVED_PROPERTY",
    "SOSA_RESULT_TIME",
    "SOSA_SENSOR",
    "SSN",
    "SSN_PROPERTY",
    "TIME",
    "TIME_PROPER_INTERVAL",
]
