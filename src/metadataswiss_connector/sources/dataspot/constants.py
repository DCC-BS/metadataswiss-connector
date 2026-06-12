"""Dataspot-specific constant values used across the source extraction."""

import os

from metadataswiss_connector.dcat.i14y_models import CodeListEntryValueType

# Value of the ``publicState`` field that marks an entity as visible to
# the public. We filter on this to avoid syncing internal/draft records.
PUBLIC_STATE = "PUBLIC"

# Stereotype assigned in Dataspot to collections that represent an
# organizational unit (and therefore have a stateCalendarId we can
# resolve via the staatskalender API).
STEREOTYPE_ORGANIZATIONAL_UNIT = "organizationalUnit"

# Stereotypes set on data products in Dataspot. OGD / GEO records are
# published to I14Y as Datasets; API records become DataServices on a
# different endpoint with a different input model.
STEREOTYPE_DATA_PRODUCT_OGD = "OGD"
STEREOTYPE_DATA_PRODUCT_GEO = "GEO"
STEREOTYPE_DATA_PRODUCT_API = "API"
DATASET_STEREOTYPES = frozenset(
    {STEREOTYPE_DATA_PRODUCT_OGD, STEREOTYPE_DATA_PRODUCT_GEO}
)
DATA_SERVICE_STEREOTYPES = frozenset({STEREOTYPE_DATA_PRODUCT_API})

# Qualifier on a Dataspot ``Derivation`` that marks "this API produces
# that dataset". An API's ``derivedFrom`` link returns derivations of
# several kinds; only ``SPEZ2`` ones map to I14Y ``servesDatasets``.
# Other qualifiers describe unrelated lineage we must not surface.
DERIVATION_QUALIFIER_PRODUCES = "SPEZ2"

# Role UUID identifying the "data owner" attribution in Dataspot.
# The collection's ``attributedTo`` endpoint returns Attribution
# objects whose ``attributedTo`` value points to a Post (a role
# binding), not directly to a Person. The Post's ``postAgents`` link
# resolves to the Person(s) currently holding that post.
# Tenant-specific, so required from the environment.
DATA_OWNER_ROLE_UUID = os.environ["DATA_OWNER_ROLE_UUID"]

# Staatskalender (kanton BS) public API used to enrich organizational
# units (collection stereotype ``organizationalUnit``) with contact data.
# Required from the environment so other cantons can point at their own
# directory.
STAATSKALENDER_BASE_URL = os.environ["STAATSKALENDER_BASE_URL"].rstrip("/")
# Fallback wait when the staatskalender 429 response carries no
# x-ratelimit-reset header. The API resets quotas on a 6-minute window.
STAATSKALENDER_DEFAULT_WAIT_SECONDS = 360.0
# Hard cap for any computed wait so a malformed reset header can't
# stall the pipeline indefinitely.
STAATSKALENDER_MAX_WAIT_SECONDS = 900.0
STAATSKALENDER_MAX_RETRIES = 5

# Sentinel epoch-ms values Dataspot uses for "no bound" on validity:
# 1900-01-01 and 3000-01-01 UTC. Skip these when mapping to I14Y so we
# don't emit nonsense validFrom/validTo dates on code-list entries.
DATASPOT_VALID_FROM_SENTINEL = -2208988800000  # 1900-01-01 UTC
DATASPOT_VALID_TO_SENTINEL = 32503593600000    # 3000-01-01 UTC

# Fixed contacts used for responsiblePerson / responsibleDeputy on every
# code-list concept. Dataspot enumerations expose no stable contact field,
# so we publish deployment-wide constants rather than deriving them per
# record. Required from the environment.
CONCEPT_RESPONSIBLE_PERSON_EMAIL = os.environ["CONCEPT_RESPONSIBLE_PERSON_EMAIL"]
CONCEPT_RESPONSIBLE_DEPUTY_EMAIL = os.environ["CONCEPT_RESPONSIBLE_DEPUTY_EMAIL"]

# Version string used for every code-list concept.
CONCEPT_VERSION = "1.0.0"

# Default code-list value-type / max-length used when Dataspot doesn't
# expose them via custom properties on the enumeration. I14Y requires
# both fields on a CodeListConceptInput.
DEFAULT_CODE_LIST_VALUE_TYPE = CodeListEntryValueType.string
DEFAULT_CODE_LIST_VALUE_MAX_LENGTH = 255

# IRI namespace used to mint stable URIs for dataspot resources in the
# SHACL structure documents we upload to I14Y. The URIs don't need to
# resolve — SHACL only requires globally unique IRIs — but anchoring them
# at the public catalog domain gives provenance and avoids urn churn.
# Required from the environment so each deployment anchors at its own
# catalog domain.
SOURCES__DATASPOT__BASE_URL = os.environ["SOURCES__DATASPOT__BASE_URL"].rstrip("/")

# Dataspot ``baseType`` → XML Schema datatype IRI. Unknown / missing
# base types fall back to ``xsd:string`` (handled in the SHACL builder).
DATASPOT_BASETYPE_TO_XSD = {
    "STRING": "xsd:string",
    "TEXT": "xsd:string",
    "INTEGER": "xsd:integer",
    "DECIMAL": "xsd:decimal",
    "FLOAT": "xsd:double",
    "DOUBLE": "xsd:double",
    "BOOLEAN": "xsd:boolean",
    "DATE": "xsd:date",
    "DATETIME": "xsd:dateTime",
    "TIME": "xsd:time",
}
DATASPOT_BASETYPE_DEFAULT_XSD = "xsd:string"

# Fallback mapping for datatypes whose payload has no ``baseType`` (some
# legacy datatypes carry only a label). Matched case-insensitively after
# baseType lookup. Geometry types stay ``xsd:string`` rather than
# ``geo:wktLiteral`` to keep the output prefix list minimal; switch if
# downstream needs GeoSPARQL semantics.
DATASPOT_DATATYPE_LABEL_TO_XSD = {
    "text": "xsd:string",
    "formatierter text": "xsd:string",
    "url": "xsd:anyURI",
    "datum": "xsd:date",
    "ganzzahl": "xsd:integer",
    "dezimalzahl": "xsd:decimal",
    "boolescher wert": "xsd:boolean",
    "geometrie (fläche)": "xsd:string",
    "geometrie (linie)": "xsd:string",
    "geometrie (punkt)": "xsd:string",
    "geo_point_2d": "xsd:string",
}

# Dataspot ``_type`` values returned by the resolver for an attribute's
# ``hasRange``. ``DataDomain`` = primitive datatype, ``ReferenceObject`` =
# code list (enumeration). Anything else falls back to primitive
# handling.
DATATYPE_KIND_DATATYPE = "DataDomain"
DATATYPE_KIND_ENUMERATION = "ReferenceObject"
