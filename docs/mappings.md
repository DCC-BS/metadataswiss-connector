<!--
GENERATED FILE - DO NOT EDIT BY HAND.
Regenerate with: uv run python scripts/gen_mapping_docs.py
Sources of truth:
  src/metadataswiss_connector/sources/dataspot/transform.py
  src/metadataswiss_connector/sources/dataspot/mappings.py
-->

# Dataspot to I14Y mappings

How Dataspot records are mapped into the I14Y DCAT format: the field-level
mapping per record type, then the controlled-vocabulary code tables. Run
`uv run python scripts/gen_mapping_docs.py` to refresh.

## Field mappings

How each I14Y model field is populated from a Dataspot source record. **I14Y field** is the model constructor keyword. **Dataspot source** lists the source fields read from the record; `(child)` marks a 1:n child table, `(param)` a pipeline parameter, and a quoted/bare value is a constant or computed expression. **Transform** lists the helper functions applied (the `dcat.` module prefix is dropped; `mappings.*` is kept as it points at the vocabulary tables below). A `-` in *Transform* with a `-` source means the field is computed by the listed helper - see that helper's docstring for specifics.

### Dataset - Dataspot data product to I14Y dataset

Map a flattened Dataspot dataset row to a DcatDatasetInputModel.

_Built by `transform_to_dataset()` -> `DcatDatasetInputModel`._

| I14Y field | Dataspot source | Transform |
| --- | --- | --- |
| `data_owner` | `id` | _resolve_data_owner |
| `title` | `label` | multi_language |
| `description` | `description` | multi_language, html_to_plain_text |
| `identifiers` | `id` | - |
| `publisher` | `publisher` (param) | - |
| `responsible_person` | `RESPONSIBLE_PERSON_EMAIL` | - |
| `responsible_deputy` | `RESPONSIBLE_DEPUTY_EMAIL` | - |
| `access_rights` | `"PUBLIC"` | - |
| `confidentiality_person` | `custom_properties__personal_data` (child) | mappings.confidentiality_person |
| `issued` | `custom_properties__publication_date` | epoch_ms_to_datetime |
| `modified` | `custom_properties__last_update` | epoch_ms_to_datetime |
| `frequency` | `accrual_periodicity` | frequency |
| `keywords` | `tags` (child) | keywords |
| `themes` | `custom_properties__geo_category`, `custom_properties__topics` (child) | mappings.themes |
| `spatial` | `spatial` | - |
| `temporal_coverage` | `temporal_start`, `temporal_end` | temporal_coverage |
| `contact_points` | `id` | _organizational_unit_contact_points |
| `languages` | `"de"` | - |
| `retention_period_complement` | `custom_properties__retention_period`, `custom_properties__retention_justification` | mappings.retention_period_complement |
| `landing_pages` | `custom_properties__i14y_dataset_landing_page` | landing_pages |
| `version` | `custom_properties__i14y_dataset_version` | - |
| `version_notes` | `custom_properties__i14y_dataset_version_notes` | multi_language |
| `distributions` | `distributions` (child) | _map_distribution |

> When the dataset has structure components, a SHACL/Turtle structure is emitted as a sidecar payload (`extras['structure']`, built by `build_dataset_shacl_turtle`).

### Data service - Dataspot API to I14Y dataservice

Map a flattened Dataspot API record to a DataServiceInputModel.

_Built by `transform_to_dataservice()` -> `DataServiceInputModel`._

| I14Y field | Dataspot source | Transform |
| --- | --- | --- |
| `title` | `label` | multi_language |
| `description` | `description` | multi_language, html_to_plain_text |
| `identifiers` | `id` | - |
| `publisher` | `publisher` (param) | - |
| `responsible_person` | `RESPONSIBLE_PERSON_EMAIL` | - |
| `responsible_deputy` | `RESPONSIBLE_DEPUTY_EMAIL` | - |
| `serves_datasets` | `id` | _resolve_serves_datasets |
| `access_rights` | - | _dataservice_access_rights |
| `issued` | `custom_properties__publication_date` | epoch_ms_to_datetime |
| `modified` | `custom_properties__last_update` | epoch_ms_to_datetime |
| `keywords` | `tags` (child) | keywords |
| `contact_points` | `id` | _organizational_unit_contact_points |
| `landing_pages` | `custom_properties__i14y_api_landing_page` | landing_pages |
| `endpoint_urls` | `"custom_properties__i14y_api_endpoint_url"` | _dataservice_resource_list |
| `endpoint_descriptions` | `"custom_properties__i14y_api_endpoint_description"` | _dataservice_resource_list |

### Concept - Dataspot enumeration to I14Y code list

Map a Dataspot enumeration row to a CodeListConceptInput.

_Built by `transform_to_concept()` -> `CodeListConceptInput`._

| I14Y field | Dataspot source | Transform |
| --- | --- | --- |
| `identifier` | `id` | - |
| `name` | `label` | multi_language |
| `description` | `label`, `description` | multi_language, html_to_plain_text |
| `publisher` | `publisher` (param) | - |
| `responsible_person` | `RESPONSIBLE_PERSON_EMAIL` | - |
| `responsible_deputy` | `RESPONSIBLE_DEPUTY_EMAIL` | - |
| `valid_from` | `date_created`, `DATASPOT_VALID_FROM_SENTINEL` | epoch_ms_to_datetime |
| `version` | `CONCEPT_VERSION` | - |
| `code_list_entry_value_type` | `code_list_entries` (child) | _value_type_from_entries |
| `code_list_entry_value_max_length` | `code_list_entries` (child) | _max_code_length |
| `code_list_entry_default_sort_property` | `CodeListEntrySortProperty.code` | - |

> Code-list entries are emitted as a sidecar payload (`extras['entries']`, built by `_build_entries`): each entry maps `code`, `name` (long/short text), `description`, validity bounds and `parentCode`.

### Distribution - Dataspot distribution row to I14Y distribution

Map a Dataspot distribution row to a DcatDistributionInputModel.

_Built by `_map_distribution()` -> `DcatDistributionInputModel`._

| I14Y field | Dataspot source | Transform |
| --- | --- | --- |
| `title` | `label` | multi_language |
| `description` | `description`, `label` | multi_language, html_to_plain_text |
| `access_url` | `access_url` | - |
| `identifier` | `id` | - |
| `issued` | `date_created` | epoch_ms_to_datetime |
| `format` | `format` | file_format |

## Vocabulary mappings

Controlled-vocabulary code tables applied by the transforms above. Labels are taken verbatim from the trailing comments in `mappings.py`.

### Dataspot topic (BFS) to I14Y dataset theme

Dataspot topic code (BFS) → I14Y dataset-theme code. Comments show "<dataspot label> → <i14y label>" so the mapping is readable without consulting the vocabularies. I14Y theme codes 101–126 come from https://input-backend.i14y.a.c.bfs.admin.ch/api/Vocabulary/Concept_DATASET_THEME

_Source: `_TOPIC_TO_THEME`._

| Dataspot code | Dataspot label | I14Y code | I14Y label |
| --- | --- | --- | --- |
| `001` | Bevölkerung | `117` | Einwohner |
| `002` | Gesetzgebung | `123` | Rechtssammlung |
| `003` | Geographie | `122` | Geoinformationen |
| `004` | Gesundheit | `114` | Gesundheit |
| `005` | Kultur, Medien, Informationsgesellschaft, Sport | `108` | Kultur |
| `006` | Bildung, Wissenschaft | `103` | Bildung |
| `007` | Raum und Umwelt | `113` | Umwelt |
| `008` | Mobilität und Verkehr | `116` | Mobilität |
| `009` | Arbeit, Erwerb | `101` | Arbeit |
| `010` | Volkswirtschaft | `115` | Wirtschaft |
| `011` | Verwaltung | `119` | Behörden |
| `012` | Kriminalität, Strafrecht | `105` | Gerichtsbarkeit |
| `013` | Bau- und Wohnungswesen | `102` | Bauen |
| `014` | Energie | `124` | Energie |
| `015` | Soziale Sicherheit | `126` | Soziale Sicherheit |
| `016` | Finanzen | `112` | Steuern |
| `017` | Handel | `115` | Wirtschaft |
| `018` | Industrie, Dienstleistungen | `118` | Unternehmen |
| `019` | Land- und Forstwirtschaft | `109` | Landwirtschaft |
| `020` | Öffentliche Ordnung und Sicherheit | `111` | Sicherheit |
| `021` | Politik | `107` | Politische Aktivitäten |
| `022` | Preise | `115` | Wirtschaft |
| `023` | Statistische Grundlagen | `125` | Öffentliche Statistik |
| `024` | Tourismus | `115` | Wirtschaft |

### Dataspot geoCategory (eCH-166) to I14Y dataset theme

Dataspot geoCategory (eCH-166) → I14Y dataset-theme code.

_Source: `_GEOCATEGORY_TO_THEME`._

| Dataspot code | Dataspot label | I14Y code | I14Y label |
| --- | --- | --- | --- |
| `imageryBaseMapsEarthCover_BaseMaps` | Basiskarten, Landschaftsmodelle | `122` | Geoinformationen |
| `imageryBaseMapsEarthCover_EarthCover` | Bodenbedeckung, Bodennutzung | `122` | Geoinformationen |
| `imageryBaseMapsEarthCover_Imagery` | Luft-, Satellitenbilder | `122` | Geoinformationen |
| `boundaries` | Politische und administrative Grenzen | `122` | Geoinformationen |
| `elevation` | Höhen | `122` | Geoinformationen |
| `location` | Ortsangaben, Referenzsysteme | `122` | Geoinformationen |
| `geoscientificInformation_Geology` | Geologie | `122` | Geoinformationen |
| `society` | Bevölkerung, Gesellschaft, Kultur | `106` | Gesellschaft |
| `inlandWaters` | Gewässer | `113` | Umwelt |
| `environment_EnvironmentalProtection` | Umweltschutz, Lärm | `113` | Umwelt |
| `environment_NatureProtection` | Natur- und Landschaftsschutz | `113` | Umwelt |
| `climatologyMeteorologyAtmosphere` | Atmosphäre, Luft, Klima | `113` | Umwelt |
| `geoscientificInformation_NaturalHazards` | Naturbedingte Risiken | `113` | Umwelt |
| `geoscientificInformation_Soils` | Boden | `113` | Umwelt |
| `farming` | Landwirtschaft | `109` | Landwirtschaft |
| `biota` | Wald, Flora, Fauna | `121` | Tiere |
| `intelligenceMilitary` | Militär, Sicherheit | `111` | Sicherheit |
| `utilitiesCommunication_Utilities` | Wasser- und Abfallsysteme | `110` | Infrastruktur |
| `utilitiesCommunication_Communication` | Kommunikation | `110` | Infrastruktur |
| `utilitiesCommunication_Energy` | Energie | `124` | Energie |
| `health` | Gesundheit | `114` | Gesundheit |
| `economy` | Wirtschaftliche Aktivitäten | `115` | Wirtschaft |
| `structure` | Gebäude, Anlagen | `120` | Gebäude und Grundstücke |
| `planningCadastre_Cadastre` | Grundstückskataster | `120` | Gebäude und Grundstücke |
| `planningCadastre_Planning` | Raumplanung, Raumentwicklung | `102` | Bauen |
| `transportation` | Verkehr | `116` | Mobilität |

### Dataspot personal_data to I14Y confidentialityPerson

Dataspot personal_data value → I14Y confidentialityPerson code. Ordered most-protective first; when a dataset carries multiple values the strongest classification wins.

_Source: `_PERSONAL_DATA_PRIORITY`._

| Priority | Dataspot value | I14Y code |
| --- | --- | --- |
| 1 | `particularly_protected` | `protect_person` |
| 2 | `natural_person` | `person` |
| 3 | `legal_person` | `person` |
| 4 | `none` | `no_person` |
