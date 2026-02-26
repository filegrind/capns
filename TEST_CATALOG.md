# Capns Test Catalog

**Total Tests:** 762

This catalog lists all numbered tests in the capns codebase.

| Test # | Function Name | Description | Location |
|--------|---------------|-------------|----------|
| test001 | `test001_cap_urn_creation` | TEST001: Test that cap URN is created with tags parsed correctly and direction specs accessible | src/urn/cap_urn.rs:711 |
| test002 | `test002_direction_specs_default_to_wildcard` | TEST002: Test that missing 'in' or 'out' defaults to media: wildcard | src/urn/cap_urn.rs:723 |
| test003 | `test003_direction_matching` | TEST003: Test that direction specs must match exactly, different in/out types don't match, wildcard matches any | src/urn/cap_urn.rs:745 |
| test004 | `test004_unquoted_values_lowercased` | TEST004: Test that unquoted keys and values are normalized to lowercase | src/urn/cap_urn.rs:774 |
| test005 | `test005_quoted_values_preserve_case` | TEST005: Test that quoted values preserve case while unquoted are lowercased | src/urn/cap_urn.rs:795 |
| test006 | `test006_quoted_value_special_chars` | TEST006: Test that quoted values can contain special characters (semicolons, equals, spaces) | src/urn/cap_urn.rs:814 |
| test007 | `test007_quoted_value_escape_sequences` | TEST007: Test that escape sequences in quoted values (\" and \\) are parsed correctly | src/urn/cap_urn.rs:833 |
| test008 | `test008_mixed_quoted_unquoted` | TEST008: Test that mixed quoted and unquoted values in same URN parse correctly | src/urn/cap_urn.rs:852 |
| test009 | `test009_unterminated_quote_error` | TEST009: Test that unterminated quote produces UnterminatedQuote error | src/urn/cap_urn.rs:860 |
| test010 | `test010_invalid_escape_sequence_error` | TEST010: Test that invalid escape sequences (like \n, \x) produce InvalidEscapeSequence error | src/urn/cap_urn.rs:870 |
| test011 | `test011_serialization_smart_quoting` | TEST011: Test that serialization uses smart quoting (no quotes for simple lowercase, quotes for special chars/uppercase) | src/urn/cap_urn.rs:887 |
| test012 | `test012_round_trip_simple` | TEST012: Test that simple cap URN round-trips (parse -> serialize -> parse equals original) | src/urn/cap_urn.rs:922 |
| test013 | `test013_round_trip_quoted` | TEST013: Test that quoted values round-trip preserving case and spaces | src/urn/cap_urn.rs:932 |
| test014 | `test014_round_trip_escapes` | TEST014: Test that escape sequences round-trip correctly | src/urn/cap_urn.rs:946 |
| test015 | `test015_cap_prefix_required` | TEST015: Test that cap: prefix is required and case-insensitive | src/urn/cap_urn.rs:960 |
| test016 | `test016_trailing_semicolon_equivalence` | TEST016: Test that trailing semicolon is equivalent (same hash, same string, matches) | src/urn/cap_urn.rs:983 |
| test017 | `test017_tag_matching` | TEST017: Test tag matching: exact match, subset match, wildcard match, value mismatch | src/urn/cap_urn.rs:1016 |
| test018 | `test018_matching_case_sensitive_values` | TEST018: Test that quoted values with different case do NOT match (case-sensitive) | src/urn/cap_urn.rs:1043 |
| test019 | `test019_missing_tag_handling` | TEST019: Missing tag in instance causes rejection — pattern's tags are constraints | src/urn/cap_urn.rs:1057 |
| test020 | `test020_specificity` | TEST020: Test specificity calculation (direction specs use MediaUrn tag count, wildcards don't count) | src/urn/cap_urn.rs:1076 |
| test021 | `test021_builder` | TEST021: Test builder creates cap URN with correct tags and direction specs | src/urn/cap_urn.rs:1096 |
| test022 | `test022_builder_requires_direction` | TEST022: Test builder requires both in_spec and out_spec | src/urn/cap_urn.rs:1113 |
| test023 | `test023_builder_preserves_case` | TEST023: Test builder lowercases keys but preserves value case | src/urn/cap_urn.rs:1138 |
| test024 | `test024_directional_accepts` | TEST024: Directional accepts — pattern's tags are constraints, instance must satisfy | src/urn/cap_urn.rs:1152 |
| test025 | `test025_best_match` | TEST025: Test find_best_match returns most specific matching cap | src/urn/cap_urn.rs:1183 |
| test026 | `test026_merge_and_subset` | TEST026: Test merge combines tags from both caps, subset keeps only specified tags | src/urn/cap_urn.rs:1199 |
| test027 | `test027_wildcard_tag` | TEST027: Test with_wildcard_tag sets tag to wildcard, including in/out | src/urn/cap_urn.rs:1223 |
| test028 | `test028_empty_cap_urn_defaults_to_wildcard` | TEST028: Test empty cap URN defaults to media: wildcard | src/urn/cap_urn.rs:1239 |
| test029 | `test029_minimal_cap_urn` | TEST029: Test minimal valid cap URN has just in and out, empty tags | src/urn/cap_urn.rs:1253 |
| test030 | `test030_extended_character_support` | TEST030: Test extended characters (forward slashes, colons) in tag values | src/urn/cap_urn.rs:1264 |
| test031 | `test031_wildcard_restrictions` | TEST031: Test wildcard rejected in keys but accepted in values | src/urn/cap_urn.rs:1277 |
| test032 | `test032_duplicate_key_rejection` | TEST032: Test duplicate keys are rejected with DuplicateKey error | src/urn/cap_urn.rs:1288 |
| test033 | `test033_numeric_key_restriction` | TEST033: Test pure numeric keys rejected, mixed alphanumeric allowed, numeric values allowed | src/urn/cap_urn.rs:1298 |
| test034 | `test034_empty_value_error` | TEST034: Test empty values are rejected | src/urn/cap_urn.rs:1312 |
| test035 | `test035_has_tag_case_sensitive` | TEST035: Test has_tag is case-sensitive for values, case-insensitive for keys, works for in/out | src/urn/cap_urn.rs:1319 |
| test036 | `test036_with_tag_preserves_value` | TEST036: Test with_tag preserves value case | src/urn/cap_urn.rs:1340 |
| test037 | `test037_with_tag_rejects_empty_value` | TEST037: Test with_tag rejects empty value | src/urn/cap_urn.rs:1348 |
| test038 | `test038_semantic_equivalence` | TEST038: Test semantic equivalence of unquoted and quoted simple lowercase values | src/urn/cap_urn.rs:1356 |
| test039 | `test039_get_tag_returns_direction_specs` | TEST039: Test get_tag returns direction specs (in/out) with case-insensitive lookup | src/urn/cap_urn.rs:1369 |
| test040 | `test040_matching_semantics_test1_exact_match` | TEST040: Matching semantics - exact match succeeds | src/urn/cap_urn.rs:1397 |
| test041 | `test041_matching_semantics_test2_cap_missing_tag` | TEST041: Matching semantics - cap missing tag matches (implicit wildcard) | src/urn/cap_urn.rs:1406 |
| test042 | `test042_matching_semantics_test3_cap_has_extra_tag` | TEST042: Pattern rejects instance missing required tags | src/urn/cap_urn.rs:1418 |
| test043 | `test043_matching_semantics_test4_request_has_wildcard` | TEST043: Matching semantics - request wildcard matches specific cap value | src/urn/cap_urn.rs:1429 |
| test044 | `test044_matching_semantics_test5_cap_has_wildcard` | TEST044: Matching semantics - cap wildcard matches specific request value | src/urn/cap_urn.rs:1441 |
| test045 | `test045_matching_semantics_test6_value_mismatch` | TEST045: Matching semantics - value mismatch does not match | src/urn/cap_urn.rs:1450 |
| test046 | `test046_matching_semantics_test7_fallback_pattern` | TEST046: Matching semantics - fallback pattern (cap missing tag = implicit wildcard) | src/urn/cap_urn.rs:1462 |
| test047 | `test047_matching_semantics_test7b_thumbnail_void_input` | TEST047: Matching semantics - thumbnail fallback with void input | src/urn/cap_urn.rs:1483 |
| test048 | `test048_matching_semantics_test8_wildcard_direction_matches_anything` | TEST048: Matching semantics - wildcard direction matches anything | src/urn/cap_urn.rs:1504 |
| test049 | `test049_matching_semantics_test9_cross_dimension_independence` | TEST049: Non-overlapping tags — neither direction accepts | src/urn/cap_urn.rs:1520 |
| test050 | `test050_matching_semantics_test10_direction_mismatch` | TEST050: Matching semantics - direction mismatch prevents matching | src/urn/cap_urn.rs:1530 |
| test051 | `test051_input_validation_success` | TEST051: Test input validation succeeds with valid positional argument | src/cap/validation.rs:1065 |
| test052 | `test052_input_validation_missing_required` | TEST052: Test input validation fails with MissingRequiredArgument when required arg missing | src/cap/validation.rs:1086 |
| test053 | `test053_input_validation_wrong_type` | TEST053: Test input validation fails with InvalidArgumentType when wrong type provided | src/cap/validation.rs:1114 |
| test054 | `test054_xv5_inline_spec_redefinition_detected` | TEST054: XV5 - Test inline media spec redefinition of existing registry spec is detected and rejected | src/cap/validation.rs:1156 |
| test055 | `test055_xv5_new_inline_spec_allowed` | TEST055: XV5 - Test new inline media spec (not in registry) is allowed | src/cap/validation.rs:1190 |
| test056 | `test056_xv5_empty_media_specs_allowed` | TEST056: XV5 - Test empty media_specs (no inline specs) passes XV5 validation | src/cap/validation.rs:1220 |
| test060 | `test060_wrong_prefix_fails` | TEST060: Test wrong prefix fails with InvalidPrefix error showing expected and actual prefix | src/urn/media_urn.rs:483 |
| test061 | `test061_is_binary` | TEST061: Test is_binary returns true when textable tag is absent (binary = not textable) | src/urn/media_urn.rs:496 |
| test062 | `test062_is_map` | TEST062: Test is_map returns true when form=map tag is present indicating key-value structure | src/urn/media_urn.rs:513 |
| test063 | `test063_is_scalar` | TEST063: Test is_scalar returns true when form=scalar tag is present indicating single value | src/urn/media_urn.rs:525 |
| test064 | `test064_is_list` | TEST064: Test is_list returns true when form=list tag is present indicating ordered collection | src/urn/media_urn.rs:538 |
| test065 | `test065_is_structured` | TEST065: Test is_structured returns true for map or list forms indicating structured data types | src/urn/media_urn.rs:550 |
| test066 | `test066_is_json` | TEST066: Test is_json returns true only when json marker tag is present for JSON representation | src/urn/media_urn.rs:564 |
| test067 | `test067_is_text` | TEST067: Test is_text returns true only when textable marker tag is present | src/urn/media_urn.rs:575 |
| test068 | `test068_is_void` | TEST068: Test is_void returns true when void flag or type=void tag is present | src/urn/media_urn.rs:587 |
| test071 | `test071_to_string_roundtrip` | TEST071: Test to_string roundtrip ensures serialization and deserialization preserve URN structure | src/urn/media_urn.rs:594 |
| test072 | `test072_constants_parse` | TEST072: Test all media URN constants parse successfully as valid media URNs | src/urn/media_urn.rs:604 |
| test073 | `test073_extension_helpers` | TEST073: Test extension helper functions create media URNs with ext tag and correct format | src/urn/media_urn.rs:638 |
| test074 | `test074_media_urn_matching` | TEST074: Test media URN conforms_to using tagged URN semantics with specific and generic requirements | src/urn/media_urn.rs:654 |
| test075 | `test075_matching` | TEST075: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests | src/urn/media_urn.rs:674 |
| test076 | `test076_specificity` | TEST076: Test specificity increases with more tags for ranking conformance | src/urn/media_urn.rs:690 |
| test077 | `test077_serde_roundtrip` | TEST077: Test serde roundtrip serializes to JSON string and deserializes back correctly | src/urn/media_urn.rs:709 |
| test088 | `test088_resolve_from_registry_str` | TEST088: Test resolving string media URN from registry returns correct media type and profile | src/media/spec.rs:636 |
| test089 | `test089_resolve_from_registry_obj` | TEST089: Test resolving object media URN from registry returns JSON media type | src/media/spec.rs:646 |
| test090 | `test090_resolve_from_registry_binary` | TEST090: Test resolving binary media URN returns octet-stream and is_binary true | src/media/spec.rs:654 |
| test091 | `test091_resolve_custom_media_spec` | TEST091: Test resolving custom media URN from local media_specs takes precedence over registry | src/media/spec.rs:678 |
| test092 | `test092_resolve_custom_with_schema` | TEST092: Test resolving custom object form media spec with schema from local media_specs | src/media/spec.rs:707 |
| test093 | `test093_resolve_unresolvable_fails_hard` | TEST093: Test resolving unknown media URN fails with UnresolvableMediaUrn error | src/media/spec.rs:741 |
| test094 | `test094_local_overrides_registry` | TEST094: Test local media_specs definition overrides registry definition for same URN | src/media/spec.rs:755 |
| test095 | `test095_media_spec_def_serialize` | TEST095: Test MediaSpecDef serializes with required fields and skips None fields | src/media/spec.rs:787 |
| test096 | `test096_media_spec_def_deserialize` | TEST096: Test deserializing MediaSpecDef from JSON object | src/media/spec.rs:812 |
| test097 | `test097_validate_no_duplicate_urns_catches_duplicates` | TEST097: Test duplicate URN validation catches duplicates | src/media/spec.rs:827 |
| test098 | `test098_validate_no_duplicate_urns_passes_for_unique` | TEST098: Test duplicate URN validation passes for unique URNs | src/media/spec.rs:843 |
| test099 | `test099_resolved_is_binary` | TEST099: Test ResolvedMediaSpec is_binary returns true when textable tag is absent | src/media/spec.rs:858 |
| test100 | `test100_resolved_is_map` | TEST100: Test ResolvedMediaSpec is_map returns true for form=map media URN | src/media/spec.rs:877 |
| test101 | `test101_resolved_is_scalar` | TEST101: Test ResolvedMediaSpec is_scalar returns true for form=scalar media URN | src/media/spec.rs:897 |
| test102 | `test102_resolved_is_list` | TEST102: Test ResolvedMediaSpec is_list returns true for form=list media URN | src/media/spec.rs:916 |
| test103 | `test103_resolved_is_json` | TEST103: Test ResolvedMediaSpec is_json returns true when json tag is present | src/media/spec.rs:935 |
| test104 | `test104_resolved_is_text` | TEST104: Test ResolvedMediaSpec is_text returns true when textable tag is present | src/media/spec.rs:954 |
| test105 | `test105_metadata_propagation` | TEST105: Test metadata propagates from media spec def to resolved media spec | src/media/spec.rs:977 |
| test106 | `test106_metadata_with_validation` | TEST106: Test metadata and validation can coexist in media spec definition | src/media/spec.rs:1005 |
| test107 | `test107_extensions_propagation` | TEST107: Test extensions field propagates from media spec def to resolved | src/media/spec.rs:1051 |
| test108 | `test108_cap_creation` | TEST108: Test creating new cap with URN, title, and command verifies correct initialization | src/cap/definition.rs:858 |
| test109 | `test109_cap_with_metadata` | TEST109: Test creating cap with metadata initializes and retrieves metadata correctly | src/cap/definition.rs:874 |
| test110 | `test110_cap_matching` | TEST110: Test cap matching with subset semantics for request fulfillment | src/cap/definition.rs:891 |
| test111 | `test111_cap_title` | TEST111: Test getting and setting cap title updates correctly | src/cap/definition.rs:904 |
| test112 | `test112_cap_definition_equality` | TEST112: Test cap equality based on URN and title matching | src/cap/definition.rs:918 |
| test113 | `test113_cap_stdin` | TEST113: Test cap stdin support via args with stdin source and serialization roundtrip | src/cap/definition.rs:933 |
| test114 | `test114_arg_source_types` | TEST114: Test ArgSource type variants stdin, position, and cli_flag with their accessors | src/cap/definition.rs:966 |
| test115 | `test115_cap_arg_serialization` | TEST115: Test CapArg serialization and deserialization with multiple sources | src/cap/definition.rs:991 |
| test116 | `test116_cap_arg_constructors` | TEST116: Test CapArg constructor methods basic and with_description create args correctly | src/cap/definition.rs:1016 |
| test117 | `test117_register_and_find_cap_set` | TEST117: Test registering cap set and finding by exact and subset matching | src/urn/cap_matrix.rs:980 |
| test118 | `test118_best_cap_set_selection` | TEST118: Test selecting best cap set based on specificity ranking | src/urn/cap_matrix.rs:1017 |
| test119 | `test119_invalid_urn_handling` | TEST119: Test invalid URN returns InvalidUrn error | src/urn/cap_matrix.rs:1068 |
| test120 | `test120_accepts_request` | TEST120: Test accepts_request checks if registry can handle a capability request | src/urn/cap_matrix.rs:1078 |
| test121 | `test121_cap_block_more_specific_wins` | TEST121: Test CapBlock selects more specific cap over less specific regardless of registry order | src/urn/cap_matrix.rs:1132 |
| test122 | `test122_cap_block_tie_goes_to_first` | TEST122: Test CapBlock breaks specificity ties by first registered registry | src/urn/cap_matrix.rs:1183 |
| test123 | `test123_cap_block_polls_all` | TEST123: Test CapBlock polls all registries to find most specific match | src/urn/cap_matrix.rs:1212 |
| test124 | `test124_cap_block_no_match` | TEST124: Test CapBlock returns error when no registries match the request | src/urn/cap_matrix.rs:1248 |
| test125 | `test125_cap_block_fallback_scenario` | TEST125: Test CapBlock prefers specific plugin over generic provider fallback | src/urn/cap_matrix.rs:1261 |
| test126 | `test126_composite_can_method` | TEST126: Test composite can method returns CapCaller for capability execution | src/urn/cap_matrix.rs:1321 |
| test127 | `test127_cap_graph_basic_construction` | TEST127: Test CapGraph adds nodes and edges from capability definitions | src/urn/cap_matrix.rs:1356 |
| test128 | `test128_cap_graph_outgoing_incoming` | TEST128: Test CapGraph tracks outgoing and incoming edges for spec conversions | src/urn/cap_matrix.rs:1387 |
| test129 | `test129_cap_graph_can_convert` | TEST129: Test CapGraph detects direct and indirect conversion paths between specs | src/urn/cap_matrix.rs:1436 |
| test130 | `test130_cap_graph_find_path` | TEST130: Test CapGraph finds shortest path for spec conversion chain | src/urn/cap_matrix.rs:1489 |
| test131 | `test131_cap_graph_find_all_paths` | TEST131: Test CapGraph finds all conversion paths sorted by length | src/urn/cap_matrix.rs:1545 |
| test132 | `test132_cap_graph_get_direct_edges_sorted` | TEST132: Test CapGraph returns direct edges sorted by specificity | src/urn/cap_matrix.rs:1603 |
| test133 | `test133_cap_block_graph_integration` | TEST133: Test CapBlock graph integration with multiple registries and conversion paths | src/urn/cap_matrix.rs:1645 |
| test134 | `test134_cap_graph_stats` | TEST134: Test CapGraph stats provides counts of nodes and edges | src/urn/cap_matrix.rs:1730 |
| test135 | `test135_registry_creation` | TEST135: Test registry creation with temporary cache directory succeeds | src/cap/registry.rs:589 |
| test136 | `test136_cache_key_generation` | TEST136: Test cache key generation produces consistent hashes for same URN | src/cap/registry.rs:596 |
| test137 | `test137_parse_registry_json` | TEST137: Test parsing registry JSON without stdin args verifies cap structure | src/cap/registry.rs:614 |
| test138 | `test138_parse_registry_json_with_stdin` | TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction | src/cap/registry.rs:627 |
| test139 | `test139_url_keeps_cap_prefix_literal` | / Test that URL construction keeps "cap:" literal and only encodes the tags part / This guards against the bug where encoding "cap:" as "cap%3A" causes 404s TEST139: Test URL construction keeps cap prefix literal and only encodes tags part | src/cap/registry.rs:646 |
| test140 | `test140_url_encodes_quoted_media_urns` | / Test that media URNs in cap URNs are properly URL-encoded TEST140: Test URL encodes media URNs with proper percent encoding for special characters | src/cap/registry.rs:662 |
| test141 | `test141_exact_url_format` | / Test the URL format for a simple cap URN TEST141: Test exact URL format contains properly encoded media URN components | src/cap/registry.rs:682 |
| test142 | `test142_normalize_handles_different_tag_orders` | / Test that normalization handles various input formats TEST142: Test normalize handles different tag orders producing same canonical form | src/cap/registry.rs:699 |
| test143 | `test143_default_config` | TEST143: Test default config uses capns.org or environment variable values | src/cap/registry.rs:717 |
| test144 | `test144_custom_registry_url` | TEST144: Test custom registry URL updates both registry and schema base URLs | src/cap/registry.rs:729 |
| test145 | `test145_custom_registry_and_schema_url` | TEST145: Test custom registry and schema URLs set independently | src/cap/registry.rs:738 |
| test146 | `test146_schema_url_not_overwritten_when_explicit` | TEST146: Test schema URL not overwritten when set explicitly before registry URL | src/cap/registry.rs:748 |
| test147 | `test147_registry_for_test_with_config` | TEST147: Test registry for test with custom config creates registry with specified URLs | src/cap/registry.rs:759 |
| test148 | `test148_cap_manifest_creation` | TEST148: Test creating cap manifest with name, version, description, and caps | src/bifaci/manifest.rs:102 |
| test149 | `test149_cap_manifest_with_author` | TEST149: Test cap manifest with author field sets author correctly | src/bifaci/manifest.rs:122 |
| test150 | `test150_cap_manifest_json_serialization` | TEST150: Test cap manifest JSON serialization and deserialization roundtrip | src/bifaci/manifest.rs:138 |
| test151 | `test151_cap_manifest_required_fields` | TEST151: Test cap manifest deserialization fails when required fields are missing | src/bifaci/manifest.rs:178 |
| test152 | `test152_cap_manifest_with_multiple_caps` | TEST152: Test cap manifest with multiple caps stores and retrieves all capabilities | src/bifaci/manifest.rs:191 |
| test153 | `test153_cap_manifest_empty_caps` | TEST153: Test cap manifest with empty caps list serializes and deserializes correctly | src/bifaci/manifest.rs:218 |
| test154 | `test154_cap_manifest_optional_author_field` | TEST154: Test cap manifest optional author field skipped in serialization when None | src/bifaci/manifest.rs:236 |
| test155 | `test155_component_metadata_trait` | TEST155: Test ComponentMetadata trait provides manifest and caps accessor methods | src/bifaci/manifest.rs:258 |
| test156 | `test156_stdin_source_data_creation` | TEST156: Test creating StdinSource Data variant with byte vector | src/cap/caller.rs:324 |
| test157 | `test157_stdin_source_file_reference_creation` | TEST157: Test creating StdinSource FileReference variant with all required fields | src/cap/caller.rs:336 |
| test158 | `test158_stdin_source_empty_data` | TEST158: Test StdinSource Data with empty vector stores and retrieves correctly | src/cap/caller.rs:367 |
| test159 | `test159_stdin_source_binary_content` | TEST159: Test StdinSource Data with binary content like PNG header bytes | src/cap/caller.rs:378 |
| test160 | `test160_stdin_source_clone` | TEST160: Test StdinSource Data clone creates independent copy with same data | src/cap/caller.rs:396 |
| test161 | `test161_stdin_source_file_reference_clone` | TEST161: Test StdinSource FileReference clone creates independent copy with same fields | src/cap/caller.rs:409 |
| test162 | `test162_stdin_source_debug` | TEST162: Test StdinSource Debug format displays variant type and relevant fields | src/cap/caller.rs:444 |
| test163 | `test163_argument_schema_validation_success` | TEST163: Test argument schema validation succeeds with valid JSON matching schema | src/cap/schema_validation.rs:233 |
| test164 | `test164_argument_schema_validation_failure` | TEST164: Test argument schema validation fails with JSON missing required fields | src/cap/schema_validation.rs:273 |
| test165 | `test165_output_schema_validation_success` | TEST165: Test output schema validation succeeds with valid JSON matching schema | src/cap/schema_validation.rs:312 |
| test166 | `test166_skip_validation_without_schema` | TEST166: Test validation skipped when resolved media spec has no schema | src/cap/schema_validation.rs:348 |
| test167 | `test167_unresolvable_media_urn_fails_hard` | TEST167: Test validation fails hard when media URN cannot be resolved from any source | src/cap/schema_validation.rs:370 |
| test168 | `test168_json_response` | TEST168: Test ResponseWrapper from JSON deserializes to correct structured type | src/cap/response.rs:253 |
| test169 | `test169_primitive_types` | TEST169: Test ResponseWrapper converts to primitive types integer, float, boolean, string | src/cap/response.rs:267 |
| test170 | `test170_binary_response` | TEST170: Test ResponseWrapper from binary stores and retrieves raw bytes correctly | src/cap/response.rs:287 |
| test171 | `test171_frame_type_roundtrip` | TEST171: Test all FrameType discriminants roundtrip through u8 conversion preserving identity | src/bifaci/frame.rs:895 |
| test172 | `test172_invalid_frame_type` | TEST172: Test FrameType::from_u8 returns None for values outside the valid discriminant range | src/bifaci/frame.rs:918 |
| test173 | `test173_frame_type_discriminant_values` | TEST173: Test FrameType discriminant values match the wire protocol specification exactly | src/bifaci/frame.rs:926 |
| test174 | `test174_message_id_uuid` | TEST174: Test MessageId::new_uuid generates valid UUID that roundtrips through string conversion | src/bifaci/frame.rs:943 |
| test175 | `test175_message_id_uuid_uniqueness` | TEST175: Test two MessageId::new_uuid calls produce distinct IDs (no collisions) | src/bifaci/frame.rs:952 |
| test176 | `test176_message_id_uint_has_no_uuid_string` | TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns None | src/bifaci/frame.rs:960 |
| test177 | `test177_message_id_from_invalid_uuid_str` | TEST177: Test MessageId::from_uuid_str rejects invalid UUID strings | src/bifaci/frame.rs:967 |
| test178 | `test178_message_id_as_bytes` | TEST178: Test MessageId::as_bytes produces correct byte representations for Uuid and Uint variants | src/bifaci/frame.rs:975 |
| test179 | `test179_message_id_default_is_uuid` | TEST179: Test MessageId::default creates a UUID variant (not Uint) | src/bifaci/frame.rs:988 |
| test180 | `test180_hello_frame` | TEST180: Test Frame::hello without manifest produces correct HELLO frame for host side | src/bifaci/frame.rs:995 |
| test181 | `test181_hello_frame_with_manifest` | TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes for plugin side | src/bifaci/frame.rs:1009 |
| test182 | `test182_req_frame` | TEST182: Test Frame::req stores cap URN, payload, and content_type correctly | src/bifaci/frame.rs:1021 |
| test184 | `test184_chunk_frame` | TEST184: Test Frame::chunk stores seq and payload for streaming (with stream_id) | src/bifaci/frame.rs:1037 |
| test185 | `test185_err_frame` | TEST185: Test Frame::err stores error code and message in metadata | src/bifaci/frame.rs:1053 |
| test186 | `test186_log_frame` | TEST186: Test Frame::log stores level and message in metadata | src/bifaci/frame.rs:1063 |
| test187 | `test187_end_frame_with_payload` | TEST187: Test Frame::end with payload sets eof and optional final payload | src/bifaci/frame.rs:1074 |
| test188 | `test188_end_frame_without_payload` | TEST188: Test Frame::end without payload still sets eof marker | src/bifaci/frame.rs:1084 |
| test189 | `test189_chunk_with_offset` | TEST189: Test chunk_with_offset sets offset on all chunks but len only on seq=0 (with stream_id) | src/bifaci/frame.rs:1094 |
| test190 | `test190_heartbeat_frame` | TEST190: Test Frame::heartbeat creates minimal frame with no payload or metadata | src/bifaci/frame.rs:1120 |
| test191 | `test191_error_accessors_on_non_err_frame` | TEST191: Test error_code and error_message return None for non-Err frame types | src/bifaci/frame.rs:1132 |
| test192 | `test192_log_accessors_on_non_log_frame` | TEST192: Test log_level and log_message return None for non-Log frame types | src/bifaci/frame.rs:1143 |
| test193 | `test193_hello_accessors_on_non_hello_frame` | TEST193: Test hello_max_frame and hello_max_chunk return None for non-Hello frame types | src/bifaci/frame.rs:1151 |
| test194 | `test194_frame_new_defaults` | TEST194: Test Frame::new sets version and defaults correctly, optional fields are None | src/bifaci/frame.rs:1160 |
| test195 | `test195_frame_default` | TEST195: Test Frame::default creates a Req frame (the documented default) | src/bifaci/frame.rs:1178 |
| test196 | `test196_is_eof_when_none` | TEST196: Test is_eof returns false when eof field is None (unset) | src/bifaci/frame.rs:1186 |
| test197 | `test197_is_eof_when_false` | TEST197: Test is_eof returns false when eof field is explicitly Some(false) | src/bifaci/frame.rs:1193 |
| test198 | `test198_limits_default` | TEST198: Test Limits::default provides the documented default values | src/bifaci/frame.rs:1201 |
| test199 | `test199_protocol_version_constant` | TEST199: Test PROTOCOL_VERSION is 2 | src/bifaci/frame.rs:1211 |
| test200 | `test200_key_constants` | TEST200: Test integer key constants match the protocol specification | src/bifaci/frame.rs:1217 |
| test201 | `test201_hello_manifest_binary_data` | TEST201: Test hello_with_manifest preserves binary manifest data (not just JSON text) | src/bifaci/frame.rs:1233 |
| test202 | `test202_message_id_equality_and_hash` | TEST202: Test MessageId Eq/Hash semantics: equal UUIDs are equal, different ones are not | src/bifaci/frame.rs:1241 |
| test203 | `test203_message_id_cross_variant_inequality` | TEST203: Test Uuid and Uint variants of MessageId are never equal even for coincidental byte values | src/bifaci/frame.rs:1264 |
| test204 | `test204_req_frame_empty_payload` | TEST204: Test Frame::req with empty payload stores Some(empty vec) not None | src/bifaci/frame.rs:1272 |
| test205 | `test205_encode_decode_roundtrip` | TEST205: Test REQ frame encode/decode roundtrip preserves all fields | src/bifaci/io.rs:1004 |
| test206 | `test206_hello_frame_roundtrip` | TEST206: Test HELLO frame encode/decode roundtrip preserves max_frame, max_chunk, max_reorder_buffer | src/bifaci/io.rs:1021 |
| test207 | `test207_err_frame_roundtrip` | TEST207: Test ERR frame encode/decode roundtrip preserves error code and message | src/bifaci/io.rs:1034 |
| test208 | `test208_log_frame_roundtrip` | TEST208: Test LOG frame encode/decode roundtrip preserves level and message | src/bifaci/io.rs:1047 |
| test210 | `test210_end_frame_roundtrip` | TEST210: Test END frame encode/decode roundtrip preserves eof marker and optional payload | src/bifaci/io.rs:1063 |
| test211 | `test211_hello_with_manifest_roundtrip` | TEST211: Test HELLO with manifest encode/decode roundtrip preserves manifest bytes and limits | src/bifaci/io.rs:1077 |
| test212 | `test212_chunk_with_offset_roundtrip` | TEST212: Test chunk_with_offset encode/decode roundtrip preserves offset, len, eof (with stream_id) | src/bifaci/io.rs:1091 |
| test213 | `test213_heartbeat_roundtrip` | TEST213: Test heartbeat frame encode/decode roundtrip preserves ID with no extra fields | src/bifaci/io.rs:1111 |
| test214 | `test214_frame_io_roundtrip` | TEST214: Test write_frame/read_frame IO roundtrip through length-prefixed wire format | src/bifaci/io.rs:1125 |
| test215 | `test215_multiple_frames` | TEST215: Test reading multiple sequential frames from a single buffer | src/bifaci/io.rs:1150 |
| test216 | `test216_frame_too_large` | TEST216: Test write_frame rejects frames exceeding max_frame limit | src/bifaci/io.rs:1188 |
| test217 | `test217_read_frame_too_large` | TEST217: Test read_frame rejects incoming frames exceeding the negotiated max_frame limit | src/bifaci/io.rs:1206 |
| test218 | `test218_write_chunked` | TEST218: Test write_chunked splits data into chunks respecting max_chunk and reconstructs correctly Chunks from write_chunked have seq=0. SeqAssigner at the output stage assigns final seq. Chunk ordering within a stream is tracked by chunk_index (chunk_index field). | src/bifaci/io.rs:1226 |
| test219 | `test219_write_chunked_empty_data` | TEST219: Test write_chunked with empty data produces a single EOF chunk | src/bifaci/io.rs:1293 |
| test220 | `test220_write_chunked_exact_fit` | TEST220: Test write_chunked with data exactly equal to max_chunk produces exactly one chunk | src/bifaci/io.rs:1311 |
| test221 | `test221_eof_handling` | TEST221: Test read_frame returns Ok(None) on clean EOF (empty stream) | src/bifaci/io.rs:1331 |
| test222 | `test222_truncated_length_prefix` | TEST222: Test read_frame handles truncated length prefix (fewer than 4 bytes available) | src/bifaci/io.rs:1340 |
| test223 | `test223_truncated_frame_body` | TEST223: Test read_frame returns error on truncated frame body (length prefix says more bytes than available) | src/bifaci/io.rs:1356 |
| test224 | `test224_message_id_uint` | TEST224: Test MessageId::Uint roundtrips through encode/decode | src/bifaci/io.rs:1368 |
| test225 | `test225_decode_non_map_value` | TEST225: Test decode_frame rejects non-map CBOR values (e.g., array, integer, string) | src/bifaci/io.rs:1380 |
| test226 | `test226_decode_missing_version` | TEST226: Test decode_frame rejects CBOR map missing required version field | src/bifaci/io.rs:1392 |
| test227 | `test227_decode_invalid_frame_type_value` | TEST227: Test decode_frame rejects CBOR map with invalid frame_type value | src/bifaci/io.rs:1407 |
| test228 | `test228_decode_missing_id` | TEST228: Test decode_frame rejects CBOR map missing required id field | src/bifaci/io.rs:1422 |
| test229 | `test229_frame_reader_writer_set_limits` | TEST229: Test FrameReader/FrameWriter set_limits updates the negotiated limits | src/bifaci/io.rs:1437 |
| test230 | `test230_sync_handshake` | TEST230: Test sync handshake exchanges HELLO frames and negotiates minimum limits | src/bifaci/io.rs:1454 |
| test231 | `test231_handshake_rejects_non_hello` | TEST231: Test handshake fails when peer sends non-HELLO frame | src/bifaci/io.rs:1486 |
| test232 | `test232_handshake_rejects_missing_manifest` | TEST232: Test handshake fails when plugin HELLO is missing required manifest | src/bifaci/io.rs:1513 |
| test233 | `test233_binary_payload_all_byte_values` | TEST233: Test binary payload with all 256 byte values roundtrips through encode/decode | src/bifaci/io.rs:1536 |
| test234 | `test234_decode_garbage_bytes` | TEST234: Test decode_frame handles garbage CBOR bytes gracefully with an error | src/bifaci/io.rs:1553 |
| test235 | `test235_response_chunk` | TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly | src/bifaci/host_runtime.rs:1368 |
| test236 | `test236_response_chunk_with_all_fields` | TEST236: Test ResponseChunk with all fields populated preserves offset, len, and eof | src/bifaci/host_runtime.rs:1384 |
| test237 | `test237_plugin_response_single` | TEST237: Test PluginResponse::Single final_payload returns the single payload slice | src/bifaci/host_runtime.rs:1400 |
| test238 | `test238_plugin_response_single_empty` | TEST238: Test PluginResponse::Single with empty payload returns empty slice and empty vec | src/bifaci/host_runtime.rs:1408 |
| test239 | `test239_plugin_response_streaming` | TEST239: Test PluginResponse::Streaming concatenated joins all chunk payloads in order | src/bifaci/host_runtime.rs:1416 |
| test240 | `test240_plugin_response_streaming_final_payload` | TEST240: Test PluginResponse::Streaming final_payload returns the last chunk's payload | src/bifaci/host_runtime.rs:1427 |
| test241 | `test241_plugin_response_streaming_empty_chunks` | TEST241: Test PluginResponse::Streaming with empty chunks vec returns empty concatenation | src/bifaci/host_runtime.rs:1438 |
| test242 | `test242_plugin_response_streaming_large_payload` | TEST242: Test PluginResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads | src/bifaci/host_runtime.rs:1446 |
| test243 | `test243_async_host_error_display` | TEST243: Test AsyncHostError variants display correct error messages | src/bifaci/host_runtime.rs:1462 |
| test244 | `test244_async_host_error_from_cbor` | TEST244: Test AsyncHostError::from converts CborError to Cbor variant | src/bifaci/host_runtime.rs:1476 |
| test245 | `test245_async_host_error_from_io` | TEST245: Test AsyncHostError::from converts io::Error to Io variant | src/bifaci/host_runtime.rs:1487 |
| test246 | `test246_async_host_error_clone` | TEST246: Test AsyncHostError Clone implementation produces equal values | src/bifaci/host_runtime.rs:1498 |
| test247 | `test247_response_chunk_clone` | TEST247: Test ResponseChunk Clone produces independent copy with same data | src/bifaci/host_runtime.rs:1506 |
| test248 | `test248_register_and_find_handler` | TEST248: Test register_op and find_handler by exact cap URN | src/bifaci/plugin_runtime.rs:3080 |
| test249 | `test249_raw_handler` | TEST249: Test register_op handler echoes bytes directly | src/bifaci/plugin_runtime.rs:3088 |
| test250 | `test250_typed_handler_deserialization` | TEST250: Test Op handler collects input and processes it | src/bifaci/plugin_runtime.rs:3106 |
| test251 | `test251_typed_handler_rejects_invalid_json` | TEST251: Test Op handler propagates errors through RuntimeError::Handler | src/bifaci/plugin_runtime.rs:3149 |
| test252 | `test252_find_handler_unknown_cap` | TEST252: Test find_handler returns None for unregistered cap URNs | src/bifaci/plugin_runtime.rs:3182 |
| test253 | `test253_handler_is_send_sync` | TEST253: Test OpFactory can be cloned via Arc and sent across threads (Send + Sync) | src/bifaci/plugin_runtime.rs:3189 |
| test254 | `test254_no_peer_invoker` | TEST254: Test NoPeerInvoker always returns PeerRequest error | src/bifaci/plugin_runtime.rs:3234 |
| test255 | `test255_no_peer_invoker_with_arguments` | TEST255: Test NoPeerInvoker call_with_bytes also returns error | src/bifaci/plugin_runtime.rs:3248 |
| test256 | `test256_with_manifest_json` | TEST256: Test PluginRuntime::with_manifest_json stores manifest data and parses when valid | src/bifaci/plugin_runtime.rs:3256 |
| test257 | `test257_new_with_invalid_json` | TEST257: Test PluginRuntime::new with invalid JSON still creates runtime (manifest is None) | src/bifaci/plugin_runtime.rs:3273 |
| test258 | `test258_with_manifest_struct` | TEST258: Test PluginRuntime::with_manifest creates runtime with valid manifest data | src/bifaci/plugin_runtime.rs:3281 |
| test259 | `test259_extract_effective_payload_non_cbor` | TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged | src/bifaci/plugin_runtime.rs:3290 |
| test260 | `test260_extract_effective_payload_no_content_type` | TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged | src/bifaci/plugin_runtime.rs:3300 |
| test261 | `test261_extract_effective_payload_cbor_match` | TEST261: Test extract_effective_payload with CBOR content extracts matching argument value | src/bifaci/plugin_runtime.rs:3310 |
| test262 | `test262_extract_effective_payload_cbor_no_match` | TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input | src/bifaci/plugin_runtime.rs:3358 |
| test263 | `test263_extract_effective_payload_invalid_cbor` | TEST263: Test extract_effective_payload with invalid CBOR bytes returns deserialization error | src/bifaci/plugin_runtime.rs:3387 |
| test264 | `test264_extract_effective_payload_cbor_not_array` | TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error | src/bifaci/plugin_runtime.rs:3401 |
| test266 | `test266_cli_frame_sender_construction` | TEST266: Test CliFrameSender wraps CliStreamEmitter correctly (basic construction) | src/bifaci/plugin_runtime.rs:3425 |
| test268 | `test268_runtime_error_display` | TEST268: Test RuntimeError variants display correct messages | src/bifaci/plugin_runtime.rs:3436 |
| test270 | `test270_multiple_handlers` | TEST270: Test registering multiple Op handlers for different caps and finding each independently | src/bifaci/plugin_runtime.rs:3458 |
| test271 | `test271_handler_replacement` | TEST271: Test Op handler replacing an existing registration for the same cap URN | src/bifaci/plugin_runtime.rs:3483 |
| test272 | `test272_extract_effective_payload_multiple_args` | TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one | src/bifaci/plugin_runtime.rs:3530 |
| test273 | `test273_extract_effective_payload_binary_value` | TEST273: Test extract_effective_payload with binary data in CBOR value (not just text) | src/bifaci/plugin_runtime.rs:3603 |
| test274 | `test274_cap_argument_value_new` | TEST274: Test CapArgumentValue::new stores media_urn and raw byte value | src/cap/caller.rs:463 |
| test275 | `test275_cap_argument_value_from_str` | TEST275: Test CapArgumentValue::from_str converts string to UTF-8 bytes | src/cap/caller.rs:471 |
| test276 | `test276_cap_argument_value_as_str_valid` | TEST276: Test CapArgumentValue::value_as_str succeeds for UTF-8 data | src/cap/caller.rs:479 |
| test277 | `test277_cap_argument_value_as_str_invalid_utf8` | TEST277: Test CapArgumentValue::value_as_str fails for non-UTF-8 binary data | src/cap/caller.rs:486 |
| test278 | `test278_cap_argument_value_empty` | TEST278: Test CapArgumentValue::new with empty value stores empty vec | src/cap/caller.rs:493 |
| test279 | `test279_cap_argument_value_clone` | TEST279: Test CapArgumentValue Clone produces independent copy with same data | src/cap/caller.rs:501 |
| test280 | `test280_cap_argument_value_debug` | TEST280: Test CapArgumentValue Debug format includes media_urn and value | src/cap/caller.rs:510 |
| test281 | `test281_cap_argument_value_into_string` | TEST281: Test CapArgumentValue::new accepts Into<String> for media_urn (String and &str) | src/cap/caller.rs:518 |
| test282 | `test282_cap_argument_value_unicode` | TEST282: Test CapArgumentValue::from_str with Unicode string preserves all characters | src/cap/caller.rs:529 |
| test283 | `test283_cap_argument_value_large_binary` | TEST283: Test CapArgumentValue with large binary payload preserves all bytes | src/cap/caller.rs:536 |
| test284 | `test284_handshake_host_plugin` | TEST284: Handshake exchanges HELLO frames, negotiates limits | src/bifaci/integration_tests.rs:795 |
| test285 | `test285_request_response_simple` | TEST285: Simple request-response flow (REQ → END with payload) | src/bifaci/integration_tests.rs:826 |
| test286 | `test286_streaming_chunks` | TEST286: Streaming response with multiple CHUNK frames | src/bifaci/integration_tests.rs:869 |
| test287 | `test287_heartbeat_from_host` | TEST287: Host-initiated heartbeat | src/bifaci/integration_tests.rs:936 |
| test290 | `test290_limits_negotiation` | TEST290: Limit negotiation picks minimum | src/bifaci/integration_tests.rs:976 |
| test291 | `test291_binary_payload_roundtrip` | TEST291: Binary payload roundtrip (all 256 byte values) | src/bifaci/integration_tests.rs:1004 |
| test292 | `test292_message_id_uniqueness` | TEST292: Sequential requests get distinct MessageIds | src/bifaci/integration_tests.rs:1057 |
| test293 | `test293_plugin_runtime_handler_registration` | TEST293: Test PluginRuntime Op registration and lookup by exact and non-existent cap URN | src/bifaci/integration_tests.rs:21 |
| test299 | `test299_empty_payload_roundtrip` | TEST299: Empty payload request/response roundtrip | src/bifaci/integration_tests.rs:1110 |
| test304 | `test304_media_availability_output_constant` | TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags | src/urn/media_urn.rs:748 |
| test305 | `test305_media_path_output_constant` | TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags | src/urn/media_urn.rs:760 |
| test306 | `test306_availability_and_path_output_distinct` | TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs | src/urn/media_urn.rs:771 |
| test307 | `test307_model_availability_urn` | TEST307: Test model_availability_urn builds valid cap URN with correct op and media specs | src/standard/caps.rs:720 |
| test308 | `test308_model_path_urn` | TEST308: Test model_path_urn builds valid cap URN with correct op and media specs | src/standard/caps.rs:729 |
| test309 | `test309_model_availability_and_path_are_distinct` | TEST309: Test model_availability_urn and model_path_urn produce distinct URNs | src/standard/caps.rs:738 |
| test310 | `test310_llm_conversation_urn_unconstrained` | TEST310: Test llm_conversation_urn uses unconstrained tag (not constrained) | src/standard/caps.rs:747 |
| test311 | `test311_llm_conversation_urn_specs` | TEST311: Test llm_conversation_urn in/out specs match the expected media URNs semantically | src/standard/caps.rs:756 |
| test312 | `test312_all_urn_builders_produce_valid_urns` | TEST312: Test all URN builders produce parseable cap URNs | src/standard/caps.rs:774 |
| test320 | `test320_plugin_info_construction` | TEST320-335: PluginRepoServer and PluginRepoClient tests | src/bifaci/plugin_repo.rs:724 |
| test321 | `test321_plugin_info_is_signed` |  | src/bifaci/plugin_repo.rs:757 |
| test322 | `test322_plugin_info_has_binary` |  | src/bifaci/plugin_repo.rs:795 |
| test323 | `test323_plugin_repo_server_validate_registry` |  | src/bifaci/plugin_repo.rs:833 |
| test324 | `test324_plugin_repo_server_transform_to_array` |  | src/bifaci/plugin_repo.rs:857 |
| test325 | `test325_plugin_repo_server_get_plugins` |  | src/bifaci/plugin_repo.rs:912 |
| test326 | `test326_plugin_repo_server_get_plugin_by_id` |  | src/bifaci/plugin_repo.rs:961 |
| test327 | `test327_plugin_repo_server_search_plugins` |  | src/bifaci/plugin_repo.rs:1013 |
| test328 | `test328_plugin_repo_server_get_by_category` |  | src/bifaci/plugin_repo.rs:1065 |
| test329 | `test329_plugin_repo_server_get_by_cap` |  | src/bifaci/plugin_repo.rs:1117 |
| test330 | `test330_plugin_repo_client_update_cache` |  | src/bifaci/plugin_repo.rs:1174 |
| test331 | `test331_plugin_repo_client_get_suggestions` |  | src/bifaci/plugin_repo.rs:1220 |
| test332 | `test332_plugin_repo_client_get_plugin` |  | src/bifaci/plugin_repo.rs:1269 |
| test333 | `test333_plugin_repo_client_get_all_caps` |  | src/bifaci/plugin_repo.rs:1315 |
| test334 | `test334_plugin_repo_client_needs_sync` |  | src/bifaci/plugin_repo.rs:1394 |
| test335 | `test335_plugin_repo_server_client_integration` |  | src/bifaci/plugin_repo.rs:1413 |
| test336 | `test336_file_path_reads_file_passes_bytes` | TEST336: Single file-path arg with stdin source reads file and passes bytes to handler | src/bifaci/plugin_runtime.rs:3649 |
| test337 | `test337_file_path_without_stdin_passes_string` | TEST337: file-path arg without stdin source passes path as string (no conversion) | src/bifaci/plugin_runtime.rs:3714 |
| test338 | `test338_file_path_via_cli_flag` | TEST338: file-path arg reads file via --file CLI flag | src/bifaci/plugin_runtime.rs:3746 |
| test339 | `test339_file_path_array_glob_expansion` | TEST339: file-path-array reads multiple files with glob pattern | src/bifaci/plugin_runtime.rs:3778 |
| test340 | `test340_file_not_found_clear_error` | TEST340: File not found error provides clear message | src/bifaci/plugin_runtime.rs:3821 |
| test341 | `test341_stdin_precedence_over_file_path` | TEST341: stdin takes precedence over file-path in source order | src/bifaci/plugin_runtime.rs:3862 |
| test342 | `test342_file_path_position_zero_reads_first_arg` | TEST342: file-path with position 0 reads first positional arg as file | src/bifaci/plugin_runtime.rs:3900 |
| test343 | `test343_non_file_path_args_unaffected` | TEST343: Non-file-path args are not affected by file reading | src/bifaci/plugin_runtime.rs:3933 |
| test344 | `test344_file_path_array_invalid_json_fails` | TEST344: file-path-array with nonexistent path fails clearly | src/bifaci/plugin_runtime.rs:3964 |
| test345 | `test345_file_path_array_one_file_missing_fails_hard` | TEST345: file-path-array with literal nonexistent path fails hard | src/bifaci/plugin_runtime.rs:4005 |
| test346 | `test346_large_file_reads_successfully` | TEST346: Large file (1MB) reads successfully | src/bifaci/plugin_runtime.rs:4049 |
| test347 | `test347_empty_file_reads_as_empty_bytes` | TEST347: Empty file reads as empty bytes | src/bifaci/plugin_runtime.rs:4085 |
| test348 | `test348_file_path_conversion_respects_source_order` | TEST348: file-path conversion respects source order | src/bifaci/plugin_runtime.rs:4117 |
| test349 | `test349_file_path_multiple_sources_fallback` | TEST349: file-path arg with multiple sources tries all in order | src/bifaci/plugin_runtime.rs:4154 |
| test350 | `test350_full_cli_mode_with_file_path_integration` | TEST350: Integration test - full CLI mode invocation with file-path | src/bifaci/plugin_runtime.rs:4191 |
| test351 | `test351_file_path_array_empty_array` | TEST351: file-path array with empty CBOR array returns empty (CBOR mode) | src/bifaci/plugin_runtime.rs:4255 |
| test352 | `test352_file_permission_denied_clear_error` |  | src/bifaci/plugin_runtime.rs:4305 |
| test353 | `test353_cbor_payload_format_consistency` | TEST353: CBOR payload format matches between CLI and CBOR mode | src/bifaci/plugin_runtime.rs:4373 |
| test354 | `test354_glob_pattern_no_matches_empty_array` | TEST354: Glob pattern with no matches fails hard (NO FALLBACK) | src/bifaci/plugin_runtime.rs:4437 |
| test355 | `test355_glob_pattern_skips_directories` | TEST355: Glob pattern skips directories | src/bifaci/plugin_runtime.rs:4480 |
| test356 | `test356_multiple_glob_patterns_combined` | TEST356: Multiple glob patterns combined | src/bifaci/plugin_runtime.rs:4524 |
| test357 | `test357_symlinks_followed` |  | src/bifaci/plugin_runtime.rs:4608 |
| test358 | `test358_binary_file_non_utf8` | TEST358: Binary file with non-UTF8 data reads correctly | src/bifaci/plugin_runtime.rs:4651 |
| test359 | `test359_invalid_glob_pattern_fails` | TEST359: Invalid glob pattern fails with clear error | src/bifaci/plugin_runtime.rs:4686 |
| test360 | `test360_extract_effective_payload_with_file_data` | TEST360: Extract effective payload handles file-path data correctly | src/bifaci/plugin_runtime.rs:4728 |
| test361 | `test361_cli_mode_file_path` | TEST361: CLI mode with file path - pass file path as command-line argument | src/bifaci/plugin_runtime.rs:4814 |
| test362 | `test362_cli_mode_piped_binary` | TEST362: CLI mode with binary piped in - pipe binary data via stdin  This test simulates real-world conditions: - Pure binary data piped to stdin (NOT CBOR) - CLI mode detected (command arg present) - Cap accepts stdin source - Binary is chunked on-the-fly and accumulated - Handler receives complete CBOR payload | src/bifaci/plugin_runtime.rs:4860 |
| test363 | `test363_cbor_mode_chunked_content` | TEST363: CBOR mode with chunked content - send file content streaming as chunks | src/bifaci/plugin_runtime.rs:4927 |
| test364 | `test364_cbor_mode_file_path` | TEST364: CBOR mode with file path - send file path in CBOR arguments (auto-conversion) | src/bifaci/plugin_runtime.rs:4973 |
| test365 | `test365_stream_start_frame` | TEST365: Frame::stream_start stores request_id, stream_id, and media_urn | src/bifaci/frame.rs:1279 |
| test366 | `test366_stream_end_frame` | TEST366: Frame::stream_end stores request_id and stream_id | src/bifaci/frame.rs:1296 |
| test367 | `test367_stream_start_with_empty_stream_id` | TEST367: StreamStart frame with empty stream_id still constructs (validation happens elsewhere) | src/bifaci/frame.rs:1312 |
| test368 | `test368_stream_start_with_empty_media_urn` | TEST368: StreamStart frame with empty media_urn still constructs (validation happens elsewhere) | src/bifaci/frame.rs:1323 |
| test389 | `test389_stream_start_roundtrip` | TEST389: StreamStart encode/decode roundtrip preserves stream_id and media_urn | src/bifaci/io.rs:1593 |
| test390 | `test390_stream_end_roundtrip` | TEST390: StreamEnd encode/decode roundtrip preserves stream_id, no media_urn | src/bifaci/io.rs:1610 |
| test395 | `test395_build_payload_small` | TEST395: Small payload (< max_chunk) produces correct CBOR arguments | src/bifaci/plugin_runtime.rs:5133 |
| test396 | `test396_build_payload_large` | TEST396: Large payload (> max_chunk) accumulates across chunks correctly | src/bifaci/plugin_runtime.rs:5176 |
| test397 | `test397_build_payload_empty` | TEST397: Empty reader produces valid empty CBOR arguments | src/bifaci/plugin_runtime.rs:5217 |
| test398 | `test398_build_payload_io_error` | TEST398: IO error from reader propagates as RuntimeError::Io | src/bifaci/plugin_runtime.rs:5255 |
| test399 | `test399_relay_notify_discriminant_roundtrip` | TEST399: Verify RelayNotify frame type discriminant roundtrips through u8 (value 10) | src/bifaci/frame.rs:1334 |
| test400 | `test400_relay_state_discriminant_roundtrip` | TEST400: Verify RelayState frame type discriminant roundtrips through u8 (value 11) | src/bifaci/frame.rs:1343 |
| test401 | `test401_relay_notify_frame` | TEST401: Verify relay_notify factory stores manifest and limits, and accessors extract them | src/bifaci/frame.rs:1352 |
| test402 | `test402_relay_state_frame` | TEST402: Verify relay_state factory stores resource payload in frame payload field | src/bifaci/frame.rs:1367 |
| test403 | `test403_invalid_frame_type_past_relay_state` | TEST403: Verify from_u8 returns None for value 12 (one past RelayState) | src/bifaci/frame.rs:1379 |
| test404 | `test404_slave_sends_relay_notify_on_connect` | TEST404: Slave sends RelayNotify on connect (initial_notify parameter) | src/bifaci/relay.rs:452 |
| test405 | `test405_master_reads_relay_notify` | TEST405: Master reads RelayNotify and extracts manifest + limits | src/bifaci/relay.rs:486 |
| test406 | `test406_slave_stores_relay_state` | TEST406: Slave stores RelayState from master | src/bifaci/relay.rs:516 |
| test407 | `test407_protocol_frames_pass_through` | TEST407: Protocol frames pass through slave transparently (both directions) | src/bifaci/relay.rs:559 |
| test408 | `test408_relay_frames_not_forwarded` | TEST408: RelayNotify/RelayState are NOT forwarded through relay | src/bifaci/relay.rs:651 |
| test409 | `test409_slave_injects_relay_notify_midstream` | TEST409: Slave can inject RelayNotify mid-stream (cap change) | src/bifaci/relay.rs:703 |
| test410 | `test410_master_receives_updated_relay_notify` | TEST410: Master receives updated RelayNotify (cap change callback via read_frame) | src/bifaci/relay.rs:776 |
| test411 | `test411_socket_close_detection` | TEST411: Socket close detection (both directions) | src/bifaci/relay.rs:847 |
| test412 | `test412_bidirectional_concurrent_flow` | TEST412: Bidirectional concurrent frame flow through relay | src/bifaci/relay.rs:888 |
| test413 | `test413_register_plugin_adds_to_cap_table` | TEST413: Register plugin adds entries to cap_table | src/bifaci/host_runtime.rs:1518 |
| test414 | `test414_capabilities_empty_initially` | TEST414: capabilities() returns empty JSON initially (no running plugins) | src/bifaci/host_runtime.rs:1536 |
| test415 | `test415_req_for_known_cap_triggers_spawn` | TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary) | src/bifaci/host_runtime.rs:1549 |
| test416 | `test416_attach_plugin_handshake_updates_capabilities` | TEST416: Attach plugin performs HELLO handshake, extracts manifest, updates capabilities | src/bifaci/host_runtime.rs:1604 |
| test417 | `test417_route_req_to_correct_plugin` | TEST417: Route REQ to correct plugin by cap_urn (with two attached plugins) | src/bifaci/host_runtime.rs:1650 |
| test418 | `test418_route_continuation_frames_by_req_id` | TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn) Verifies that after the initial REQ→plugin routing, all subsequent continuation frames with the same req_id are routed to the same plugin — even though no cap_urn is present on those frames. | src/bifaci/host_runtime.rs:2013 |
| test419 | `test419_plugin_heartbeat_handled_locally` | TEST419: Plugin HEARTBEAT handled locally (not forwarded to relay) | src/bifaci/host_runtime.rs:1797 |
| test420 | `test420_plugin_frames_forwarded_to_relay` | TEST420: Plugin non-HELLO/non-HB frames forwarded to relay (pass-through) | src/bifaci/host_runtime.rs:1878 |
| test421 | `test421_plugin_death_updates_capabilities` | TEST421: Plugin death updates capability list (caps removed) | src/bifaci/host_runtime.rs:2155 |
| test422 | `test422_plugin_death_sends_err_for_pending_requests` |  | src/bifaci/host_runtime.rs:2239 |
| test423 | `test423_multiple_plugins_route_independently` | TEST423: Multiple plugins registered with distinct caps route independently | src/bifaci/host_runtime.rs:2324 |
| test424 | `test424_concurrent_requests_to_same_plugin` | TEST424: Concurrent requests to the same plugin are handled independently | src/bifaci/host_runtime.rs:2484 |
| test425 | `test425_find_plugin_for_cap_unknown` | TEST425: find_plugin_for_cap returns None for unregistered cap | src/bifaci/host_runtime.rs:2621 |
| test426 | `test426_single_master_req_response` | TEST426: Single master REQ/response routing | src/bifaci/relay_switch.rs:1385 |
| test427 | `test427_multi_master_cap_routing` | TEST427: Multi-master cap routing | src/bifaci/relay_switch.rs:1432 |
| test428 | `test428_unknown_cap_returns_error` | TEST428: Unknown cap returns error | src/bifaci/relay_switch.rs:1504 |
| test429 | `test429_find_master_for_cap` | TEST429: Cap routing logic (find_master_for_cap) | src/bifaci/relay_switch.rs:1348 |
| test430 | `test430_tie_breaking_same_cap_multiple_masters` | TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent) | src/bifaci/relay_switch.rs:1523 |
| test431 | `test431_continuation_frame_routing` | TEST431: Continuation frame routing (CHUNK, END follow REQ) | src/bifaci/relay_switch.rs:1597 |
| test432 | `test432_empty_masters_allowed` | TEST432: Empty masters list creates empty switch, add_master works | src/bifaci/relay_switch.rs:1644 |
| test433 | `test433_capability_aggregation_deduplicates` | TEST433: Capability aggregation deduplicates caps | src/bifaci/relay_switch.rs:1657 |
| test434 | `test434_limits_negotiation_minimum` | TEST434: Limits negotiation takes minimum | src/bifaci/relay_switch.rs:1692 |
| test435 | `test435_urn_matching_exact_and_accepts` | TEST435: URN matching (exact vs accepts()) | src/bifaci/relay_switch.rs:1722 |
| test436 | `test436_compute_checksum` | TEST436: Verify FNV-1a checksum function produces consistent results | src/bifaci/frame.rs:1386 |
| test437 | `test437_preferred_cap_routes_to_generic` | TEST437: find_master_for_cap with preferred_cap routes to generic handler | src/bifaci/relay_switch.rs:1772 |
| test438 | `test438_preferred_cap_falls_back_when_not_comparable` | TEST438: find_master_for_cap with preference falls back to closest-specificity when preferred cap is not in the comparable set | src/bifaci/relay_switch.rs:1815 |
| test439 | `test439_no_preference_specific_rejects_generic` | TEST439: Without preference, specific request does NOT match generic handler (confirms accepts semantics are unchanged) | src/bifaci/relay_switch.rs:1839 |
| test440 | `test440_chunk_index_checksum_roundtrip` | TEST440: CHUNK frame with chunk_index and checksum roundtrips through encode/decode | src/bifaci/io.rs:1656 |
| test441 | `test441_stream_end_chunk_count_roundtrip` | TEST441: STREAM_END frame with chunk_count roundtrips through encode/decode | src/bifaci/io.rs:1678 |
| test442 | `test442_seq_assigner_monotonic_same_rid` | TEST442: SeqAssigner assigns seq 0,1,2,3 for consecutive frames with same RID | src/bifaci/frame.rs:1446 |
| test443 | `test443_seq_assigner_independent_rids` | TEST443: SeqAssigner maintains independent counters for different RIDs | src/bifaci/frame.rs:1468 |
| test444 | `test444_seq_assigner_skips_non_flow` | TEST444: SeqAssigner skips non-flow frames (Heartbeat, RelayNotify, RelayState, Hello) | src/bifaci/frame.rs:1494 |
| test445 | `test445_seq_assigner_remove_by_flow_key` | TEST445: SeqAssigner.remove with FlowKey(rid, None) resets that flow; FlowKey(rid, Some(xid)) is unaffected | src/bifaci/frame.rs:1515 |
| test446 | `test446_seq_assigner_mixed_types` | TEST446: SeqAssigner handles mixed frame types (REQ, CHUNK, LOG, END) for same RID | src/bifaci/frame.rs:1586 |
| test447 | `test447_flow_key_with_xid` | TEST447: FlowKey::from_frame extracts (rid, Some(xid)) when routing_id present | src/bifaci/frame.rs:1612 |
| test448 | `test448_flow_key_without_xid` | TEST448: FlowKey::from_frame extracts (rid, None) when routing_id absent | src/bifaci/frame.rs:1625 |
| test449 | `test449_flow_key_equality` | TEST449: FlowKey equality: same rid+xid equal, different xid different key | src/bifaci/frame.rs:1636 |
| test450 | `test450_flow_key_hash_lookup` | TEST450: FlowKey hash: same keys hash equal (HashMap lookup) | src/bifaci/frame.rs:1653 |
| test451 | `test451_reorder_buffer_in_order` | TEST451: ReorderBuffer in-order delivery: seq 0,1,2 delivered immediately | src/bifaci/frame.rs:1679 |
| test452 | `test452_reorder_buffer_out_of_order` | TEST452: ReorderBuffer out-of-order: seq 1 then 0 delivers both in order | src/bifaci/frame.rs:1698 |
| test453 | `test453_reorder_buffer_gap_fill` | TEST453: ReorderBuffer gap fill: seq 0,2,1 delivers 0, buffers 2, then delivers 1+2 | src/bifaci/frame.rs:1713 |
| test454 | `test454_reorder_buffer_stale_seq` | TEST454: ReorderBuffer stale seq is hard error | src/bifaci/frame.rs:1731 |
| test455 | `test455_reorder_buffer_overflow` | TEST455: ReorderBuffer overflow triggers protocol error | src/bifaci/frame.rs:1746 |
| test456 | `test456_reorder_buffer_independent_flows` | TEST456: Multiple concurrent flows reorder independently | src/bifaci/frame.rs:1762 |
| test457 | `test457_reorder_buffer_cleanup` | TEST457: cleanup_flow removes state; new frames start at seq 0 | src/bifaci/frame.rs:1785 |
| test458 | `test458_reorder_buffer_non_flow_bypass` | TEST458: Non-flow frames bypass reorder entirely | src/bifaci/frame.rs:1802 |
| test459 | `test459_reorder_buffer_end_frame` | TEST459: Terminal END frame flows through correctly | src/bifaci/frame.rs:1818 |
| test460 | `test460_reorder_buffer_err_frame` | TEST460: Terminal ERR frame flows through correctly | src/bifaci/frame.rs:1836 |
| test461 | `test461_write_chunked_seq_zero` | TEST461: write_chunked produces frames with seq=0; SeqAssigner assigns at output stage | src/bifaci/io.rs:1695 |
| test472 | `test472_handshake_negotiates_reorder_buffer` | TEST472: Handshake negotiates max_reorder_buffer (minimum of both sides) | src/bifaci/io.rs:1734 |
| test473 | `test473_cap_discard_parses_as_valid_urn` | TEST473: CAP_DISCARD parses as valid CapUrn with in=media: and out=media:void | src/standard/caps.rs:792 |
| test474 | `test474_cap_discard_accepts_specific_void_cap` | TEST474: CAP_DISCARD accepts specific-input/void-output caps | src/standard/caps.rs:803 |
| test475 | `test475_validate_passes_with_identity` | TEST475: CapManifest::validate() passes when CAP_IDENTITY is present | src/bifaci/manifest.rs:295 |
| test476 | `test476_validate_fails_without_identity` | TEST476: CapManifest::validate() fails when CAP_IDENTITY is missing | src/bifaci/manifest.rs:309 |
| test478 | `test478_auto_registers_identity_handler` | TEST478: PluginRuntime auto-registers identity and discard handlers on construction | src/bifaci/plugin_runtime.rs:5285 |
| test479 | `test479_custom_identity_overrides_default` | TEST479: Custom identity Op overrides auto-registered default | src/bifaci/plugin_runtime.rs:5304 |
| test480 | `test480_parse_caps_rejects_manifest_without_identity` | TEST480: parse_caps_from_manifest rejects manifest without CAP_IDENTITY | src/bifaci/host_runtime.rs:1350 |
| test481 | `test481_verify_identity_succeeds` | TEST481: verify_identity succeeds with standard identity echo handler | src/bifaci/io.rs:1830 |
| test482 | `test482_verify_identity_fails_on_err` | TEST482: verify_identity fails when plugin returns ERR on identity call | src/bifaci/io.rs:1858 |
| test483 | `test483_verify_identity_fails_on_close` | TEST483: verify_identity fails when connection closes before response | src/bifaci/io.rs:1897 |
| test485 | `test485_attach_plugin_identity_verification_succeeds` | TEST485: attach_plugin completes identity verification with working plugin | src/bifaci/host_runtime.rs:2634 |
| test486 | `test486_attach_plugin_identity_verification_fails` | TEST486: attach_plugin rejects plugin that fails identity verification | src/bifaci/host_runtime.rs:2668 |
| test487 | `test487_relay_switch_identity_verification_succeeds` | TEST487: RelaySwitch construction verifies identity through relay chain | src/bifaci/relay_switch.rs:1868 |
| test488 | `test488_relay_switch_identity_verification_fails` | TEST488: RelaySwitch construction fails when master's identity verification fails | src/bifaci/relay_switch.rs:1887 |
| test489 | `test489_add_master_dynamic` | TEST489: add_master dynamically connects new host to running switch | src/bifaci/relay_switch.rs:2051 |
| test490 | `test490_identity_verification_multiple_plugins` | TEST490: Identity verification with multiple plugins through single relay  Both plugins must pass identity verification independently before any real requests are routed. | src/bifaci/integration_tests.rs:1276 |
| test491 | `test491_chunk_requires_chunk_index_and_checksum` | TEST491: Frame::chunk constructor requires and sets chunk_index and checksum | src/bifaci/frame.rs:1858 |
| test492 | `test492_stream_end_requires_chunk_count` | TEST492: Frame::stream_end constructor requires and sets chunk_count | src/bifaci/frame.rs:1873 |
| test493 | `test493_compute_checksum_fnv1a_test_vectors` | TEST493: compute_checksum produces correct FNV-1a hash for known test vectors | src/bifaci/frame.rs:1885 |
| test494 | `test494_compute_checksum_deterministic` | TEST494: compute_checksum is deterministic | src/bifaci/frame.rs:1894 |
| test495 | `test495_cbor_rejects_chunk_without_chunk_index` | TEST495: CBOR decode REJECTS CHUNK frame missing chunk_index field | src/bifaci/frame.rs:1906 |
| test496 | `test496_cbor_rejects_chunk_without_checksum` | TEST496: CBOR decode REJECTS CHUNK frame missing checksum field | src/bifaci/frame.rs:1932 |
| test497 | `test497_chunk_corrupted_payload_rejected` | TEST497: Verify CHUNK frame with corrupted payload is rejected by checksum | src/bifaci/io.rs:1626 |
| test498 | `test498_routing_id_cbor_roundtrip` | TEST498: routing_id field roundtrips through CBOR encoding | src/bifaci/frame.rs:1979 |
| test499 | `test499_chunk_index_checksum_cbor_roundtrip` | TEST499: chunk_index and checksum roundtrip through CBOR encoding | src/bifaci/frame.rs:1997 |
| test500 | `test500_chunk_count_cbor_roundtrip` | TEST500: chunk_count roundtrips through CBOR encoding | src/bifaci/frame.rs:2016 |
| test501 | `test501_frame_new_initializes_optional_fields_none` | TEST501: Frame::new initializes new fields to None | src/bifaci/frame.rs:2032 |
| test502 | `test502_keys_module_new_field_constants` | TEST502: Keys module has constants for new fields | src/bifaci/frame.rs:2043 |
| test503 | `test503_compute_checksum_empty_data` | TEST503: compute_checksum handles empty data correctly | src/bifaci/frame.rs:2052 |
| test504 | `test504_compute_checksum_large_payload` | TEST504: compute_checksum handles large payloads without overflow | src/bifaci/frame.rs:2059 |
| test505 | `test505_chunk_with_offset_sets_chunk_index` | TEST505: chunk_with_offset sets chunk_index correctly | src/bifaci/frame.rs:2071 |
| test506 | `test506_compute_checksum_different_data_different_hash` | TEST506: Different data produces different checksums | src/bifaci/frame.rs:2095 |
| test507 | `test507_reorder_buffer_xid_isolation` | TEST507: ReorderBuffer isolates flows by XID (routing_id) - same RID different XIDs | src/bifaci/frame.rs:2111 |
| test508 | `test508_reorder_buffer_duplicate_buffered_seq` | TEST508: ReorderBuffer rejects duplicate seq already in buffer | src/bifaci/frame.rs:2139 |
| test509 | `test509_reorder_buffer_large_gap_rejected` | TEST509: ReorderBuffer handles large seq gaps without DOS | src/bifaci/frame.rs:2156 |
| test510 | `test510_reorder_buffer_multiple_gaps` | TEST510: ReorderBuffer with multiple interleaved gaps fills correctly | src/bifaci/frame.rs:2181 |
| test511 | `test511_reorder_buffer_cleanup_with_buffered_frames` | TEST511: ReorderBuffer cleanup with buffered frames discards them | src/bifaci/frame.rs:2214 |
| test512 | `test512_reorder_buffer_burst_delivery` | TEST512: ReorderBuffer delivers burst of consecutive buffered frames | src/bifaci/frame.rs:2237 |
| test513 | `test513_reorder_buffer_mixed_types_same_flow` | TEST513: ReorderBuffer different frame types in same flow maintain order | src/bifaci/frame.rs:2257 |
| test514 | `test514_reorder_buffer_xid_cleanup_isolation` | TEST514: ReorderBuffer with XID cleanup doesn't affect different XID | src/bifaci/frame.rs:2282 |
| test515 | `test515_reorder_buffer_overflow_error_details` | TEST515: ReorderBuffer overflow error includes diagnostic information | src/bifaci/frame.rs:2307 |
| test516 | `test516_reorder_buffer_stale_error_details` | TEST516: ReorderBuffer stale error includes diagnostic information | src/bifaci/frame.rs:2330 |
| test517 | `test517_flow_key_none_vs_some_xid` | TEST517: FlowKey with None XID differs from Some(xid) | src/bifaci/frame.rs:2350 |
| test518 | `test518_reorder_buffer_empty_ready_vec` | TEST518: ReorderBuffer handles zero-length ready vec correctly | src/bifaci/frame.rs:2376 |
| test519 | `test519_reorder_buffer_state_persistence` | TEST519: ReorderBuffer state persists across accept calls | src/bifaci/frame.rs:2388 |
| test520 | `test520_reorder_buffer_per_flow_limit` | TEST520: ReorderBuffer max_buffer_per_flow is per-flow not global | src/bifaci/frame.rs:2406 |
| test521 | `test521_relay_notify_cbor_roundtrip` | TEST521: RelayNotify CBOR roundtrip preserves manifest and limits | src/bifaci/frame.rs:2434 |
| test522 | `test522_relay_state_cbor_roundtrip` | TEST522: RelayState CBOR roundtrip preserves payload | src/bifaci/frame.rs:2460 |
| test523 | `test523_relay_notify_not_flow_frame` | TEST523: is_flow_frame returns false for RelayNotify | src/bifaci/frame.rs:2477 |
| test524 | `test524_relay_state_not_flow_frame` | TEST524: is_flow_frame returns false for RelayState | src/bifaci/frame.rs:2488 |
| test525 | `test525_relay_notify_empty_manifest` | TEST525: RelayNotify with empty manifest is valid | src/bifaci/frame.rs:2498 |
| test526 | `test526_relay_state_empty_payload` | TEST526: RelayState with empty payload is valid | src/bifaci/frame.rs:2509 |
| test527 | `test527_relay_notify_large_manifest` | TEST527: RelayNotify with large manifest roundtrips correctly | src/bifaci/frame.rs:2519 |
| test528 | `test528_relay_frames_use_uint_zero_id` | TEST528: RelayNotify and RelayState use MessageId::Uint(0) | src/bifaci/frame.rs:2546 |
| test529 | `test529_input_stream_iterator_order` | TEST529: InputStream iterator yields chunks in order | src/bifaci/plugin_runtime.rs:5364 |
| test530 | `test530_input_stream_collect_bytes` | TEST530: InputStream::collect_bytes concatenates byte chunks | src/bifaci/plugin_runtime.rs:5381 |
| test531 | `test531_input_stream_collect_bytes_text` | TEST531: InputStream::collect_bytes handles text chunks | src/bifaci/plugin_runtime.rs:5395 |
| test532 | `test532_input_stream_empty` | TEST532: InputStream empty stream produces empty bytes | src/bifaci/plugin_runtime.rs:5408 |
| test533 | `test533_input_stream_error_propagation` | TEST533: InputStream propagates errors | src/bifaci/plugin_runtime.rs:5418 |
| test534 | `test534_input_stream_media_urn` | TEST534: InputStream::media_urn returns correct URN | src/bifaci/plugin_runtime.rs:5437 |
| test535 | `test535_input_package_iteration` | TEST535: InputPackage iterator yields streams | src/bifaci/plugin_runtime.rs:5446 |
| test536 | `test536_input_package_collect_all_bytes` | TEST536: InputPackage::collect_all_bytes aggregates all streams | src/bifaci/plugin_runtime.rs:5479 |
| test537 | `test537_input_package_empty` | TEST537: InputPackage empty package produces empty bytes | src/bifaci/plugin_runtime.rs:5513 |
| test538 | `test538_input_package_error_propagation` | TEST538: InputPackage propagates stream errors | src/bifaci/plugin_runtime.rs:5528 |
| test539 | `test539_output_stream_sends_stream_start` | TEST539: OutputStream sends STREAM_START on first write | src/bifaci/plugin_runtime.rs:5584 |
| test540 | `test540_output_stream_close_sends_stream_end` | TEST540: OutputStream::close sends STREAM_END with correct chunk_count | src/bifaci/plugin_runtime.rs:5606 |
| test541 | `test541_output_stream_chunks_large_data` | TEST541: OutputStream chunks large data correctly | src/bifaci/plugin_runtime.rs:5633 |
| test542 | `test542_output_stream_empty` | TEST542: OutputStream empty stream sends STREAM_START and STREAM_END only | src/bifaci/plugin_runtime.rs:5660 |
| test543 | `test543_peer_call_arg_creates_stream` | TEST543: PeerCall::arg creates OutputStream with correct stream_id | src/bifaci/plugin_runtime.rs:5685 |
| test544 | `test544_peer_call_finish_sends_end` | TEST544: PeerCall::finish sends END frame | src/bifaci/plugin_runtime.rs:5704 |
| test545 | `test545_peer_call_finish_returns_response_stream` | TEST545: PeerCall::finish returns InputStream for response | src/bifaci/plugin_runtime.rs:5730 |
| test546 | `test546_is_image` | TEST546: is_image returns true only when image marker tag is present | src/urn/media_urn.rs:785 |
| test547 | `test547_is_audio` | TEST547: is_audio returns true only when audio marker tag is present | src/urn/media_urn.rs:798 |
| test548 | `test548_is_video` | TEST548: is_video returns true only when video marker tag is present | src/urn/media_urn.rs:810 |
| test549 | `test549_is_numeric` | TEST549: is_numeric returns true only when numeric marker tag is present | src/urn/media_urn.rs:821 |
| test550 | `test550_is_bool` | TEST550: is_bool returns true only when bool marker tag is present | src/urn/media_urn.rs:834 |
| test551 | `test551_is_file_path` | TEST551: is_file_path returns true for scalar file-path, false for array | src/urn/media_urn.rs:847 |
| test552 | `test552_is_file_path_array` | TEST552: is_file_path_array returns true for list file-path, false for scalar | src/urn/media_urn.rs:858 |
| test553 | `test553_is_any_file_path` | TEST553: is_any_file_path returns true for both scalar and array file-path | src/urn/media_urn.rs:868 |
| test554 | `test554_is_collection` | TEST554: is_collection returns true when collection marker tag is present | src/urn/media_urn.rs:878 |
| test555 | `test555_with_tag_and_without_tag` | TEST555: with_tag adds a tag and without_tag removes it | src/urn/media_urn.rs:888 |
| test556 | `test556_image_media_urn_for_ext` | TEST556: image_media_urn_for_ext creates valid image media URN | src/urn/media_urn.rs:905 |
| test557 | `test557_audio_media_urn_for_ext` | TEST557: audio_media_urn_for_ext creates valid audio media URN | src/urn/media_urn.rs:915 |
| test558 | `test558_predicate_constant_consistency` | TEST558: predicates are consistent with constants — every constant triggers exactly the expected predicates | src/urn/media_urn.rs:925 |
| test559 | `test559_without_tag` | TEST559: without_tag removes tag, ignores in/out, case-insensitive for keys | src/urn/cap_urn.rs:1828 |
| test560 | `test560_with_in_out_spec` | TEST560: with_in_spec and with_out_spec change direction specs | src/urn/cap_urn.rs:1853 |
| test561 | `test561_in_out_media_urn` | TEST561: in_media_urn and out_media_urn parse direction specs into MediaUrn | src/urn/cap_urn.rs:1877 |
| test562 | `test562_canonical_option` | TEST562: canonical_option returns None for None input, canonical string for Some | src/urn/cap_urn.rs:1899 |
| test563 | `test563_find_all_matches` | TEST563: CapMatcher::find_all_matches returns all matching caps sorted by specificity | src/urn/cap_urn.rs:1921 |
| test564 | `test564_are_compatible` | TEST564: CapMatcher::are_compatible detects bidirectional overlap | src/urn/cap_urn.rs:1940 |
| test565 | `test565_tags_to_string` | TEST565: tags_to_string returns only tags portion without prefix | src/urn/cap_urn.rs:1964 |
| test566 | `test566_with_tag_ignores_in_out` | TEST566: with_tag silently ignores in/out keys | src/urn/cap_urn.rs:1977 |
| test567 | `test567_str_variants` | TEST567: conforms_to_str and accepts_str work with string arguments | src/urn/cap_urn.rs:1991 |
| test568 | `test568_cap_graph_find_best_path` | TEST568: CapGraph::find_best_path returns highest-specificity path over shortest | src/urn/cap_matrix.rs:1771 |
| test569 | `test569_unregister_cap_set` | TEST569: unregister_cap_set removes a host and returns true, false if not found | src/urn/cap_matrix.rs:1819 |
| test570 | `test570_clear` | TEST570: clear removes all registered sets | src/urn/cap_matrix.rs:1839 |
| test571 | `test571_get_all_capabilities` | TEST571: get_all_capabilities returns caps from all hosts | src/urn/cap_matrix.rs:1856 |
| test572 | `test572_get_capabilities_for_host` | TEST572: get_capabilities_for_host returns caps for specific host, None for unknown | src/urn/cap_matrix.rs:1874 |
| test573 | `test573_iter_hosts_and_caps` | TEST573: iter_hosts_and_caps iterates all hosts with their capabilities | src/urn/cap_matrix.rs:1891 |
| test574 | `test574_cap_block_remove_registry` | TEST574: CapBlock::remove_registry removes by name, returns Arc | src/urn/cap_matrix.rs:1910 |
| test575 | `test575_cap_block_get_registry` | TEST575: CapBlock::get_registry returns Arc clone by name | src/urn/cap_matrix.rs:1931 |
| test576 | `test576_cap_block_get_registry_names` | TEST576: CapBlock::get_registry_names returns names in insertion order | src/urn/cap_matrix.rs:1947 |
| test577 | `test577_cap_graph_input_output_specs` | TEST577: CapGraph::get_input_specs and get_output_specs return correct sets | src/urn/cap_matrix.rs:1962 |
| test578 | `test578_rule1_duplicate_media_urns` | TEST578: RULE1 - duplicate media_urns rejected | src/cap/validation.rs:1246 |
| test579 | `test579_rule2_empty_sources` | TEST579: RULE2 - empty sources rejected | src/cap/validation.rs:1259 |
| test580 | `test580_rule3_different_stdin_urns` | TEST580: RULE3 - multiple stdin sources with different URNs rejected | src/cap/validation.rs:1271 |
| test581 | `test581_rule3_same_stdin_urns_ok` | TEST581: RULE3 - multiple stdin sources with same URN is OK | src/cap/validation.rs:1284 |
| test582 | `test582_rule4_duplicate_source_type` | TEST582: RULE4 - duplicate source type in single arg rejected | src/cap/validation.rs:1295 |
| test583 | `test583_rule5_duplicate_position` | TEST583: RULE5 - duplicate position across args rejected | src/cap/validation.rs:1310 |
| test584 | `test584_rule6_position_gap` | TEST584: RULE6 - position gap rejected (0, 2 without 1) | src/cap/validation.rs:1323 |
| test585 | `test585_rule6_sequential_ok` | TEST585: RULE6 - sequential positions (0, 1, 2) pass | src/cap/validation.rs:1336 |
| test586 | `test586_rule7_position_and_cli_flag` | TEST586: RULE7 - arg with both position and cli_flag rejected | src/cap/validation.rs:1347 |
| test587 | `test587_rule9_duplicate_cli_flag` | TEST587: RULE9 - duplicate cli_flag across args rejected | src/cap/validation.rs:1362 |
| test588 | `test588_rule10_reserved_cli_flags` | TEST588: RULE10 - reserved cli_flags rejected | src/cap/validation.rs:1375 |
| test589 | `test589_all_rules_pass` | TEST589: valid cap args with mixed sources pass all rules | src/cap/validation.rs:1389 |
| test590 | `test590_cli_flag_only_args` | TEST590: validate_cap_args accepts cap with only cli_flag sources (no positions) | src/cap/validation.rs:1405 |
| test591 | `test591_is_more_specific_than` | TEST591: is_more_specific_than returns true when self has more tags for same request | src/cap/definition.rs:1042 |
| test592 | `test592_remove_metadata` | TEST592: remove_metadata adds then removes metadata correctly | src/cap/definition.rs:1078 |
| test593 | `test593_registered_by_lifecycle` | TEST593: registered_by lifecycle — set, get, clear | src/cap/definition.rs:1098 |
| test594 | `test594_metadata_json_lifecycle` | TEST594: metadata_json lifecycle — set, get, clear | src/cap/definition.rs:1119 |
| test595 | `test595_with_args_constructor` | TEST595: with_args constructor stores args correctly | src/cap/definition.rs:1138 |
| test596 | `test596_with_full_definition_constructor` | TEST596: with_full_definition constructor stores all fields | src/cap/definition.rs:1155 |
| test597 | `test597_cap_arg_with_full_definition` | TEST597: CapArg::with_full_definition stores all fields including optional ones | src/cap/definition.rs:1183 |
| test598 | `test598_cap_output_lifecycle` | TEST598: CapOutput lifecycle — set_output, set/clear metadata | src/cap/definition.rs:1211 |
| test599 | `test599_is_empty` | TEST599: is_empty returns true for empty response, false for non-empty | src/cap/response.rs:297 |
| test600 | `test600_size` | TEST600: size returns exact byte count for all content types | src/cap/response.rs:313 |
| test601 | `test601_get_content_type` | TEST601: get_content_type returns correct MIME type for each variant | src/cap/response.rs:329 |
| test602 | `test602_as_type_binary_error` | TEST602: as_type on binary response returns error (cannot deserialize binary) | src/cap/response.rs:342 |
| test603 | `test603_as_bool_edge_cases` | TEST603: as_bool handles all accepted truthy/falsy variants and rejects garbage | src/cap/response.rs:352 |
| test605 | `test605_all_coercion_paths_build_valid_urns` | TEST605: all_coercion_paths each entry builds a valid parseable CapUrn | src/standard/caps.rs:827 |
| test606 | `test606_coercion_urn_specs` | TEST606: coercion_urn in/out specs match the type's media URN constant | src/standard/caps.rs:849 |
| test607 | `test607_media_urns_for_extension_unknown` | TEST607: media_urns_for_extension returns error for unknown extension | src/media/registry.rs:726 |
| test608 | `test608_media_urns_for_extension_populated` | TEST608: media_urns_for_extension returns URNs after adding a spec with extensions | src/media/registry.rs:740 |
| test609 | `test609_get_extension_mappings` | TEST609: get_extension_mappings returns all registered extension->URN pairs | src/media/registry.rs:774 |
| test610 | `test610_get_cached_spec` | TEST610: get_cached_spec returns None for unknown and Some for known | src/media/registry.rs:799 |
| test611 | `test611_is_embedded_profile_comprehensive` | TEST611: is_embedded_profile recognizes all 9 embedded profiles and rejects non-embedded | src/media/profile.rs:650 |
| test612 | `test612_clear_cache` | TEST612: clear_cache empties all in-memory schemas | src/media/profile.rs:671 |
| test613 | `test613_validate_cached` | TEST613: validate_cached validates against cached standard schemas | src/media/profile.rs:688 |
| test614 | `test614_registry_creation` | TEST614: Verify registry creation succeeds and cache directory exists | src/media/registry.rs:672 |
| test615 | `test615_cache_key_generation` | TEST615: Verify cache key generation is deterministic and distinct for different URNs | src/media/registry.rs:679 |
| test616 | `test616_stored_media_spec_to_def` | TEST616: Verify StoredMediaSpec converts to MediaSpecDef preserving all fields | src/media/registry.rs:691 |
| test617 | `test617_normalize_media_urn` | TEST617: Verify normalize_media_urn produces consistent non-empty results | src/media/registry.rs:715 |
| test618 | `test618_registry_creation` | TEST618: Verify profile schema registry creation succeeds with temp cache | src/media/profile.rs:526 |
| test619 | `test619_embedded_schemas_loaded` | TEST619: Verify all 9 embedded standard schemas are loaded on creation | src/media/profile.rs:533 |
| test620 | `test620_string_validation` | TEST620: Verify string schema validates strings and rejects non-strings | src/media/profile.rs:550 |
| test621 | `test621_integer_validation` | TEST621: Verify integer schema validates integers and rejects floats and strings | src/media/profile.rs:562 |
| test622 | `test622_number_validation` | TEST622: Verify number schema validates integers and floats, rejects strings | src/media/profile.rs:577 |
| test623 | `test623_boolean_validation` | TEST623: Verify boolean schema validates true/false and rejects string "true" | src/media/profile.rs:592 |
| test624 | `test624_object_validation` | TEST624: Verify object schema validates objects and rejects arrays | src/media/profile.rs:605 |
| test625 | `test625_string_array_validation` | TEST625: Verify string array schema validates string arrays and rejects mixed arrays | src/media/profile.rs:617 |
| test626 | `test626_unknown_profile_skips_validation` | TEST626: Verify unknown profile URL skips validation and returns Ok | src/media/profile.rs:632 |
| test627 | `test627_is_embedded_profile` | TEST627: Verify is_embedded_profile recognizes standard and rejects custom URLs | src/media/profile.rs:642 |
| test628 | `test628_media_urn_constants_format` | TEST628: Verify media URN constants all start with "media:" prefix | src/standard/media.rs:62 |
| test629 | `test629_profile_constants_format` | TEST629: Verify profile URL constants all start with capns.org schema prefix | src/standard/media.rs:72 |
| test630 | `test630_plugin_repo_creation` | TEST630: Verify PluginRepo creation starts with empty plugin list | src/bifaci/plugin_repo.rs:592 |
| test631 | `test631_needs_sync_empty_cache` | TEST631: Verify needs_sync returns true with empty cache and non-empty URLs | src/bifaci/plugin_repo.rs:599 |
| test632 | `test632_deserialize_cap_summary_with_null_description` | TEST632: Verify PluginCapSummary deserializes null description as empty string | src/bifaci/plugin_repo.rs:607 |
| test633 | `test633_deserialize_cap_summary_with_missing_description` | TEST633: Verify PluginCapSummary deserializes missing description as empty string | src/bifaci/plugin_repo.rs:617 |
| test634 | `test634_deserialize_cap_summary_with_present_description` | TEST634: Verify PluginCapSummary deserializes present description correctly | src/bifaci/plugin_repo.rs:625 |
| test635 | `test635_deserialize_plugin_info_with_null_fields` | TEST635: Verify PluginInfo deserializes null version/description/author as empty strings | src/bifaci/plugin_repo.rs:633 |
| test636 | `test636_deserialize_registry_with_null_descriptions` | TEST636: Verify PluginRegistryResponse deserializes with mixed null/present descriptions | src/bifaci/plugin_repo.rs:656 |
| test637 | `test637_deserialize_full_plugin_with_signature` | TEST637: Verify full PluginInfo deserialization with signature and binary fields | src/bifaci/plugin_repo.rs:678 |
| test638 | `test638_no_peer_router_rejects_all` | TEST638: Verify NoPeerRouter rejects all requests with PeerInvokeNotSupported | src/bifaci/router.rs:95 |
| test639 | `test639_wildcard_001_empty_cap_defaults_to_media_wildcard` | TEST639: cap: (empty) defaults to in=media:;out=media: | src/urn/cap_urn.rs:1640 |
| test640 | `test640_wildcard_002_in_only_defaults_out_to_media` | TEST640: cap:in defaults out to media: | src/urn/cap_urn.rs:1649 |
| test641 | `test641_wildcard_003_out_only_defaults_in_to_media` | TEST641: cap:out defaults in to media: | src/urn/cap_urn.rs:1657 |
| test642 | `test642_wildcard_004_in_out_no_values_become_media` | TEST642: cap:in;out both become media: | src/urn/cap_urn.rs:1665 |
| test643 | `test643_wildcard_005_explicit_asterisk_becomes_media` | TEST643: cap:in=*;out=* becomes media: | src/urn/cap_urn.rs:1673 |
| test644 | `test644_wildcard_006_specific_in_wildcard_out` | TEST644: cap:in=media:;out=* has specific in, wildcard out | src/urn/cap_urn.rs:1681 |
| test645 | `test645_wildcard_007_wildcard_in_specific_out` | TEST645: cap:in=*;out=media:text has wildcard in, specific out | src/urn/cap_urn.rs:1689 |
| test646 | `test646_wildcard_008_invalid_in_spec_fails` | TEST646: cap:in=foo fails (invalid media URN) | src/urn/cap_urn.rs:1697 |
| test647 | `test647_wildcard_009_invalid_out_spec_fails` | TEST647: cap:in=media:;out=bar fails (invalid media URN) | src/urn/cap_urn.rs:1706 |
| test648 | `test648_wildcard_010_wildcard_accepts_specific` | TEST648: Wildcard in/out match specific caps | src/urn/cap_urn.rs:1715 |
| test649 | `test649_wildcard_011_specificity_scoring` | TEST649: Specificity - wildcard has 0, specific has tag count | src/urn/cap_urn.rs:1725 |
| test650 | `test650_wildcard_012_preserve_other_tags` | TEST650: cap:in;out;op=test preserves other tags | src/urn/cap_urn.rs:1735 |
| test651 | `test651_wildcard_013_identity_forms_equivalent` | TEST651: All identity forms produce the same CapUrn | src/urn/cap_urn.rs:1744 |
| test652 | `test652_wildcard_014_cap_identity_constant_works` | TEST652: CAP_IDENTITY constant matches identity caps regardless of string form | src/urn/cap_urn.rs:1769 |
| test653 | `test653_wildcard_015_identity_routing_isolation` | TEST653: Identity (no tags) does not match specific requests via routing | src/urn/cap_urn.rs:1799 |
| test654 | `test654_routes_req_to_handler` | TEST654: InProcessPluginHost routes REQ to matching handler and returns response | src/bifaci/in_process_host.rs:610 |
| test655 | `test655_identity_verification` | TEST655: InProcessPluginHost handles identity verification (echo nonce) | src/bifaci/in_process_host.rs:687 |
| test656 | `test656_no_handler_returns_err` | TEST656: InProcessPluginHost returns NO_HANDLER for unregistered cap | src/bifaci/in_process_host.rs:756 |
| test657 | `test657_manifest_includes_all_caps` | TEST657: InProcessPluginHost manifest includes identity cap and handler caps | src/bifaci/in_process_host.rs:795 |
| test658 | `test658_heartbeat_response` | TEST658: InProcessPluginHost handles heartbeat by echoing same ID | src/bifaci/in_process_host.rs:812 |
| test659 | `test659_handler_error_returns_err_frame` | TEST659: InProcessPluginHost handler error returns ERR frame | src/bifaci/in_process_host.rs:843 |
| test660 | `test660_closest_specificity_routing` | TEST660: InProcessPluginHost closest-specificity routing prefers specific over identity | src/bifaci/in_process_host.rs:913 |
| test661 | `test661_plugin_death_keeps_known_caps_advertised` | TEST661: Plugin death keeps known_caps advertised for on-demand respawn | src/bifaci/host_runtime.rs:2708 |
| test662 | `test662_rebuild_capabilities_includes_non_running_plugins` | TEST662: rebuild_capabilities includes non-running plugins' known_caps | src/bifaci/host_runtime.rs:2739 |
| test663 | `test663_hello_failed_plugin_removed_from_capabilities` | TEST663: Plugin with hello_failed is permanently removed from capabilities | src/bifaci/host_runtime.rs:2772 |
| test664 | `test664_running_plugin_uses_manifest_caps` | TEST664: Running plugin uses manifest caps, not known_caps | src/bifaci/host_runtime.rs:2808 |
| test665 | `test665_cap_table_mixed_running_and_non_running` | TEST665: Cap table uses manifest caps for running, known_caps for non-running | src/bifaci/host_runtime.rs:2862 |
| test666 | `test666_preferred_cap_routing` | TEST666: Preferred cap routing - routes to exact equivalent when multiple masters match | src/bifaci/relay_switch.rs:2202 |
| test667 | `test667_verify_chunk_checksum_detects_corruption` | TEST667: verify_chunk_checksum detects corrupted payload | src/bifaci/frame.rs:2562 |
| test668 | `test668_resolve_slot_with_populated_byte_slot_values` |  | src/planner/argument_binding.rs:718 |
| test669 | `test669_resolve_slot_falls_back_to_default` |  | src/planner/argument_binding.rs:748 |
| test670 | `test670_resolve_required_slot_no_value_returns_err` |  | src/planner/argument_binding.rs:771 |
| test671 | `test671_resolve_optional_slot_no_value_returns_none` |  | src/planner/argument_binding.rs:793 |
| test675 | `test675_build_request_frames_preserves_media_urn_in_stream_start` | TEST675: build_request_frames with full media URN preserves it in STREAM_START frame | src/cap/caller.rs:545 |
| test676 | `test676_build_request_frames_round_trip_find_stream_succeeds` | TEST676: Full round-trip: build_request_frames → extract streams → find_stream succeeds | src/cap/caller.rs:568 |
| test677 | `test677_base_urn_does_not_match_full_urn_in_find_stream` | TEST677: build_request_frames with BASE URN → find_stream with FULL URN FAILS This documents the root cause of the cartridge_client.rs bug: sender used "media:llm-generation-request" (base), receiver looked for "media:llm-generation-request;json;form=map" (full). is_equivalent requires exact tag set match, so base != full. | src/cap/caller.rs:621 |
| test678 | `test678_find_stream_equivalent_urn_different_tag_order` | TEST678: find_stream with exact equivalent URN (same tags, different order) succeeds | src/bifaci/plugin_runtime.rs:5779 |
| test679 | `test679_find_stream_base_urn_does_not_match_full_urn` | TEST679: find_stream with base URN vs full URN fails — is_equivalent is strict This is the root cause of the cartridge_client.rs bug. Sender sent "media:llm-generation-request" but receiver looked for "media:llm-generation-request;json;form=map". | src/bifaci/plugin_runtime.rs:5794 |
| test680 | `test680_require_stream_missing_urn_returns_error` | TEST680: require_stream with missing URN returns hard StreamError | src/bifaci/plugin_runtime.rs:5807 |
| test681 | `test681_find_stream_multiple_streams_returns_correct` | TEST681: find_stream with multiple streams returns the correct one | src/bifaci/plugin_runtime.rs:5823 |
| test682 | `test682_require_stream_str_returns_utf8` | TEST682: require_stream_str returns UTF-8 string for text data | src/bifaci/plugin_runtime.rs:5836 |
| test683 | `test683_find_stream_invalid_urn_returns_none` | TEST683: find_stream returns None for invalid media URN string (not a parse error — just None) | src/bifaci/plugin_runtime.rs:5846 |
| test684 | `test684_from_media_urn_single` | TEST684: Tests InputCardinality correctly identifies single-value media URNs Verifies that URNs without form=list are parsed as Single cardinality | src/planner/cardinality.rs:281 |
| test685 | `test685_from_media_urn_vector` | TEST685: Tests InputCardinality correctly identifies list/vector media URNs Verifies that URNs with form=list tag are parsed as Sequence cardinality | src/planner/cardinality.rs:290 |
| test686 | `test686_from_media_urn_vector_tag_position` | TEST686: Tests that form=list tag position doesn't affect vector detection Verifies cardinality parsing is independent of tag order in URN | src/planner/cardinality.rs:299 |
| test687 | `test687_from_media_urn_no_false_positives` | TEST687: Tests that URN content doesn't cause false positive vector detection Verifies that "sequence" in media type name doesn't trigger Sequence cardinality | src/planner/cardinality.rs:307 |
| test688 | `test688_is_multiple` | TEST688: Tests is_multiple method correctly identifies multi-value cardinalities Verifies Single returns false while Sequence and AtLeastOne return true | src/planner/cardinality.rs:314 |
| test689 | `test689_accepts_single` | TEST689: Tests accepts_single method identifies cardinalities that accept single values Verifies Single and AtLeastOne accept singles while Sequence does not | src/planner/cardinality.rs:323 |
| test690 | `test690_compatibility_single_to_single` | TEST690: Tests cardinality compatibility for single-to-single data flow Verifies Direct compatibility when both input and output are Single | src/planner/cardinality.rs:334 |
| test691 | `test691_compatibility_single_to_vector` | TEST691: Tests cardinality compatibility when wrapping single value into array Verifies WrapInArray compatibility when Sequence expects Single input | src/planner/cardinality.rs:341 |
| test692 | `test692_compatibility_vector_to_single` | TEST692: Tests cardinality compatibility when unwrapping array to singles Verifies RequiresFanOut compatibility when Single expects Sequence input | src/planner/cardinality.rs:348 |
| test693 | `test693_compatibility_vector_to_vector` | TEST693: Tests cardinality compatibility for sequence-to-sequence data flow Verifies Direct compatibility when both input and output are Sequence | src/planner/cardinality.rs:355 |
| test694 | `test694_apply_to_urn_add_vector` | TEST694: Tests applying Sequence cardinality adds form=list tag to URN Verifies that apply_to_urn correctly modifies URN to indicate vector form | src/planner/cardinality.rs:364 |
| test695 | `test695_apply_to_urn_remove_vector` | TEST695: Tests applying Single cardinality removes form=list tag from URN Verifies that apply_to_urn correctly strips vector form indicator | src/planner/cardinality.rs:372 |
| test696 | `test696_apply_to_urn_no_change_needed` | TEST696: Tests apply_to_urn is idempotent when URN already matches cardinality Verifies that URN remains unchanged when form already matches desired cardinality | src/planner/cardinality.rs:380 |
| test697 | `test697_cap_cardinality_info_one_to_one` | TEST697: Tests CapCardinalityInfo correctly identifies one-to-one pattern Verifies Single input and Single output result in OneToOne pattern | src/planner/cardinality.rs:392 |
| test698 | `test698_cap_cardinality_info_one_to_many` | TEST698: Tests CapCardinalityInfo correctly identifies one-to-many pattern Verifies Single input and Sequence output result in OneToMany pattern | src/planner/cardinality.rs:402 |
| test699 | `test699_cap_cardinality_info_many_to_one` | TEST699: Tests CapCardinalityInfo correctly identifies many-to-one pattern Verifies Sequence input and Single output result in ManyToOne pattern | src/planner/cardinality.rs:412 |
| test700 | `test700_filepath_conversion_scalar` | TEST700: File-path conversion with test-edge1 (scalar file input) | testcartridge/tests/integration_tests.rs:18 |
| test701 | `test701_filepath_array_glob` | TEST701: File-path array with glob expansion (test-edge3) | testcartridge/tests/integration_tests.rs:46 |
| test702 | `test702_large_payload_1mb` | TEST702: Large payload auto-chunking (1MB response) | testcartridge/tests/integration_tests.rs:72 |
| test703 | `test703_peer_invoke_chain` |  | testcartridge/tests/integration_tests.rs:96 |
| test704 | `test704_multi_argument` | TEST704: Multi-argument cap (test-edge5) | testcartridge/tests/integration_tests.rs:103 |
| test705 | `test705_piped_stdin` | TEST705: Piped stdin input (no file-path conversion) | testcartridge/tests/integration_tests.rs:132 |
| test706 | `test706_empty_file` | TEST706: Empty file handling | testcartridge/tests/integration_tests.rs:155 |
| test707 | `test707_utf8_file` | TEST707: UTF-8 file handling (textable constraint) | testcartridge/tests/integration_tests.rs:177 |
| test708 | `test708_missing_file` | TEST708: Missing file error handling | testcartridge/tests/integration_tests.rs:200 |
| test709 | `test709_pattern_produces_vector` | TEST709: Tests CardinalityPattern correctly identifies patterns that produce vectors Verifies OneToMany and ManyToMany return true, others return false | src/planner/cardinality.rs:424 |
| test710 | `test710_pattern_requires_vector` | TEST710: Tests CardinalityPattern correctly identifies patterns that require vectors Verifies ManyToOne and ManyToMany return true, others return false | src/planner/cardinality.rs:434 |
| test711 | `test711_chain_analysis_simple_linear` | TEST711: Tests chain analysis for simple linear one-to-one capability chains Verifies chains with no fan-out are valid and require no transformation | src/planner/cardinality.rs:446 |
| test712 | `test712_chain_analysis_with_fan_out` | TEST712: Tests chain analysis detects fan-out points in capability chains Verifies chains with one-to-many transitions are marked for transformation | src/planner/cardinality.rs:460 |
| test713 | `test713_chain_analysis_empty` | TEST713: Tests chain analysis handles empty capability chains correctly Verifies empty chains are valid and require no transformation | src/planner/cardinality.rs:474 |
| test714 | `test714_cardinality_serialization` | TEST714: Tests InputCardinality serializes and deserializes correctly to/from JSON Verifies JSON round-trip preserves cardinality values | src/planner/cardinality.rs:485 |
| test715 | `test715_pattern_serialization` | TEST715: Tests CardinalityPattern serializes and deserializes correctly to/from JSON Verifies JSON round-trip preserves pattern values with snake_case formatting | src/planner/cardinality.rs:496 |
| test716 | `test716_empty_collection` | TEST716: Tests CapInputCollection empty collection has zero files and folders Verifies is_empty() returns true and counts are zero for new collection | src/planner/collection_input.rs:158 |
| test717 | `test717_collection_with_files` | TEST717: Tests CapInputCollection correctly counts files in flat collection Verifies total_file_count() returns 2 for collection with 2 files, no folders | src/planner/collection_input.rs:171 |
| test718 | `test718_nested_collection` | TEST718: Tests CapInputCollection correctly counts files and folders in nested structure Verifies total_file_count() includes subfolder files and total_folder_count() counts subfolders | src/planner/collection_input.rs:195 |
| test719 | `test719_flatten_to_files` | TEST719: Tests CapInputCollection flatten_to_files recursively collects all files Verifies flatten() extracts files from root and all subfolders into flat list | src/planner/collection_input.rs:230 |
| test720 | `test720_serialization_roundtrip` | TEST720: Tests CapInputCollection serializes to JSON and deserializes correctly Verifies JSON round-trip preserves folder_id, folder_name, files and file metadata | src/planner/collection_input.rs:262 |
| test721 | `test721_single_cap_plan` | TEST721: Tests creation of a simple execution plan with a single capability Verifies that single_cap() generates a valid plan with input_slot, cap node, and output node | src/planner/plan.rs:621 |
| test722 | `test722_linear_chain_plan` | TEST722: Tests creation of a linear chain of capabilities connected in sequence Verifies that linear_chain() correctly links multiple caps with proper edges and topological order | src/planner/plan.rs:637 |
| test723 | `test723_empty_plan` | TEST723: Tests creation and validation of an empty execution plan with no nodes Verifies that plans without capabilities are valid and handle zero nodes correctly | src/planner/plan.rs:655 |
| test724 | `test724_plan_with_metadata` | TEST724: Tests storing and retrieving metadata attached to an execution plan Verifies that arbitrary JSON metadata can be associated with a plan for context preservation | src/planner/plan.rs:664 |
| test725 | `test725_validate_invalid_edge` | TEST725: Tests plan validation detects edges pointing to non-existent nodes Verifies that validate() returns an error when an edge references a missing to_node | src/planner/plan.rs:681 |
| test726 | `test726_topological_order_diamond` | TEST726: Tests topological sort correctly orders a diamond-shaped DAG (A→B,C→D) Verifies that nodes with multiple paths respect dependency constraints (A first, D last) | src/planner/plan.rs:698 |
| test727 | `test727_topological_order_detects_cycle` | TEST727: Tests topological sort detects and rejects cyclic dependencies (A→B→C→A) Verifies that circular references produce a "Cycle detected" error | src/planner/plan.rs:724 |
| test728 | `test728_cap_node_helpers` | TEST728: Tests CapNode helper methods for identifying node types (cap, fan-out, fan-in) Verifies is_cap(), is_fan_out(), is_fan_in(), and cap_urn() correctly classify node types | src/planner/plan.rs:745 |
| test729 | `test729_edge_types` | TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, JsonField) Verifies that edge constructors produce correct EdgeType variants | src/planner/plan.rs:767 |
| test730 | `test730_execution_result` | TEST730: Tests CapChainExecutionResult structure for successful execution outcomes Verifies that success status, outputs, and primary_output() accessor work correctly | src/planner/plan.rs:784 |
| test731 | `test731_validate_invalid_from_node` | TEST731: Tests plan validation detects edges originating from non-existent nodes Verifies that validate() returns an error when an edge references a missing from_node | src/planner/plan.rs:803 |
| test732 | `test732_validate_invalid_entry_node` | TEST732: Tests plan validation detects invalid entry node references Verifies that validate() returns an error when entry_nodes contains a non-existent node ID | src/planner/plan.rs:820 |
| test733 | `test733_validate_invalid_output_node` | TEST733: Tests plan validation detects invalid output node references Verifies that validate() returns an error when output_nodes contains a non-existent node ID | src/planner/plan.rs:838 |
| test734 | `test734_topological_order_self_loop` | TEST734: Tests topological sort detects self-referencing cycles (A→A) Verifies that self-loops are recognized as cycles and produce an error | src/planner/plan.rs:856 |
| test735 | `test735_topological_order_multiple_entry_points` | TEST735: Tests topological sort handles graphs with multiple independent starting nodes Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering | src/planner/plan.rs:872 |
| test736 | `test736_topological_order_complex_dag` | TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes Verifies that all dependency constraints are satisfied in a graph with multiple converging paths | src/planner/plan.rs:902 |
| test737 | `test737_linear_chain_single_cap` | TEST737: Tests linear_chain() with exactly one capability Verifies that a single-element chain produces a valid plan with input_slot, cap, and output | src/planner/plan.rs:948 |
| test738 | `test738_linear_chain_empty` | TEST738: Tests linear_chain() with empty capability list Verifies that an empty chain produces a plan with zero nodes and edges | src/planner/plan.rs:963 |
| test739 | `test739_node_execution_result_success` | TEST739: Tests NodeExecutionResult structure for successful node execution Verifies that success status, outputs (binary and text), and error fields work correctly | src/planner/plan.rs:977 |
| test740 | `test740_node_execution_result_failure` | TEST740: Tests NodeExecutionResult structure for failed node execution Verifies that failure status, error message, and absence of outputs are correctly represented | src/planner/plan.rs:995 |
| test741 | `test741_execution_result_failure` | TEST741: Tests CapChainExecutionResult structure for failed chain execution Verifies that failure status, error message, and absence of outputs are correctly represented | src/planner/plan.rs:1013 |
| test742 | `test742_edge_type_serialization` | TEST742: Tests EdgeType enum serialization and deserialization to/from JSON Verifies that edge types like Direct and JsonField correctly round-trip through serde_json | src/planner/plan.rs:1030 |
| test743 | `test743_execution_node_type_serialization` | TEST743: Tests ExecutionNodeType enum serialization and deserialization to/from JSON Verifies that node types like Cap and ForEach correctly serialize with their fields | src/planner/plan.rs:1047 |
| test744 | `test744_plan_serialization` | TEST744: Tests CapExecutionPlan serialization and deserialization to/from JSON Verifies that complete plans with nodes and edges correctly round-trip through JSON | src/planner/plan.rs:1069 |
| test745 | `test745_merge_strategy_serialization` | TEST745: Tests MergeStrategy enum serialization to JSON Verifies that merge strategies like Concat and ZipWith serialize to correct string values | src/planner/plan.rs:1090 |
| test746 | `test746_cap_node_output` | TEST746: Tests creation of Output node type that references a source node Verifies that CapNode::output() correctly constructs an Output node with name and source | src/planner/plan.rs:1103 |
| test747 | `test747_cap_node_merge` | TEST747: Tests creation and validation of Merge node that combines multiple inputs Verifies that Merge nodes with multiple input nodes and a strategy can be added to plans | src/planner/plan.rs:1117 |
| test748 | `test748_cap_node_split` | TEST748: Tests creation of Split node that distributes input to multiple outputs Verifies that Split nodes correctly specify an input node and output count | src/planner/plan.rs:1142 |
| test749 | `test749_get_node` | TEST749: Tests get_node() method for looking up nodes by ID in a plan Verifies that existing nodes are found and non-existent nodes return None | src/planner/plan.rs:1164 |
| test750 | `test750_no_duplicates_with_unique_caps` | TEST750: Tests duplicate detection passes for caps with unique URN combinations Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations | src/planner/plan_builder.rs:1332 |
| test751 | `test751_detects_duplicate_cap_urns` | TEST751: Tests duplicate detection identifies caps with identical URNs Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn | src/planner/plan_builder.rs:1347 |
| test752 | `test752_different_ops_same_types_not_duplicates` | TEST752: Tests caps with different operations but same input/output types are not duplicates Verifies that only the complete URN (including op) is used for duplicate detection | src/planner/plan_builder.rs:1364 |
| test753 | `test753_same_op_different_input_types_not_duplicates` | TEST753: Tests caps with same operation but different input types are not duplicates Verifies that input type differences distinguish caps with the same operation name | src/planner/plan_builder.rs:1378 |
| test754 | `test754_input_arg_first_cap_auto_resolved_from_input` | TEST754: Tests first cap's input argument is automatically resolved from input file Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain | src/planner/plan_builder.rs:1417 |
| test755 | `test755_input_arg_subsequent_cap_auto_resolved_from_previous` | TEST755: Tests subsequent caps' input arguments are automatically resolved from previous output Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first | src/planner/plan_builder.rs:1428 |
| test756 | `test756_output_arg_auto_resolved` | TEST756: Tests output arguments are automatically resolved from previous cap's output Verifies that arguments matching the output spec are always resolved as FromPreviousOutput | src/planner/plan_builder.rs:1443 |
| test757 | `test757_file_path_type_fallback_first_cap` | TEST757: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap Verifies that generic file-path arguments are bound to input file in the first cap | src/planner/plan_builder.rs:1454 |
| test758 | `test758_file_path_type_fallback_subsequent_cap` | TEST758: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps Verifies that generic file-path arguments are bound to previous cap's output after the first cap | src/planner/plan_builder.rs:1465 |
| test759 | `test759_file_path_array_fallback` | TEST759: Tests MEDIA_FILE_PATH_ARRAY argument type resolution for first and subsequent caps Verifies that file-path array arguments follow the same resolution pattern as single file paths | src/planner/plan_builder.rs:1476 |
| test760 | `test760_non_io_arg_with_default_has_default` | TEST760: Tests required non-IO arguments with default values are marked as HasDefault Verifies that arguments like integers with defaults don't require user input | src/planner/plan_builder.rs:1490 |
| test761 | `test761_non_io_arg_without_default_requires_user_input` | TEST761: Tests required non-IO arguments without defaults require user input Verifies that arguments like strings without defaults are marked as RequiresUserInput | src/planner/plan_builder.rs:1502 |
| test762 | `test762_optional_non_io_arg_with_default_has_default` | TEST762: Tests optional non-IO arguments with default values are marked as HasDefault Verifies that optional arguments with defaults behave the same as required ones with defaults | src/planner/plan_builder.rs:1513 |
| test763 | `test763_optional_non_io_arg_without_default_requires_user_input` | TEST763: Tests optional non-IO arguments without defaults still require user input Verifies that optional arguments without defaults must be explicitly provided or skipped | src/planner/plan_builder.rs:1525 |
| test764 | `test764_validation_to_json_none` | TEST764: Tests validation_to_json() returns None for None input Verifies that missing validation metadata is converted to JSON None | src/planner/plan_builder.rs:1536 |
| test765 | `test765_validation_to_json_empty` | TEST765: Tests validation_to_json() returns None for empty validation constraints Verifies that default MediaValidation with no constraints produces JSON None | src/planner/plan_builder.rs:1544 |
| test766 | `test766_validation_to_json_with_constraints` | TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON Verifies that min/max validation rules are correctly serialized as JSON fields | src/planner/plan_builder.rs:1553 |
| test767 | `test767_argument_info_serialization` | TEST767: Tests ArgumentInfo struct serialization to JSON Verifies that argument metadata including resolution status and validation is correctly serialized | src/planner/plan_builder.rs:1572 |
| test768 | `test768_path_argument_requirements_structure` | TEST768: Tests PathArgumentRequirements structure for single-step execution paths Verifies that argument requirements are correctly organized by step with resolution information | src/planner/plan_builder.rs:1592 |
| test769 | `test769_path_with_required_slot` | TEST769: Tests PathArgumentRequirements tracking of required user-input slots Verifies that arguments requiring user input are collected in slots and can_execute_without_input is false | src/planner/plan_builder.rs:1628 |
| test770 | `test770_is_cap_available_with_filter` | TEST770: Tests is_cap_available() correctly applies availability filter when set Verifies that only caps in the available_caps set are considered available | src/planner/plan_builder.rs:1701 |
| test771 | `test771_is_cap_available_without_filter` | TEST771: Tests is_cap_available() treats all caps as available when no filter is set Verifies that without an availability filter, any cap URN is considered available | src/planner/plan_builder.rs:1714 |
| test772 | `test772_find_all_paths_filters_by_availability` | TEST772: Tests find_all_paths() excludes unavailable caps from pathfinding Verifies that only paths using available caps are returned when filter is set | src/planner/plan_builder.rs:1724 |
| test773 | `test773_find_all_paths_returns_empty_when_no_available_caps` | TEST773: Tests find_all_paths() returns empty result when all caps are filtered out Verifies that pathfinding returns no paths when the availability filter excludes all relevant caps | src/planner/plan_builder.rs:1750 |
| test774 | `test774_get_reachable_targets_filters_by_availability` | TEST774: Tests get_reachable_targets() only considers available caps for reachability Verifies that target specs are only reachable via caps in the availability filter | src/planner/plan_builder.rs:1769 |
| test775 | `test775_find_path_filters_by_availability` | TEST775: Tests find_path() selects from available caps when multiple paths exist Verifies that find_path() respects availability filter and prefers available direct paths | src/planner/plan_builder.rs:1795 |
| test776 | `test776_find_path_returns_error_when_path_unavailable` | TEST776: Tests find_path() returns error when required caps are filtered out by availability Verifies that "No path found" error is returned when filter blocks the only viable path | src/planner/plan_builder.rs:1818 |
| test777 | `test777_type_mismatch_pdf_cap_does_not_match_png_input` | TEST777: Tests type checking prevents using PDF-specific cap with PNG input Verifies that media type compatibility is enforced during pathfinding (PNG cannot use PDF cap) | src/planner/plan_builder.rs:1845 |
| test778 | `test778_type_mismatch_png_cap_does_not_match_pdf_input` | TEST778: Tests type checking prevents using PNG-specific cap with PDF input Verifies that media type compatibility is enforced during pathfinding (PDF cannot use PNG cap) | src/planner/plan_builder.rs:1865 |
| test779 | `test779_get_reachable_targets_respects_type_matching` | TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps Verifies that PNG and PDF inputs reach different targets based on cap input type requirements | src/planner/plan_builder.rs:1885 |
| test780 | `test780_reachable_targets_with_metadata_respects_type_matching` | TEST780: Tests get_reachable_targets_with_metadata() respects type compatibility constraints Verifies that reachable target metadata only includes type-compatible transformations | src/planner/plan_builder.rs:1913 |
| test781 | `test781_find_all_paths_respects_type_chain` | TEST781: Tests find_all_paths() enforces type compatibility across multi-step chains Verifies that paths are only found when all intermediate types are compatible | src/planner/plan_builder.rs:1939 |
| test782 | `test782_coherence_score_zero_for_direct_path` | TEST782: Tests coherence scoring gives 0 deviations for direct single-step paths Verifies that paths going directly from source to target without detours have perfect coherence | src/planner/plan_builder.rs:1968 |
| test783 | `test783_coherence_score_penalizes_unrelated_intermediate` | TEST783: Tests coherence scoring penalizes paths through semantically unrelated intermediates Verifies that going from textable→thumbnail→textable incurs deviation penalty (thumbnail unrelated) | src/planner/plan_builder.rs:1994 |
| test784 | `test784_coherence_score_related_intermediate_not_penalized` | TEST784: Tests coherence scoring does not penalize paths through semantically related intermediates Verifies that going through a supertype (txt→textable→md) maintains coherence with 0 deviations | src/planner/plan_builder.rs:2029 |
| test785 | `test785_find_all_paths_filters_deviating_when_coherent_exists` | TEST785: Tests find_all_paths() filters out deviating paths when coherent alternatives exist Verifies that semantically wandering paths are excluded if direct coherent paths are available | src/planner/plan_builder.rs:2063 |
| test786 | `test786_find_all_paths_keeps_all_when_all_deviate` | TEST786: Tests find_all_paths() keeps all paths when no coherent path exists Verifies that all deviating paths are returned if they're the only viable options | src/planner/plan_builder.rs:2089 |
| test787 | `test787_find_all_paths_coherent_sorting_prefers_shorter` | TEST787: Tests find_all_paths() sorts coherent paths by length, preferring shorter ones Verifies that among multiple coherent paths, the shortest is ranked first | src/planner/plan_builder.rs:2121 |
| test788 | `test788_cap_input_file_new` | TEST788: Tests CapInputFile constructor creates file with correct path and media URN Verifies new() initializes file_path, media_urn and leaves metadata/source_id as None | src/planner/argument_binding.rs:511 |
| test789 | `test789_cap_input_file_from_listing` | TEST789: Tests CapInputFile from_listing sets source metadata correctly Verifies from_listing() populates source_id and source_type as Listing | src/planner/argument_binding.rs:522 |
| test790 | `test790_cap_input_file_filename` | TEST790: Tests CapInputFile extracts filename from full path correctly Verifies filename() returns just the basename without directory path | src/planner/argument_binding.rs:531 |
| test791 | `test791_argument_binding_literal_string` | TEST791: Tests ArgumentBinding literal_string creates Literal variant with string value Verifies literal_string() wraps string in JSON Value::String | src/planner/argument_binding.rs:539 |
| test792 | `test792_argument_binding_requires_input` | TEST792: Tests ArgumentBinding requires_input distinguishes Slots from Literals Verifies Slot returns true (needs user input) while Literal returns false | src/planner/argument_binding.rs:551 |
| test793 | `test793_argument_binding_serialization` | TEST793: Tests ArgumentBinding PreviousOutput serializes/deserializes correctly Verifies JSON round-trip preserves node_id and output_field values | src/planner/argument_binding.rs:561 |
| test794 | `test794_argument_bindings_add_file_path` | TEST794: Tests ArgumentBindings add_file_path adds InputFilePath binding Verifies add_file_path() creates binding map entry with InputFilePath variant | src/planner/argument_binding.rs:581 |
| test795 | `test795_argument_bindings_unresolved_slots` | TEST795: Tests ArgumentBindings identifies unresolved Slot bindings Verifies has_unresolved_slots() and get_unresolved_slots() detect Slots needing values | src/planner/argument_binding.rs:591 |
| test796 | `test796_resolve_input_file_path` | TEST796: Tests resolve_binding resolves InputFilePath to current file path Verifies InputFilePath binding resolves to file path bytes with InputFile source | src/planner/argument_binding.rs:602 |
| test797 | `test797_resolve_literal` | TEST797: Tests resolve_binding resolves Literal to JSON-encoded bytes Verifies Literal binding serializes value to bytes with Literal source | src/planner/argument_binding.rs:622 |
| test798 | `test798_resolve_previous_output` | TEST798: Tests resolve_binding extracts value from previous node output Verifies PreviousOutput binding fetches field from earlier execution results | src/planner/argument_binding.rs:642 |
| test799 | `test799_cap_chain_input_single` | TEST799: Tests CapChainInput single constructor creates valid Single cardinality input Verifies single() wraps one file with Single cardinality and validates correctly | src/planner/argument_binding.rs:666 |
| test800 | `test800_cap_chain_input_vector` | TEST800: Tests CapChainInput sequence constructor creates valid Sequence cardinality input Verifies sequence() wraps multiple files with Sequence cardinality | src/planner/argument_binding.rs:677 |
| test801 | `test801_cap_input_file_deserialization_from_dry_context` | TEST801: Tests CapInputFile deserializes from JSON with source metadata fields Verifies JSON with source_id and source_type deserializes to CapInputFile correctly | src/planner/argument_binding.rs:691 |
| test802 | `test802_cap_input_file_deserialization_via_value` | TEST802: Tests CapInputFile deserializes from compact JSON via serde_json::Value Verifies deserialization through Value intermediate works correctly | src/planner/argument_binding.rs:710 |
| test803 | `test803_cap_chain_input_invalid_single` | TEST803: Tests CapChainInput validation detects mismatched Single cardinality with multiple files Verifies is_valid() returns false when Single cardinality has more than one file | src/planner/argument_binding.rs:815 |
| test804 | `test804_extract_json_path_simple` | TEST804: Tests basic JSON path extraction with dot notation for nested objects Verifies that simple paths like "data.message" correctly extract values from nested JSON structures | src/planner/executor.rs:536 |
| test805 | `test805_extract_json_path_with_array` | TEST805: Tests JSON path extraction with array indexing syntax Verifies that bracket notation like "items[0].name" correctly accesses array elements and their nested fields | src/planner/executor.rs:550 |
| test806 | `test806_extract_json_path_missing_field` | TEST806: Tests error handling when JSON path references non-existent fields Verifies that accessing missing fields returns an appropriate error message | src/planner/executor.rs:565 |
| test807 | `test807_apply_edge_type_direct` | TEST807: Tests EdgeType::Direct passes JSON values through unchanged Verifies that Direct edge type acts as a transparent passthrough without transformation | src/planner/executor.rs:576 |
| test808 | `test808_apply_edge_type_json_field` | TEST808: Tests EdgeType::JsonField extracts specific top-level fields from JSON objects Verifies that JsonField edge type correctly isolates a single named field from the source output | src/planner/executor.rs:586 |
| test809 | `test809_apply_edge_type_json_field_missing` | TEST809: Tests EdgeType::JsonField error handling for missing fields Verifies that attempting to extract a non-existent field returns an error | src/planner/executor.rs:596 |
| test810 | `test810_apply_edge_type_json_path` | TEST810: Tests EdgeType::JsonPath extracts values using nested path expressions Verifies that JsonPath edge type correctly navigates through multiple levels like "data.nested.value" | src/planner/executor.rs:605 |
| test811 | `test811_apply_edge_type_iteration` | TEST811: Tests EdgeType::Iteration preserves array values for iterative processing Verifies that Iteration edge type passes through arrays unchanged to enable ForEach patterns | src/planner/executor.rs:615 |
| test812 | `test812_apply_edge_type_collection` | TEST812: Tests EdgeType::Collection preserves collected values without transformation Verifies that Collection edge type maintains structure for aggregation patterns | src/planner/executor.rs:625 |
| test813 | `test813_extract_json_path_deeply_nested` | TEST813: Tests JSON path extraction through deeply nested object hierarchies (4+ levels) Verifies that paths can traverse multiple nested levels like "level1.level2.level3.level4.value" | src/planner/executor.rs:635 |
| test814 | `test814_extract_json_path_array_out_of_bounds` | TEST814: Tests error handling when array index exceeds available elements Verifies that out-of-bounds array access returns a descriptive error message | src/planner/executor.rs:655 |
| test815 | `test815_extract_json_path_single_segment` | TEST815: Tests JSON path extraction with single-level paths (no nesting) Verifies that simple field names without dots correctly extract top-level values | src/planner/executor.rs:668 |
| test816 | `test816_extract_json_path_with_special_characters` | TEST816: Tests JSON path extraction preserves special characters in string values Verifies that quotes, backslashes, and other special characters are correctly maintained | src/planner/executor.rs:678 |
| test817 | `test817_extract_json_path_with_null_value` | TEST817: Tests JSON path extraction correctly handles explicit null values Verifies that null is returned as serde_json::Value::Null rather than an error | src/planner/executor.rs:692 |
| test818 | `test818_extract_json_path_with_empty_array` | TEST818: Tests JSON path extraction correctly returns empty arrays Verifies that zero-length arrays are extracted as valid empty array values | src/planner/executor.rs:702 |
| test819 | `test819_extract_json_path_with_numeric_types` | TEST819: Tests JSON path extraction handles various numeric types correctly Verifies extraction of integers, floats, negative numbers, and zero | src/planner/executor.rs:712 |
| test820 | `test820_extract_json_path_with_boolean` | TEST820: Tests JSON path extraction correctly handles boolean values Verifies that true and false are extracted as proper boolean JSON values | src/planner/executor.rs:728 |
| test821 | `test821_extract_json_path_with_nested_arrays` | TEST821: Tests JSON path extraction with multi-dimensional arrays (matrix access) Verifies that nested array structures like "matrix[1]" correctly extract inner arrays | src/planner/executor.rs:742 |
| test822 | `test822_extract_json_path_invalid_array_index` | TEST822: Tests error handling for non-numeric array indices Verifies that invalid indices like "items[abc]" return a descriptive parse error | src/planner/executor.rs:757 |
| test890 | `test890_direction_semantic_matching` | TEST890: Semantic direction matching - generic provider matches specific request | src/urn/cap_urn.rs:1552 |
| test891 | `test891_direction_semantic_specificity` | TEST891: Semantic direction specificity - more media URN tags = higher specificity | src/urn/cap_urn.rs:1609 |
| test892 | `test892_extensions_serialization` | TEST892: Test extensions serializes/deserializes correctly in MediaSpecDef | src/media/spec.rs:1073 |
| test893 | `test893_extensions_with_metadata_and_validation` | TEST893: Test extensions can coexist with metadata and validation | src/media/spec.rs:1095 |
| test894 | `test894_multiple_extensions` | TEST894: Test multiple extensions in a media spec | src/media/spec.rs:1130 |
| test895 | `test895_cbor_array_file_paths_in_cbor_mode` | TEST895: CBOR Array of file-paths in CBOR mode (validates new Array support) | src/bifaci/plugin_runtime.rs:5045 |
| test896 | `test896_full_path_engine_req_to_plugin_response` | TEST896: Full path: engine REQ → runtime → plugin → response back through relay | src/bifaci/integration_tests.rs:165 |
| test897 | `test897_plugin_error_flows_to_engine` | TEST897: Plugin ERR frame flows back to engine through relay | src/bifaci/integration_tests.rs:274 |
| test898 | `test898_binary_integrity_through_relay` | TEST898: Binary data integrity through full relay path (256 byte values) | src/bifaci/integration_tests.rs:346 |
| test899 | `test899_streaming_chunks_through_relay` | TEST899: Streaming chunks flow through relay without accumulation | src/bifaci/integration_tests.rs:460 |
| test900 | `test900_two_plugins_routed_independently` | TEST900: Two plugins routed independently by cap_urn | src/bifaci/integration_tests.rs:556 |
| test901 | `test901_req_for_unknown_cap_returns_err_frame` | TEST901: REQ for unknown cap returns ERR frame (not fatal) | src/bifaci/integration_tests.rs:689 |
| test902 | `test902_compute_checksum_empty` | TEST902: Verify FNV-1a checksum handles empty data | src/bifaci/frame.rs:1402 |
| test903 | `test903_chunk_with_chunk_index_and_checksum` | TEST903: Verify CHUNK frame can store chunk_index and checksum fields | src/bifaci/frame.rs:1410 |
| test904 | `test904_stream_end_with_chunk_count` | TEST904: Verify STREAM_END frame can store chunk_count field | src/bifaci/frame.rs:1428 |
| test905 | `test905_send_to_master_build_request_frames_roundtrip` | TEST905: send_to_master + build_request_frames through RelaySwitch → RelaySlave → InProcessPluginHost roundtrip | src/bifaci/relay_switch.rs:1916 |
| test906 | `test906_full_path_identity_verification` | TEST489: Full path identity verification: engine → host (attach_plugin) → plugin  This verifies that attach_plugin completes identity verification end-to-end and the plugin is ready to handle subsequent requests. | src/bifaci/integration_tests.rs:1158 |
| test907 | `test907_cbor_rejects_stream_end_without_chunk_count` | TEST907: CBOR decode REJECTS STREAM_END frame missing chunk_count field | src/bifaci/frame.rs:1957 |

---

*Generated from capns source tree*
*Total numbered tests: 762*

