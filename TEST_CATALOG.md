# Rust Test Catalog

**Total Tests:** 1061

**Numbered Tests:** 1051

**Unnumbered Tests:** 10

**Numbered Tests Missing Descriptions:** 0

**Numbering Mismatches:** 0

**⚠ Duplicate test numbers detected: 1 number(s) used more than once.**
Unique numbered tests are listed first. Duplicate-number entries are grouped after them and marked with ⚠. Unnumbered tests are listed in their own group.

This catalog lists all tests in the Rust codebase.

| Test # | Function Name | Description | File |
|--------|---------------|-------------|------|
| test001 | `test001_cap_urn_creation` | TEST001: Test that cap URN is created with tags parsed correctly and direction specs accessible | src/urn/cap_urn.rs:1029 |
| test002 | `test002_direction_specs_default_to_wildcard` | TEST002: Test that missing 'in' or 'out' defaults to media: wildcard | src/urn/cap_urn.rs:1041 |
| test003 | `test003_direction_matching` | TEST003: Test that direction specs must match exactly, different in/out types don't match, wildcard matches any | src/urn/cap_urn.rs:1066 |
| test004 | `test004_unquoted_values_lowercased` | TEST004: Test that unquoted keys and values are normalized to lowercase | src/urn/cap_urn.rs:1111 |
| test005 | `test005_quoted_values_preserve_case` | TEST005: Test that quoted values preserve case while unquoted are lowercased | src/urn/cap_urn.rs:1132 |
| test006 | `test006_quoted_value_special_chars` | TEST006: Test that quoted values can contain special characters (semicolons, equals, spaces) | src/urn/cap_urn.rs:1151 |
| test007 | `test007_quoted_value_escape_sequences` | TEST007: Test that escape sequences in quoted values (\" and \\) are parsed correctly | src/urn/cap_urn.rs:1170 |
| test008 | `test008_mixed_quoted_unquoted` | TEST008: Test that mixed quoted and unquoted values in same URN parse correctly | src/urn/cap_urn.rs:1189 |
| test009 | `test009_unterminated_quote_error` | TEST009: Test that unterminated quote produces UnterminatedQuote error | src/urn/cap_urn.rs:1197 |
| test010 | `test010_invalid_escape_sequence_error` | TEST010: Test that invalid escape sequences (like \n, \x) produce InvalidEscapeSequence error | src/urn/cap_urn.rs:1207 |
| test011 | `test011_serialization_smart_quoting` | TEST011: Test that serialization uses smart quoting (no quotes for simple lowercase, quotes for special chars/uppercase) | src/urn/cap_urn.rs:1224 |
| test012 | `test012_round_trip_simple` | TEST012: Test that simple cap URN round-trips (parse -> serialize -> parse equals original) | src/urn/cap_urn.rs:1259 |
| test013 | `test013_round_trip_quoted` | TEST013: Test that quoted values round-trip preserving case and spaces | src/urn/cap_urn.rs:1269 |
| test014 | `test014_round_trip_escapes` | TEST014: Test that escape sequences round-trip correctly | src/urn/cap_urn.rs:1283 |
| test015 | `test015_cap_prefix_required` | TEST015: Test that cap: prefix is required and case-insensitive | src/urn/cap_urn.rs:1297 |
| test016 | `test016_trailing_semicolon_equivalence` | TEST016: Test that trailing semicolon is equivalent (same hash, same string, matches) | src/urn/cap_urn.rs:1320 |
| test017 | `test017_tag_matching` | TEST017: Test tag matching: exact match, subset match, wildcard match, value mismatch | src/urn/cap_urn.rs:1352 |
| test018 | `test018_matching_case_sensitive_values` | TEST018: Test that quoted values with different case do NOT match (case-sensitive) | src/urn/cap_urn.rs:1379 |
| test019 | `test019_missing_tag_handling` | TEST019: Missing tag in instance causes rejection — pattern's tags are constraints | src/urn/cap_urn.rs:1393 |
| test020 | `test020_specificity` | TEST020: Test specificity calculation (direction specs use MediaUrn tag count, wildcards don't count) | src/urn/cap_urn.rs:1412 |
| test021 | `test021_builder` | TEST021: Test builder creates cap URN with correct tags and direction specs | src/urn/cap_urn.rs:1432 |
| test022 | `test022_builder_requires_direction` | TEST022: Test builder requires both in_spec and out_spec | src/urn/cap_urn.rs:1449 |
| test023 | `test023_builder_preserves_case` | TEST023: Test builder lowercases keys but preserves value case | src/urn/cap_urn.rs:1474 |
| test024 | `test024_directional_accepts` | TEST024: Directional accepts — pattern's tags are constraints, instance must satisfy | src/urn/cap_urn.rs:1488 |
| test025 | `test025_best_match` | TEST025: Test find_best_match returns most specific matching cap | src/urn/cap_urn.rs:1519 |
| test026 | `test026_merge_and_subset` | TEST026: Test merge combines tags from both caps, subset keeps only specified tags | src/urn/cap_urn.rs:1535 |
| test027 | `test027_wildcard_tag` | TEST027: Test with_wildcard_tag sets tag to wildcard, including in/out | src/urn/cap_urn.rs:1559 |
| test028 | `test028_empty_cap_urn_defaults_to_wildcard` | TEST028: Test empty cap URN defaults to media: wildcard | src/urn/cap_urn.rs:1575 |
| test029 | `test029_minimal_cap_urn` | TEST029: Test minimal valid cap URN has just in and out, empty tags | src/urn/cap_urn.rs:1589 |
| test030 | `test030_extended_character_support` | TEST030: Test extended characters (forward slashes, colons) in tag values | src/urn/cap_urn.rs:1603 |
| test031 | `test031_wildcard_restrictions` | TEST031: Test wildcard rejected in keys but accepted in values | src/urn/cap_urn.rs:1616 |
| test032 | `test032_duplicate_key_rejection` | TEST032: Test duplicate keys are rejected with DuplicateKey error | src/urn/cap_urn.rs:1627 |
| test033 | `test033_numeric_key_restriction` | TEST033: Test pure numeric keys rejected, mixed alphanumeric allowed, numeric values allowed | src/urn/cap_urn.rs:1637 |
| test034 | `test034_empty_value_error` | TEST034: Test empty values are rejected | src/urn/cap_urn.rs:1651 |
| test035 | `test035_has_tag_case_sensitive` | TEST035: Test has_tag is case-sensitive for values, case-insensitive for keys, works for in/out | src/urn/cap_urn.rs:1658 |
| test036 | `test036_with_tag_preserves_value` | TEST036: Test with_tag preserves value case | src/urn/cap_urn.rs:1679 |
| test037 | `test037_with_tag_rejects_empty_value` | TEST037: Test with_tag rejects empty value | src/urn/cap_urn.rs:1692 |
| test038 | `test038_semantic_equivalence` | TEST038: Test semantic equivalence of unquoted and quoted simple lowercase values | src/urn/cap_urn.rs:1705 |
| test039 | `test039_get_tag_returns_direction_specs` | TEST039: Test get_tag returns direction specs (in/out) with case-insensitive lookup | src/urn/cap_urn.rs:1718 |
| test040 | `test040_matching_semantics_test1_exact_match` | TEST040: Matching semantics - exact match succeeds | src/urn/cap_urn.rs:1746 |
| test041 | `test041_matching_semantics_test2_cap_missing_tag` | TEST041: Matching semantics - cap missing tag matches (implicit wildcard) | src/urn/cap_urn.rs:1755 |
| test042 | `test042_matching_semantics_test3_cap_has_extra_tag` | TEST042: Pattern rejects instance missing required tags | src/urn/cap_urn.rs:1767 |
| test043 | `test043_matching_semantics_test4_request_has_wildcard` | TEST043: Matching semantics - request wildcard matches specific cap value | src/urn/cap_urn.rs:1784 |
| test044 | `test044_matching_semantics_test5_cap_has_wildcard` | TEST044: Matching semantics - cap wildcard matches specific request value | src/urn/cap_urn.rs:1796 |
| test045 | `test045_matching_semantics_test6_value_mismatch` | TEST045: Matching semantics - value mismatch does not match | src/urn/cap_urn.rs:1805 |
| test046 | `test046_matching_semantics_test7_fallback_pattern` | TEST046: Matching semantics - fallback pattern (cap missing tag = implicit wildcard) | src/urn/cap_urn.rs:1817 |
| test047 | `test047_matching_semantics_test7b_thumbnail_void_input` | TEST047: Matching semantics - thumbnail fallback with void input | src/urn/cap_urn.rs:1838 |
| test048 | `test048_matching_semantics_test8_wildcard_direction_matches_anything` | TEST048: Matching semantics - wildcard direction matches anything | src/urn/cap_urn.rs:1859 |
| test049 | `test049_matching_semantics_test9_cross_dimension_independence` | TEST049: Non-overlapping tags — neither direction accepts | src/urn/cap_urn.rs:1875 |
| test050 | `test050_matching_semantics_test10_direction_mismatch` | TEST050: Matching semantics - direction mismatch prevents matching | src/urn/cap_urn.rs:1891 |
| test051 | `test051_input_validation_success` | TEST051: Test input validation succeeds with valid positional argument | src/cap/validation.rs:1244 |
| test052 | `test052_input_validation_missing_required` | TEST052: Test input validation fails with MissingRequiredArgument when required arg missing | src/cap/validation.rs:1272 |
| test053 | `test053_input_validation_wrong_type` | TEST053: Test input validation fails with InvalidArgumentType when wrong type provided | src/cap/validation.rs:1306 |
| test054 | `test054_xv5_inline_spec_redefinition_detected` | TEST054: XV5 - Test inline media spec redefinition of existing registry spec is detected and rejected | src/cap/validation.rs:1355 |
| test055 | `test055_xv5_new_inline_spec_allowed` | TEST055: XV5 - Test new inline media spec (not in registry) is allowed | src/cap/validation.rs:1394 |
| test056 | `test056_xv5_empty_media_specs_allowed` | TEST056: XV5 - Test empty media_specs (no inline specs) passes XV5 validation | src/cap/validation.rs:1429 |
| test060 | `test060_wrong_prefix_fails` | TEST060: Test wrong prefix fails with InvalidPrefix error showing expected and actual prefix | src/urn/media_urn.rs:642 |
| test061 | `test061_is_binary` | TEST061: Test is_binary returns true when textable tag is absent (binary = not textable) | src/urn/media_urn.rs:655 |
| test062 | `test062_is_record` | TEST062: Test is_record returns true when record marker tag is present indicating key-value structure | src/urn/media_urn.rs:674 |
| test063 | `test063_is_scalar` | TEST063: Test is_scalar returns true when list marker tag is absent (scalar is default) | src/urn/media_urn.rs:691 |
| test064 | `test064_is_list` | TEST064: Test is_list returns true when list marker tag is present indicating ordered collection | src/urn/media_urn.rs:710 |
| test065 | `test065_is_opaque` | TEST065: Test is_opaque returns true when record marker is absent (opaque is default) | src/urn/media_urn.rs:725 |
| test066 | `test066_is_json` | TEST066: Test is_json returns true only when json marker tag is present for JSON representation | src/urn/media_urn.rs:743 |
| test067 | `test067_is_text` | TEST067: Test is_text returns true only when textable marker tag is present | src/urn/media_urn.rs:756 |
| test068 | `test068_is_void` | TEST068: Test is_void returns true when void flag or type=void tag is present | src/urn/media_urn.rs:769 |
| test071 | `test071_to_string_roundtrip` | TEST071: Test to_string roundtrip ensures serialization and deserialization preserve URN structure | src/urn/media_urn.rs:776 |
| test072 | `test072_constants_parse` | TEST072: Test all media URN constants parse successfully as valid media URNs | src/urn/media_urn.rs:786 |
| test073 | `test073_extension_helpers` | TEST073: Test extension helper functions create media URNs with ext tag and correct format | src/urn/media_urn.rs:820 |
| test074 | `test074_media_urn_matching` | TEST074: Test media URN conforms_to using tagged URN semantics with specific and generic requirements | src/urn/media_urn.rs:842 |
| test075 | `test075_matching` | TEST075: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests | src/urn/media_urn.rs:868 |
| test076 | `test076_specificity` | TEST076: Test specificity increases with more tags for ranking conformance | src/urn/media_urn.rs:884 |
| test077 | `test077_serde_roundtrip` | TEST077: Test serde roundtrip serializes to JSON string and deserializes back correctly | src/urn/media_urn.rs:913 |
| test078 | `test078_object_does_not_conform_to_string` | TEST078: conforms_to behavior between MEDIA_OBJECT and MEDIA_STRING | src/urn/media_urn.rs:929 |
| test088 | `test088_resolve_from_registry_str` | TEST088: Test resolving string media URN from registry returns correct media type and profile | src/media/spec.rs:674 |
| test089 | `test089_resolve_from_registry_obj` | TEST089: Test resolving JSON media URN from registry returns JSON media type | src/media/spec.rs:686 |
| test090 | `test090_resolve_from_registry_binary` | TEST090: Test resolving binary media URN returns octet-stream and is_binary true | src/media/spec.rs:697 |
| test091 | `test091_resolve_custom_media_spec` | TEST091: Test resolving custom media URN from local media_specs takes precedence over registry | src/media/spec.rs:722 |
| test092 | `test092_resolve_custom_with_schema` | TEST092: Test resolving custom record media spec with schema from local media_specs | src/media/spec.rs:752 |
| test093 | `test093_resolve_unresolvable_fails_hard` | TEST093: Test resolving unknown media URN fails with UnresolvableMediaUrn error | src/media/spec.rs:791 |
| test094 | `test094_local_overrides_registry` | TEST094: Test local media_specs definition overrides registry definition for same URN | src/media/spec.rs:810 |
| test095 | `test095_media_spec_def_serialize` | TEST095: Test MediaSpecDef serializes with required fields and skips None fields | src/media/spec.rs:843 |
| test096 | `test096_media_spec_def_deserialize` | TEST096: Test deserializing MediaSpecDef from JSON object | src/media/spec.rs:869 |
| test097 | `test097_validate_no_duplicate_urns_catches_duplicates` | TEST097: Test duplicate URN validation catches duplicates | src/media/spec.rs:884 |
| test098 | `test098_validate_no_duplicate_urns_passes_for_unique` | TEST098: Test duplicate URN validation passes for unique URNs | src/media/spec.rs:900 |
| test099 | `test099_resolved_is_binary` | TEST099: Test ResolvedMediaSpec is_binary returns true when textable tag is absent | src/media/spec.rs:915 |
| test100 | `test100_resolved_is_record` | TEST100: Test ResolvedMediaSpec is_record returns true when record marker is present | src/media/spec.rs:935 |
| test101 | `test101_resolved_is_scalar` | TEST101: Test ResolvedMediaSpec is_scalar returns true when list marker is absent | src/media/spec.rs:956 |
| test102 | `test102_resolved_is_list` | TEST102: Test ResolvedMediaSpec is_list returns true when list marker is present | src/media/spec.rs:976 |
| test103 | `test103_resolved_is_json` | TEST103: Test ResolvedMediaSpec is_json returns true when json tag is present | src/media/spec.rs:996 |
| test104 | `test104_resolved_is_text` | TEST104: Test ResolvedMediaSpec is_text returns true when textable tag is present | src/media/spec.rs:1016 |
| test105 | `test105_metadata_propagation` | TEST105: Test metadata propagates from media spec def to resolved media spec | src/media/spec.rs:1040 |
| test106 | `test106_metadata_with_validation` | TEST106: Test metadata and validation can coexist in media spec definition | src/media/spec.rs:1069 |
| test107 | `test107_extensions_propagation` | TEST107: Test extensions field propagates from media spec def to resolved | src/media/spec.rs:1120 |
| test108 | `test108_cap_creation` | TEST108: Test creating new cap with URN, title, and command verifies correct initialization | src/cap/definition.rs:943 |
| test109 | `test109_cap_with_metadata` | TEST109: Test creating cap with metadata initializes and retrieves metadata correctly | src/cap/definition.rs:964 |
| test110 | `test110_cap_matching` | TEST110: Test cap matching with subset semantics for request fulfillment | src/cap/definition.rs:992 |
| test111 | `test111_cap_title` | TEST111: Test getting and setting cap title updates correctly | src/cap/definition.rs:1010 |
| test112 | `test112_cap_definition_equality` | TEST112: Test cap equality based on URN and title matching | src/cap/definition.rs:1028 |
| test113 | `test113_cap_stdin` | TEST113: Test cap stdin support via args with stdin source and serialization roundtrip | src/cap/definition.rs:1055 |
| test114 | `test114_arg_source_types` | TEST114: Test ArgSource type variants stdin, position, and cli_flag with their accessors | src/cap/definition.rs:1095 |
| test115 | `test115_cap_arg_serialization` | TEST115: Test CapArg serialization and deserialization with multiple sources | src/cap/definition.rs:1124 |
| test116 | `test116_cap_arg_constructors` | TEST116: Test CapArg constructor methods basic and with_description create args correctly | src/cap/definition.rs:1152 |
| test135 | `test135_registry_creation` | TEST135: Test registry creation with temporary cache directory succeeds | src/cap/registry.rs:694 |
| test136 | `test136_cache_key_generation` | TEST136: Test cache key generation produces consistent hashes for same URN | src/cap/registry.rs:701 |
| test137 | `test137_parse_registry_json` | TEST137: Test parsing registry JSON without stdin args verifies cap structure | src/cap/registry.rs:797 |
| test138 | `test138_parse_registry_json_with_stdin` | TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction | src/cap/registry.rs:810 |
| test139 | `test139_url_keeps_cap_prefix_literal` | TEST139: Test URL construction keeps cap prefix literal and only encodes tags part | src/cap/registry.rs:829 |
| test140 | `test140_url_encodes_quoted_media_urns` | TEST140: Test URL encodes media URNs with proper percent encoding for special characters | src/cap/registry.rs:851 |
| test141 | `test141_exact_url_format` | TEST141: Test exact URL format contains properly encoded media URN components | src/cap/registry.rs:874 |
| test142 | `test142_normalize_handles_different_tag_orders` | TEST142: Test normalize handles different tag orders producing same canonical form | src/cap/registry.rs:897 |
| test143 | `test143_default_config` | TEST143: Test default config uses capdag.com or environment variable values | src/cap/registry.rs:918 |
| test144 | `test144_custom_registry_url` | TEST144: Test custom registry URL updates both registry and schema base URLs | src/cap/registry.rs:934 |
| test145 | `test145_custom_registry_and_schema_url` | TEST145: Test custom registry and schema URLs set independently | src/cap/registry.rs:942 |
| test146 | `test146_schema_url_not_overwritten_when_explicit` | TEST146: Test schema URL not overwritten when set explicitly before registry URL | src/cap/registry.rs:952 |
| test147 | `test147_registry_for_test_with_config` | TEST147: Test registry for test with custom config creates registry with specified URLs | src/cap/registry.rs:963 |
| test148 | `test148_cap_manifest_creation` | TEST148: Manifest creation with cap groups | src/bifaci/manifest.rs:239 |
| test149 | `test149_cap_manifest_with_author` | TEST149: Author field | src/bifaci/manifest.rs:361 |
| test150 | `test150_cap_manifest_json_serialization` | TEST150: JSON roundtrip | src/bifaci/manifest.rs:380 |
| test151 | `test151_cap_manifest_required_fields` | TEST151: Missing required fields fail | src/bifaci/manifest.rs:413 |
| test152 | `test152_cap_manifest_with_multiple_caps` | TEST152: Multiple caps across groups | src/bifaci/manifest.rs:421 |
| test153 | `test153_cap_manifest_empty_cap_groups` | TEST153: Empty cap groups | src/bifaci/manifest.rs:448 |
| test154 | `test154_cap_manifest_optional_author_field` | TEST154: Optional author field omitted in serialization | src/bifaci/manifest.rs:467 |
| test155 | `test155_component_metadata_trait` | TEST155: ComponentMetadata trait | src/bifaci/manifest.rs:486 |
| test156 | `test156_stdin_source_data_creation` | TEST156: Test creating StdinSource Data variant with byte vector | src/cap/caller.rs:165 |
| test157 | `test157_stdin_source_file_reference_creation` | TEST157: Test creating StdinSource FileReference variant with all required fields | src/cap/caller.rs:177 |
| test158 | `test158_stdin_source_empty_data` | TEST158: Test StdinSource Data with empty vector stores and retrieves correctly | src/cap/caller.rs:208 |
| test159 | `test159_stdin_source_binary_content` | TEST159: Test StdinSource Data with binary content like PNG header bytes | src/cap/caller.rs:219 |
| test160 | `test160_stdin_source_clone` | TEST160: Test StdinSource Data clone creates independent copy with same data | src/cap/caller.rs:237 |
| test161 | `test161_stdin_source_file_reference_clone` | TEST161: Test StdinSource FileReference clone creates independent copy with same fields | src/cap/caller.rs:250 |
| test162 | `test162_stdin_source_debug` | TEST162: Test StdinSource Debug format displays variant type and relevant fields | src/cap/caller.rs:285 |
| test163 | `test163_argument_schema_validation_success` | TEST163: Test argument schema validation succeeds with valid JSON matching schema | src/cap/schema_validation.rs:241 |
| test164 | `test164_argument_schema_validation_failure` | TEST164: Test argument schema validation fails with JSON missing required fields | src/cap/schema_validation.rs:285 |
| test165 | `test165_output_schema_validation_success` | TEST165: Test output schema validation succeeds with valid JSON matching schema | src/cap/schema_validation.rs:328 |
| test166 | `test166_skip_validation_without_schema` | TEST166: Test validation skipped when resolved media spec has no schema | src/cap/schema_validation.rs:368 |
| test167 | `test167_unresolvable_media_urn_fails_hard` | TEST167: Test validation fails hard when media URN cannot be resolved from any source | src/cap/schema_validation.rs:393 |
| test168 | `test168_json_response` | TEST168: Test ResponseWrapper from JSON deserializes to correct structured type | src/cap/response.rs:274 |
| test169 | `test169_primitive_types` | TEST169: Test ResponseWrapper converts to primitive types integer, float, boolean, string | src/cap/response.rs:288 |
| test170 | `test170_binary_response` | TEST170: Test ResponseWrapper from binary stores and retrieves raw bytes correctly | src/cap/response.rs:308 |
| test171 | `test171_frame_type_roundtrip` | TEST171: Test all FrameType discriminants roundtrip through u8 conversion preserving identity | src/bifaci/frame.rs:1038 |
| test172 | `test172_invalid_frame_type` | TEST172: Test FrameType::from_u8 returns None for values outside the valid discriminant range | src/bifaci/frame.rs:1062 |
| test173 | `test173_frame_type_discriminant_values` | TEST173: Test FrameType discriminant values match the wire protocol specification exactly | src/bifaci/frame.rs:1073 |
| test174 | `test174_message_id_uuid` | TEST174: Test MessageId::new_uuid generates valid UUID that roundtrips through string conversion | src/bifaci/frame.rs:1091 |
| test175 | `test175_message_id_uuid_uniqueness` | TEST175: Test two MessageId::new_uuid calls produce distinct IDs (no collisions) | src/bifaci/frame.rs:1100 |
| test176 | `test176_message_id_uint_has_no_uuid_string` | TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns None | src/bifaci/frame.rs:1108 |
| test177 | `test177_message_id_from_invalid_uuid_str` | TEST177: Test MessageId::from_uuid_str rejects invalid UUID strings | src/bifaci/frame.rs:1118 |
| test178 | `test178_message_id_as_bytes` | TEST178: Test MessageId::as_bytes produces correct byte representations for Uuid and Uint variants | src/bifaci/frame.rs:1126 |
| test179 | `test179_message_id_default_is_uuid` | TEST179: Test MessageId::default creates a UUID variant (not Uint) | src/bifaci/frame.rs:1142 |
| test180 | `test180_hello_frame` | TEST180: Test Frame::hello without manifest produces correct HELLO frame for host side | src/bifaci/frame.rs:1152 |
| test181 | `test181_hello_frame_with_manifest` | TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes for cartridge side | src/bifaci/frame.rs:1173 |
| test182 | `test182_req_frame` | TEST182: Test Frame::req stores cap URN, payload, and content_type correctly | src/bifaci/frame.rs:1195 |
| test184 | `test184_chunk_frame` | TEST184: Test Frame::chunk stores seq and payload for streaming (with stream_id) | src/bifaci/frame.rs:1219 |
| test185 | `test185_err_frame` | TEST185: Test Frame::err stores error code and message in metadata | src/bifaci/frame.rs:1235 |
| test186 | `test186_log_frame` | TEST186: Test Frame::log stores level and message in metadata | src/bifaci/frame.rs:1245 |
| test187 | `test187_end_frame_with_payload` | TEST187: Test Frame::end with payload sets eof and optional final payload | src/bifaci/frame.rs:1256 |
| test188 | `test188_end_frame_without_payload` | TEST188: Test Frame::end without payload still sets eof marker | src/bifaci/frame.rs:1266 |
| test189 | `test189_chunk_with_offset` | TEST189: Test chunk_with_offset sets offset on all chunks but len only on seq=0 (with stream_id) | src/bifaci/frame.rs:1276 |
| test190 | `test190_heartbeat_frame` | TEST190: Test Frame::heartbeat creates minimal frame with no payload or metadata | src/bifaci/frame.rs:1326 |
| test191 | `test191_error_accessors_on_non_err_frame` | TEST191: Test error_code and error_message return None for non-Err frame types | src/bifaci/frame.rs:1377 |
| test192 | `test192_log_accessors_on_non_log_frame` | TEST192: Test log_level and log_message return None for non-Log frame types | src/bifaci/frame.rs:1400 |
| test193 | `test193_hello_accessors_on_non_hello_frame` | TEST193: Test hello_max_frame and hello_max_chunk return None for non-Hello frame types | src/bifaci/frame.rs:1413 |
| test194 | `test194_frame_new_defaults` | TEST194: Test Frame::new sets version and defaults correctly, optional fields are None | src/bifaci/frame.rs:1422 |
| test195 | `test195_frame_default` | TEST195: Test Frame::default creates a Req frame (the documented default) | src/bifaci/frame.rs:1440 |
| test196 | `test196_is_eof_when_none` | TEST196: Test is_eof returns false when eof field is None (unset) | src/bifaci/frame.rs:1448 |
| test197 | `test197_is_eof_when_false` | TEST197: Test is_eof returns false when eof field is explicitly Some(false) | src/bifaci/frame.rs:1455 |
| test198 | `test198_limits_default` | TEST198: Test Limits::default provides the documented default values | src/bifaci/frame.rs:1463 |
| test199 | `test199_protocol_version_constant` | TEST199: Test PROTOCOL_VERSION is 2 | src/bifaci/frame.rs:1473 |
| test200 | `test200_key_constants` | TEST200: Test integer key constants match the protocol specification | src/bifaci/frame.rs:1479 |
| test201 | `test201_hello_manifest_binary_data` | TEST201: Test hello_with_manifest preserves binary manifest data (not just JSON text) | src/bifaci/frame.rs:1495 |
| test202 | `test202_message_id_equality_and_hash` | TEST202: Test MessageId Eq/Hash semantics: equal UUIDs are equal, different ones are not | src/bifaci/frame.rs:1510 |
| test203 | `test203_message_id_cross_variant_inequality` | TEST203: Test Uuid and Uint variants of MessageId are never equal even for coincidental byte values | src/bifaci/frame.rs:1533 |
| test204 | `test204_req_frame_empty_payload` | TEST204: Test Frame::req with empty payload stores Some(empty vec) not None | src/bifaci/frame.rs:1541 |
| test205 | `test205_encode_decode_roundtrip` | TEST205: Test REQ frame encode/decode roundtrip preserves all fields | src/bifaci/io.rs:910 |
| test206 | `test206_hello_frame_roundtrip` | TEST206: Test HELLO frame encode/decode roundtrip preserves max_frame, max_chunk, max_reorder_buffer | src/bifaci/io.rs:932 |
| test207 | `test207_err_frame_roundtrip` | TEST207: Test ERR frame encode/decode roundtrip preserves error code and message | src/bifaci/io.rs:949 |
| test208 | `test208_log_frame_roundtrip` | TEST208: Test LOG frame encode/decode roundtrip preserves level and message | src/bifaci/io.rs:962 |
| test210 | `test210_end_frame_roundtrip` | TEST210: Test END frame encode/decode roundtrip preserves eof marker and optional payload | src/bifaci/io.rs:1064 |
| test211 | `test211_hello_with_manifest_roundtrip` | TEST211: Test HELLO with manifest encode/decode roundtrip preserves manifest bytes and limits | src/bifaci/io.rs:1078 |
| test212 | `test212_chunk_with_offset_roundtrip` | TEST212: Test chunk_with_offset encode/decode roundtrip preserves offset, len, eof (with stream_id) | src/bifaci/io.rs:1099 |
| test213 | `test213_heartbeat_roundtrip` | TEST213: Test heartbeat frame encode/decode roundtrip preserves ID with no extra fields | src/bifaci/io.rs:1129 |
| test214 | `test214_frame_io_roundtrip` | TEST214: Test write_frame/read_frame IO roundtrip through length-prefixed wire format | src/bifaci/io.rs:1143 |
| test215 | `test215_multiple_frames` | TEST215: Test reading multiple sequential frames from a single buffer | src/bifaci/io.rs:1173 |
| test216 | `test216_frame_too_large` | TEST216: Test write_frame rejects frames exceeding max_frame limit | src/bifaci/io.rs:1223 |
| test217 | `test217_read_frame_too_large` | TEST217: Test read_frame rejects incoming frames exceeding the negotiated max_frame limit | src/bifaci/io.rs:1246 |
| test218 | `test218_write_chunked` | TEST218: Test write_chunked splits data into chunks respecting max_chunk and reconstructs correctly Chunks from write_chunked have seq=0. SeqAssigner at the output stage assigns final seq. Chunk ordering within a stream is tracked by chunk_index (chunk_index field). | src/bifaci/io.rs:1282 |
| test219 | `test219_write_chunked_empty_data` | TEST219: Test write_chunked with empty data produces a single EOF chunk | src/bifaci/io.rs:1370 |
| test220 | `test220_write_chunked_exact_fit` | TEST220: Test write_chunked with data exactly equal to max_chunk produces exactly one chunk | src/bifaci/io.rs:1398 |
| test221 | `test221_eof_handling` | TEST221: Test read_frame returns Ok(None) on clean EOF (empty stream) | src/bifaci/io.rs:1428 |
| test222 | `test222_truncated_length_prefix` | TEST222: Test read_frame handles truncated length prefix (fewer than 4 bytes available) | src/bifaci/io.rs:1438 |
| test223 | `test223_truncated_frame_body` | TEST223: Test read_frame returns error on truncated frame body (length prefix says more bytes than available) | src/bifaci/io.rs:1457 |
| test224 | `test224_message_id_uint` | TEST224: Test MessageId::Uint roundtrips through encode/decode | src/bifaci/io.rs:1472 |
| test225 | `test225_decode_non_map_value` | TEST225: Test decode_frame rejects non-map CBOR values (e.g., array, integer, string) | src/bifaci/io.rs:1484 |
| test226 | `test226_decode_missing_version` | TEST226: Test decode_frame rejects CBOR map missing required version field | src/bifaci/io.rs:1496 |
| test227 | `test227_decode_invalid_frame_type_value` | TEST227: Test decode_frame rejects CBOR map with invalid frame_type value | src/bifaci/io.rs:1517 |
| test228 | `test228_decode_missing_id` | TEST228: Test decode_frame rejects CBOR map missing required id field | src/bifaci/io.rs:1541 |
| test229 | `test229_frame_reader_writer_set_limits` | TEST229: Test FrameReader/FrameWriter set_limits updates the negotiated limits | src/bifaci/io.rs:1562 |
| test230 | `test230_async_handshake` | TEST230: Test async handshake exchanges HELLO frames and negotiates minimum limits | src/bifaci/io.rs:1583 |
| test231 | `test231_handshake_rejects_non_hello` | TEST231: Test handshake fails when peer sends non-HELLO frame | src/bifaci/io.rs:1618 |
| test232 | `test232_handshake_rejects_missing_manifest` | TEST232: Test handshake fails when cartridge HELLO is missing required manifest | src/bifaci/io.rs:1653 |
| test233 | `test233_binary_payload_all_byte_values` | TEST233: Test binary payload with all 256 byte values roundtrips through encode/decode | src/bifaci/io.rs:1683 |
| test234 | `test234_decode_garbage_bytes` | TEST234: Test decode_frame handles garbage CBOR bytes gracefully with an error | src/bifaci/io.rs:1705 |
| test235 | `test235_response_chunk` | TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly | src/bifaci/host_runtime.rs:2486 |
| test236 | `test236_response_chunk_with_all_fields` | TEST236: Test ResponseChunk with all fields populated preserves offset, len, and eof | src/bifaci/host_runtime.rs:2502 |
| test237 | `test237_cartridge_response_single` | TEST237: Test CartridgeResponse::Single final_payload returns the single payload slice | src/bifaci/host_runtime.rs:2518 |
| test238 | `test238_cartridge_response_single_empty` | TEST238: Test CartridgeResponse::Single with empty payload returns empty slice and empty vec | src/bifaci/host_runtime.rs:2526 |
| test239 | `test239_cartridge_response_streaming` | TEST239: Test CartridgeResponse::Streaming concatenated joins all chunk payloads in order | src/bifaci/host_runtime.rs:2534 |
| test240 | `test240_cartridge_response_streaming_final_payload` | TEST240: Test CartridgeResponse::Streaming final_payload returns the last chunk's payload | src/bifaci/host_runtime.rs:2557 |
| test241 | `test241_cartridge_response_streaming_empty_chunks` | TEST241: Test CartridgeResponse::Streaming with empty chunks vec returns empty concatenation | src/bifaci/host_runtime.rs:2580 |
| test242 | `test242_cartridge_response_streaming_large_payload` | TEST242: Test CartridgeResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads | src/bifaci/host_runtime.rs:2588 |
| test243 | `test243_async_host_error_display` | TEST243: Test AsyncHostError variants display correct error messages | src/bifaci/host_runtime.rs:2616 |
| test244 | `test244_async_host_error_from_cbor` | TEST244: Test AsyncHostError::from converts CborError to Cbor variant | src/bifaci/host_runtime.rs:2642 |
| test245 | `test245_async_host_error_from_io` | TEST245: Test AsyncHostError::from converts io::Error to Io variant | src/bifaci/host_runtime.rs:2653 |
| test246 | `test246_async_host_error_clone` | TEST246: Test AsyncHostError Clone implementation produces equal values | src/bifaci/host_runtime.rs:2664 |
| test247 | `test247_response_chunk_clone` | TEST247: Test ResponseChunk Clone produces independent copy with same data | src/bifaci/host_runtime.rs:2675 |
| test248 | `test248_register_and_find_handler` | TEST248: Test register_op and find_handler by exact cap URN | src/bifaci/cartridge_runtime.rs:4545 |
| test249 | `test249_raw_handler` | TEST249: Test register_op handler echoes bytes directly | src/bifaci/cartridge_runtime.rs:4557 |
| test250 | `test250_typed_handler_deserialization` | TEST250: Test Op handler collects input and processes it | src/bifaci/cartridge_runtime.rs:4581 |
| test251 | `test251_typed_handler_rejects_invalid_json` | TEST251: Test Op handler propagates errors through RuntimeError::Handler | src/bifaci/cartridge_runtime.rs:4639 |
| test252 | `test252_find_handler_unknown_cap` | TEST252: Test find_handler returns None for unregistered cap URNs | src/bifaci/cartridge_runtime.rs:4682 |
| test253 | `test253_handler_is_send_sync` | TEST253: Test OpFactory can be cloned via Arc and sent across tasks (Send + Sync) | src/bifaci/cartridge_runtime.rs:4689 |
| test254 | `test254_no_peer_invoker` | TEST254: Test NoPeerInvoker always returns PeerRequest error | src/bifaci/cartridge_runtime.rs:4745 |
| test255 | `test255_no_peer_invoker_with_arguments` | TEST255: Test NoPeerInvoker call_with_bytes also returns error | src/bifaci/cartridge_runtime.rs:4762 |
| test256 | `test256_with_manifest_json` | TEST256: Test CartridgeRuntime::with_manifest_json stores manifest data and parses when valid | src/bifaci/cartridge_runtime.rs:4772 |
| test257 | `test257_new_with_invalid_json` | TEST257: Test CartridgeRuntime::new with invalid JSON still creates runtime (manifest is None) | src/bifaci/cartridge_runtime.rs:4795 |
| test258 | `test258_with_manifest_struct` | TEST258: Test CartridgeRuntime::with_manifest creates runtime with valid manifest data | src/bifaci/cartridge_runtime.rs:4806 |
| test259 | `test259_extract_effective_payload_non_cbor` | TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged | src/bifaci/cartridge_runtime.rs:4816 |
| test260 | `test260_extract_effective_payload_no_content_type` | TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged | src/bifaci/cartridge_runtime.rs:4829 |
| test261 | `test261_extract_effective_payload_cbor_match` | TEST261: Test extract_effective_payload with CBOR content extracts matching argument value | src/bifaci/cartridge_runtime.rs:4841 |
| test262 | `test262_extract_effective_payload_cbor_no_match` | TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input | src/bifaci/cartridge_runtime.rs:4900 |
| test263 | `test263_extract_effective_payload_invalid_cbor` | TEST263: Test extract_effective_payload with invalid CBOR bytes returns deserialization error | src/bifaci/cartridge_runtime.rs:4935 |
| test264 | `test264_extract_effective_payload_cbor_not_array` | TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error | src/bifaci/cartridge_runtime.rs:4949 |
| test266 | `test266_cli_frame_sender_construction` | TEST266: Test CliFrameSender wraps CliStreamEmitter correctly (basic construction) | src/bifaci/cartridge_runtime.rs:4973 |
| test268 | `test268_runtime_error_display` | TEST268: Test RuntimeError variants display correct messages | src/bifaci/cartridge_runtime.rs:4984 |
| test270 | `test270_multiple_handlers` | TEST270: Test registering multiple Op handlers for different caps and finding each independently | src/bifaci/cartridge_runtime.rs:5006 |
| test271 | `test271_handler_replacement` | TEST271: Test Op handler replacing an existing registration for the same cap URN | src/bifaci/cartridge_runtime.rs:5035 |
| test272 | `test272_extract_effective_payload_multiple_args` | TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one | src/bifaci/cartridge_runtime.rs:5108 |
| test273 | `test273_extract_effective_payload_binary_value` | TEST273: Test extract_effective_payload with binary data in CBOR value (not just text) | src/bifaci/cartridge_runtime.rs:5202 |
| test274 | `test274_cap_argument_value_new` | TEST274: Test CapArgumentValue::new stores media_urn and raw byte value | src/cap/caller.rs:304 |
| test275 | `test275_cap_argument_value_from_str` | TEST275: Test CapArgumentValue::from_str converts string to UTF-8 bytes | src/cap/caller.rs:312 |
| test276 | `test276_cap_argument_value_as_str_valid` | TEST276: Test CapArgumentValue::value_as_str succeeds for UTF-8 data | src/cap/caller.rs:320 |
| test277 | `test277_cap_argument_value_as_str_invalid_utf8` | TEST277: Test CapArgumentValue::value_as_str fails for non-UTF-8 binary data | src/cap/caller.rs:327 |
| test278 | `test278_cap_argument_value_empty` | TEST278: Test CapArgumentValue::new with empty value stores empty vec | src/cap/caller.rs:334 |
| test279 | `test279_cap_argument_value_clone` | TEST279: Test CapArgumentValue Clone produces independent copy with same data | src/cap/caller.rs:342 |
| test280 | `test280_cap_argument_value_debug` | TEST280: Test CapArgumentValue Debug format includes media_urn and value | src/cap/caller.rs:351 |
| test281 | `test281_cap_argument_value_into_string` | TEST281: Test CapArgumentValue::new accepts Into<String> for media_urn (String and &str) | src/cap/caller.rs:359 |
| test282 | `test282_cap_argument_value_unicode` | TEST282: Test CapArgumentValue::from_str with Unicode string preserves all characters | src/cap/caller.rs:370 |
| test283 | `test283_cap_argument_value_large_binary` | TEST283: Test CapArgumentValue with large binary payload preserves all bytes | src/cap/caller.rs:377 |
| test284 | `test284_handshake_host_cartridge` | TEST284: Handshake exchanges HELLO frames, negotiates limits | src/bifaci/integration_tests.rs:944 |
| test285 | `test285_request_response_simple` | TEST285: Simple request-response flow (REQ → END with payload) | src/bifaci/integration_tests.rs:981 |
| test286 | `test286_streaming_chunks` | TEST286: Streaming response with multiple CHUNK frames | src/bifaci/integration_tests.rs:1029 |
| test287 | `test287_heartbeat_from_host` | TEST287: Host-initiated heartbeat | src/bifaci/integration_tests.rs:1109 |
| test290 | `test290_limits_negotiation` | TEST290: Limit negotiation picks minimum | src/bifaci/integration_tests.rs:1149 |
| test291 | `test291_binary_payload_roundtrip` | TEST291: Binary payload roundtrip (all 256 byte values) | src/bifaci/integration_tests.rs:1179 |
| test292 | `test292_message_id_uniqueness` | TEST292: Sequential requests get distinct MessageIds | src/bifaci/integration_tests.rs:1237 |
| test293 | `test293_cartridge_runtime_handler_registration` | TEST293: Test CartridgeRuntime Op registration and lookup by exact and non-existent cap URN | src/bifaci/integration_tests.rs:21 |
| test299 | `test299_empty_payload_roundtrip` | TEST299: Empty payload request/response roundtrip | src/bifaci/integration_tests.rs:1296 |
| test304 | `test304_media_availability_output_constant` | TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags | src/urn/media_urn.rs:949 |
| test305 | `test305_media_path_output_constant` | TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags | src/urn/media_urn.rs:967 |
| test306 | `test306_availability_and_path_output_distinct` | TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs | src/urn/media_urn.rs:981 |
| test307 | `test307_model_availability_urn` | TEST307: Test model_availability_urn builds valid cap URN with correct op and media specs | src/standard/caps.rs:991 |
| test308 | `test308_model_path_urn` | TEST308: Test model_path_urn builds valid cap URN with correct op and media specs | src/standard/caps.rs:1007 |
| test309 | `test309_model_availability_and_path_are_distinct` | TEST309: Test model_availability_urn and model_path_urn produce distinct URNs | src/standard/caps.rs:1023 |
| test310 | `test310_llm_generate_text_urn_shape` | TEST310: llm_generate_text_urn() produces a valid cap URN with textable in/out specs | src/standard/caps.rs:1035 |
| test312 | `test312_all_urn_builders_produce_valid_urns` | TEST312: Test all URN builders produce parseable cap URNs | src/standard/caps.rs:1065 |
| test320 | `test320_cartridge_info_construction` | TEST320: Construct CartridgeInfo and verify round-trip of fields. | src/bifaci/cartridge_repo.rs:1424 |
| test321 | `test321_cartridge_info_is_signed` | TEST321: CartridgeInfo.is_signed() returns true when signature (team_id + signed_at) is present, false when either is empty. | src/bifaci/cartridge_repo.rs:1439 |
| test322 | `test322_cartridge_info_build_for_platform` | TEST322: CartridgeInfo.build_for_platform() returns the build that matches the requested platform string and None otherwise. | src/bifaci/cartridge_repo.rs:1454 |
| test323 | `test323_cartridge_repo_server_validate_registry` | TEST323: CartridgeRepoServer requires schema 5.0 and rejects older. | src/bifaci/cartridge_repo.rs:1508 |
| test324 | `test324_cartridge_repo_server_transform_to_array` | TEST324: CartridgeRepoServer transforms a v4.0 entry into a flat CartridgeInfo, preserving cap_groups verbatim. | src/bifaci/cartridge_repo.rs:1522 |
| test325 | `test325_cartridge_repo_server_get_cartridges` | TEST325: get_cartridges() wraps the transformed array in the response envelope. | src/bifaci/cartridge_repo.rs:1542 |
| test326 | `test326_cartridge_repo_server_get_cartridge_by_id` | TEST326: get_cartridge_by_id requires a channel and returns Some for a known (channel, id), None otherwise. The same id looked up in the wrong channel must miss — channels are independent namespaces. | src/bifaci/cartridge_repo.rs:1558 |
| test327 | `test327_cartridge_repo_server_search_cartridges` | TEST327: search_cartridges matches against name/description/tags and cap titles, but never against cap URN strings. | src/bifaci/cartridge_repo.rs:1612 |
| test328 | `test328_cartridge_repo_server_get_by_category` | TEST328: get_cartridges_by_category filters on the categories string list. | src/bifaci/cartridge_repo.rs:1643 |
| test329 | `test329_cartridge_repo_server_get_by_cap` | TEST329: get_cartridges_by_cap parses the input URN and matches each cartridge cap via tagged-URN equivalence — not string ==. This proves a request URN whose tags appear in a different order than the cap's declared form still resolves. | src/bifaci/cartridge_repo.rs:1659 |
| test330 | `test330_cartridge_repo_client_update_cache` | TEST330: update_cache populates the cartridge map keyed by (channel, id) and the cap-to-cartridge index keyed by normalized URNs. | src/bifaci/cartridge_repo.rs:1699 |
| test331 | `test331_cartridge_repo_client_get_suggestions` | TEST331: get_suggestions_for_cap returns a suggestion when the cache has a cartridge whose cap is tagged-URN equivalent to the request, even if declared with different tag order. | src/bifaci/cartridge_repo.rs:1732 |
| test332 | `test332_cartridge_repo_client_get_cartridge` | TEST332: get_cartridge requires a (channel, id) pair and returns the cached entry for known pairs, None otherwise. The same id in the wrong channel must miss. | src/bifaci/cartridge_repo.rs:1772 |
| test333 | `test333_cartridge_repo_client_get_all_caps` | TEST333: get_all_available_caps returns the deduplicated set of normalized URNs across cartridges. | src/bifaci/cartridge_repo.rs:1808 |
| test334 | `test334_cartridge_repo_client_needs_sync` | TEST334: needs_sync returns true on an empty cache, false right after a successful update. | src/bifaci/cartridge_repo.rs:1840 |
| test335 | `test335_cartridge_repo_server_client_integration` | TEST335: A v4.0 nested registry round-trips through Server → CartridgeInfo → fingerprint, preserving the cap_groups structure and the signed flag. | src/bifaci/cartridge_repo.rs:1858 |
| test337 | `test337_file_path_without_stdin_passes_string` | TEST337: file-path arg without stdin source passes path as string (no conversion) | src/bifaci/cartridge_runtime.rs:5332 |
| test338 | `test338_file_path_via_cli_flag` | TEST338: file-path arg reads file via --file CLI flag | src/bifaci/cartridge_runtime.rs:5369 |
| test339 | `test339_file_path_array_glob_expansion` | TEST339: file-path-array reads multiple files with glob pattern | src/bifaci/cartridge_runtime.rs:5411 |
| test340 | `test340_file_not_found_clear_error` | TEST340: File not found error provides clear message | src/bifaci/cartridge_runtime.rs:5461 |
| test341 | `test341_stdin_precedence_over_file_path` | TEST341: stdin takes precedence over file-path in source order | src/bifaci/cartridge_runtime.rs:5520 |
| test342 | `test342_file_path_position_zero_reads_first_arg` | TEST342: file-path with position 0 reads first positional arg as file | src/bifaci/cartridge_runtime.rs:5565 |
| test343 | `test343_non_file_path_args_unaffected` | TEST343: Non-file-path args are not affected by file reading | src/bifaci/cartridge_runtime.rs:5600 |
| test344 | `test344_file_path_array_invalid_json_fails` | TEST344: file-path-array with nonexistent path fails clearly | src/bifaci/cartridge_runtime.rs:5635 |
| test345 | `test345_file_path_array_one_file_missing_fails_hard` | TEST345: file-path-array with literal nonexistent path fails hard | src/bifaci/cartridge_runtime.rs:5692 |
| test346 | `test346_large_file_reads_successfully` | TEST346: Large file (1MB) reads successfully | src/bifaci/cartridge_runtime.rs:5755 |
| test347 | `test347_empty_file_reads_as_empty_bytes` | TEST347: Empty file reads as empty bytes | src/bifaci/cartridge_runtime.rs:5793 |
| test348 | `test348_file_path_conversion_respects_source_order` | TEST348: file-path conversion respects source order | src/bifaci/cartridge_runtime.rs:5827 |
| test349 | `test349_file_path_multiple_sources_fallback` | TEST349: file-path arg with multiple sources tries all in order | src/bifaci/cartridge_runtime.rs:5869 |
| test350 | `test350_full_cli_mode_with_file_path_integration` | TEST350: Integration test - full CLI mode invocation with file-path | src/bifaci/cartridge_runtime.rs:5913 |
| test351 | `test351_file_path_array_empty_array` | TEST351: sequence-declared file-path arg with empty input array (CBOR mode) passes through as an empty CBOR Array — no implicit expansion, no spurious error. Declaring `is_sequence = true` is what makes the runtime emit an Array shape; URN tags are semantic only. | src/bifaci/cartridge_runtime.rs:5988 |
| test352 | `test352_file_permission_denied_clear_error` | TEST352: file permission denied error is clear (Unix-specific) | src/bifaci/cartridge_runtime.rs:6050 |
| test353 | `test353_cbor_payload_format_consistency` | TEST353: CBOR payload format matches between CLI and CBOR mode | src/bifaci/cartridge_runtime.rs:6131 |
| test354 | `test354_glob_pattern_no_matches_empty_array` | TEST354: Glob pattern with no matches fails hard (NO FALLBACK) | src/bifaci/cartridge_runtime.rs:6199 |
| test355 | `test355_glob_pattern_skips_directories` | TEST355: Glob pattern skips directories | src/bifaci/cartridge_runtime.rs:6258 |
| test356 | `test356_multiple_glob_patterns_combined` | TEST356: Multiple glob patterns combined | src/bifaci/cartridge_runtime.rs:6311 |
| test357 | `test357_symlinks_followed` | TEST357: Symlinks are followed when reading files | src/bifaci/cartridge_runtime.rs:6412 |
| test358 | `test358_binary_file_non_utf8` | TEST358: Binary file with non-UTF8 data reads correctly | src/bifaci/cartridge_runtime.rs:6460 |
| test359 | `test359_invalid_glob_pattern_fails` | TEST359: Invalid glob pattern fails with clear error | src/bifaci/cartridge_runtime.rs:6497 |
| test360 | `test360_extract_effective_payload_with_file_data` | TEST360: Extract effective payload handles file-path data correctly | src/bifaci/cartridge_runtime.rs:6550 |
| test361 | `test361_cli_mode_file_path` | TEST361: CLI mode with file path - pass file path as command-line argument | src/bifaci/cartridge_runtime.rs:6643 |
| test362 | `test362_cli_mode_piped_binary` | TEST362: CLI mode with binary piped in - pipe binary data via stdin This test simulates real-world conditions: - Pure binary data piped to stdin (NOT CBOR) - CLI mode detected (command arg present) - Cap accepts stdin source - Binary is chunked on-the-fly and accumulated - Handler receives complete CBOR payload | src/bifaci/cartridge_runtime.rs:6694 |
| test363 | `test363_cbor_mode_chunked_content` | TEST363: CBOR mode with chunked content - send file content streaming as chunks | src/bifaci/cartridge_runtime.rs:6769 |
| test364 | `test364_cbor_mode_file_path` | TEST364: CBOR mode with file path - send file path in CBOR arguments (auto-conversion) | src/bifaci/cartridge_runtime.rs:6827 |
| test365 | `test365_stream_start_frame` | TEST365: Frame::stream_start stores request_id, stream_id, and media_urn | src/bifaci/frame.rs:1557 |
| test366 | `test366_stream_end_frame` | TEST366: Frame::stream_end stores request_id and stream_id | src/bifaci/frame.rs:1574 |
| test367 | `test367_stream_start_with_empty_stream_id` | TEST367: StreamStart frame with empty stream_id still constructs (validation happens elsewhere) | src/bifaci/frame.rs:1593 |
| test368 | `test368_stream_start_with_empty_media_urn` | TEST368: StreamStart frame with empty media_urn still constructs (validation happens elsewhere) | src/bifaci/frame.rs:1604 |
| test389 | `test389_stream_start_roundtrip` | TEST389: StreamStart encode/decode roundtrip preserves stream_id and media_urn | src/bifaci/io.rs:1749 |
| test390 | `test390_stream_end_roundtrip` | TEST390: StreamEnd encode/decode roundtrip preserves stream_id, no media_urn | src/bifaci/io.rs:1766 |
| test394 | `test394_peer_invoke_roundtrip` | TEST394: Test peer invoke round-trip (testcartridge calls itself) Disabled: LocalCartridgeRouter feature not implemented - uses non-existent modules | tests/orchestrator_integration.rs:860 |
| test395 | `test395_build_payload_small` | TEST395: Small payload (< max_chunk) produces correct CBOR arguments | src/bifaci/cartridge_runtime.rs:7033 |
| test396 | `test396_build_payload_large` | TEST396: Large payload (> max_chunk) accumulates across chunks correctly | src/bifaci/cartridge_runtime.rs:7079 |
| test397 | `test397_build_payload_empty` | TEST397: Empty reader produces valid empty CBOR arguments | src/bifaci/cartridge_runtime.rs:7123 |
| test398 | `test398_build_payload_io_error` | TEST398: IO error from reader propagates as RuntimeError::Io | src/bifaci/cartridge_runtime.rs:7164 |
| test399 | `test399_relay_notify_discriminant_roundtrip` | TEST399: Verify RelayNotify frame type discriminant roundtrips through u8 (value 10) | src/bifaci/frame.rs:1616 |
| test400 | `test400_relay_state_discriminant_roundtrip` | TEST400: Verify RelayState frame type discriminant roundtrips through u8 (value 11) | src/bifaci/frame.rs:1625 |
| test401 | `test401_relay_notify_frame` | TEST401: Verify relay_notify factory stores manifest and limits, and accessors extract them | src/bifaci/frame.rs:1634 |
| test402 | `test402_relay_state_frame` | TEST402: Verify relay_state factory stores resource payload in frame payload field | src/bifaci/frame.rs:1654 |
| test403 | `test403_invalid_frame_type_past_cancel` | TEST403: Verify from_u8 returns None for values past the last valid frame type | src/bifaci/frame.rs:1669 |
| test404 | `test404_slave_sends_relay_notify_on_connect` | TEST404: Slave sends RelayNotify on connect (initial_notify parameter) | src/bifaci/relay.rs:393 |
| test405 | `test405_master_reads_relay_notify` | TEST405: Master reads RelayNotify and extracts manifest + limits | src/bifaci/relay.rs:431 |
| test406 | `test406_slave_stores_relay_state` | TEST406: Slave stores RelayState from master | src/bifaci/relay.rs:461 |
| test407 | `test407_protocol_frames_pass_through` | TEST407: Protocol frames pass through slave transparently (both directions) | src/bifaci/relay.rs:510 |
| test408 | `test408_relay_frames_not_forwarded` | TEST408: RelayNotify/RelayState are NOT forwarded through relay | src/bifaci/relay.rs:613 |
| test409 | `test409_slave_injects_relay_notify_midstream` | TEST409: Slave can inject RelayNotify mid-stream (cap change) | src/bifaci/relay.rs:673 |
| test410 | `test410_master_receives_updated_relay_notify` | TEST410: Master receives updated RelayNotify (cap change callback via read_frame) | src/bifaci/relay.rs:750 |
| test411 | `test411_socket_close_detection` | TEST411: Socket close detection (both directions) | src/bifaci/relay.rs:841 |
| test412 | `test412_bidirectional_concurrent_flow` | TEST412: Bidirectional concurrent frame flow through relay | src/bifaci/relay.rs:882 |
| test413 | `test413_register_cartridge_adds_to_cap_table` | TEST413: Register cartridge adds entries to cap_table | src/bifaci/host_runtime.rs:2693 |
| test414 | `test414_capabilities_empty_initially` | TEST414: capabilities() returns empty JSON initially (no running cartridges) | src/bifaci/host_runtime.rs:2722 |
| test415 | `test415_req_for_known_cap_triggers_spawn` | TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary) | src/bifaci/host_runtime.rs:2745 |
| test416 | `test416_attach_cartridge_handshake_updates_capabilities` | TEST416: Attach cartridge performs HELLO handshake, extracts manifest, updates capabilities | src/bifaci/host_runtime.rs:2828 |
| test417 | `test417_route_req_to_correct_cartridge` | TEST417: Route REQ to correct cartridge by cap_urn (with two attached cartridges) | src/bifaci/host_runtime.rs:2882 |
| test418 | `test418_route_continuation_frames_by_req_id` | TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn) Verifies that after the initial REQ→cartridge routing, all subsequent continuation frames with the same req_id are routed to the same cartridge — even though no cap_urn is present on those frames. | src/bifaci/host_runtime.rs:3282 |
| test419 | `test419_cartridge_heartbeat_handled_locally` | TEST419: Cartridge HEARTBEAT handled locally (not forwarded to relay) | src/bifaci/host_runtime.rs:3042 |
| test420 | `test420_cartridge_frames_forwarded_to_relay` | TEST420: Cartridge non-HELLO/non-HB frames forwarded to relay (pass-through) | src/bifaci/host_runtime.rs:3117 |
| test421 | `test421_cartridge_death_updates_capabilities` | TEST421: Cartridge death updates capability list (caps removed) | src/bifaci/host_runtime.rs:3444 |
| test422 | `test422_cartridge_death_sends_err_for_pending_requests` | TEST422: Cartridge death sends ERR for all pending requests via relay | src/bifaci/host_runtime.rs:3536 |
| test423 | `test423_multiple_cartridges_route_independently` | TEST423: Multiple cartridges registered with distinct caps route independently | src/bifaci/host_runtime.rs:3627 |
| test424 | `test424_concurrent_requests_to_same_cartridge` | TEST424: Concurrent requests to the same cartridge are handled independently | src/bifaci/host_runtime.rs:3820 |
| test425 | `test425_find_cartridge_for_cap_unknown` | TEST425: find_cartridge_for_cap returns None for unregistered cap | src/bifaci/host_runtime.rs:3980 |
| test426 | `test426_single_master_req_response` | TEST426: Single master REQ/response routing | src/bifaci/relay_switch.rs:2842 |
| test427 | `test427_multi_master_cap_routing` | TEST427: Multi-master cap routing | src/bifaci/relay_switch.rs:2906 |
| test428 | `test428_unknown_cap_returns_error` | TEST428: Unknown cap returns error | src/bifaci/relay_switch.rs:3019 |
| test429 | `test429_find_master_for_cap` | TEST429: Cap routing logic (find_master_for_cap) | src/bifaci/relay_switch.rs:2783 |
| test430 | `test430_tie_breaking_same_cap_multiple_masters` | TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent) | src/bifaci/relay_switch.rs:3051 |
| test431 | `test431_continuation_frame_routing` | TEST431: Continuation frame routing (CHUNK, END follow REQ) | src/bifaci/relay_switch.rs:3154 |
| test432 | `test432_empty_masters_allowed` | TEST432: Empty masters list creates empty switch, add_master works | src/bifaci/relay_switch.rs:3238 |
| test433 | `test433_capability_aggregation_deduplicates` | TEST433: Capability aggregation deduplicates caps | src/bifaci/relay_switch.rs:3256 |
| test434 | `test434_limits_negotiation_minimum` | TEST434: Limits negotiation takes minimum | src/bifaci/relay_switch.rs:3305 |
| test435 | `test435_urn_matching_exact_and_accepts` | TEST435: URN matching (exact vs accepts()) | src/bifaci/relay_switch.rs:3346 |
| test436 | `test436_compute_checksum` | TEST436: Verify FNV-1a checksum function produces consistent results | src/bifaci/frame.rs:1687 |
| test437 | `test437_preferred_cap_routes_to_generic` | TEST437: find_master_for_cap with preferred_cap routes to generic handler With is_dispatchable semantics: - Generic provider (in=media:) CAN dispatch specific request (in="media:pdf") because media: (wildcard) accepts any input type - Preference routes to preferred among dispatchable candidates | src/bifaci/relay_switch.rs:3449 |
| test438 | `test438_preferred_cap_falls_back_when_not_comparable` | TEST438: find_master_for_cap with preference falls back to closest-specificity when preferred cap is not in the comparable set | src/bifaci/relay_switch.rs:3505 |
| test439 | `test439_generic_provider_can_dispatch_specific_request` | TEST439: Generic provider CAN dispatch specific request (but only matches if no more specific provider exists) With is_dispatchable: generic provider (in=media:) CAN handle specific request (in="media:pdf") because media: accepts any input type. With preference, can route to generic even when more specific exists. | src/bifaci/relay_switch.rs:3543 |
| test440 | `test440_chunk_index_checksum_roundtrip` | TEST440: CHUNK frame with chunk_index and checksum roundtrips through encode/decode | src/bifaci/io.rs:1829 |
| test441 | `test441_stream_end_chunk_count_roundtrip` | TEST441: STREAM_END frame with chunk_count roundtrips through encode/decode | src/bifaci/io.rs:1858 |
| test442 | `test442_seq_assigner_monotonic_same_rid` | TEST442: SeqAssigner assigns seq 0,1,2,3 for consecutive frames with same RID | src/bifaci/frame.rs:1763 |
| test443 | `test443_seq_assigner_independent_rids` | TEST443: SeqAssigner maintains independent counters for different RIDs | src/bifaci/frame.rs:1785 |
| test444 | `test444_seq_assigner_skips_non_flow` | TEST444: SeqAssigner skips non-flow frames (Heartbeat, RelayNotify, RelayState, Hello) | src/bifaci/frame.rs:1811 |
| test445 | `test445_seq_assigner_remove_by_flow_key` | TEST445: SeqAssigner.remove with FlowKey(rid, None) resets that flow; FlowKey(rid, Some(xid)) is unaffected | src/bifaci/frame.rs:1832 |
| test446 | `test446_seq_assigner_mixed_types` | TEST446: SeqAssigner handles mixed frame types (REQ, CHUNK, LOG, END) for same RID | src/bifaci/frame.rs:1912 |
| test447 | `test447_flow_key_with_xid` | TEST447: FlowKey::from_frame extracts (rid, Some(xid)) when routing_id present | src/bifaci/frame.rs:1938 |
| test448 | `test448_flow_key_without_xid` | TEST448: FlowKey::from_frame extracts (rid, None) when routing_id absent | src/bifaci/frame.rs:1951 |
| test449 | `test449_flow_key_equality` | TEST449: FlowKey equality: same rid+xid equal, different xid different key | src/bifaci/frame.rs:1962 |
| test450 | `test450_flow_key_hash_lookup` | TEST450: FlowKey hash: same keys hash equal (HashMap lookup) | src/bifaci/frame.rs:2000 |
| test451 | `test451_reorder_buffer_in_order` | TEST451: ReorderBuffer in-order delivery: seq 0,1,2 delivered immediately | src/bifaci/frame.rs:2036 |
| test452 | `test452_reorder_buffer_out_of_order` | TEST452: ReorderBuffer out-of-order: seq 1 then 0 delivers both in order | src/bifaci/frame.rs:2055 |
| test453 | `test453_reorder_buffer_gap_fill` | TEST453: ReorderBuffer gap fill: seq 0,2,1 delivers 0, buffers 2, then delivers 1+2 | src/bifaci/frame.rs:2070 |
| test454 | `test454_reorder_buffer_stale_seq` | TEST454: ReorderBuffer stale seq is hard error | src/bifaci/frame.rs:2088 |
| test455 | `test455_reorder_buffer_overflow` | TEST455: ReorderBuffer overflow triggers protocol error | src/bifaci/frame.rs:2107 |
| test456 | `test456_reorder_buffer_independent_flows` | TEST456: Multiple concurrent flows reorder independently | src/bifaci/frame.rs:2127 |
| test457 | `test457_reorder_buffer_cleanup` | TEST457: cleanup_flow removes state; new frames start at seq 0 | src/bifaci/frame.rs:2150 |
| test458 | `test458_reorder_buffer_non_flow_bypass` | TEST458: Non-flow frames bypass reorder entirely | src/bifaci/frame.rs:2170 |
| test459 | `test459_reorder_buffer_end_frame` | TEST459: Terminal END frame flows through correctly | src/bifaci/frame.rs:2202 |
| test460 | `test460_reorder_buffer_err_frame` | TEST460: Terminal ERR frame flows through correctly | src/bifaci/frame.rs:2220 |
| test461 | `test461_write_chunked_seq_zero` | TEST461: write_chunked produces frames with seq=0; SeqAssigner assigns at output stage | src/bifaci/io.rs:1875 |
| test472 | `test472_handshake_negotiates_reorder_buffer` | TEST472: Handshake negotiates max_reorder_buffer (minimum of both sides) | src/bifaci/io.rs:1928 |
| test473 | `test473_cap_discard_parses_as_valid_urn` | TEST473: CAP_DISCARD parses as valid CapUrn with in=media: and out=media:void | src/standard/caps.rs:1091 |
| test474 | `test474_cap_discard_accepts_specific_void_cap` | TEST474: CAP_DISCARD accepts specific-input/void-output caps | src/standard/caps.rs:1110 |
| test475 | `test475_validate_passes_with_identity` | TEST475: validate() passes with CAP_IDENTITY in a cap group | src/bifaci/manifest.rs:520 |
| test476 | `test476_validate_fails_without_identity` | TEST476: validate() fails without CAP_IDENTITY | src/bifaci/manifest.rs:536 |
| test478 | `test478_auto_registers_identity_handler` | TEST478: CartridgeRuntime auto-registers identity and discard handlers on construction | src/bifaci/cartridge_runtime.rs:7201 |
| test479 | `test479_custom_identity_overrides_default` | TEST479: Custom identity Op overrides auto-registered default | src/bifaci/cartridge_runtime.rs:7269 |
| test480 | `test480_parse_caps_rejects_manifest_without_identity` | TEST480: parse_caps_from_manifest classifies failures by kind Manifest JSON that parses but lacks CAP_IDENTITY is `Incompatible` (schema-rejected). Manifest bytes that don't parse as CapManifest are `ManifestInvalid` (JSON-level failure). The split lets the host's attachment-error reporter surface the right kind to the UI. | src/bifaci/host_runtime.rs:2441 |
| test481 | `test481_verify_identity_succeeds` | TEST481: verify_identity succeeds with standard identity echo handler | src/bifaci/io.rs:2031 |
| test482 | `test482_verify_identity_fails_on_err` | TEST482: verify_identity fails when cartridge returns ERR on identity call | src/bifaci/io.rs:2058 |
| test483 | `test483_verify_identity_fails_on_close` | TEST483: verify_identity fails when connection closes before response | src/bifaci/io.rs:2098 |
| test485 | `test485_attach_cartridge_identity_verification_succeeds` | TEST485: attach_cartridge completes identity verification with working cartridge | src/bifaci/host_runtime.rs:4002 |
| test486 | `test486_attach_cartridge_identity_verification_fails` | TEST486: attach_cartridge rejects cartridge that fails identity verification | src/bifaci/host_runtime.rs:4041 |
| test487 | `test487_relay_switch_identity_verification_succeeds` | TEST487: RelaySwitch construction verifies identity through relay chain | src/bifaci/relay_switch.rs:3585 |
| test488 | `test488_relay_switch_identity_verification_fails` | TEST488: RelaySwitch construction fails when master's identity verification fails | src/bifaci/relay_switch.rs:3621 |
| test489 | `test489_add_master_dynamic` | TEST489: add_master dynamically connects new host to running switch | src/bifaci/relay_switch.rs:3826 |
| test490 | `test490_identity_verification_multiple_cartridges` | TEST490: Identity verification with multiple cartridges through single relay Both cartridges must pass identity verification independently before any real requests are routed. | src/bifaci/integration_tests.rs:1497 |
| test491 | `test491_chunk_requires_chunk_index_and_checksum` | TEST491: Frame::chunk constructor requires and sets chunk_index and checksum | src/bifaci/frame.rs:2242 |
| test492 | `test492_stream_end_requires_chunk_count` | TEST492: Frame::stream_end constructor requires and sets chunk_count | src/bifaci/frame.rs:2264 |
| test493 | `test493_compute_checksum_fnv1a_test_vectors` | TEST493: compute_checksum produces correct FNV-1a hash for known test vectors | src/bifaci/frame.rs:2276 |
| test494 | `test494_compute_checksum_deterministic` | TEST494: compute_checksum is deterministic | src/bifaci/frame.rs:2297 |
| test495 | `test495_cbor_rejects_chunk_without_chunk_index` | TEST495: CBOR decode REJECTS CHUNK frame missing chunk_index field | src/bifaci/frame.rs:2309 |
| test496 | `test496_cbor_rejects_chunk_without_checksum` | TEST496: CBOR decode REJECTS CHUNK frame missing checksum field | src/bifaci/frame.rs:2341 |
| test497 | `test497_chunk_corrupted_payload_rejected` | TEST497: Verify CHUNK frame with corrupted payload is rejected by checksum | src/bifaci/io.rs:1785 |
| test498 | `test498_routing_id_cbor_roundtrip` | TEST498: routing_id field roundtrips through CBOR encoding | src/bifaci/frame.rs:2397 |
| test499 | `test499_chunk_index_checksum_cbor_roundtrip` | TEST499: chunk_index and checksum roundtrip through CBOR encoding | src/bifaci/frame.rs:2424 |
| test500 | `test500_chunk_count_cbor_roundtrip` | TEST500: chunk_count roundtrips through CBOR encoding | src/bifaci/frame.rs:2450 |
| test501 | `test501_frame_new_initializes_optional_fields_none` | TEST501: Frame::new initializes new fields to None | src/bifaci/frame.rs:2466 |
| test502 | `test502_keys_module_new_field_constants` | TEST502: Keys module has constants for new fields | src/bifaci/frame.rs:2477 |
| test503 | `test503_compute_checksum_empty_data` | TEST503: compute_checksum handles empty data correctly | src/bifaci/frame.rs:2487 |
| test504 | `test504_compute_checksum_large_payload` | TEST504: compute_checksum handles large payloads without overflow | src/bifaci/frame.rs:2497 |
| test505 | `test505_chunk_with_offset_sets_chunk_index` | TEST505: chunk_with_offset sets chunk_index correctly | src/bifaci/frame.rs:2509 |
| test506 | `test506_compute_checksum_different_data_different_hash` | TEST506: Different data produces different checksums | src/bifaci/frame.rs:2533 |
| test507 | `test507_reorder_buffer_xid_isolation` | TEST507: ReorderBuffer isolates flows by XID (routing_id) - same RID different XIDs | src/bifaci/frame.rs:2549 |
| test508 | `test508_reorder_buffer_duplicate_buffered_seq` | TEST508: ReorderBuffer rejects duplicate seq already in buffer | src/bifaci/frame.rs:2577 |
| test509 | `test509_reorder_buffer_large_gap_rejected` | TEST509: ReorderBuffer handles large seq gaps without DOS | src/bifaci/frame.rs:2597 |
| test510 | `test510_reorder_buffer_multiple_gaps` | TEST510: ReorderBuffer with multiple interleaved gaps fills correctly | src/bifaci/frame.rs:2629 |
| test511 | `test511_reorder_buffer_cleanup_with_buffered_frames` | TEST511: ReorderBuffer cleanup with buffered frames discards them | src/bifaci/frame.rs:2662 |
| test512 | `test512_reorder_buffer_burst_delivery` | TEST512: ReorderBuffer delivers burst of consecutive buffered frames | src/bifaci/frame.rs:2688 |
| test513 | `test513_reorder_buffer_mixed_types_same_flow` | TEST513: ReorderBuffer different frame types in same flow maintain order | src/bifaci/frame.rs:2708 |
| test514 | `test514_reorder_buffer_xid_cleanup_isolation` | TEST514: ReorderBuffer with XID cleanup doesn't affect different XID | src/bifaci/frame.rs:2733 |
| test515 | `test515_reorder_buffer_overflow_error_details` | TEST515: ReorderBuffer overflow error includes diagnostic information | src/bifaci/frame.rs:2761 |
| test516 | `test516_reorder_buffer_stale_error_details` | TEST516: ReorderBuffer stale error includes diagnostic information | src/bifaci/frame.rs:2787 |
| test517 | `test517_flow_key_none_vs_some_xid` | TEST517: FlowKey with None XID differs from Some(xid) | src/bifaci/frame.rs:2810 |
| test518 | `test518_reorder_buffer_empty_ready_vec` | TEST518: ReorderBuffer handles zero-length ready vec correctly | src/bifaci/frame.rs:2842 |
| test519 | `test519_reorder_buffer_state_persistence` | TEST519: ReorderBuffer state persists across accept calls | src/bifaci/frame.rs:2854 |
| test520 | `test520_reorder_buffer_per_flow_limit` | TEST520: ReorderBuffer max_buffer_per_flow is per-flow not global | src/bifaci/frame.rs:2872 |
| test521 | `test521_relay_notify_cbor_roundtrip` | TEST521: RelayNotify CBOR roundtrip preserves manifest and limits | src/bifaci/frame.rs:2900 |
| test522 | `test522_relay_state_cbor_roundtrip` | TEST522: RelayState CBOR roundtrip preserves payload | src/bifaci/frame.rs:2940 |
| test523 | `test523_relay_notify_not_flow_frame` | TEST523: is_flow_frame returns false for RelayNotify | src/bifaci/frame.rs:2960 |
| test524 | `test524_relay_state_not_flow_frame` | TEST524: is_flow_frame returns false for RelayState | src/bifaci/frame.rs:2973 |
| test525 | `test525_relay_notify_empty_manifest` | TEST525: RelayNotify with empty manifest is valid | src/bifaci/frame.rs:2985 |
| test526 | `test526_relay_state_empty_payload` | TEST526: RelayState with empty payload is valid | src/bifaci/frame.rs:2999 |
| test527 | `test527_relay_notify_large_manifest` | TEST527: RelayNotify with large manifest roundtrips correctly | src/bifaci/frame.rs:3009 |
| test528 | `test528_relay_frames_use_uint_zero_id` | TEST528: RelayNotify and RelayState use MessageId::Uint(0) | src/bifaci/frame.rs:3041 |
| test529 | `test529_input_stream_recv_order` | TEST529: InputStream recv yields chunks in order | src/bifaci/cartridge_runtime.rs:7347 |
| test530 | `test530_input_stream_collect_bytes` | TEST530: InputStream::collect_bytes concatenates byte chunks | src/bifaci/cartridge_runtime.rs:7376 |
| test531 | `test531_input_stream_collect_bytes_text` | TEST531: InputStream::collect_bytes handles text chunks | src/bifaci/cartridge_runtime.rs:7390 |
| test532 | `test532_input_stream_empty` | TEST532: InputStream empty stream produces empty bytes | src/bifaci/cartridge_runtime.rs:7403 |
| test533 | `test533_input_stream_error_propagation` | TEST533: InputStream propagates errors | src/bifaci/cartridge_runtime.rs:7416 |
| test534 | `test534_input_stream_media_urn` | TEST534: InputStream::media_urn returns correct URN | src/bifaci/cartridge_runtime.rs:7435 |
| test535 | `test535_input_package_iteration` | TEST535: InputPackage recv yields streams | src/bifaci/cartridge_runtime.rs:7444 |
| test536 | `test536_input_package_collect_all_bytes` | TEST536: InputPackage::collect_all_bytes aggregates all streams | src/bifaci/cartridge_runtime.rs:7487 |
| test537 | `test537_input_package_empty` | TEST537: InputPackage empty package produces empty bytes | src/bifaci/cartridge_runtime.rs:7529 |
| test538 | `test538_input_package_error_propagation` | TEST538: InputPackage propagates stream errors | src/bifaci/cartridge_runtime.rs:7547 |
| test539 | `test539_output_stream_sends_stream_start` | TEST539: OutputStream sends STREAM_START on first write | src/bifaci/cartridge_runtime.rs:7611 |
| test540 | `test540_output_stream_close_sends_stream_end` | TEST540: OutputStream::close sends STREAM_END with correct chunk_count | src/bifaci/cartridge_runtime.rs:7639 |
| test541 | `test541_output_stream_chunks_large_data` | TEST541: OutputStream chunks large data correctly | src/bifaci/cartridge_runtime.rs:7669 |
| test542 | `test542_output_stream_empty` | TEST542: OutputStream empty stream sends STREAM_START and STREAM_END only | src/bifaci/cartridge_runtime.rs:7702 |
| test543 | `test543_peer_call_arg_creates_stream` | TEST543: PeerCall::arg creates OutputStream with correct stream_id | src/bifaci/cartridge_runtime.rs:7733 |
| test544 | `test544_peer_call_finish_sends_end` | TEST544: PeerCall::finish sends END frame | src/bifaci/cartridge_runtime.rs:7754 |
| test545 | `test545_peer_call_finish_returns_response_stream` | TEST545: PeerCall::finish returns PeerResponse with data | src/bifaci/cartridge_runtime.rs:7782 |
| test546 | `test546_is_image` | TEST546: is_image returns true only when image marker tag is present | src/urn/media_urn.rs:997 |
| test547 | `test547_is_audio` | TEST547: is_audio returns true only when audio marker tag is present | src/urn/media_urn.rs:1012 |
| test548 | `test548_is_video` | TEST548: is_video returns true only when video marker tag is present | src/urn/media_urn.rs:1026 |
| test549 | `test549_is_numeric` | TEST549: is_numeric returns true only when numeric marker tag is present | src/urn/media_urn.rs:1037 |
| test550 | `test550_is_bool` | TEST550: is_bool returns true only when bool marker tag is present | src/urn/media_urn.rs:1054 |
| test551 | `test551_is_file_path` | TEST551: is_file_path returns true for the single file-path media URN, false for everything else. There is no "array" variant — cardinality is carried by is_sequence on the wire, not by URN tags. | src/urn/media_urn.rs:1069 |
| test555 | `test555_with_tag_and_without_tag` | TEST555: with_tag adds a tag and without_tag removes it | src/urn/media_urn.rs:1081 |
| test556 | `test556_image_media_urn_for_ext` | TEST556: image_media_urn_for_ext creates valid image media URN | src/urn/media_urn.rs:1098 |
| test557 | `test557_audio_media_urn_for_ext` | TEST557: audio_media_urn_for_ext creates valid audio media URN | src/urn/media_urn.rs:1111 |
| test558 | `test558_predicate_constant_consistency` | TEST558: predicates are consistent with constants — every constant triggers exactly the expected predicates | src/urn/media_urn.rs:1124 |
| test559 | `test559_without_tag` | TEST559: without_tag removes tag, ignores in/out, case-insensitive for keys | src/urn/cap_urn.rs:2265 |
| test560 | `test560_with_in_out_spec` | TEST560: with_in_spec and with_out_spec change direction specs | src/urn/cap_urn.rs:2289 |
| test561 | `test561_in_out_media_urn` | TEST561: in_media_urn and out_media_urn parse direction specs into MediaUrn | src/urn/cap_urn.rs:2311 |
| test562 | `test562_canonical_option` | TEST562: canonical_option returns None for None input, canonical string for Some | src/urn/cap_urn.rs:2339 |
| test563 | `test563_find_all_matches` | TEST563: CapMatcher::find_all_matches returns all matching caps sorted by specificity | src/urn/cap_urn.rs:2361 |
| test564 | `test564_are_compatible` | TEST564: CapMatcher::are_compatible detects bidirectional overlap | src/urn/cap_urn.rs:2381 |
| test565 | `test565_tags_to_string` | TEST565: tags_to_string returns only tags portion without prefix | src/urn/cap_urn.rs:2408 |
| test566 | `test566_with_tag_ignores_in_out` | TEST566: with_tag silently ignores in/out keys | src/urn/cap_urn.rs:2419 |
| test567 | `test567_str_variants` | TEST567: conforms_to_str and accepts_str work with string arguments | src/urn/cap_urn.rs:2445 |
| test568 | `test568_dispatch_output_tag_order` | TEST568: is_dispatchable with different tag order in output spec | src/urn/cap_urn.rs:2468 |
| test578 | `test578_rule1_duplicate_media_urns` | TEST578: RULE1 - duplicate media_urns rejected | src/cap/validation.rs:1470 |
| test579 | `test579_rule2_empty_sources` | TEST579: RULE2 - empty sources rejected | src/cap/validation.rs:1491 |
| test580 | `test580_rule3_different_stdin_urns` | TEST580: RULE3 - multiple stdin sources with different URNs rejected | src/cap/validation.rs:1503 |
| test581 | `test581_rule3_same_stdin_urns_ok` | TEST581: RULE3 - multiple stdin sources with same URN is OK | src/cap/validation.rs:1528 |
| test582 | `test582_rule4_duplicate_source_type` | TEST582: RULE4 - duplicate source type in single arg rejected | src/cap/validation.rs:1555 |
| test583 | `test583_rule5_duplicate_position` | TEST583: RULE5 - duplicate position across args rejected | src/cap/validation.rs:1572 |
| test584 | `test584_rule6_position_gap` | TEST584: RULE6 - position gap rejected (0, 2 without 1) | src/cap/validation.rs:1593 |
| test585 | `test585_rule6_sequential_ok` | TEST585: RULE6 - sequential positions (0, 1, 2) pass | src/cap/validation.rs:1614 |
| test586 | `test586_rule7_position_and_cli_flag` | TEST586: RULE7 - arg with both position and cli_flag rejected | src/cap/validation.rs:1637 |
| test587 | `test587_rule9_duplicate_cli_flag` | TEST587: RULE9 - duplicate cli_flag across args rejected | src/cap/validation.rs:1656 |
| test588 | `test588_rule10_reserved_cli_flags` | TEST588: RULE10 - reserved cli_flags rejected | src/cap/validation.rs:1681 |
| test589 | `test589_all_rules_pass` | TEST589: valid cap args with mixed sources pass all rules | src/cap/validation.rs:1708 |
| test590 | `test590_cli_flag_only_args` | TEST590: validate_cap_args accepts cap with only cli_flag sources (no positions) | src/cap/validation.rs:1814 |
| test591 | `test591_is_more_specific_than` | TEST591: is_more_specific_than returns true when self has more tags for same request | src/cap/definition.rs:1180 |
| test592 | `test592_remove_metadata` | TEST592: remove_metadata adds then removes metadata correctly | src/cap/definition.rs:1216 |
| test593 | `test593_registered_by_lifecycle` | TEST593: registered_by lifecycle — set, get, clear | src/cap/definition.rs:1236 |
| test594 | `test594_metadata_json_lifecycle` | TEST594: metadata_json lifecycle — set, get, clear | src/cap/definition.rs:1257 |
| test595 | `test595_with_args_constructor` | TEST595: with_args constructor stores args correctly | src/cap/definition.rs:1276 |
| test596 | `test596_with_full_definition_constructor` | TEST596: with_full_definition constructor stores all fields | src/cap/definition.rs:1303 |
| test597 | `test597_cap_arg_with_full_definition` | TEST597: CapArg::with_full_definition stores all fields including optional ones | src/cap/definition.rs:1337 |
| test598 | `test598_cap_output_lifecycle` | TEST598: CapOutput lifecycle — set_output, set/clear metadata | src/cap/definition.rs:1369 |
| test599 | `test599_is_empty` | TEST599: is_empty returns true for empty response, false for non-empty | src/cap/response.rs:318 |
| test600 | `test600_size` | TEST600: size returns exact byte count for all content types | src/cap/response.rs:334 |
| test601 | `test601_get_content_type` | TEST601: get_content_type returns correct MIME type for each variant | src/cap/response.rs:350 |
| test602 | `test602_as_type_binary_error` | TEST602: as_type on binary response returns error (cannot deserialize binary) | src/cap/response.rs:363 |
| test603 | `test603_as_bool_edge_cases` | TEST603: as_bool handles all accepted truthy/falsy variants and rejects garbage | src/cap/response.rs:380 |
| test605 | `test605_all_coercion_paths_build_valid_urns` | TEST605: all_coercion_paths each entry builds a valid parseable CapUrn | src/standard/caps.rs:1138 |
| test606 | `test606_coercion_urn_specs` | TEST606: coercion_urn in/out specs match the type's media URN constant | src/standard/caps.rs:1166 |
| test607 | `test607_media_urns_for_extension_unknown` | TEST607: media_urns_for_extension returns error for unknown extension | src/media/registry.rs:810 |
| test608 | `test608_media_urns_for_extension_populated` | TEST608: media_urns_for_extension returns URNs after adding a spec with extensions | src/media/registry.rs:828 |
| test609 | `test609_get_extension_mappings` | TEST609: get_extension_mappings returns all registered extension->URN pairs | src/media/registry.rs:871 |
| test610 | `test610_get_cached_spec` | TEST610: get_cached_spec returns None for unknown and Some for known | src/media/registry.rs:909 |
| test611 | `test611_is_embedded_profile_comprehensive` | TEST611: is_embedded_profile recognizes all 9 embedded profiles and rejects non-embedded | src/media/profile.rs:730 |
| test612 | `test612_clear_cache` | TEST612: clear_cache empties all in-memory schemas | src/media/profile.rs:763 |
| test613 | `test613_validate_cached` | TEST613: validate_cached validates against cached standard schemas | src/media/profile.rs:780 |
| test614 | `test614_registry_creation` | TEST614: Verify registry creation succeeds and cache directory exists | src/media/registry.rs:755 |
| test615 | `test615_cache_key_generation` | TEST615: Verify cache key generation is deterministic and distinct for different URNs | src/media/registry.rs:762 |
| test616 | `test616_stored_media_spec_to_def` | TEST616: Verify StoredMediaSpec converts to MediaSpecDef preserving all fields | src/media/registry.rs:774 |
| test617 | `test617_normalize_media_urn` | TEST617: Verify normalize_media_urn produces consistent non-empty results | src/media/registry.rs:799 |
| test618 | `test618_registry_creation` | TEST618: Verify profile schema registry creation succeeds with temp cache | src/media/profile.rs:575 |
| test619 | `test619_embedded_schemas_loaded` | TEST619: Verify all 9 embedded standard schemas are loaded on creation | src/media/profile.rs:582 |
| test620 | `test620_string_validation` | TEST620: Verify string schema validates strings and rejects non-strings | src/media/profile.rs:599 |
| test621 | `test621_integer_validation` | TEST621: Verify integer schema validates integers and rejects floats and strings | src/media/profile.rs:614 |
| test622 | `test622_number_validation` | TEST622: Verify number schema validates integers and floats, rejects strings | src/media/profile.rs:632 |
| test623 | `test623_boolean_validation` | TEST623: Verify boolean schema validates true/false and rejects string "true" | src/media/profile.rs:650 |
| test624 | `test624_object_validation` | TEST624: Verify object schema validates objects and rejects arrays | src/media/profile.rs:666 |
| test625 | `test625_string_array_validation` | TEST625: Verify string array schema validates string arrays and rejects mixed arrays | src/media/profile.rs:684 |
| test626 | `test626_unknown_profile_skips_validation` | TEST626: Verify unknown profile URL skips validation and returns Ok | src/media/profile.rs:708 |
| test627 | `test627_is_embedded_profile` | TEST627: Verify is_embedded_profile recognizes standard and rejects custom URLs | src/media/profile.rs:720 |
| test628 | `test628_media_urn_constants_format` | TEST628: Verify media URN constants all start with "media:" prefix | src/standard/media.rs:117 |
| test629 | `test629_profile_constants_format` | TEST629: Verify profile URL constants all start with capdag.com schema prefix | src/standard/media.rs:127 |
| test630 | `test630_cartridge_repo_creation` | TEST630: CartridgeRepo creation starts with empty cartridge list. | src/bifaci/cartridge_repo.rs:1195 |
| test631 | `test631_needs_sync_empty_cache` | TEST631: needs_sync returns true with empty cache and non-empty URLs. | src/bifaci/cartridge_repo.rs:1202 |
| test632 | `test632_deserialize_minimal_registry_cap` | TEST632: A registry cap with only the three required fields parses. | src/bifaci/cartridge_repo.rs:1216 |
| test633 | `test633_deserialize_rich_registry_cap` | TEST633: A registry cap with cap_description, args, output all parses. | src/bifaci/cartridge_repo.rs:1230 |
| test634 | `test634_deserialize_cap_group` | TEST634: A registry cap_group parses with caps + adapter_urns. | src/bifaci/cartridge_repo.rs:1269 |
| test635 | `test635_deserialize_cartridge_info_wire_shape` | TEST635: CartridgeInfo deserializes the wire shape exactly as returned by /api/cartridges (camelCase top-level + snake_case cap_groups). Null camelCase string fields fall back to empty. | src/bifaci/cartridge_repo.rs:1287 |
| test636 | `test636_deserialize_cartridge_info_with_null_strings` | TEST636: CartridgeInfo with null version/description/author still deserializes (the null_as_empty_string deserializer is the only tolerated coercion — every other malformed input is a hard error). | src/bifaci/cartridge_repo.rs:1329 |
| test637 | `test637_deserialize_full_registry_response` | TEST637: A full /api/cartridges-shaped response with two cartridges and nested cap_groups round-trips through the response wrapper. | src/bifaci/cartridge_repo.rs:1351 |
| test638 | `test638_no_peer_router_rejects_all` | TEST638: Verify NoPeerRouter rejects all requests with PeerInvokeNotSupported | src/bifaci/router.rs:95 |
| test639 | `test639_wildcard_001_empty_cap_defaults_to_media_wildcard` | TEST639: cap: (empty) defaults to in=media:;out=media: | src/urn/cap_urn.rs:2028 |
| test640 | `test640_wildcard_002_in_only_defaults_out_to_media` | TEST640: cap:in defaults out to media: | src/urn/cap_urn.rs:2037 |
| test641 | `test641_wildcard_003_out_only_defaults_in_to_media` | TEST641: cap:out defaults in to media: | src/urn/cap_urn.rs:2045 |
| test642 | `test642_wildcard_004_in_out_no_values_become_media` | TEST642: cap:in;out both become media: | src/urn/cap_urn.rs:2053 |
| test643 | `test643_wildcard_005_explicit_asterisk_becomes_media` | TEST643: cap:in=*;out=* becomes media: | src/urn/cap_urn.rs:2061 |
| test644 | `test644_wildcard_006_specific_in_wildcard_out` | TEST644: cap:in=media:;out=* has specific in, wildcard out | src/urn/cap_urn.rs:2069 |
| test645 | `test645_wildcard_007_wildcard_in_specific_out` | TEST645: cap:in=*;out=media:text has wildcard in, specific out | src/urn/cap_urn.rs:2077 |
| test646 | `test646_wildcard_008_invalid_in_spec_fails` | TEST646: cap:in=foo fails (invalid media URN) | src/urn/cap_urn.rs:2086 |
| test647 | `test647_wildcard_009_invalid_out_spec_fails` | TEST647: cap:in=media:;out=bar fails (invalid media URN) | src/urn/cap_urn.rs:2095 |
| test648 | `test648_wildcard_010_wildcard_accepts_specific` | TEST648: Wildcard in/out match specific caps | src/urn/cap_urn.rs:2104 |
| test649 | `test649_wildcard_011_specificity_scoring` | TEST649: Specificity - wildcard has 0, specific has tag count | src/urn/cap_urn.rs:2120 |
| test650 | `test650_wildcard_012_preserve_other_tags` | TEST650: cap:in;out;op=test preserves other tags | src/urn/cap_urn.rs:2137 |
| test651 | `test651_wildcard_013_identity_forms_equivalent` | TEST651: All identity forms produce the same CapUrn | src/urn/cap_urn.rs:2146 |
| test652 | `test652_wildcard_014_cap_identity_constant_works` | TEST652: CAP_IDENTITY constant matches identity caps regardless of string form | src/urn/cap_urn.rs:2189 |
| test653 | `test653_wildcard_015_identity_routing_isolation` | TEST653: Identity (no tags) does not match specific requests via routing | src/urn/cap_urn.rs:2231 |
| test654 | `test654_routes_req_to_handler` | TEST654: InProcessCartridgeHost routes REQ to matching handler and returns response | src/bifaci/in_process_host.rs:923 |
| test655 | `test655_identity_verification` | TEST655: InProcessCartridgeHost handles identity verification (echo nonce) | src/bifaci/in_process_host.rs:1001 |
| test656 | `test656_no_handler_returns_err` | TEST656: InProcessCartridgeHost returns NO_HANDLER for unregistered cap | src/bifaci/in_process_host.rs:1070 |
| test657 | `test657_manifest_includes_all_caps` | TEST657: InProcessCartridgeHost manifest includes identity cap and handler caps | src/bifaci/in_process_host.rs:1108 |
| test658 | `test658_heartbeat_response` | TEST658: InProcessCartridgeHost handles heartbeat by echoing same ID | src/bifaci/in_process_host.rs:1126 |
| test659 | `test659_handler_error_returns_err_frame` | TEST659: InProcessCartridgeHost handler error returns ERR frame | src/bifaci/in_process_host.rs:1156 |
| test660 | `test660_closest_specificity_routing` | TEST660: InProcessCartridgeHost closest-specificity routing prefers specific over identity | src/bifaci/in_process_host.rs:1226 |
| test661 | `test661_cartridge_death_keeps_known_caps_advertised` | TEST661: Cartridge death keeps known_caps advertised for on-demand respawn | src/bifaci/host_runtime.rs:4085 |
| test662 | `test662_rebuild_capabilities_includes_non_running_cartridges` | TEST662: rebuild_capabilities includes non-running cartridges' known_caps | src/bifaci/host_runtime.rs:4122 |
| test663 | `test663_hello_failed_cartridge_removed_from_capabilities` | TEST663: Cartridge with hello_failed is permanently removed from capabilities | src/bifaci/host_runtime.rs:4158 |
| test664 | `test664_running_cartridge_uses_manifest_caps` | TEST664: Running cartridge uses manifest caps, not known_caps | src/bifaci/host_runtime.rs:4204 |
| test665 | `test665_cap_table_mixed_running_and_non_running` | TEST665: Cap table uses manifest caps for running, known_caps for non-running | src/bifaci/host_runtime.rs:4264 |
| test666 | `test666_preferred_cap_routing` | TEST666: Preferred cap routing - routes to exact equivalent when multiple masters match | src/bifaci/relay_switch.rs:3992 |
| test667 | `test667_verify_chunk_checksum_detects_corruption` | TEST667: verify_chunk_checksum detects corrupted payload | src/bifaci/frame.rs:3063 |
| test668 | `test668_resolve_slot_with_populated_byte_slot_values` | TEST668: resolve_binding returns byte values when slot is populated with data | src/planner/argument_binding.rs:800 |
| test669 | `test669_resolve_slot_falls_back_to_default` | TEST669: resolve_binding falls back to cap default value when slot has no data | src/planner/argument_binding.rs:836 |
| test670 | `test670_resolve_required_slot_no_value_returns_err` | TEST670: resolve_binding returns error when required slot has no value and no default | src/planner/argument_binding.rs:868 |
| test671 | `test671_resolve_optional_slot_no_value_returns_none` | TEST671: resolve_binding returns None when optional slot has no value and no default | src/planner/argument_binding.rs:891 |
| test675 | `test675_build_request_frames_preserves_media_urn_in_stream_start` | TEST675: build_request_frames with full media URN preserves it in STREAM_START frame | src/cap/caller.rs:386 |
| test676 | `test676_build_request_frames_round_trip_find_stream_succeeds` | TEST676: Full round-trip: build_request_frames → extract streams → find_stream succeeds | src/cap/caller.rs:410 |
| test677 | `test677_base_urn_does_not_match_full_urn_in_find_stream` | TEST677: build_request_frames with BASE URN → find_stream with FULL URN FAILS This documents the root cause of the cartridge_client.rs bug: sender used "media:llm-generation-request" (base), receiver looked for "media:llm-generation-request;json;record" (full). is_equivalent requires exact tag set match, so base != full. | src/cap/caller.rs:476 |
| test678 | `test678_find_stream_equivalent_urn_different_tag_order` | TEST678: find_stream with exact equivalent URN (same tags, different order) succeeds | src/bifaci/cartridge_runtime.rs:8089 |
| test679 | `test679_find_stream_base_urn_does_not_match_full_urn` | TEST679: find_stream with base URN vs full URN fails — is_equivalent is strict This is the root cause of the cartridge_client.rs bug. Sender sent "media:llm-generation-request" but receiver looked for "media:llm-generation-request;json;record". | src/bifaci/cartridge_runtime.rs:8109 |
| test680 | `test680_require_stream_missing_urn_returns_error` | TEST680: require_stream with missing URN returns hard StreamError | src/bifaci/cartridge_runtime.rs:8124 |
| test681 | `test681_find_stream_multiple_streams_returns_correct` | TEST681: find_stream with multiple streams returns the correct one | src/bifaci/cartridge_runtime.rs:8142 |
| test682 | `test682_require_stream_str_returns_utf8` | TEST682: require_stream_str returns UTF-8 string for text data | src/bifaci/cartridge_runtime.rs:8167 |
| test683 | `test683_find_stream_invalid_urn_returns_none` | TEST683: find_stream returns None for invalid media URN string (not a parse error — just None) | src/bifaci/cartridge_runtime.rs:8175 |
| test688 | `test688_is_multiple` | TEST688: Tests is_multiple method correctly identifies multi-value cardinalities Verifies Single returns false while Sequence and AtLeastOne return true | src/planner/cardinality.rs:550 |
| test689 | `test689_accepts_single` | TEST689: Tests accepts_single method identifies cardinalities that accept single values Verifies Single and AtLeastOne accept singles while Sequence does not | src/planner/cardinality.rs:559 |
| test690 | `test690_compatibility_single_to_single` | TEST690: Tests cardinality compatibility for single-to-single data flow Verifies Direct compatibility when both input and output are Single | src/planner/cardinality.rs:570 |
| test691 | `test691_compatibility_single_to_vector` | TEST691: Tests cardinality compatibility when wrapping single value into array Verifies WrapInArray compatibility when Sequence expects Single input | src/planner/cardinality.rs:580 |
| test692 | `test692_compatibility_vector_to_single` | TEST692: Tests cardinality compatibility when unwrapping array to singles Verifies RequiresFanOut compatibility when Single expects Sequence input | src/planner/cardinality.rs:590 |
| test693 | `test693_compatibility_vector_to_vector` | TEST693: Tests cardinality compatibility for sequence-to-sequence data flow Verifies Direct compatibility when both input and output are Sequence | src/planner/cardinality.rs:600 |
| test697 | `test697_cap_shape_info_one_to_one` | TEST697: Tests CapShapeInfo correctly identifies one-to-one pattern Verifies Single input and Single output result in OneToOne pattern | src/planner/cardinality.rs:612 |
| test698 | `test698_cap_shape_info_cardinality_always_single_from_urn` | TEST698: CapShapeInfo cardinality is always Single when derived from URN Cardinality comes from context (is_sequence), not from URN tags. The list tag is a semantic type property, not a cardinality indicator. | src/planner/cardinality.rs:623 |
| test699 | `test699_cap_shape_info_list_urn_still_single_cardinality` | TEST699: CapShapeInfo cardinality from URN is always Single; ManyToOne requires is_sequence | src/planner/cardinality.rs:632 |
| test709 | `test709_pattern_produces_vector` | TEST709: Tests CardinalityPattern correctly identifies patterns that produce vectors Verifies OneToMany and ManyToMany return true, others return false | src/planner/cardinality.rs:661 |
| test710 | `test710_pattern_requires_vector` | TEST710: Tests CardinalityPattern correctly identifies patterns that require vectors Verifies ManyToOne and ManyToMany return true, others return false | src/planner/cardinality.rs:671 |
| test711 | `test711_strand_shape_analysis_simple_linear` | TEST711: Tests shape chain analysis for simple linear one-to-one capability chains Verifies chains with no fan-out are valid and require no transformation | src/planner/cardinality.rs:683 |
| test712 | `test712_strand_shape_analysis_with_fan_out` | TEST712: Tests shape chain analysis detects fan-out points in capability chains Fan-out requires is_sequence=true on the cap's output, not a "list" URN tag | src/planner/cardinality.rs:697 |
| test713 | `test713_strand_shape_analysis_empty` | TEST713: Tests shape chain analysis handles empty capability chains correctly Verifies empty chains are valid and require no transformation | src/planner/cardinality.rs:717 |
| test714 | `test714_cardinality_serialization` | TEST714: Tests InputCardinality serializes and deserializes correctly to/from JSON Verifies JSON round-trip preserves cardinality values | src/planner/cardinality.rs:728 |
| test715 | `test715_pattern_serialization` | TEST715: Tests CardinalityPattern serializes and deserializes correctly to/from JSON Verifies JSON round-trip preserves pattern values with snake_case formatting | src/planner/cardinality.rs:739 |
| test716 | `test716_empty_collection` | TEST716: Tests CapInputCollection empty collection has zero files and folders Verifies is_empty() returns true and counts are zero for new collection | src/planner/collection_input.rs:158 |
| test717 | `test717_collection_with_files` | TEST717: Tests CapInputCollection correctly counts files in flat collection Verifies total_file_count() returns 2 for collection with 2 files, no folders | src/planner/collection_input.rs:169 |
| test718 | `test718_nested_collection` | TEST718: Tests CapInputCollection correctly counts files and folders in nested structure Verifies total_file_count() includes subfolder files and total_folder_count() counts subfolders | src/planner/collection_input.rs:191 |
| test719 | `test719_flatten_to_files` | TEST719: Tests CapInputCollection flatten_to_files recursively collects all files Verifies flatten() extracts files from root and all subfolders into flat list | src/planner/collection_input.rs:221 |
| test720 | `test720_from_media_urn_opaque` | TEST720: Tests InputStructure correctly identifies opaque media URNs Verifies that URNs without record marker are parsed as Opaque | src/planner/cardinality.rs:752 |
| test721 | `test721_from_media_urn_record` | TEST721: Tests InputStructure correctly identifies record media URNs Verifies that URNs with record marker tag are parsed as Record | src/planner/cardinality.rs:775 |
| test722 | `test722_structure_compatibility_opaque_to_opaque` | TEST722: Tests structure compatibility for opaque-to-opaque data flow | src/planner/cardinality.rs:797 |
| test723 | `test723_structure_compatibility_record_to_record` | TEST723: Tests structure compatibility for record-to-record data flow | src/planner/cardinality.rs:806 |
| test724 | `test724_structure_incompatibility_opaque_to_record` | TEST724: Tests structure incompatibility for opaque-to-record flow | src/planner/cardinality.rs:815 |
| test725 | `test725_structure_incompatibility_record_to_opaque` | TEST725: Tests structure incompatibility for record-to-opaque flow | src/planner/cardinality.rs:823 |
| test726 | `test726_apply_structure_add_record` | TEST726: Tests applying Record structure adds record marker to URN | src/planner/cardinality.rs:831 |
| test727 | `test727_apply_structure_remove_record` | TEST727: Tests applying Opaque structure removes record marker from URN | src/planner/cardinality.rs:838 |
| test728 | `test728_cap_node_helpers` | TEST728: Tests MachineNode helper methods for identifying node types (cap, fan-out, fan-in) Verifies is_cap(), is_fan_out(), is_fan_in(), and cap_urn() correctly classify node types | src/planner/plan.rs:1165 |
| test729 | `test729_edge_types` | TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, JsonField) Verifies that edge constructors produce correct EdgeType variants | src/planner/plan.rs:1187 |
| test730 | `test730_media_shape_from_urn_all_combinations` | TEST730: Tests MediaShape correctly parses all four combinations | src/planner/cardinality.rs:847 |
| test731 | `test731_media_shape_compatible_direct` | TEST731: Tests MediaShape compatibility for matching shapes | src/planner/cardinality.rs:871 |
| test732 | `test732_media_shape_cardinality_changes` | TEST732: Tests MediaShape compatibility for cardinality changes with matching structure | src/planner/cardinality.rs:898 |
| test733 | `test733_media_shape_structure_mismatch` | TEST733: Tests MediaShape incompatibility when structures don't match | src/planner/cardinality.rs:927 |
| test734 | `test734_topological_order_self_loop` | TEST734: Tests topological sort detects self-referencing cycles (A→A) Verifies that self-loops are recognized as cycles and produce an error | src/planner/plan.rs:1272 |
| test735 | `test735_topological_order_multiple_entry_points` | TEST735: Tests topological sort handles graphs with multiple independent starting nodes Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering | src/planner/plan.rs:1289 |
| test736 | `test736_topological_order_complex_dag` | TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes Verifies that all dependency constraints are satisfied in a graph with multiple converging paths | src/planner/plan.rs:1323 |
| test737 | `test737_linear_chain_single_cap` | TEST737: Tests linear_chain() with exactly one capability Verifies that a single-element chain produces a valid plan with input_slot, cap, and output | src/planner/plan.rs:1369 |
| test738 | `test738_linear_chain_empty` | TEST738: Tests linear_chain() with empty capability list Verifies that an empty chain produces a plan with zero nodes and edges | src/planner/plan.rs:1384 |
| test739 | `test739_node_execution_result_success` | TEST739: Tests NodeExecutionResult structure for successful node execution Verifies that success status, outputs (binary and text), and error fields work correctly | src/planner/plan.rs:1398 |
| test740 | `test740_cap_shape_info_from_specs` | TEST740: Tests CapShapeInfo correctly parses cap specs | src/planner/cardinality.rs:948 |
| test741 | `test741_cap_shape_info_pattern` | TEST741: Tests CapShapeInfo pattern detection — OneToMany requires output is_sequence=true | src/planner/cardinality.rs:958 |
| test742 | `test742_edge_type_serialization` | TEST742: Tests EdgeType enum serialization and deserialization to/from JSON Verifies that edge types like Direct and JsonField correctly round-trip through serde_json | src/planner/plan.rs:1460 |
| test743 | `test743_execution_node_type_serialization` | TEST743: Tests ExecutionNodeType enum serialization and deserialization to/from JSON Verifies that node types like Cap and ForEach correctly serialize with their fields | src/planner/plan.rs:1479 |
| test744 | `test744_plan_serialization` | TEST744: Tests MachinePlan serialization and deserialization to/from JSON Verifies that complete plans with nodes and edges correctly round-trip through JSON | src/planner/plan.rs:1501 |
| test745 | `test745_merge_strategy_serialization` | TEST745: Tests MergeStrategy enum serialization to JSON Verifies that merge strategies like Concat and ZipWith serialize to correct string values | src/planner/plan.rs:1522 |
| test746 | `test746_cap_node_output` | TEST746: Tests creation of Output node type that references a source node Verifies that MachineNode::output() correctly constructs an Output node with name and source | src/planner/plan.rs:1535 |
| test747 | `test747_cap_node_merge` | TEST747: Tests creation and validation of Merge node that combines multiple inputs Verifies that Merge nodes with multiple input nodes and a strategy can be added to plans | src/planner/plan.rs:1552 |
| test748 | `test748_cap_node_split` | TEST748: Tests creation of Split node that distributes input to multiple outputs Verifies that Split nodes correctly specify an input node and output count | src/planner/plan.rs:1579 |
| test749 | `test749_get_node` | TEST749: Tests get_node() method for looking up nodes by ID in a plan Verifies that existing nodes are found and non-existent nodes return None | src/planner/plan.rs:1604 |
| test750 | `test750_strand_shape_valid` | TEST750: Tests shape chain analysis for valid chain with matching structures | src/planner/cardinality.rs:976 |
| test751 | `test751_strand_shape_structure_mismatch` | TEST751: Tests shape chain analysis detects structure mismatch | src/planner/cardinality.rs:988 |
| test752 | `test752_strand_shape_with_fanout` | TEST752: Tests shape chain analysis with fan-out (matching structures) Fan-out requires output is_sequence=true on the disbind cap | src/planner/cardinality.rs:1003 |
| test753 | `test753_strand_shape_list_record_to_list_record` | TEST753: Tests shape chain analysis correctly handles list-to-list record flow | src/planner/cardinality.rs:1022 |
| test754 | `test754_extract_prefix_nonexistent` | TEST754: extract_prefix_to with nonexistent node returns error | src/planner/plan.rs:1789 |
| test755 | `test755_extract_foreach_body` | TEST755: extract_foreach_body extracts body as standalone plan | src/planner/plan.rs:1797 |
| test756 | `test756_extract_foreach_body_unclosed` | TEST756: extract_foreach_body for unclosed ForEach (single body cap) | src/planner/plan.rs:1839 |
| test757 | `test757_extract_foreach_body_wrong_type` | TEST757: extract_foreach_body fails for non-ForEach node | src/planner/plan.rs:1857 |
| test758 | `test758_extract_suffix_from` | TEST758: extract_suffix_from extracts collect → cap_post → output | src/planner/plan.rs:1867 |
| test759 | `test759_extract_suffix_nonexistent` | TEST759: extract_suffix_from fails for nonexistent node | src/planner/plan.rs:1889 |
| test760 | `test760_decomposition_covers_all_caps` | TEST760: Full decomposition roundtrip — prefix + body + suffix cover all cap nodes | src/planner/plan.rs:1897 |
| test761 | `test761_prefix_is_dag` | TEST761: Prefix sub-plan can be topologically sorted (is a valid DAG) | src/planner/plan.rs:1951 |
| test762 | `test762_body_is_dag` | TEST762: Body sub-plan can be topologically sorted (is a valid DAG) | src/planner/plan.rs:1959 |
| test763 | `test763_suffix_is_dag` | TEST763: Suffix sub-plan can be topologically sorted (is a valid DAG) | src/planner/plan.rs:1969 |
| test764 | `test764_extract_prefix_to_input_slot` | TEST764: extract_prefix_to with InputSlot as target (trivial prefix) | src/planner/plan.rs:1979 |
| test765 | `test765_validation_to_json_empty` | TEST765: Tests validation_to_json() returns None for empty validation constraints Verifies that default MediaValidation with no constraints produces JSON None | src/planner/plan_builder.rs:1125 |
| test766 | `test766_validation_to_json_with_constraints` | TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON Verifies that min/max validation rules are correctly serialized as JSON fields | src/planner/plan_builder.rs:1134 |
| test767 | `test767_argument_info_serialization` | TEST767: Tests ArgumentInfo struct serialization to JSON Verifies that argument metadata including resolution status and validation is correctly serialized | src/planner/plan_builder.rs:1156 |
| test768 | `test768_path_argument_requirements_structure` | TEST768: Tests PathArgumentRequirements structure for single-step execution paths Verifies that argument requirements are correctly organized by step with resolution information | src/planner/plan_builder.rs:1177 |
| test769 | `test769_path_with_required_slot` | TEST769: Tests PathArgumentRequirements tracking of required user-input slots Verifies that arguments requiring user input are collected in slots and can_execute_without_input is false | src/planner/plan_builder.rs:1212 |
| test770 | `test770_rejects_foreach` | TEST770: plan_to_resolved_graph rejects plans containing ForEach nodes | src/orchestrator/plan_converter.rs:298 |
| test771 | `test771_rejects_collect` | TEST771: plan_to_resolved_graph rejects plans containing Collect nodes | src/orchestrator/plan_converter.rs:349 |
| test772 | `test772_find_paths_finds_multi_step_paths` | TEST772: Tests find_paths_to_exact_target() finds multi-step paths Verifies that paths through intermediate nodes are found correctly | src/planner/live_cap_fab.rs:1477 |
| test773 | `test773_find_paths_returns_empty_when_no_path` | TEST773: Tests find_paths_to_exact_target() returns empty when no path exists Verifies that pathfinding returns no paths when target is unreachable | src/planner/live_cap_fab.rs:1508 |
| test774 | `test774_get_reachable_targets_finds_all_targets` | TEST774: Tests get_reachable_targets() returns all reachable targets Verifies that reachable targets include direct cap targets and cardinality variants (list versions via Collect) | src/planner/live_cap_fab.rs:1530 |
| test777 | `test777_type_mismatch_pdf_cap_does_not_match_png_input` | TEST777: Tests type checking prevents using PDF-specific cap with PNG input Verifies that media type compatibility is enforced during pathfinding | src/planner/live_cap_fab.rs:1561 |
| test778 | `test778_type_mismatch_png_cap_does_not_match_pdf_input` | TEST778: Tests type checking prevents using PNG-specific cap with PDF input Verifies that media type compatibility is enforced during pathfinding | src/planner/live_cap_fab.rs:1583 |
| test779 | `test779_get_reachable_targets_respects_type_matching` | TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps Verifies that PNG and PDF inputs reach different cap targets (not each other's) | src/planner/live_cap_fab.rs:1610 |
| test780 | `test780_split_integer_array` | TEST780: split_cbor_array splits a simple array of integers | src/orchestrator/cbor_util.rs:150 |
| test781 | `test781_find_paths_respects_type_chain` | TEST781: Tests find_paths_to_exact_target() enforces type compatibility across multi-step chains Verifies that paths are only found when all intermediate types are compatible | src/planner/live_cap_fab.rs:1664 |
| test782 | `test782_split_non_array` | TEST782: split_cbor_array rejects non-array input | src/orchestrator/cbor_util.rs:193 |
| test783 | `test783_split_empty_array` | TEST783: split_cbor_array rejects empty array | src/orchestrator/cbor_util.rs:203 |
| test784 | `test784_split_invalid_cbor` | TEST784: split_cbor_array rejects invalid CBOR bytes | src/orchestrator/cbor_util.rs:213 |
| test785 | `test785_assemble_integer_array` | TEST785: assemble_cbor_array creates array from individual items | src/orchestrator/cbor_util.rs:220 |
| test786 | `test786_roundtrip_split_assemble` | TEST786: split then assemble roundtrip preserves data | src/orchestrator/cbor_util.rs:244 |
| test787 | `test787_find_paths_sorting_prefers_shorter` | TEST787: Tests find_paths_to_exact_target() sorts paths by length, preferring shorter ones Verifies that among multiple paths, the shortest is ranked first | src/planner/live_cap_fab.rs:1840 |
| test788 | `test788_foreach_only_with_sequence_input` | TEST788: ForEach is only synthesized when is_sequence=true With scalar input (is_sequence=false), disbind output goes directly to choose since media:page;textable conforms to media:textable. With sequence input (is_sequence=true), ForEach splits the sequence so each item can be processed by disbind individually, then choose. | src/planner/live_cap_fab.rs:1704 |
| test789 | `test789_cap_from_json_has_valid_specs` | TEST789: Tests that caps loaded from JSON have correct in_spec/out_spec | src/planner/live_cap_fab.rs:1813 |
| test790 | `test790_identity_urn_is_specific` | TEST790: Tests identity_urn is specific and doesn't match everything | src/planner/live_cap_fab.rs:1792 |
| test791 | `test791_sync_from_cap_urns_adds_edges` | TEST791: Tests sync_from_cap_urns actually adds edges | src/planner/live_cap_fab.rs:1757 |
| test792 | `test792_argument_binding_requires_input` | TEST792: Tests ArgumentBinding requires_input distinguishes Slots from Literals Verifies Slot returns true (needs user input) while Literal returns false | src/planner/argument_binding.rs:597 |
| test793 | `test793_argument_binding_serialization` | TEST793: Tests ArgumentBinding PreviousOutput serializes/deserializes correctly Verifies JSON round-trip preserves node_id and output_field values | src/planner/argument_binding.rs:610 |
| test794 | `test794_argument_bindings_add_file_path` | TEST794: Tests ArgumentBindings add_file_path adds InputFilePath binding Verifies add_file_path() creates binding map entry with InputFilePath variant | src/planner/argument_binding.rs:634 |
| test795 | `test795_argument_bindings_unresolved_slots` | TEST795: Tests ArgumentBindings identifies unresolved Slot bindings Verifies has_unresolved_slots() and get_unresolved_slots() detect Slots needing values | src/planner/argument_binding.rs:647 |
| test796 | `test796_resolve_input_file_path` | TEST796: Tests resolve_binding resolves InputFilePath to current file path Verifies InputFilePath binding resolves to file path bytes with InputFile source | src/planner/argument_binding.rs:667 |
| test797 | `test797_resolve_literal` | TEST797: Tests resolve_binding resolves Literal to JSON-encoded bytes Verifies Literal binding serializes value to bytes with Literal source | src/planner/argument_binding.rs:692 |
| test798 | `test798_resolve_previous_output` | TEST798: Tests resolve_binding extracts value from previous node output Verifies PreviousOutput binding fetches field from earlier execution results | src/planner/argument_binding.rs:714 |
| test799 | `test799_machine_input_single` | TEST799: Tests StrandInput single constructor creates valid Single cardinality input Verifies single() wraps one file with Single cardinality and validates correctly | src/planner/argument_binding.rs:743 |
| test800 | `test800_machine_input_vector` | TEST800: Tests StrandInput sequence constructor creates valid Sequence cardinality input Verifies sequence() wraps multiple files with Sequence cardinality | src/planner/argument_binding.rs:754 |
| test801 | `test801_cap_input_file_deserialization_from_dry_context` | TEST801: Tests CapInputFile deserializes from JSON with source metadata fields Verifies JSON with source_id and source_type deserializes to CapInputFile correctly | src/planner/argument_binding.rs:768 |
| test802 | `test802_cap_input_file_deserialization_via_value` | TEST802: Tests CapInputFile deserializes from compact JSON via serde_json::Value Verifies deserialization through Value intermediate works correctly | src/planner/argument_binding.rs:791 |
| test803 | `test803_machine_input_invalid_single` | TEST803: Tests StrandInput validation detects mismatched Single cardinality with multiple files Verifies is_valid() returns false when Single cardinality has more than one file | src/planner/argument_binding.rs:914 |
| test804 | `test804_extract_json_path_simple` | TEST804: Tests basic JSON path extraction with dot notation for nested objects Verifies that simple paths like "data.message" correctly extract values from nested JSON structures | src/planner/executor.rs:644 |
| test805 | `test805_extract_json_path_with_array` | TEST805: Tests JSON path extraction with array indexing syntax Verifies that bracket notation like "items[0].name" correctly accesses array elements and their nested fields | src/planner/executor.rs:658 |
| test806 | `test806_extract_json_path_missing_field` | TEST806: Tests error handling when JSON path references non-existent fields Verifies that accessing missing fields returns an appropriate error message | src/planner/executor.rs:673 |
| test807 | `test807_apply_edge_type_direct` | TEST807: Tests EdgeType::Direct passes JSON values through unchanged Verifies that Direct edge type acts as a transparent passthrough without transformation | src/planner/executor.rs:684 |
| test808 | `test808_apply_edge_type_json_field` | TEST808: Tests EdgeType::JsonField extracts specific top-level fields from JSON objects Verifies that JsonField edge type correctly isolates a single named field from the source output | src/planner/executor.rs:694 |
| test809 | `test809_apply_edge_type_json_field_missing` | TEST809: Tests EdgeType::JsonField error handling for missing fields Verifies that attempting to extract a non-existent field returns an error | src/planner/executor.rs:709 |
| test810 | `test810_apply_edge_type_json_path` | TEST810: Tests EdgeType::JsonPath extracts values using nested path expressions Verifies that JsonPath edge type correctly navigates through multiple levels like "data.nested.value" | src/planner/executor.rs:723 |
| test811 | `test811_apply_edge_type_iteration` | TEST811: Tests EdgeType::Iteration preserves array values for iterative processing Verifies that Iteration edge type passes through arrays unchanged to enable ForEach patterns | src/planner/executor.rs:738 |
| test812 | `test812_apply_edge_type_collection` | TEST812: Tests EdgeType::Collection preserves collected values without transformation Verifies that Collection edge type maintains structure for aggregation patterns | src/planner/executor.rs:748 |
| test813 | `test813_extract_json_path_deeply_nested` | TEST813: Tests JSON path extraction through deeply nested object hierarchies (4+ levels) Verifies that paths can traverse multiple nested levels like "level1.level2.level3.level4.value" | src/planner/executor.rs:758 |
| test814 | `test814_extract_json_path_array_out_of_bounds` | TEST814: Tests error handling when array index exceeds available elements Verifies that out-of-bounds array access returns a descriptive error message | src/planner/executor.rs:778 |
| test815 | `test815_extract_json_path_single_segment` | TEST815: Tests JSON path extraction with single-level paths (no nesting) Verifies that simple field names without dots correctly extract top-level values | src/planner/executor.rs:791 |
| test816 | `test816_extract_json_path_with_special_characters` | TEST816: Tests JSON path extraction preserves special characters in string values Verifies that quotes, backslashes, and other special characters are correctly maintained | src/planner/executor.rs:801 |
| test817 | `test817_extract_json_path_with_null_value` | TEST817: Tests JSON path extraction correctly handles explicit null values Verifies that null is returned as serde_json::Value::Null rather than an error | src/planner/executor.rs:818 |
| test818 | `test818_extract_json_path_with_empty_array` | TEST818: Tests JSON path extraction correctly returns empty arrays Verifies that zero-length arrays are extracted as valid empty array values | src/planner/executor.rs:828 |
| test819 | `test819_extract_json_path_with_numeric_types` | TEST819: Tests JSON path extraction handles various numeric types correctly Verifies extraction of integers, floats, negative numbers, and zero | src/planner/executor.rs:838 |
| test820 | `test820_extract_json_path_with_boolean` | TEST820: Tests JSON path extraction correctly handles boolean values Verifies that true and false are extracted as proper boolean JSON values | src/planner/executor.rs:854 |
| test821 | `test821_extract_json_path_with_nested_arrays` | TEST821: Tests JSON path extraction with multi-dimensional arrays (matrix access) Verifies that nested array structures like "matrix[1]" correctly extract inner arrays | src/planner/executor.rs:874 |
| test822 | `test822_extract_json_path_invalid_array_index` | TEST822: Tests error handling for non-numeric array indices Verifies that invalid indices like "items[abc]" return a descriptive parse error | src/planner/executor.rs:889 |
| test823 | `test823_dispatch_exact_match` | TEST823: is_dispatchable — exact match provider dispatches request | src/urn/cap_urn.rs:2494 |
| test824 | `test824_dispatch_contravariant_input` | TEST824: is_dispatchable — provider with broader input handles specific request (contravariance) | src/urn/cap_urn.rs:2506 |
| test825 | `test825_dispatch_request_unconstrained_input` | TEST825: is_dispatchable — request with unconstrained input dispatches to specific provider media: on the request input axis means "unconstrained" — vacuously true | src/urn/cap_urn.rs:2519 |
| test826 | `test826_dispatch_covariant_output` | TEST826: is_dispatchable — provider output must satisfy request output (covariance) | src/urn/cap_urn.rs:2534 |
| test827 | `test827_dispatch_generic_output_fails` | TEST827: is_dispatchable — provider with generic output cannot satisfy specific request | src/urn/cap_urn.rs:2548 |
| test828 | `test828_dispatch_wildcard_requires_tag_presence` | TEST828: is_dispatchable — wildcard * tag in request, provider missing tag → reject | src/urn/cap_urn.rs:2562 |
| test829 | `test829_dispatch_wildcard_with_tag_present` | TEST829: is_dispatchable — wildcard * tag in request, provider has tag → accept | src/urn/cap_urn.rs:2579 |
| test830 | `test830_dispatch_provider_extra_tags` | TEST830: is_dispatchable — provider extra tags are refinement, always OK | src/urn/cap_urn.rs:2595 |
| test831 | `test831_dispatch_cross_backend_mismatch` | TEST831: is_dispatchable — cross-backend mismatch prevented | src/urn/cap_urn.rs:2611 |
| test832 | `test832_dispatch_asymmetric` | TEST832: is_dispatchable is NOT symmetric | src/urn/cap_urn.rs:2628 |
| test833 | `test833_comparable_symmetric` | TEST833: is_comparable — both directions checked | src/urn/cap_urn.rs:2647 |
| test834 | `test834_comparable_unrelated` | TEST834: is_comparable — unrelated caps are NOT comparable | src/urn/cap_urn.rs:2658 |
| test835 | `test835_equivalent_identical` | TEST835: is_equivalent — identical caps | src/urn/cap_urn.rs:2671 |
| test836 | `test836_equivalent_non_equivalent` | TEST836: is_equivalent — non-equivalent comparable caps | src/urn/cap_urn.rs:2682 |
| test837 | `test837_dispatch_op_mismatch` | TEST837: is_dispatchable — op tag mismatch rejects | src/urn/cap_urn.rs:2693 |
| test838 | `test838_dispatch_request_wildcard_output` | TEST838: is_dispatchable — request with wildcard output accepts any provider output | src/urn/cap_urn.rs:2705 |
| test839 | `test839_peer_response_delivers_logs_before_stream_start` | TEST839: LOG frames arriving BEFORE StreamStart are delivered immediately This tests the critical fix: during a peer call, the peer (e.g., modelcartridge) sends LOG frames for minutes during model download BEFORE sending any data (StreamStart + Chunk). The handler must receive these LOGs in real-time so it can re-emit progress and keep the engine's activity timer alive. Previously, demux_single_stream blocked on awaiting StreamStart before returning PeerResponse, which meant the handler couldn't call recv() until data arrived — causing 120s activity timeouts during long downloads. | src/bifaci/cartridge_runtime.rs:7848 |
| test840 | `test840_peer_response_collect_bytes_discards_logs` | TEST840: PeerResponse::collect_bytes discards LOG frames | src/bifaci/cartridge_runtime.rs:7959 |
| test841 | `test841_peer_response_collect_value_discards_logs` | TEST841: PeerResponse::collect_value discards LOG frames | src/bifaci/cartridge_runtime.rs:8025 |
| test842 | `test842_run_with_keepalive_returns_result` | TEST842: run_with_keepalive returns closure result (fast operation, no keepalive frames) | src/bifaci/cartridge_runtime.rs:8184 |
| test843 | `test843_run_with_keepalive_returns_result_type` | TEST843: run_with_keepalive returns Ok/Err from closure | src/bifaci/cartridge_runtime.rs:8216 |
| test844 | `test844_run_with_keepalive_propagates_error` | TEST844: run_with_keepalive propagates errors from closure | src/bifaci/cartridge_runtime.rs:8235 |
| test845 | `test845_progress_sender_emits_frames` | TEST845: ProgressSender emits progress and log frames independently of OutputStream | src/bifaci/cartridge_runtime.rs:8261 |
| test846 | `test846_progress_frame_roundtrip` | TEST846: Test progress LOG frame encode/decode roundtrip preserves progress float | src/bifaci/io.rs:975 |
| test847 | `test847_progress_double_roundtrip` | TEST847: Double roundtrip (modelcartridge → relay → candlecartridge) | src/bifaci/io.rs:1024 |
| test848 | `test848_relay_notify_roundtrip` | TEST848: RelayNotify encode/decode roundtrip preserves manifest and limits | src/bifaci/io.rs:1713 |
| test849 | `test849_relay_state_roundtrip` | TEST849: RelayState encode/decode roundtrip preserves resource payload | src/bifaci/io.rs:1735 |
| test850 | `test850_all_format_conversion_paths_build_valid_urns` | TEST850: all_format_conversion_paths each entry builds a valid parseable CapUrn | src/standard/caps.rs:1194 |
| test851 | `test851_format_conversion_urn_specs` | TEST851: format_conversion_urn in/out specs match the input constants | src/standard/caps.rs:1222 |
| test852 | `test852_lub_identical` | TEST852: LUB of identical URNs returns the same URN | src/urn/media_urn.rs:1167 |
| test853 | `test853_lub_no_common_tags` | TEST853: LUB of URNs with no common tags returns media: (universal) | src/urn/media_urn.rs:1175 |
| test854 | `test854_lub_partial_overlap` | TEST854: LUB keeps common tags, drops differing ones | src/urn/media_urn.rs:1189 |
| test855 | `test855_lub_list_vs_scalar` | TEST855: LUB of list and non-list drops list tag | src/urn/media_urn.rs:1203 |
| test856 | `test856_lub_empty` | TEST856: LUB of empty input returns universal type | src/urn/media_urn.rs:1217 |
| test857 | `test857_lub_single` | TEST857: LUB of single input returns that input | src/urn/media_urn.rs:1225 |
| test858 | `test858_lub_three_inputs` | TEST858: LUB with three+ inputs narrows correctly | src/urn/media_urn.rs:1233 |
| test859 | `test859_lub_valued_tags` | TEST859: LUB with valued tags (non-marker) that differ | src/urn/media_urn.rs:1248 |
| test860 | `test860_seq_assigner_same_rid_different_xids_independent` | TEST860: Same RID with different XIDs get independent seq counters | src/bifaci/frame.rs:1880 |
| test880 | `test880_no_duplicates_with_unique_caps` | TEST880: Tests duplicate detection passes for caps with unique URN combinations Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations | src/planner/plan_builder.rs:816 |
| test886 | `test886_optional_non_io_arg_with_default_has_default` | TEST886: Tests optional non-IO arguments with default values are marked as HasDefault Verifies that optional arguments with defaults behave the same as required ones with defaults | src/planner/plan_builder.rs:1080 |
| test887 | `test887_execute_with_file_input` | TEST887: Execute with file-path input | tests/orchestrator_integration.rs:406 |
| test888 | `test888_execute_edge1_to_edge2_chain` | TEST888: Execute two-edge chain (test-edge1 -> test-edge2) | tests/orchestrator_integration.rs:359 |
| test889 | `test889_execute_single_edge_dag` | TEST889: Execute single-edge DAG (test-edge1) | tests/orchestrator_integration.rs:311 |
| test890 | `test890_direction_semantic_matching` | TEST890: Semantic direction matching - generic provider matches specific request | src/urn/cap_urn.rs:1913 |
| test891 | `test891_direction_semantic_specificity` | TEST891: Semantic direction specificity - more media URN tags = higher specificity | src/urn/cap_urn.rs:1989 |
| test892 | `test892_extensions_serialization` | TEST892: Test extensions serializes/deserializes correctly in MediaSpecDef | src/media/spec.rs:1143 |
| test893 | `test893_extensions_with_metadata_and_validation` | TEST893: Test extensions can coexist with metadata and validation | src/media/spec.rs:1166 |
| test894 | `test894_multiple_extensions` | TEST894: Test multiple extensions in a media spec | src/media/spec.rs:1202 |
| test895 | `test895_cap_output_media_specs_have_extensions` | TEST895: All cap output media specs must have file extensions defined. This is a regression guard: every media URN used as a cap output (out= in cap TOML) produces user-facing files. If a spec lacks extensions, save_cap_output and FinderImportService will fail at runtime. | src/media/registry.rs:947 |
| test896 | `test896_cap_input_media_specs_have_extensions` | TEST896: All cap input media specs that represent user files must have extensions. These are the entry points — the file types users can right-click on. | src/media/registry.rs:998 |
| test897 | `test897_cap_output_extension_values_correct` | TEST897: Verify that specific cap output URNs resolve to the correct extension. This catches misconfigurations where a spec exists but has the wrong extension. | src/media/registry.rs:1045 |
| test898 | `test898_binary_integrity_through_relay` | TEST898: Binary data integrity through full relay path (256 byte values) | src/bifaci/integration_tests.rs:406 |
| test899 | `test899_streaming_chunks_through_relay` | TEST899: Streaming chunks flow through relay without accumulation | src/bifaci/integration_tests.rs:532 |
| test900 | `test900_two_cartridges_routed_independently` | TEST900: Two cartridges routed independently by cap_urn | src/bifaci/integration_tests.rs:647 |
| test901 | `test901_req_for_unknown_cap_returns_err_frame` | TEST901: REQ for unknown cap returns ERR frame (not fatal) | src/bifaci/integration_tests.rs:823 |
| test902 | `test902_compute_checksum_empty` | TEST902: Verify FNV-1a checksum handles empty data | src/bifaci/frame.rs:1709 |
| test903 | `test903_chunk_with_chunk_index_and_checksum` | TEST903: Verify CHUNK frame can store chunk_index and checksum fields | src/bifaci/frame.rs:1720 |
| test904 | `test904_stream_end_with_chunk_count` | TEST904: Verify STREAM_END frame can store chunk_count field | src/bifaci/frame.rs:1745 |
| test905 | `test905_send_to_master_build_request_frames_roundtrip` | TEST905: send_to_master + build_request_frames through RelaySwitch → RelaySlave → InProcessCartridgeHost roundtrip | src/bifaci/relay_switch.rs:3660 |
| test906 | `test906_full_path_identity_verification` | TEST906: Full path identity verification: engine → host (attach_cartridge) → cartridge This verifies that attach_cartridge completes identity verification end-to-end and the cartridge is ready to handle subsequent requests. | src/bifaci/integration_tests.rs:1351 |
| test907 | `test907_offline_blocks_fetch` | TEST907: Offline flag blocks fetch_from_registry without making HTTP request | src/cap/registry.rs:716 |
| test908 | `test908_cached_caps_accessible_when_offline` | TEST908: Cached caps remain accessible when offline | src/cap/registry.rs:741 |
| test909 | `test909_set_offline_false_restores_fetch` | TEST909: set_offline(false) restores fetch ability (would fail with HTTP error, not NetworkBlocked) | src/cap/registry.rs:765 |
| test910 | `test910_map_progress_monotonic` | TEST910: map_progress output is monotonic for monotonically increasing input | src/orchestrator/executor.rs:1612 |
| test911 | `test911_map_progress_bounded` | TEST911: map_progress output is bounded within [base, base+weight] | src/orchestrator/executor.rs:1630 |
| test912 | `test912_progress_mapper_reports_through_parent` | TEST912: ProgressMapper correctly maps through a CapProgressFn | src/orchestrator/executor.rs:1651 |
| test913 | `test913_progress_mapper_as_cap_progress_fn` | TEST913: ProgressMapper.as_cap_progress_fn produces same mapping | src/orchestrator/executor.rs:1675 |
| test914 | `test914_progress_mapper_sub_mapper` | TEST914: ProgressMapper.sub_mapper chains correctly | src/orchestrator/executor.rs:1698 |
| test915 | `test915_per_group_subdivision_monotonic_bounded` | TEST915: Per-group subdivision produces monotonic, bounded progress for N groups Uses pre-computed boundaries (same pattern as production code) to guarantee monotonicity regardless of f32 rounding. | src/orchestrator/executor.rs:1725 |
| test916 | `test916_foreach_item_subdivision` | TEST916: ForEach item subdivision produces correct, monotonic ranges Mirrors the production code in interpreter.rs: pre-compute item boundaries from the same formula so the end of item N and the start of item N+1 are the same f32 value (no divergent accumulation paths). | src/orchestrator/executor.rs:1782 |
| test917 | `test917_high_frequency_progress_bounded` | TEST917: High-frequency progress emission does not violate bounds (Regression test for the deadlock scenario — verifies computation stays bounded) | src/orchestrator/executor.rs:1846 |
| test918 | `test918_activity_timeout_error_display` | TEST918: ActivityTimeout error formats correctly | src/orchestrator/executor.rs:1883 |
| test919 | `test919_parse_simple_testcartridge_graph` | TEST919: Parse simple machine notation graph with test-edge1 | tests/orchestrator_integration.rs:287 |
| test920 | `test920_single_cap_plan` | TEST920: Tests creation of a simple execution plan with a single capability Verifies that single_cap() generates a valid plan with input_slot, cap node, and output node | src/planner/plan.rs:1035 |
| test921 | `test921_linear_chain_plan` | TEST921: Tests creation of a linear chain of capabilities connected in sequence Verifies that linear_chain() correctly links multiple caps with proper edges and topological order | src/planner/plan.rs:1051 |
| test922 | `test922_empty_plan` | TEST922: Tests creation and validation of an empty execution plan with no nodes Verifies that plans without capabilities are valid and handle zero nodes correctly | src/planner/plan.rs:1069 |
| test923 | `test923_plan_with_metadata` | TEST923: Tests storing and retrieving metadata attached to an execution plan Verifies that arbitrary JSON metadata can be associated with a plan for context preservation | src/planner/plan.rs:1078 |
| test924 | `test924_validate_invalid_edge` | TEST924: Tests plan validation detects edges pointing to non-existent nodes Verifies that validate() returns an error when an edge references a missing to_node | src/planner/plan.rs:1095 |
| test925 | `test925_topological_order_diamond` | TEST925: Tests topological sort correctly orders a diamond-shaped DAG (A->B,C->D) Verifies that nodes with multiple paths respect dependency constraints (A first, D last) | src/planner/plan.rs:1111 |
| test926 | `test926_topological_order_detects_cycle` | TEST926: Tests topological sort detects and rejects cyclic dependencies (A->B->C->A) Verifies that circular references produce a "Cycle detected" error | src/planner/plan.rs:1141 |
| test927 | `test927_execution_result` | TEST927: Tests MachineResult structure for successful execution outcomes Verifies that success status, outputs, and primary_output() accessor work correctly | src/planner/plan.rs:1204 |
| test928 | `test928_validate_invalid_from_node` | TEST928: Tests plan validation detects edges originating from non-existent nodes Verifies that validate() returns an error when an edge references a missing from_node | src/planner/plan.rs:1224 |
| test929 | `test929_validate_invalid_entry_node` | TEST929: Tests plan validation detects invalid entry node references Verifies that validate() returns an error when entry_nodes contains a non-existent node ID | src/planner/plan.rs:1240 |
| test930 | `test930_validate_invalid_output_node` | TEST930: Tests plan validation detects invalid output node references Verifies that validate() returns an error when output_nodes contains a non-existent node ID | src/planner/plan.rs:1256 |
| test931 | `test931_node_execution_result_failure` | TEST931: Tests NodeExecutionResult structure for failed node execution Verifies that failure status, error message, and absence of outputs are correctly represented | src/planner/plan.rs:1420 |
| test932 | `test932_execution_result_failure` | TEST932: Tests MachineResult structure for failed chain execution Verifies that failure status, error message, and absence of outputs are correctly represented | src/planner/plan.rs:1442 |
| test933 | `test933_serialization_roundtrip` | TEST933: Tests CapInputCollection serializes to JSON and deserializes correctly Verifies JSON round-trip preserves folder_id, folder_name, files and file metadata | src/planner/collection_input.rs:248 |
| test934 | `test934_find_first_foreach` | TEST934: find_first_foreach detects ForEach in a plan | src/planner/plan.rs:1706 |
| test935 | `test935_find_first_foreach_linear` | TEST935: find_first_foreach returns None for linear plans | src/planner/plan.rs:1714 |
| test936 | `test936_has_foreach` | TEST936: has_foreach detects ForEach nodes | src/planner/plan.rs:1726 |
| test937 | `test937_extract_prefix_to` | TEST937: extract_prefix_to extracts input_slot -> cap_0 as a standalone plan | src/planner/plan.rs:1767 |
| test943 | `test943_same_media_different_names_is_not_a_cycle` | TEST943: Two nodes with the same media type but different names are two distinct graph positions — NOT a loop. The identity cap has `in = out` by type, so its upstream and downstream node carry the same media URN; this must not collapse them into a self-loop. Node identity comes from the user-written name, not the media URN. | tests/orchestrator_integration.rs:579 |
| test944 | `test944_six_machine` | TEST944: 6-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9 -> edge10 Full cycle: node1 -> node2 -> node3 -> node6 -> node7 -> node8 -> node1 Completes the round trip: unwrap markers + lowercase | tests/orchestrator_integration.rs:762 |
| test945 | `test945_five_machine` | TEST945: 5-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9 node1 -> node2 -> node3 -> node6 -> node7 -> node8 adds <<...>> wrapping around the reversed string | tests/orchestrator_integration.rs:707 |
| test946 | `test946_four_machine` | TEST946: 4-machine: edge1 -> edge2 -> edge7 -> edge8 node1 -> node2 -> node3 -> node6 -> node7 "hello" -> "[PREPEND]hello" -> "[PREPEND]hello[APPEND]" -> "[PREPEND]HELLO[APPEND]" -> "]DNEPPA[OLLEH]DNEPERP[" | tests/orchestrator_integration.rs:652 |
| test947 | `test947_cap_not_found` | TEST947: Cap not found in registry | tests/orchestrator_integration.rs:625 |
| test948 | `test948_invalid_cap_urn` | TEST948: Invalid cap URN in machine notation | tests/orchestrator_integration.rs:614 |
| test949 | `test949_empty_graph` | TEST949: Empty machine notation (no edges) | tests/orchestrator_integration.rs:596 |
| test950 | `test950_reject_cycles` | TEST950: Validate that cycles are rejected | tests/orchestrator_integration.rs:553 |
| test951 | `test951_fan_in_pattern` | TEST951: Multi-input DAG (fan-in pattern) | tests/orchestrator_integration.rs:501 |
| test952 | `test952_execute_large_payload` | TEST952: Execute large payload (test-large cap) | tests/orchestrator_integration.rs:453 |
| test953 | `test953_linear_plan_still_works` | TEST953: Linear plans (no ForEach/Collect) still convert successfully | src/orchestrator/plan_converter.rs:398 |
| test954 | `test954_standalone_collect_passthrough` | TEST954: Standalone Collect nodes are handled as pass-through Plan: input → cap_0 → Collect → cap_1 → output The standalone Collect is transparent — the resolved edge from Collect to cap_1 should be rewritten to go from cap_0 to cap_1 directly. | src/orchestrator/plan_converter.rs:431 |
| test955 | `test955_split_map_array` | TEST955: split_cbor_array with nested maps | src/orchestrator/cbor_util.rs:170 |
| test956 | `test956_roundtrip_assemble_split` | TEST956: assemble then split roundtrip preserves data | src/orchestrator/cbor_util.rs:263 |
| test957 | `test957_cap_input_file_new` | TEST957: Tests CapInputFile constructor creates file with correct path and media URN Verifies new() initializes file_path, media_urn and leaves metadata/source_id as None | src/planner/argument_binding.rs:557 |
| test958 | `test958_cap_input_file_from_listing` | TEST958: Tests CapInputFile from_listing sets source metadata correctly Verifies from_listing() populates source_id and source_type as Listing | src/planner/argument_binding.rs:568 |
| test959 | `test959_cap_input_file_filename` | TEST959: Tests CapInputFile extracts filename from full path correctly Verifies filename() returns just the basename without directory path | src/planner/argument_binding.rs:577 |
| test960 | `test960_argument_binding_literal_string` | TEST960: Tests ArgumentBinding literal_string creates Literal variant with string value Verifies literal_string() wraps string in JSON Value::String | src/planner/argument_binding.rs:585 |
| test961 | `test961_assemble_empty` | TEST961: assemble empty list produces empty CBOR array | src/orchestrator/cbor_util.rs:279 |
| test962 | `test962_assemble_invalid_item` | TEST962: assemble rejects invalid CBOR item | src/orchestrator/cbor_util.rs:287 |
| test963 | `test963_split_binary_items` | TEST963: split preserves CBOR byte strings (binary data — the common case in bifaci) | src/orchestrator/cbor_util.rs:299 |
| test964 | `test964_split_sequence_bytes` | TEST964: split_cbor_sequence splits concatenated CBOR Bytes values | src/orchestrator/cbor_util.rs:333 |
| test965 | `test965_split_sequence_text` | TEST965: split_cbor_sequence splits concatenated CBOR Text values | src/orchestrator/cbor_util.rs:357 |
| test966 | `test966_split_sequence_mixed` | TEST966: split_cbor_sequence handles mixed types | src/orchestrator/cbor_util.rs:374 |
| test967 | `test967_split_sequence_single` | TEST967: split_cbor_sequence single-item sequence | src/orchestrator/cbor_util.rs:396 |
| test968 | `test968_roundtrip_assemble_split_sequence` | TEST968: roundtrip — assemble then split preserves items | src/orchestrator/cbor_util.rs:407 |
| test969 | `test969_roundtrip_split_assemble_sequence` | TEST969: roundtrip — split then assemble preserves byte-for-byte | src/orchestrator/cbor_util.rs:426 |
| test970 | `test970_split_sequence_empty` | TEST970: split_cbor_sequence rejects empty data | src/orchestrator/cbor_util.rs:443 |
| test971 | `test971_split_sequence_truncated` | TEST971: split_cbor_sequence rejects truncated CBOR | src/orchestrator/cbor_util.rs:450 |
| test972 | `test972_assemble_sequence_invalid_item` | TEST972: assemble_cbor_sequence rejects invalid CBOR item | src/orchestrator/cbor_util.rs:467 |
| test973 | `test973_assemble_sequence_empty` | TEST973: assemble_cbor_sequence with empty items list produces empty bytes | src/orchestrator/cbor_util.rs:479 |
| test974 | `test974_sequence_is_not_array` | TEST974: CBOR sequence is NOT a CBOR array — split_cbor_array rejects a sequence | src/orchestrator/cbor_util.rs:489 |
| test975 | `test975_single_value_sequence` | TEST975: split_cbor_sequence works on data that is also a valid CBOR array (single top-level value) | src/orchestrator/cbor_util.rs:507 |
| test977 | `test977_os_files_excluded_integration` | TEST977: OS files excluded in resolve_paths | src/input_resolver/resolver.rs:532 |
| test991 | `test991_detects_duplicate_cap_urns` | TEST991: Tests duplicate detection identifies caps with identical URNs Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn | src/planner/plan_builder.rs:850 |
| test992 | `test992_different_ops_same_types_not_duplicates` | TEST992: Tests caps with different operations but same input/output types are not duplicates Verifies that only the complete URN (including op) is used for duplicate detection | src/planner/plan_builder.rs:890 |
| test993 | `test993_same_op_different_input_types_not_duplicates` | TEST993: Tests caps with same operation but different input types are not duplicates Verifies that input type differences distinguish caps with the same operation name | src/planner/plan_builder.rs:916 |
| test994 | `test994_input_arg_first_cap_auto_resolved_from_input` | TEST994: Tests first cap's input argument is automatically resolved from input file Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain | src/planner/plan_builder.rs:966 |
| test995 | `test995_input_arg_subsequent_cap_auto_resolved_from_previous` | TEST995: Tests subsequent caps' input arguments are automatically resolved from previous output Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first | src/planner/plan_builder.rs:978 |
| test996 | `test996_output_arg_auto_resolved` | TEST996: Tests output arguments are automatically resolved from previous cap's output Verifies that arguments matching the output spec are always resolved as FromPreviousOutput | src/planner/plan_builder.rs:995 |
| test997 | `test997_file_path_type_fallback_first_cap` | TEST997: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap Verifies that generic file-path arguments are bound to input file in the first cap | src/planner/plan_builder.rs:1007 |
| test998 | `test998_file_path_type_fallback_subsequent_cap` | TEST998: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps Verifies that generic file-path arguments are bound to previous cap's output after the first cap | src/planner/plan_builder.rs:1025 |
| test1000 | `test1000_single_existing_file` | TEST1000: Single existing file | src/input_resolver/path_resolver.rs:263 |
| test1001 | `test1001_nonexistent_file` | TEST1001: Single non-existent file | src/input_resolver/path_resolver.rs:275 |
| test1002 | `test1002_empty_directory` | TEST1002: Empty directory | src/input_resolver/path_resolver.rs:282 |
| test1003 | `test1003_directory_with_files` | TEST1003: Directory with files | src/input_resolver/path_resolver.rs:291 |
| test1004 | `test1004_directory_with_subdirs` | TEST1004: Directory with subdirs (recursive) | src/input_resolver/path_resolver.rs:303 |
| test1005 | `test1005_glob_matching_files` | TEST1005: Glob matching files | src/input_resolver/path_resolver.rs:315 |
| test1006 | `test1006_glob_matching_nothing` | TEST1006: Glob matching nothing | src/input_resolver/path_resolver.rs:328 |
| test1007 | `test1007_recursive_glob` | TEST1007: Recursive glob | src/input_resolver/path_resolver.rs:339 |
| test1008 | `test1008_mixed_file_dir` | TEST1008: Mixed file + dir | src/input_resolver/path_resolver.rs:352 |
| test1009 | `test1009_non_io_arg_with_default_has_default` | TEST1009: Tests required non-IO arguments with default values are marked as HasDefault Verifies that arguments like integers with defaults don't require user input | src/planner/plan_builder.rs:1043 |
| test1010 | `test1010_duplicate_paths` | TEST1010: Duplicate paths are deduplicated | src/input_resolver/path_resolver.rs:370 |
| test1011 | `test1011_invalid_glob` | TEST1011: Invalid glob syntax | src/input_resolver/path_resolver.rs:386 |
| test1012 | `test1012_non_io_arg_without_default_requires_user_input` | TEST1012: Tests required non-IO arguments without defaults require user input Verifies that arguments like strings without defaults are marked as RequiresUserInput | src/planner/plan_builder.rs:1062 |
| test1013 | `test1013_empty_input` | TEST1013: Empty input array | src/input_resolver/path_resolver.rs:396 |
| test1014 | `test1014_symlink_to_file` | TEST1014: Symlink to file | src/input_resolver/path_resolver.rs:404 |
| test1015 | `test1015_optional_non_io_arg_without_default_requires_user_input` | TEST1015: Tests optional non-IO arguments without defaults still require user input Verifies that optional arguments without defaults must be explicitly provided or skipped | src/planner/plan_builder.rs:1099 |
| test1016 | `test1016_path_with_spaces` | TEST1016: Path with spaces | src/input_resolver/path_resolver.rs:419 |
| test1017 | `test1017_path_with_unicode` | TEST1017: Path with unicode | src/input_resolver/path_resolver.rs:430 |
| test1018 | `test1018_relative_path` | TEST1018: Relative path | src/input_resolver/path_resolver.rs:441 |
| test1019 | `test1019_validation_to_json_none` | TEST1019: Tests validation_to_json() returns None for None input Verifies that missing validation metadata is converted to JSON None | src/planner/plan_builder.rs:1117 |
| test1020 | `test1020_ds_store_excluded` | TEST1020: macOS .DS_Store is excluded | src/input_resolver/os_filter.rs:153 |
| test1021 | `test1021_thumbs_db_excluded` | TEST1021: Windows Thumbs.db is excluded | src/input_resolver/os_filter.rs:160 |
| test1022 | `test1022_resource_fork_excluded` | TEST1022: macOS resource fork files are excluded | src/input_resolver/os_filter.rs:167 |
| test1023 | `test1023_office_lock_excluded` | TEST1023: Office lock files are excluded | src/input_resolver/os_filter.rs:174 |
| test1024 | `test1024_git_dir_excluded` | TEST1024: .git directory is excluded | src/input_resolver/os_filter.rs:181 |
| test1025 | `test1025_macosx_dir_excluded` | TEST1025: __MACOSX archive artifact is excluded | src/input_resolver/os_filter.rs:188 |
| test1026 | `test1026_temp_files_excluded` | TEST1026: Temp files are excluded | src/input_resolver/os_filter.rs:195 |
| test1027 | `test1027_localized_excluded` | TEST1027: .localized is excluded | src/input_resolver/os_filter.rs:204 |
| test1028 | `test1028_desktop_ini_excluded` | TEST1028: desktop.ini is excluded | src/input_resolver/os_filter.rs:210 |
| test1029 | `test1029_normal_files_not_excluded` | TEST1029: Normal files are NOT excluded | src/input_resolver/os_filter.rs:216 |
| test1090 | `test1090_single_file_scalar` | TEST1090: 1 file → is_sequence=false | src/input_resolver/resolver.rs:481 |
| test1092 | `test1092_two_files` | TEST1092: 2 files → is_sequence=true | src/input_resolver/resolver.rs:493 |
| test1093 | `test1093_dir_single_file` | TEST1093: 1 dir with 1 file → is_sequence=false | src/input_resolver/resolver.rs:506 |
| test1094 | `test1094_dir_multiple_files` | TEST1094: 1 dir with 3 files → is_sequence=true | src/input_resolver/resolver.rs:518 |
| test1098 | `test1098_extension_based_pdf` | TEST1098: Extension-based detection picks up pdf tag for .pdf files | src/input_resolver/resolver.rs:545 |
| test1100 | `test1100_cap_urn_normalizes_media_urn_tag_order` | TEST1100: Tests that CapUrn normalizes media URN tags to canonical order This is the root cause fix for caps not matching when cartridges report URNs with different tag ordering than the registry (e.g., "record;textable" vs "textable;record") | src/planner/plan_builder.rs:1273 |
| test1103 | `test1103_is_dispatchable_uses_correct_directionality` | TEST1103: Tests that is_dispatchable has correct directionality The available cap (provider) must be dispatchable for the requested cap (request). This tests the directionality: provider.is_dispatchable(&request) NOTE: This now tests CapUrn::is_dispatchable directly, not via MachinePlanBuilder | src/planner/plan_builder.rs:1306 |
| test1104 | `test1104_is_dispatchable_rejects_non_dispatchable` | TEST1104: Tests that is_dispatchable rejects when provider cannot dispatch request | src/planner/plan_builder.rs:1329 |
| test1105 | `test1105_two_steps_same_cap_urn_different_slot_values` | TEST1105: Two steps with the same cap_urn get distinct slot values via different node_ids. This is the core disambiguation scenario that step-index keying was designed to solve. | src/planner/argument_binding.rs:930 |
| test1106 | `test1106_slot_falls_through_to_cap_settings_shared` | TEST1106: Slot resolution falls through to cap_settings when no slot_value exists. cap_settings are keyed by cap_urn (shared across steps), so both steps get the same value. | src/planner/argument_binding.rs:979 |
| test1107 | `test1107_slot_value_overrides_cap_settings_per_step` | TEST1107: step_0 has a slot_value override, step_1 falls through to cap_settings. Proves per-step override works while shared settings remain as fallback. | src/planner/argument_binding.rs:1018 |
| test1108 | `test1108_resolve_all_passes_node_id` | TEST1108: ResolveAll with node_id threads correctly through to each binding. | src/planner/argument_binding.rs:1063 |
| test1109 | `test1109_slot_key_uses_node_id_not_cap_urn` | TEST1109: Slot key uses node_id, NOT cap_urn — a slot_value keyed by cap_urn must not match. | src/planner/argument_binding.rs:1123 |
| test1110 | `test1110_strand_round_trips_through_serde_without_losing_step_types` | TEST1110: Strand serializes to JSON and deserializes back preserving all step types | src/planner/live_cap_fab.rs:1873 |
| test1111 | `test1111_foreach_for_user_provided_list_source` | TEST1111: ForEach works for user-provided list sources not in the graph. This is the original bug — media:list;textable;txt is a user import source, not a cap output. Previously, no ForEach edge existed for it because insert_cardinality_transitions() only pre-computed edges for cap outputs. With dynamic synthesis, ForEach is available for ANY list source. | src/planner/live_cap_fab.rs:1941 |
| test1112 | `test1112_no_collect_in_path_finding` | TEST1112: Collect is not synthesized during path finding. Reaching a list target type requires the cap itself to output a list type. | src/planner/live_cap_fab.rs:1991 |
| test1113 | `test1113_multi_cap_path_no_collect` | TEST1113: Multi-cap path without Collect — Collect is not synthesized | src/planner/live_cap_fab.rs:2016 |
| test1114 | `test1114_graph_stores_only_cap_edges` | TEST1114: Graph stores only Cap edges after sync | src/planner/live_cap_fab.rs:2039 |
| test1115 | `test1115_dynamic_foreach_with_is_sequence` | TEST1115: ForEach is synthesized when is_sequence=true AND caps can consume items | src/planner/live_cap_fab.rs:2073 |
| test1116 | `test1116_collect_never_synthesized` | TEST1116: Collect is never synthesized during path finding | src/planner/live_cap_fab.rs:2110 |
| test1117 | `test1117_no_foreach_when_not_sequence` | TEST1117: ForEach is NOT synthesized when is_sequence=false | src/planner/live_cap_fab.rs:2137 |
| test1118 | `test1118_no_foreach_without_cap_consumers` | TEST1118: ForEach not synthesized without cap consumers even with is_sequence=true | src/planner/live_cap_fab.rs:2163 |
| test1119 | `test1119_strand_knit_with_registry_returns_single_strand_machine` | TEST1119: Strand::knit returns a single-strand Machine via the new resolver. Smoke test the registry-threaded API end-to-end. | src/planner/live_cap_fab.rs:2182 |
| test1120 | `test1120_strand_knit_unknown_cap_fails_hard` | TEST1120: Strand::knit fails hard when the cap is not in the registry — the planner produces strands referencing caps that must be present in the cap registry's cache for resolution to succeed. | src/planner/live_cap_fab.rs:2228 |
| test1121 | `test1121_cbor_array_file_paths_in_cbor_mode` | TEST1121: CBOR Array of file-paths in CBOR mode (validates new Array support) | src/bifaci/cartridge_runtime.rs:6919 |
| test1122 | `test1122_full_path_engine_req_to_cartridge_response` | TEST1122: Full path: engine REQ → runtime → cartridge → response back through relay | src/bifaci/integration_tests.rs:209 |
| test1123 | `test1123_cartridge_error_flows_to_engine` | TEST1123: Cartridge ERR frame flows back to engine through relay | src/bifaci/integration_tests.rs:328 |
| test1124 | `test1124_cbor_rejects_stream_end_without_chunk_count` | TEST1124: CBOR decode REJECTS STREAM_END frame missing chunk_count field | src/bifaci/frame.rs:2369 |
| test1125 | `test1125_map_progress_basic_mapping` | TEST1125: map_progress clamps child to [0.0, 1.0] and maps to [base, base+weight] | src/orchestrator/executor.rs:1583 |
| test1126 | `test1126_map_progress_deterministic` | TEST1126: map_progress is deterministic — same inputs always produce same output | src/orchestrator/executor.rs:1601 |
| test1127 | `test1127_cap_documentation_round_trip_with_markdown_body` | TEST1127: Documentation field round-trips through JSON serialize/deserialize. The documentation field carries an arbitrary markdown body authored in the source TOML via the triple-quoted literal string syntax. The round-trip must preserve every character — including newlines, backticks, double quotes, and Unicode — because consumers (info panels, capdag.com, etc.) render it directly. JSON.stringify on the capfab side and the Rust serializer on this side must agree on escaping; this test fails hard if they don't. | src/cap/definition.rs:1412 |
| test1128 | `test1128_cap_documentation_omitted_when_none` | TEST1128: When documentation is None, the serializer must skip the field entirely. This matches the behaviour of the JS toJSON, the ObjC toDictionary, and the schema's "if present" semantics — there is no null sentinel, only absence. A bug here would silently start emitting `"documentation":null` and break consumers that distinguish between absent and explicit null. | src/cap/definition.rs:1452 |
| test1129 | `test1129_cap_documentation_parses_from_capfab_json` | TEST1129: A JSON document produced by capfab (the canonical source) with a `documentation` field must deserialize into a Cap with the body intact. Models the actual on-disk shape — not a synthetic round-trip — to catch a mismatch between the JSON schema and the Rust struct field naming. | src/cap/definition.rs:1479 |
| test1130 | `test1130_cap_documentation_set_and_clear_lifecycle` | TEST1130: documentation set/clear lifecycle parallels cap_description. Catches a regression where the setter or clearer is wired to the wrong field — for example, set_documentation accidentally writing to cap_description. | src/cap/definition.rs:1502 |
| test1131 | `test1131_media_documentation_propagates_through_resolve` | TEST1131: Documentation propagates from MediaSpecDef through resolve_media_urn into ResolvedMediaSpec. This is the resolution path used by every consumer that asks the registry for a media spec — info panels, the cap navigator, the UI — so a regression here makes the new field invisible everywhere. | src/media/spec.rs:1234 |
| test1132 | `test1132_media_spec_def_documentation_round_trip` | TEST1132: MediaSpecDef serializes documentation only when present and round-trips losslessly. Mirrors TEST1127/1128 for the cap side. | src/media/spec.rs:1267 |
| test1133 | `test1133_media_spec_def_documentation_lifecycle` | TEST1133: MediaSpecDef set/clear lifecycle for documentation. Catches a regression where the setter or clearer accidentally writes to or reads from `description` (the short field) instead of `documentation` (the long markdown body). | src/media/spec.rs:1311 |
| test1142 | `test1142_resolved_graph_to_mermaid_renders_shapes_dedupes_edges_and_escapes` | TEST1142: ResolvedGraph.to_mermaid() renders node shapes, deduplicates edges, and escapes labels | src/orchestrator/types.rs:190 |
| test1143 | `test1143_input_item_from_string_distinguishes_glob_directory_and_file` | TEST1143: InputItem::from_string distinguishes glob patterns, directories, and files | src/input_resolver/types.rs:269 |
| test1144 | `test1144_content_structure_helpers_and_display` | TEST1144: ContentStructure is_list/is_record helpers and Display implementation are correct | src/input_resolver/types.rs:284 |
| test1145 | `test1145_resolved_input_set_uses_equivalent_media_and_file_count_cardinality` | TEST1145: ResolvedInputSet uses URN equivalence for common_media and file count for is_sequence | src/input_resolver/types.rs:296 |
| test1146 | `test1146_input_resolver_error_display_and_source` | TEST1146: InputResolverError Display and source() implementations produce correct messages | src/input_resolver/types.rs:334 |
| test1147 | `test1147_machine_syntax_error_display_is_specific` | TEST1147: MachineSyntaxError Display includes position and detail for each variant | src/machine/error.rs:154 |
| test1148 | `test1148_machine_parse_error_from_syntax_preserves_variant` | TEST1148: MachineParseError::from(MachineSyntaxError) preserves the syntax error variant | src/machine/error.rs:168 |
| test1149 | `test1149_machine_parse_error_from_resolution_preserves_variant` | TEST1149: MachineParseError::from(MachineAbstractionError) preserves the resolution error variant | src/machine/error.rs:184 |
| test1150 | `test1150_add_cap_and_basic_traversal` | TEST1150: Adding one cap creates one edge and makes its output reachable in one step. | src/planner/live_cap_fab.rs:1256 |
| test1151 | `test1151_exact_vs_conformance_matching` | TEST1151: Exact target lookup prefers the direct singular or list-producing path over longer alternatives. | src/planner/live_cap_fab.rs:1287 |
| test1152 | `test1152_multi_step_path` | TEST1152: Path finding returns the expected two-cap chain through an intermediate media type. | src/planner/live_cap_fab.rs:1365 |
| test1153 | `test1153_deterministic_ordering` | TEST1153: Repeated path searches return the same path order for the same graph and target. | src/planner/live_cap_fab.rs:1394 |
| test1154 | `test1154_sync_from_caps` | TEST1154: Syncing from caps replaces the existing graph contents with the new cap set. | src/planner/live_cap_fab.rs:1441 |
| test1155 | `test1155_from_strand_produces_single_strand_machine` | TEST1155: Building a machine from one strand produces one strand with one resolved edge. | src/machine/graph.rs:648 |
| test1156 | `test1156_from_strands_keeps_strands_disjoint` | TEST1156: Building from multiple strands keeps them disjoint and preserves input strand order. | src/machine/graph.rs:657 |
| test1157 | `test1157_from_strands_empty_input_fails_hard` | TEST1157: Building from zero strands fails with NoCapabilitySteps. | src/machine/graph.rs:687 |
| test1158 | `test1158_machine_is_equivalent_is_strict_positional` | TEST1158: Machine equivalence is strict about strand order and rejects reordered strands. | src/machine/graph.rs:695 |
| test1159 | `test1159_machine_strand_is_equivalent_walks_node_bijection` | TEST1159: MachineStrand equivalence accepts two separately built but structurally identical strands. | src/machine/graph.rs:715 |
| test1160 | `test1160_machine_run_new_stores_canonical_notation` | TEST1160: Creating a MachineRun stores the canonical notation and starts in the pending state. | src/machine/graph.rs:728 |
| test1161 | `test1161_simple_linear_chain_conversion` | TEST1161: Converting a simple linear plan produces resolved edges for the cap-to-cap chain. | src/orchestrator/plan_converter.rs:235 |
| test1162 | `test1162_heartbeat_frame_with_memory_meta` | TEST1162: Heartbeat frames preserve self-reported memory values stored in metadata. | src/bifaci/frame.rs:1338 |
| test1163 | `test1163_parse_single_strand_two_caps_connected_via_shared_node` | TEST1163: Parsing one connected strand yields a single machine strand with both caps connected by the shared node. | src/machine/parser.rs:544 |
| test1164 | `test1164_parse_two_disconnected_strands_yields_two_machine_strands` | TEST1164: Parsing two disconnected strand definitions yields two separate machine strands. | src/machine/parser.rs:566 |
| test1165 | `test1165_parse_unknown_cap_in_registry_fails_hard` | TEST1165: Parsing fails hard when a referenced cap is missing from the registry cache. | src/machine/parser.rs:612 |
| test1166 | `test1166_parse_duplicate_alias_is_syntax_error` | TEST1166: Duplicate header aliases are reported as syntax errors. | src/machine/parser.rs:628 |
| test1167 | `test1167_parse_undefined_alias_is_syntax_error` | TEST1167: Wiring that references an undefined alias is reported as a syntax error. | src/machine/parser.rs:643 |
| test1168 | `test1168_parse_node_alias_collision_with_header_alias_fails_hard` | TEST1168: Parsing rejects node names that collide with declared cap aliases. | src/machine/parser.rs:657 |
| test1169 | `test1169_parse_loop_marker_sets_is_loop_on_resolved_edge` | TEST1169: Loop markers in notation set the resolved edge loop flag on the following cap step. | src/machine/parser.rs:675 |
| test1170 | `test1170_parse_then_serialize_round_trips_to_canonical_form` | TEST1170: Parsing and then serializing machine notation round-trips to the canonical form. | src/machine/parser.rs:698 |
| test1171 | `test1171_parse_empty_notation_is_syntax_error` | TEST1171: Empty machine notation is rejected as a syntax error. | src/machine/parser.rs:725 |
| test1172 | `test1172_serialize_two_step_strand_emits_global_aliases_and_node_names` | TEST1172: Serializing a two-step strand emits the expected aliases and node names. | src/machine/serializer.rs:519 |
| test1173 | `test1173_serialize_then_parse_round_trip_preserves_strict_equivalence` | TEST1173: Serializing and reparsing a machine preserves strict machine equivalence. | src/machine/serializer.rs:542 |
| test1174 | `test1174_line_based_format_round_trips_to_same_machine` | TEST1174: The line-based notation format round-trips back to the same machine. | src/machine/serializer.rs:562 |
| test1175 | `test1175_empty_machine_serializes_to_empty_string` | TEST1175: Serializing an empty machine produces an empty string. | src/machine/serializer.rs:580 |
| test1176 | `test1176_render_payload_json_includes_strand_with_anchors` | TEST1176: Rendering payload JSON includes strand anchor metadata for a populated machine. | src/machine/serializer.rs:588 |
| test1177 | `test1177_render_payload_for_empty_machine_has_empty_strands_array` | TEST1177: Rendering payload JSON for an empty machine emits an empty strands array. | src/machine/serializer.rs:620 |
| test1178 | `test1178_match_single_source_picks_unique_arg` | TEST1178: One source is assigned to the single compatible cap argument. | src/machine/resolve.rs:746 |
| test1179 | `test1179_match_more_specific_source_assigned_to_general_arg` | TEST1179: Source-to-arg matching assigns a more specific source to a compatible general argument. | src/machine/resolve.rs:763 |
| test1180 | `test1180_match_unmatched_source_fails_hard` | TEST1180: Matching fails when a source does not conform to any cap input argument. | src/machine/resolve.rs:782 |
| test1181 | `test1181_match_two_sources_disambiguated_by_specificity` | TEST1181: Two sources are matched deterministically when specificity breaks the tie. | src/machine/resolve.rs:806 |
| test1182 | `test1182_match_ambiguous_when_two_sources_could_swap` | TEST1182: Matching fails as ambiguous when two sources can be swapped at equal minimum cost. | src/machine/resolve.rs:849 |
| test1183 | `test1183_match_more_sources_than_args_fails_hard` | TEST1183: Matching fails when more sources are provided than the cap has input arguments. | src/machine/resolve.rs:873 |
| test1184 | `test1184_resolve_strand_single_cap_produces_one_edge` | TEST1184: Resolving a strand with one cap produces one resolved machine edge. | src/machine/resolve.rs:888 |
| test1185 | `test1185_resolve_strand_chained_caps_share_intermediate_node` | TEST1185: Resolving a chained strand reuses the intermediate node between adjacent caps. | src/machine/resolve.rs:930 |
| test1186 | `test1186_resolve_strand_foreach_marks_following_cap_as_loop` | TEST1186: Resolving a strand with ForEach marks the following cap edge as a loop. | src/machine/resolve.rs:998 |
| test1187 | `test1187_resolve_strand_unknown_cap_fails_hard` | TEST1187: Strand resolution fails when a referenced cap is not found in the registry. | src/machine/resolve.rs:1079 |
| test1188 | `test1188_resolve_strand_no_cap_steps_fails_hard` | TEST1188: Strand resolution fails when the strand contains no capability steps. | src/machine/resolve.rs:1096 |
| test1189 | `test1189_resolve_strand_canonical_anchor_order_is_stable` | TEST1189: Strand resolution keeps canonical anchor ordering stable across equivalent inputs. | src/machine/resolve.rs:1108 |
| test1190 | `test1190_resolve_strand_inverse_format_converters_no_cycle` | TEST1190: Inverse format converters resolve without introducing a cycle in the strand graph. | src/machine/resolve.rs:1140 |
| test1191 | `test1191_resolve_strand_disbind_pdf_with_file_path_slot_identity` | TEST1191: Disbinding a PDF with a file-path slot preserves the expected identity of the slot binding. | src/machine/resolve.rs:1204 |
| test1192 | `test1192_parse_simple_header_and_wiring` | TEST1192: Parsing a simple header and wiring produces a valid AST with both statements. | src/machine/notation_ast.rs:1704 |
| test1193 | `test1193_parse_empty_returns_error` | TEST1193: Parsing empty notation returns an error in the AST. | src/machine/notation_ast.rs:1729 |
| test1194 | `test1194_parse_invalid_returns_partial_ast` | TEST1194: Parsing invalid notation still returns a partial AST alongside the error. | src/machine/notation_ast.rs:1737 |
| test1195 | `test1195_parse_loop_wiring` | TEST1195: Parsing loop wiring records the loop structure in the notation AST. | src/machine/notation_ast.rs:1748 |
| test1196 | `test1196_parse_fan_in_group` | TEST1196: Parsing a fan-in group records grouped input sources correctly. | src/machine/notation_ast.rs:1772 |
| test1197 | `test1197_context_after_open_bracket` | TEST1197: Completion context after an opening bracket identifies header-start context. | src/machine/notation_ast.rs:1804 |
| test1198 | `test1198_context_after_cap_prefix` | TEST1198: Completion context after the cap prefix identifies cap-URN editing context. | src/machine/notation_ast.rs:1811 |
| test1199 | `test1199_context_in_media_urn` | TEST1199: Completion context inside a media URN is recognized correctly. | src/machine/notation_ast.rs:1819 |
| test1200 | `test1200_context_after_arrow` | TEST1200: Completion context after an arrow identifies the expected next token position. | src/machine/notation_ast.rs:1827 |
| test1201 | `test1201_context_outside_brackets` | TEST1201: Completion context outside brackets is recognized as the outer notation context. | src/machine/notation_ast.rs:1834 |
| test1202 | `test1202_semantic_tokens_simple` | TEST1202: Semantic token generation marks the expected token kinds for simple notation. | src/machine/notation_ast.rs:1845 |
| test1203 | `test1203_editor_model_entity_hover_for_alias_definition` | TEST1203: Editor model hover metadata resolves correctly for an alias definition. | src/machine/notation_ast.rs:1916 |
| test1204 | `test1204_editor_model_entity_hover_for_wiring_source_node` | TEST1204: Editor model hover metadata resolves correctly for a wiring source node reference. | src/machine/notation_ast.rs:1937 |
| test1205 | `test1205_editor_model_entity_hover_for_loop_keyword` | TEST1205: Editor model hover metadata resolves correctly for the loop keyword. | src/machine/notation_ast.rs:1952 |
| test1206 | `test1206_editor_model_graph_contains_nodes_and_edges` | TEST1206: The editor model graph includes the expected nodes and edges from parsed notation. | src/machine/notation_ast.rs:1971 |
| test1207 | `test1207_editor_model_cap_alias_and_arrows_share_token_id_with_graph_cap` | TEST1207: Cap alias tokens and arrow tokens share the same graph token identity for the cap. | src/machine/notation_ast.rs:2000 |
| test1208 | `test1208_editor_model_node_references_share_token_id_with_graph_node` | TEST1208: Node references share the same token identity as their graph node in the editor model. | src/machine/notation_ast.rs:2045 |
| test1209 | `test1209_parse_line_based_header_and_wiring` | TEST1209: Parsing line-based headers and wirings produces the expected AST. | src/machine/notation_ast.rs:2095 |
| test1210 | `test1210_parse_mixed_bracketed_and_line_based` | TEST1210: Parsing mixed bracketed and line-based notation works within the same document. | src/machine/notation_ast.rs:2113 |
| test1211 | `test1211_line_based_completion_context_header` | TEST1211: Line-based completion recognizes header context at the current cursor. | src/machine/notation_ast.rs:2130 |
| test1212 | `test1212_line_based_completion_context_wiring` | TEST1212: Line-based completion recognizes wiring context at the current cursor. | src/machine/notation_ast.rs:2137 |
| test1213 | `test1213_line_based_completion_context_existing_wiring_source` | TEST1213: Line-based completion recognizes an existing wiring source context. | src/machine/notation_ast.rs:2144 |
| test1214 | `test1214_bracketed_completion_context_existing_wiring_source` | TEST1214: Bracketed completion recognizes an existing wiring source context. | src/machine/notation_ast.rs:2152 |
| test1215 | `test1215_line_based_completion_context_start` | TEST1215: Line-based completion recognizes the start-of-line notation context. | src/machine/notation_ast.rs:2160 |
| test1216 | `test1216_loop_keyword_suggested_only_for_sequence_source` | TEST1216: Loop keyword completion is suggested only when the source is a sequence. | src/machine/notation_ast.rs:2167 |
| test1217 | `test1217_loop_keyword_not_suggested_for_scalar_source` | TEST1217: Loop keyword completion is not suggested for scalar sources. | src/machine/notation_ast.rs:2182 |
| test1218 | `test1218_line_based_semantic_tokens_no_brackets` | TEST1218: Semantic tokens are produced correctly for line-based notation without brackets. | src/machine/notation_ast.rs:2197 |
| test1219 | `test1219_byte_offset_to_position_works` | TEST1219: Byte offsets are converted to line and character positions correctly. | src/machine/notation_ast.rs:2228 |
| test1220 | `test1220_line_char_to_offset_works` | TEST1220: Line and character coordinates are converted back to byte offsets correctly. | src/machine/notation_ast.rs:2255 |
| test1221 | `test1221_refine_with_matching_adapter` | TEST1221: Matching value adapters refine the base media URN when the value fits. | src/input_resolver/value_adapter_registry.rs:150 |
| test1222 | `test1222_refine_no_matching_adapter` | TEST1222: Base URNs without a registered adapter are returned unchanged. | src/input_resolver/value_adapter_registry.rs:160 |
| test1223 | `test1223_refine_adapter_returns_none` | TEST1223: Adapters that decline to refine leave the original media URN intact. | src/input_resolver/value_adapter_registry.rs:170 |
| test1224 | `test1224_refine_longest_prefix_match` | TEST1224: When multiple adapter prefixes match, the longest prefix wins. | src/input_resolver/value_adapter_registry.rs:180 |
| test1225 | `test1225_empty_registry` | TEST1225: An empty value adapter registry returns the input media URN unchanged. | src/input_resolver/value_adapter_registry.rs:192 |
| test1226 | `test1226_has_adapter` | TEST1226: Adapter presence checks report only the prefixes that were registered. | src/input_resolver/value_adapter_registry.rs:200 |
| test1228 | `test1228_value_adapter_refine_match` | TEST1228: Value adapters can append a more specific marker when both base URN and value match. | src/input_resolver/value_adapter.rs:78 |
| test1229 | `test1229_value_adapter_refine_no_match_base` | TEST1229: Value adapters return no refinement when the base media URN is outside their domain. | src/input_resolver/value_adapter.rs:91 |
| test1230 | `test1230_value_adapter_refine_no_match_value` | TEST1230: Value adapters return no refinement when the inspected value does not match. | src/input_resolver/value_adapter.rs:99 |
| test1235 | `test1235_disc_1_plain_text_eliminates_model_specs` | TEST1235: Plain text without model-spec syntax eliminates model-spec TXT candidates. | src/input_resolver/resolver.rs:566 |
| test1236 | `test1236_disc_2_model_spec_content_survives_pattern` | TEST1236: Colon-delimited model spec text survives TXT candidate discrimination. | src/input_resolver/resolver.rs:586 |
| test1237 | `test1237_disc_5_empty_candidates` | TEST1237: Empty candidates → empty result | src/input_resolver/resolver.rs:604 |
| test1238 | `test1238_disc_6_unknown_urn_survives` | TEST1238: Unknown URN survives discrimination | src/input_resolver/resolver.rs:613 |
| test1243 | `test1243_roundtrip_serialize_deserialize` | TEST1243: Cartridge JSON round-trips through serde without losing required fields. | src/bifaci/cartridge_json.rs:402 |
| test1244 | `test1244_dev_install_omits_optional_fields` | TEST1244: Dev-installed cartridge metadata omits registry-only package fields when serialized. | src/bifaci/cartridge_json.rs:505 |
| test1245 | `test1245_read_from_dir_validates_entry_exists` | TEST1245: Reading cartridge metadata fails when the declared entry binary is missing. | src/bifaci/cartridge_json.rs:536 |
| test1246 | `test1246_read_from_dir_rejects_path_escape` | TEST1246: Cartridge entry points cannot escape the cartridge directory with relative paths. | src/bifaci/cartridge_json.rs:559 |
| test1247 | `test1247_read_from_dir_succeeds_with_valid_cartridge` | TEST1247: Valid cartridge directories load successfully and resolve their entry point. | src/bifaci/cartridge_json.rs:591 |
| test1248 | `test1248_hash_cartridge_directory_is_deterministic` | TEST1248: Cartridge directory hashes stay stable across metadata changes and change on content edits. | src/bifaci/cartridge_json.rs:762 |
| test1249 | `test1249_hash_single_binary_matches_flat_layout` | TEST1249: A flat single-binary cartridge directory still produces a SHA-256 content hash. | src/bifaci/cartridge_json.rs:789 |
| test1250 | `test1250_process_handle_snapshot_empty_initially` | TEST1250: Process snapshots start empty before any cartridges are attached or spawned. | src/bifaci/host_runtime.rs:4329 |
| test1251 | `test1251_process_handle_snapshot_excludes_attached_cartridges` | TEST1251: Attached cartridges without child PIDs are excluded from process snapshots. | src/bifaci/host_runtime.rs:4341 |
| test1252 | `test1252_process_handle_is_clone_and_send` | TEST1252: Cartridge process handles remain usable after clone-and-send across tasks. | src/bifaci/host_runtime.rs:4373 |
| test1253 | `test1253_process_handle_kill_unknown_pid_is_noop` | TEST1253: Killing an unknown PID is accepted as an asynchronous no-op command. | src/bifaci/host_runtime.rs:4389 |
| test1254 | `test1254_oom_kill_sends_err_with_oom_killed_code` | TEST1254: OOM shutdowns emit OOM_KILLED ERR frames for in-flight requests. | src/bifaci/host_runtime.rs:4406 |
| test1255 | `test1255_app_exit_suppresses_err_frames` | TEST1255: App-exit shutdowns suppress ERR frames and close cleanly without noise. | src/bifaci/host_runtime.rs:4533 |
| test1256 | `test1256_parse_simple_machine` | TEST1256: A single declared cap and one wiring parse into a two-node one-edge DAG. | src/orchestrator/parser.rs:303 |
| test1257 | `test1257_parse_two_step_chain` | TEST1257: Two sequential wirings preserve the intermediate node media type. | src/orchestrator/parser.rs:342 |
| test1258 | `test1258_parse_fan_out` | TEST1258: One source node can fan out into multiple caps and target nodes. | src/orchestrator/parser.rs:386 |
| test1259 | `test1259_parse_fan_in` | TEST1259: Fan-in wiring resolves multiple upstream outputs into one multi-arg cap. | src/orchestrator/parser.rs:428 |
| test1260 | `test1260_parse_loop_wiring` | TEST1260: LOOP wiring parses as a single edge while preserving the loop marker semantics. | src/orchestrator/parser.rs:475 |
| test1261 | `test1261_cap_not_found_in_registry` | TEST1261: Parsing fails with CapNotFound when a declared cap is absent from the registry. | src/orchestrator/parser.rs:501 |
| test1262 | `test1262_invalid_machine_notation` | TEST1262: Non-machine text fails with a machine syntax parse error. | src/orchestrator/parser.rs:522 |
| test1263 | `test1263_cycle_detection` | TEST1263: Cyclic wirings are rejected as non-DAG orchestrations. | src/orchestrator/parser.rs:541 |
| test1264 | `test1264_incompatible_media_types_at_shared_node` | TEST1264: Shared nodes with incompatible upstream and downstream media fail during parsing. | src/orchestrator/parser.rs:572 |
| test1265 | `test1265_compatible_media_urns_at_shared_node` | TEST1265: Shared nodes accept compatible media URNs when one is a more specific form of the other. | src/orchestrator/parser.rs:614 |
| test1266 | `test1266_structure_mismatch_record_to_opaque` | TEST1266: Record-to-opaque structure mismatches are rejected once structure checking is enabled. | src/orchestrator/parser.rs:656 |
| test1267 | `test1267_structure_match_both_record` | TEST1267: Record-shaped outputs can feed record-shaped inputs without error. | src/orchestrator/parser.rs:702 |
| test1268 | `test1268_structure_match_both_opaque` | TEST1268: Opaque outputs can feed opaque inputs without triggering structure conflicts. | src/orchestrator/parser.rs:737 |
| test1269 | `test1269_parse_multiline_machine` | TEST1269: Multi-line machine notation parses successfully with the same semantics as inline notation. | src/orchestrator/parser.rs:772 |
| test1270 | `test1270_get_own_memory_mb_returns_values` | TEST1270: Runtime memory inspection returns non-negative resident and virtual memory values. | src/bifaci/cartridge_runtime.rs:8295 |
| test1271 | `test1271_media_adapter_selection_constant` | TEST1271: MEDIA_ADAPTER_SELECTION constant parses and has expected tags | src/urn/media_urn.rs:1262 |
| test1272 | `test1272_adapter_cap_constant_parses` | TEST1272: CAP_ADAPTER_SELECTION constant parses as a valid CapUrn | src/standard/caps.rs:1253 |
| test1273 | `test1273_adapter_selection_urn_builder` | TEST1273: adapter_selection_urn() returns a valid CapUrn with correct in/out specs | src/standard/caps.rs:1264 |
| test1274 | `test1274_adapter_selection_cap_builder` | TEST1274: adapter_selection_cap() builds a valid Cap with correct args and output | src/standard/caps.rs:1282 |
| test1275 | `test1275_adapter_selection_dispatchable_by_specific_provider` | TEST1275: A cap whose output is adapter-selection can dispatch adapter-selection requests; identity (wildcard output) cannot, because wildcard output cannot satisfy a specific output requirement. | src/standard/caps.rs:1303 |
| test1276 | `test1276_register_non_conflicting` | TEST1276: Registration of a cap group with non-conflicting adapters succeeds | src/input_resolver/adapters/registry.rs:250 |
| test1277 | `test1277_reject_conforming_overlap` | TEST1277: Registration of a cap group with an adapter that conforms_to an existing adapter is rejected | src/input_resolver/adapters/registry.rs:268 |
| test1278 | `test1278_reject_entire_group` | TEST1278: Registration rejects the entire group — no partial registration | src/input_resolver/adapters/registry.rs:292 |
| test1279 | `test1279_intra_group_conflict` | TEST1279: Intra-group conflict (two adapters within same group overlap) is rejected | src/input_resolver/adapters/registry.rs:323 |
| test1280 | `test1280_find_adapters_for_extension` | TEST1280: find_adapters_for_extension returns correct cartridge IDs | src/input_resolver/adapters/registry.rs:341 |
| test1281 | `test1281_no_adapter_for_unknown` | TEST1281: has_adapter_for_extension returns false for unregistered extension | src/input_resolver/adapters/registry.rs:362 |
| test1282 | `test1282_adapter_selection_auto_registered` | TEST1282: AdapterSelectionOp is auto-registered by CartridgeRuntime | src/bifaci/cartridge_runtime.rs:7228 |
| test1283 | `test1283_adapter_selection_custom_override` | TEST1283: Custom adapter selection Op overrides the default | src/bifaci/cartridge_runtime.rs:7239 |
| test1284 | `test1284_cap_group_with_adapter_urns` | TEST1284: Cap group with adapter URNs serializes and deserializes correctly | src/bifaci/manifest.rs:554 |
| test1285 | `test1285_confirmed_no_adapters_fails` | TEST1285: detect_file_confirmed fails when no adapters are registered for the extension | src/input_resolver/resolver.rs:662 |
| test1286 | `test1286_confirmed_adapter_returns_urns` | TEST1286: detect_file_confirmed succeeds when adapter returns URNs | src/input_resolver/resolver.rs:685 |
| test1287 | `test1287_confirmed_all_adapters_no_match` | TEST1287: detect_file_confirmed fails when all adapters return empty END (no match) | src/input_resolver/resolver.rs:719 |
| test1288 | `test1288_structure_from_marker_tags` | TEST1288: structure_from_marker_tags correctly maps tag combinations to ContentStructure | src/input_resolver/resolver.rs:626 |
| test1289 | `test1289_bfs_reachable_includes_source_roundtrip` | TEST1289: BFS reachable targets includes the source itself when round-trip paths exist. A→B and B→A means A is reachable from A (via A→B→A). | src/planner/live_cap_fab.rs:2267 |
| test1290 | `test1290_iddfs_finds_roundtrip_paths` | TEST1290: IDDFS find_paths_to_exact_target finds round-trip paths when source == target. This was a bug where the visited set blocked returning to the source, and early return on target hit at wrong depth prevented exploration. | src/planner/live_cap_fab.rs:2303 |
| test1291 | `test1291_iddfs_roundtrip_with_sequence` | TEST1291: IDDFS round-trip paths are also found with is_sequence=true. The ForEach/Collect edges must not block round-trip discovery. | src/planner/live_cap_fab.rs:2342 |
| test1292 | `test1292_bfs_iddfs_roundtrip_consistency` | TEST1292: BFS and IDDFS agree that round-trip targets exist. If BFS says target X is reachable from source X, IDDFS must find at least one path. | src/planner/live_cap_fab.rs:2375 |
| test1293 | `test1293_roundtrip_requires_cap_steps` | TEST1293: IDDFS round-trip does not produce paths with 0 cap steps. Identity-only round trips (no real transformation) must be excluded. | src/planner/live_cap_fab.rs:2409 |
| test1294 | `test1294_rule11_void_input_with_stdin_rejected` | TEST1294: RULE11 - void-input cap with stdin source rejected | src/cap/validation.rs:1736 |
| test1295 | `test1295_rule11_non_void_input_without_stdin_rejected` | TEST1295: RULE11 - non-void-input cap without stdin source rejected | src/cap/validation.rs:1756 |
| test1296 | `test1296_rule11_void_input_cli_only_ok` | TEST1296: RULE11 - void-input cap with only cli_flag sources passes | src/cap/validation.rs:1778 |
| test1297 | `test1297_rule11_non_void_input_with_stdin_ok` | TEST1297: RULE11 - non-void-input cap with stdin source passes | src/cap/validation.rs:1796 |
| test1500 | `test1500_slug_for_central_registry_is_stable` | / TEST1500: The default central registry's URL hashes to a stable, / pre-computed slug. If this value ever changes silently it means / either the encoding rule shifted or the hashing algorithm / changed — either way every installed cartridge would land in / the wrong directory and stop being discovered. The slug is / pinned as a literal so a regression is loud. | src/bifaci/cartridge_slug.rs:102 |
| test1501 | `test1501_slug_for_none_is_dev` | / TEST1501: `None` (dev cartridge) maps to the literal `dev` and / never to a hex slug. The dev sentinel must remain / distinguishable from registry slugs by length alone — no / caller should ever hash the string "dev" to get this value. | src/bifaci/cartridge_slug.rs:123 |
| test1502 | `test1502_slug_byte_sensitivity` | / TEST1502: The URL is treated as raw bytes — adding a trailing / slash, changing case, or appending a query string yields a / different slug. Proves we are not normalizing the URL behind / the operator's back; if they typed two URLs that look "the / same" but differ byte-wise, those are two distinct registries. | src/bifaci/cartridge_slug.rs:135 |
| test1503 | `test1503_slug_is_deterministic` | / TEST1503: Calling `slug_for` twice on the same URL returns the / same string. Determinism is the whole point of using a hash / here — if this fails, every install/restart would land in a / different folder and discovery would be permanently broken. | src/bifaci/cartridge_slug.rs:150 |
| test1504 | `test1504_dev_never_collides_with_hex_slug` | / TEST1504: A 16-character hex slug can never equal the literal / `dev` — `dev` is 3 characters, so by-length comparison alone / rules out collision. This invariant is what lets us use the / folder name as a dev-vs-registry discriminator without / reading any file inside the directory. | src/bifaci/cartridge_slug.rs:165 |
| test1505 | `test1505_is_registry_slug_classification` | / TEST1505: `is_registry_slug` rejects the dev sentinel, accepts / 16-hex strings, rejects anything else. Used by the XPC service / and engine to distinguish dev folders from registry folders / during the pre-read scan. | src/bifaci/cartridge_slug.rs:190 |
| | | | |
| test336 ⚠ | `test336_file_path_reads_file_passes_bytes` | TEST336: Single file-path arg with stdin source reads file and passes bytes to handler | src/bifaci/cartridge_runtime.rs:5259 |
| test336 ⚠ | `test336_update_cache_rejects_malformed_cap_urn` | TEST336: A registry response with a malformed cap URN inside cap_groups must propagate as ParseError when indexed into the cache, not silently disappear. | src/bifaci/cartridge_repo.rs:1884 |
| | | | |
| unnumbered | `test1243b_channel_roundtrip_nightly` | TEST1243b: Channel round-trips correctly. A nightly cartridge.json must deserialize back to the Nightly variant — channels are independent namespaces, conflating them would be a real bug. | src/bifaci/cartridge_json.rs:432 |
| unnumbered | `test1243c_missing_channel_fails_to_parse` | TEST1243c: Reading a cartridge.json without `channel` is a hard error. We never assume a default — that would let an unrecognized install silently masquerade as release. | src/bifaci/cartridge_json.rs:462 |
| unnumbered | `test1243d_missing_registry_url_fails_to_parse` | TEST1243d: Reading a cartridge.json without `registry_url` is a hard error too. The field is required-but-nullable; absence means the file was written by a pre-registry-aware installer and we can't tell whether it was meant to be dev or registry — both cases fail the three-place check, so we surface the schema gap immediately. | src/bifaci/cartridge_json.rs:486 |
| unnumbered | `test1247b_read_from_dir_rejects_slug_mismatch` | TEST1247b: Three-place rule — a registry cartridge whose folder slug doesn't match `slug_for(registry_url)` is rejected. This catches the case where a cartridge tree was hand-copied between registry roots, or a buggy installer wrote the wrong slug. The error names both slugs so the operator can tell at a glance which side is wrong. | src/bifaci/cartridge_json.rs:624 |
| unnumbered | `test1247c_read_from_dir_rejects_dev_in_registry_folder` | TEST1247c: Three-place rule — a dev cartridge.json under a non-dev folder is rejected. Equivalent to a dev-built cartridge being moved into a registry's folder; the host refuses because the binary was never built/signed for that registry. | src/bifaci/cartridge_json.rs:668 |
| unnumbered | `test1247d_read_from_dir_rejects_registry_in_dev_folder` | TEST1247d: Three-place rule — a registry cartridge.json under the dev folder is rejected. Equivalent to a published cartridge being dropped into the dev tree; the dev tree is explicitly the only place a null `registry_url` is allowed, so a non-null one here means the layout is corrupted. | src/bifaci/cartridge_json.rs:702 |
| unnumbered | `test1247e_read_from_dir_accepts_dev_in_dev_folder` | TEST1247e: A dev cartridge.json under the dev folder is accepted. This is the only path that ends with a successful dev install; together with 1247c/1247d it pins the rule that dev provenance and the dev folder are an inseparable pair. | src/bifaci/cartridge_json.rs:735 |
| unnumbered | `test148b_cap_manifest_channel_roundtrip` | TEST148b: A manifest's channel round-trips through serde and the serialized form uses the canonical lowercase wire word ("release" / "nightly"). A missing or unrecognized channel is a hard parse error — no defaults. | src/bifaci/manifest.rs:265 |
| unnumbered | `test148c_dev_manifest_registry_url_is_explicit_null` | TEST148c: A dev manifest (built without `MFR_REGISTRY_URL`) carries `registry_url: null` and serializes the field explicitly. The null-vs-absent distinction matters because the parser refuses to accept absent (test148b) — so an old SDK can't accidentally pass for a dev build. | src/bifaci/manifest.rs:338 |
| unnumbered | `test326b_get_cartridge_by_id_channel_isolation` | TEST326b: A cartridge with the same id can independently exist in both channels. Each lookup must return the channel-specific entry. | src/bifaci/cartridge_repo.rs:1575 |

---

## ⚠ Duplicate Test Numbers

The following test numbers are assigned to more than one function. Keep the first occurrence at the existing number and renumber the rest using the suggested free numbers below.

### test336 (2 occurrences)

- `test336_file_path_reads_file_passes_bytes` — src/bifaci/cartridge_runtime.rs:5259
- `test336_update_cache_rejects_malformed_cap_urn` — src/bifaci/cartridge_repo.rs:1884

**Suggested free number(s):** test319

---

## Unnumbered Tests

The following tests are cataloged but do not currently participate in numeric test indexing.

- `test1243b_channel_roundtrip_nightly` — src/bifaci/cartridge_json.rs:432
- `test1243c_missing_channel_fails_to_parse` — src/bifaci/cartridge_json.rs:462
- `test1243d_missing_registry_url_fails_to_parse` — src/bifaci/cartridge_json.rs:486
- `test1247b_read_from_dir_rejects_slug_mismatch` — src/bifaci/cartridge_json.rs:624
- `test1247c_read_from_dir_rejects_dev_in_registry_folder` — src/bifaci/cartridge_json.rs:668
- `test1247d_read_from_dir_rejects_registry_in_dev_folder` — src/bifaci/cartridge_json.rs:702
- `test1247e_read_from_dir_accepts_dev_in_dev_folder` — src/bifaci/cartridge_json.rs:735
- `test148b_cap_manifest_channel_roundtrip` — src/bifaci/manifest.rs:265
- `test148c_dev_manifest_registry_url_is_explicit_null` — src/bifaci/manifest.rs:338
- `test326b_get_cartridge_by_id_channel_isolation` — src/bifaci/cartridge_repo.rs:1575

---

*Generated from Rust source tree*
*Total tests: 1061*
*Total numbered tests: 1051*
*Total unnumbered tests: 10*
*Total numbered tests missing descriptions: 0*
*Total numbering mismatches: 0*
