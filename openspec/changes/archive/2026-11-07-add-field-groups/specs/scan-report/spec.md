## ADDED Requirements

### Requirement: Field Groups

Each single-source scan report SHALL include `field_groups`: groups of sibling fields (the same
parent path) whose names, split on case changes and separators, share structure. A `prefix` group
SHALL have at least two fields starting with the same word of three or more letters that is not a
flag word, at least one of them longer than the word, and SHALL list the remaining part of each
name. A `suffix` group SHALL have at least two multi-word fields ending with the same word. A
`language` group SHALL hold a field and its language-suffix variants; prefix and suffix groups SHALL
compare the names without language suffixes.

#### Scenario: Object encoded in names

- **WHEN** a table has fields `place`, `placeURL`, `placeLocation` and `placeType`
- **THEN** `field_groups` has a prefix group `place` with parts `""`, `location`, `type`, `url`

#### Scenario: Language variants

- **WHEN** a table has fields `title`, `title_en` and `title_ru`
- **THEN** `field_groups` has a language group `title` with parts `""`, `en`, `ru`
