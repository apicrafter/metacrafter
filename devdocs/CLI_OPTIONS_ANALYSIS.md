# Command Line Options Analysis and Suggestions

## Current CLI Options Summary

### Existing Commands:
1. **`scan file`** - Scan data files (CSV, JSON, JSONL, BSON, Parquet, XML, etc.)
2. **`scan sql`** - Scan SQL databases via SQLAlchemy connection string
3. **`scan mongodb`** - Scan MongoDB databases
4. **`scan bulk`** - Scan multiple files in a directory
5. **`rules list`** - List all available rules
6. **`rules stats`** - Show rules statistics
7. **`server run`** - Start API server

### Current Options (across scan commands):
- `filename`, `connstr`, `host`, `port`, `dbname`, `dirname`
- `delimiter`, `tagname`, `encoding`
- `limit` (default: 100 for file, 1000 for DB)
- `contexts`, `langs` (comma-separated filters)
- `format` (short/full)
- `output` (output file path)
- `remote` (remote server URL)
- `debug` (boolean)
- `schema` (for SQL databases)
- `username`, `password` (for MongoDB)

---

## Suggested New Command Line Options

### High Priority - Core Functionality

#### 1. **Confidence Threshold** (`--confidence`, `-c`)
- **Current state**: Hardcoded to 5.0% (`MIN_CONFIDENCE_FOR_MATCH`)
- **Suggestion**: Add `--confidence FLOAT` option (default: 5.0)
- **Use case**: Allow users to set minimum confidence threshold for rule matching
- **Implementation**: Pass to `match_dict(confidence=...)`
- **Commands**: All `scan` commands

#### 2. **Stop on First Match** (`--stop-on-match`)
- **Current state**: Available in `match_dict()` but not exposed
- **Suggestion**: Add `--stop-on-match` flag
- **Use case**: Stop processing after first rule match (performance optimization)
- **Implementation**: Pass `stop_on_match=True` to `match_dict()`
- **Commands**: All `scan` commands

#### 3. **Disable Date Parsing** (`--no-dates`, `--skip-dates`)
- **Current state**: Always enabled (`parse_dates=True`)
- **Suggestion**: Add `--no-dates` flag to disable date pattern matching
- **Use case**: Skip date detection when not needed (performance)
- **Implementation**: Pass `parse_dates=False` to `match_dict()`
- **Commands**: All `scan` commands

#### 4. **Include Imprecise Rules** (`--include-imprecise`)
- **Current state**: Imprecise rules are ignored by default (`ignore_imprecise=True`)
- **Suggestion**: Add `--include-imprecise` flag
- **Use case**: Include less precise rules in matching
- **Implementation**: Pass `ignore_imprecise=False` to `match_dict()`
- **Commands**: All `scan` commands

#### 5. **Include Empty Values** (`--include-empty`)
- **Current state**: Empty values excluded by default (`except_empty=True`)
- **Suggestion**: Add `--include-empty` flag
- **Use case**: Include empty/None values in confidence calculations
- **Implementation**: Pass `except_empty=False` to `match_dict()`
- **Commands**: All `scan` commands

#### 6. **Custom Rule Paths** (`--rulepath`, `--rules`)
- **Current state**: Uses config file or default `rules/` directory
- **Suggestion**: Add `--rulepath PATH` (can be specified multiple times)
- **Use case**: Override default rule paths, use custom rule sets
- **Implementation**: Override `ConfigLoader.get_rulepath()` with CLI values
- **Commands**: All commands that use rules

#### 7. **Field Filtering** (`--fields`, `--only-fields`)
- **Current state**: Processes all fields
- **Suggestion**: Add `--fields FIELD1,FIELD2,...` option
- **Use case**: Process only specific fields (performance, focused analysis)
- **Implementation**: Pass `fields=...` to `match_dict()`
- **Commands**: All `scan` commands

---

### Medium Priority - Output and Formatting

#### 8. **Output Format Selection** (`--output-format`)
- **Current state**: Only "short" and "full" table formats
- **Suggestion**: Add `--output-format {short|full|json|csv|yaml}` option
- **Use case**: Machine-readable output formats, better integration
- **Implementation**: Extend `_write_results()` to support multiple formats
- **Commands**: All `scan` commands

#### 9. **Verbose/Quiet Modes** (`--verbose`, `--quiet`)
- **Current state**: Only `--debug` flag exists
- **Suggestion**: 
  - `--verbose` / `-v`: Show detailed progress and info
  - `--quiet` / `-q`: Suppress non-essential output
- **Use case**: Better control over output verbosity
- **Implementation**: Add logging levels and progress indicators
- **Commands**: All commands

#### 10. **Progress Indicators** (`--progress`, `--no-progress`)
- **Current state**: No progress indication for long operations
- **Suggestion**: Add `--progress` flag (default: auto-detect based on TTY)
- **Use case**: Show progress bars for large files/databases
- **Implementation**: Use `tqdm` or similar library
- **Commands**: All `scan` commands

#### 11. **Statistics Output** (`--stats-only`)
- **Current state**: Statistics calculated but not separately accessible
- **Suggestion**: Add `--stats-only` flag to output only field statistics
- **Use case**: Get data statistics without classification
- **Implementation**: Use `Analyzer.analyze()` output directly
- **Commands**: All `scan` commands

---

### Medium Priority - Configuration and Rules

#### 12. **Custom Config File** (`--config`)
- **Current state**: Uses `.metacrafter` in current or home directory
- **Suggestion**: Add `--config PATH` option
- **Use case**: Use different config files for different projects
- **Implementation**: Override config file path in `ConfigLoader`
- **Commands**: All commands

#### 13. **Date Pattern Selection** (`--date-patterns`)
- **Current state**: Uses `PATTERNS_EN + PATTERNS_RU`
- **Suggestion**: Add `--date-patterns {en|ru|en+ru|all}` option
- **Use case**: Optimize date detection for specific languages
- **Implementation**: Select patterns based on option
- **Commands**: All `scan` commands

#### 14. **Rule Validation Command** (`rules validate`)
- **Current state**: No validation command
- **Suggestion**: Add `metacrafter rules validate [RULE_FILE]` command
- **Use case**: Validate rule YAML files before use
- **Implementation**: Check YAML syntax, rule structure, PyParsing compilation
- **Commands**: New `rules validate` command

#### 15. **Rule Search/Filter** (`rules search`)
- **Current state**: Only `rules list` shows all rules
- **Suggestion**: Add `metacrafter rules search --key KEY --type TYPE --lang LANG`
- **Use case**: Find specific rules by criteria
- **Implementation**: Filter rules by criteria and display
- **Commands**: New `rules search` command

---

### Lower Priority - Advanced Features

#### 16. **Batch Size for Database Scans** (`--batch-size`)
- **Current state**: Hardcoded `DEFAULT_BATCH_SIZE = 1000`
- **Suggestion**: Add `--batch-size INT` option
- **Use case**: Optimize memory usage for large databases
- **Implementation**: Use in `scan_db()` batch processing
- **Commands**: `scan sql`, `scan mongodb`

#### 17. **Dictionary Share Threshold** (`--dict-share`)
- **Current state**: Hardcoded `DEFAULT_DICT_SHARE = 10`
- **Suggestion**: Add `--dict-share FLOAT` option
- **Use case**: Control when fields are considered "dictionary" types
- **Implementation**: Pass to `Analyzer.analyze()` options
- **Commands**: All `scan` commands

#### 18. **Custom Empty Values** (`--empty-values`)
- **Current state**: Uses `DEFAULT_EMPTY_VALUES = [None, "", "None", "NaN", "-", "N/A"]`
- **Suggestion**: Add `--empty-values VALUE1,VALUE2,...` option
- **Use case**: Customize what values are considered empty
- **Implementation**: Pass to `Analyzer.analyze()` options
- **Commands**: All `scan` commands

#### 19. **Remote API Timeout** (`--timeout`)
- **Current state**: Uses default `requests` timeout
- **Suggestion**: Add `--timeout SECONDS` option
- **Use case**: Control timeout for remote API calls
- **Implementation**: Pass to `requests.request()` timeout parameter
- **Commands**: All `scan` commands with `--remote`

#### 20. **Remote API Retries** (`--retries`, `--retry-delay`)
- **Current state**: No retry logic
- **Suggestion**: Add `--retries INT` and `--retry-delay SECONDS` options
- **Use case**: Handle transient network errors
- **Implementation**: Add retry logic with exponential backoff
- **Commands**: All `scan` commands with `--remote`

#### 21. **Compression Detection Override** (`--compression`)
- **Current state**: Auto-detected from file extension
- **Suggestion**: Add `--compression {auto|none|gz|bz2|...}` option
- **Use case**: Force compression type or disable auto-detection
- **Implementation**: Pass to file opening logic
- **Commands**: `scan file`, `scan bulk`

#### 22. **Output to Stdout** (`--stdout`, `-`)
- **Current state**: Requires `--output FILE` for file output
- **Suggestion**: Allow `--output -` or `--stdout` to write to stdout
- **Use case**: Pipe output to other commands
- **Implementation**: Check for `-` or `stdout` and use `sys.stdout`
- **Commands**: All `scan` commands

#### 23. **JSON Output Pretty Printing** (`--pretty`, `--indent`)
- **Current state**: JSON output uses default formatting
- **Suggestion**: Add `--pretty` or `--indent INT` for JSON output
- **Use case**: Human-readable JSON output
- **Implementation**: Use `json.dumps(indent=...)`
- **Commands**: All `scan` commands with JSON output

#### 24. **Table Output Format** (`--table-format`)
- **Current state**: Uses default `tabulate` format
- **Suggestion**: Add `--table-format {grid|simple|plain|...}` option
- **Use case**: Control table appearance
- **Implementation**: Pass to `tabulate()` fmt parameter
- **Commands**: All `scan` commands with table output

---

### New Command Suggestions

#### 25. **`scan stats`** - Statistics-only command
- **Purpose**: Get field statistics without classification
- **Options**: Similar to `scan file` but only outputs statistics
- **Use case**: Quick data profiling

#### 26. **`config show`** - Show current configuration
- **Purpose**: Display loaded configuration (rule paths, defaults, etc.)
- **Options**: `--format {yaml|json|table}`
- **Use case**: Debug configuration issues

#### 27. **`config init`** - Initialize configuration file
- **Purpose**: Create `.metacrafter` config file with defaults
- **Options**: `--rulepath PATH`, `--output PATH`
- **Use case**: Setup new project

#### 28. **`rules export`** - Export rules to file
- **Purpose**: Export loaded rules to JSON/YAML
- **Options**: `--format {json|yaml}`, `--output FILE`, `--filter KEY=VALUE`
- **Use case**: Backup, documentation, sharing

#### 29. **`scan compare`** - Compare scan results
- **Purpose**: Compare results from two scans
- **Options**: `--baseline FILE`, `--current FILE`, `--format {diff|table|json}`
- **Use case**: Track changes in data classification over time

---

## Implementation Priority Recommendations

### Phase 1 (High Value, Low Effort)
1. `--confidence` - Simple parameter pass-through
2. `--no-dates` - Simple boolean flag
3. `--output-format json` - Extend existing output logic
4. `--verbose` / `--quiet` - Add logging levels
5. `--rulepath` - Override config loader

### Phase 2 (High Value, Medium Effort)
6. `--fields` - Field filtering
7. `--include-imprecise` / `--include-empty` - Boolean flags
8. `--stop-on-match` - Boolean flag
9. `--stats-only` - New output mode
10. `--config` - Config file override

### Phase 3 (Medium Value, Medium Effort)
11. `rules validate` - New command
12. `rules search` - New command
13. `--progress` - Progress indicators
14. `--date-patterns` - Pattern selection
15. `--batch-size` - Database optimization

### Phase 4 (Nice to Have)
16. `--timeout` / `--retries` - Remote API improvements
17. `--dict-share` / `--empty-values` - Advanced statistics
18. `--compression` - Compression override
19. `--table-format` - Output customization
20. New commands (`config`, `scan compare`, etc.)

---

## Notes

- All new options should maintain backward compatibility
- Consider using environment variables for frequently used options
- Document default values clearly in help text
- Add validation for option values (e.g., confidence 0-100)
- Consider option aliases for common use cases (e.g., `-c` for `--confidence`)

