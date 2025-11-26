# Security Fixes Applied

This document summarizes the critical security fixes that have been implemented.

## ✅ Fixed Issues

### 1. eval() Security Vulnerability (CRITICAL)
**File:** `metacrafter/classify/processor.py`

**Problem:** Using `eval()` on user-provided YAML rule data allowed arbitrary code execution.

**Solution:**
- Created `_safe_eval_pyparsing_rule()` function that:
  - Validates rule strings against dangerous patterns
  - Uses a restricted namespace containing only allowed PyParsing objects
  - Provides clear error messages for invalid rules
- Replaced both instances of `eval()` (lines 154 and 181) with the safe evaluator
- Added proper error handling and logging

**Impact:** Prevents arbitrary code execution from malicious YAML rule files.

### 2. SQL Injection Vulnerability (CRITICAL)
**File:** `metacrafter/core.py`

**Problem:** SQL queries used string formatting without proper validation or parameterization.

**Solution:**
- Added schema name validation using regex and inspector validation
- Used SQLAlchemy's `quoted_name()` for proper identifier quoting
- Validated table names against `inspector.get_table_names()` (prevents injection)
- Parameterized the LIMIT clause using SQLAlchemy's `text()` with bind parameters
- Added proper error handling

**Impact:** Prevents SQL injection attacks through malicious schema/table names.

### 3. YAML Loading Security (HIGH)
**Files:** 
- `metacrafter/classify/processor.py`
- `metacrafter/core.py`
- `metacrafter/server/api.py`

**Problem:** Using `yaml.load()` with `FullLoader` can execute arbitrary code from untrusted YAML files.

**Solution:**
- Replaced all instances of `yaml.load(f, Loader=yaml.FullLoader)` with `yaml.safe_load(f)`
- Also improved file handling by using context managers (`with` statements)

**Impact:** Prevents code execution from malicious YAML configuration files.

### 4. Hardcoded Secret Key (HIGH)
**File:** `metacrafter/server/manager.py`

**Problem:** Secret key was hardcoded in source code, making it vulnerable if code is exposed.

**Solution:**
- Load secret key from environment variable `METACRAFTER_SECRET_KEY`
- Generate a random key using `os.urandom(32).hex()` if not provided
- Fallback to default only if `os.urandom` is unavailable (for compatibility)

**Impact:** Prevents secret key exposure in version control and allows per-deployment keys.

## Additional Improvements

### File Handling
- Converted all file operations to use context managers (`with` statements)
- Ensures proper resource cleanup even on exceptions
- Fixed undefined variable issue (`outres`) in `_write_results()` method

## Testing Recommendations

1. **Test PyParsing rule compilation:**
   - Verify existing rules still compile correctly
   - Test with malicious rule strings to ensure they're rejected

2. **Test SQL injection prevention:**
   - Attempt SQL injection through schema/table name parameters
   - Verify queries work correctly with valid inputs

3. **Test YAML loading:**
   - Verify configuration files load correctly
   - Test with malicious YAML to ensure safe_load prevents execution

4. **Test secret key:**
   - Verify server starts with environment variable
   - Verify server generates random key when variable not set

## Migration Notes

### For eval() fix:
- Existing PyParsing rules should continue to work
- Rules using dangerous patterns will now be rejected with clear error messages
- Check logs for any rules that fail to compile

### For SQL injection fix:
- No breaking changes expected
- Schema/table names are now more strictly validated
- Invalid schema names will raise `ValueError` with descriptive message

### For YAML fix:
- Should be transparent - `safe_load` works for all current use cases
- If custom YAML tags were used, they will need to be migrated

### For secret key fix:
- **Action Required:** Set `METACRAFTER_SECRET_KEY` environment variable in production
- If not set, a new random key is generated on each server start
- This means sessions will be invalidated on server restart if key not set

## Environment Variable Setup

Add to your production environment:
```bash
export METACRAFTER_SECRET_KEY="your-secret-key-here-min-32-chars"
```

Or in your deployment configuration (Docker, systemd, etc.):
```yaml
# docker-compose.yml example
environment:
  - METACRAFTER_SECRET_KEY=${METACRAFTER_SECRET_KEY}
```

## Security Best Practices Going Forward

1. **Never use eval()** - Always use safe alternatives
2. **Always parameterize SQL queries** - Never use string formatting
3. **Use yaml.safe_load()** - Unless you specifically need custom types
4. **Never commit secrets** - Always use environment variables
5. **Validate all user input** - Especially identifiers like table names
6. **Use context managers** - For all file and resource operations

## Files Modified

- `metacrafter/classify/processor.py` - eval() fix, YAML fix, file handling
- `metacrafter/core.py` - SQL injection fix, YAML fix, file handling, undefined variable fix
- `metacrafter/server/api.py` - YAML fix, file handling
- `metacrafter/server/manager.py` - Secret key fix

## Verification

To verify the fixes are working:

```bash
# Test PyParsing rule compilation
python -c "from metacrafter.classify.processor import RulesProcessor; rp = RulesProcessor(); rp.import_rules_path('rules')"

# Test YAML loading
python -c "import yaml; yaml.safe_load(open('rules/common/common.yaml'))"

# Test SQL injection prevention (should raise ValueError)
python -c "from metacrafter.core import CrafterCmd; cmd = CrafterCmd(); cmd.scan_db('sqlite:///test.db', schema='invalid; DROP TABLE')"
```

