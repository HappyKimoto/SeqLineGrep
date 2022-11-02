# settings.md format

```yaml
---
# Documents[0]: FILE_SETTINGS
file_path_filter_regexp: '.*txt'
search_recursively: false
sort_by_date_modified: true
code_page_read: 'ascii'
code_page_write: 'ascii'
output_file_extension: '.txt'
column_separator_character_integer: 9
---
# Documents[1]: COMMON_PATTERNS
field_names: []
regexp_pattern: ''
---
# Documents[2]: UNIQUE_PATTERNS

# Sample line
- table_name: ''
  key_substring: ''
  field_names: []
  regexp_pattern: '.*'
...
```
