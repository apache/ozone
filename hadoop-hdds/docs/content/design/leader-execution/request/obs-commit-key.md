# OBS Create key flow

Utility classes:
- DbChangeRecorder: record db changes
- ExecutionContext: provides index and other resources

Hsync feature includes
- hsync
- hsync recovery
- hsync overwrite handling


`class OMKeyCommitObsExecutor`


## preprocess

- validate key format and reserve keyword
- normalize key
- capture original bucket and resolve bucket (if different)
- validate hsync, hsync recovery, and hsync feature


## authorize

Acl validation for volume, resolved and original bucket, and key permission (via ranger or native acl).

## lock
Read lock for bucket, write lock for key

## unlock
unlock bucket and key

## process

- validate if bucket is changed after bucket lock
- retrieve old key from keyTable
- get key from openKeyTable
- validate hsync feature flags from old key and key commit args (Note: hsync is not currently used for obs flow)
- validate key overwrite feature
- Create new Key from open key and old key (for overwrite)
- prepare quota changes and validate, and update to ChangeRecorder
- add key to key table, delete from open key table to ChangeRecorder
- add uncommitted blocks, blocks for removal in over-write to deleteTable to changeRecorder
- update metrics and audit log
- prepare response and return

# Old Flow comparison changes
Compare to old flow, below cases are removed,ß
1. open key re-prepare with overwrite case

# Testability

For existing test code, behavior cases can be rewritten with new Test classes, with validation.
