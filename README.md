# worker health tools

## missing_workers

Helps us identify Bitbar workers that are configured in a TC queue that has pending jobs, but aren't reporting for work.

If a queue doesn't have work, we can't verify they're functioning (via the currently used method).

### usage

```
./missing_workers.sh -h
```
