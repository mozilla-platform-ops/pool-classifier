# worker health tools

Tools to ensure Taskcluster workers are not idle and performing well (success rate).

## TODO

- move code into https://github.com/mozilla-platform-ops/relops-infra

## setup

For all tools, run in the pipenv.

```
# one-time setup
# install pipenv
pipenv install

# for every use
pipenv shell
# run command
```

## overview

### fitness.py

Shows each worker's success rate and varios concerning conditions like, consecutive failures, lack of work.

Provides a report on a provisioner and worker-type.

Not specific to Bitbar (works on all taskcluster provisioners).

![fitness.py](images/fitness_py_example.png)

```
./fitness.py -h

# to report on all worker types under a provisioner
./fitness.py -p PROVISIONER
# for a specific worker-type in the provisioner
./fitness.py -p PROVISIONER WORKER-TYPE
```

### missing_workers

For static hardware pools (like moonshots, macs, and bitbar), alert if a worker hasn't worked in more than a day (they disappear from TC output in 24 hours).

If a queue doesn't have work, we can't verify they're functioning (via the currently used method - TC doesn't show static worker status unless working (I think) in https://firefox-ci-tc.services.mozilla.com/docs/reference/platform/queue/api#listWorkers).

```bash
./missing_workers.py -h
```

### quarantine_tool

Lists quarantined hosts, quarantines, lifts quarantine, and lists all workers in a workerType.

```bash
# show quarantined
./quarantine_tool.py proj-autophone gecko-t-bitbar-gw-unit-p2 show

# show all workers
./quarantine_tool.py proj-autophone gecko-t-bitbar-gw-unit-p2 show-all

./quarantine_tool.py proj-autophone gecko-t-bitbar-gw-unit-p2 quarantine pixel2-01
./quarantine_tool.py proj-autophone gecko-t-bitbar-gw-unit-p2 lift pixel2-01
```

### slack_alert

Sends scheduled slack message about problem hosts.

```
./slack_alert.py -h
```

### influx_logger

Logs worker health metrics to influx.

```
./influx_logger.py -h
```
