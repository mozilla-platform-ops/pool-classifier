# worker health tools

Tools to ensure Taskcluster workers are not idle and performing well (success rate).

## TODO

- move code into https://github.com/mozilla-platform-ops/relops-infra

## setup

### Python and related

For all tools, run in the pipenv.

```
# one-time setup
# install pipenv
pipenv install

# for every use
pipenv shell
# run command
```

### Taskcluster credentials

Many of the tools make calls against Taskcluster. We use a token stored in ~/.tc_token.

```bash
{
    "clientId": "mozilla-auth0/ad|Mozilla-LDAP|aerickson/quarantine-workers",
    "accessToken": "REDACTED"
}
```

You can create a client token at https://firefox-ci-tc.services.mozilla.com/auth/clients.

Make the expiration 100 years or similar. The only scope currently required is:

```bash
queue:quarantine-worker:*
```

## overview

### fitness.py

Shows each worker's success rate and various concerning conditions like, consecutive failures, lack of work.

Provides a report on a provisioner and worker-type.

Not specific to Bitbar (works on all taskcluster provisioners).

![fitness.py](images/fitness_py_example.png)

```
./fitness_check.py -h

# to report on all worker types under a provisioner
./fitness_check.py -p PROVISIONER
# for a specific worker-type in the provisioner
./fitness_check.py -p PROVISIONER WORKER-TYPE
```

### missing_workers

For static hardware pools (like moonshots, macs, and bitbar), alert if a worker hasn't worked in more than a day (they disappear from TC output in 24 hours).

If a queue doesn't have work, we can't verify they're functioning (via the currently used method - TC doesn't show static worker status unless working (I think) in https://firefox-ci-tc.services.mozilla.com/docs/reference/platform/queue/api#listWorkers).

```bash
./missing_workers.py -h
```

### safe_runner

Runs a command on a set of hosts once each has been quarantined and no jobs are running.

Features:
- integrates with Taskcluster API to quarantine, check no jobs are running, and lift quarantine
- ability to resume from a state file
- OS X speech support for updates
- pre-quarantine feature (quarantines several hosts) so there's less waiting for jobs to finish
- command output is logged to file

Potential issues:
- If ssh is giving you issues with Bolt, append '--native-ssh' to your Bolt command.

```bash
# for options
./safe_runner.py -h

# basic usage
./safe_runner.py -r sr_state_dir_xyz
# edit config and set options
vi sr_state_dir_xyz/runner_state.toml
./safe_runner.py -r sr_state_dir_xyz

# resume from existing state file and set options
./safe_runner.py -r sr_state_dir_xyz -t -R -P 11
```

### unsafe_runner

Similar to safe_runner, but doesn't deal with any quarantine stuff.

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
