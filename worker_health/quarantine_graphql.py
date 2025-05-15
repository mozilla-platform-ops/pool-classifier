import requests
import json


def view_workers(provisioner, workerType, continuation=None):
    """
    Query workers information using GraphQL API.

    Args:
        url (str): The GraphQL endpoint URL.
        provisioner (str): The provisioner ID.
        workerType (str): The worker type.
        continuation (str): The continuation string for pagination.

    Returns:
        dict: The parsed JSON response.
    """
    url = "https://firefox-ci-tc.services.mozilla.com/graphql"
    headers = {"content-type": "application/json"}
    payload = {
        "operationName": "ViewWorkers",
        "variables": {
            "provisionerId": provisioner,
            "workerType": workerType,
            "workersConnection": json.loads(continuation) if continuation else None,
        },
        "query": """
            query ViewWorkers($provisionerId: String!, $workerType: String!, $workersConnection: PageConnection, $quarantined: Boolean) {
              workers(provisionerId: $provisionerId, workerType: $workerType, connection: $workersConnection, isQuarantined: $quarantined) {
                pageInfo {
                  hasNextPage
                  hasPreviousPage
                  cursor
                  previousCursor
                  nextCursor
                  __typename
                }
                edges {
                  node {
                    latestTask {
                      run {
                        workerGroup
                        workerId
                        taskId
                        runId
                        started
                        resolved
                        state
                        __typename
                      }
                      __typename
                    }
                    workerGroup
                    workerId
                    quarantineUntil
                  }
                }
              }
            }
        """,
    }
    # Remove None values from variables
    payload["variables"] = {k: v for k, v in payload["variables"].items() if v is not None}
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()


# ugh, annoying that all of these fields are required
def view_quarantined_worker_details(
    provisionerId,
    workerType,
    workerGroup,
    workerId,
    workerPoolId,
    auth_token=None,
):
    """
    Query detailed information about a quarantined worker using GraphQL API.

    Args:
        provisionerId (str): The provisioner ID.
        workerType (str): The worker type.
        workerGroup (str): The worker group.
        workerId (str): The worker ID.
        workerPoolId (str): The worker pool ID.
        auth_token (str, optional): Bearer token for Authorization header.

    Returns:
        dict: The parsed JSON response.
    """
    url = "https://firefox-ci-tc.services.mozilla.com/graphql"
    headers = {
        "content-type": "application/json",
    }
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {
        "operationName": "ViewWorker",
        "variables": {
            "workerPoolId": workerPoolId,
            "provisionerId": provisionerId,
            "workerType": workerType,
            "workerGroup": workerGroup,
            "workerId": workerId,
            "workerTypesConnection": None,
        },
        "query": """
            query ViewWorker($provisionerId: String!, $workerType: String!, $workerGroup: String!, $workerId: ID!, $workerPoolId: String!, $workerTypesConnection: PageConnection) {
              worker(
                provisionerId: $provisionerId
                workerType: $workerType
                workerGroup: $workerGroup
                workerId: $workerId
              ) {
                provisionerId
                workerType
                workerGroup
                workerId
                quarantineUntil
                quarantineDetails {
                  updatedAt
                  clientId
                  quarantineUntil
                  quarantineInfo
                  __typename
                }
                expires
                firstClaim
                lastDateActive
                state
                capacity
                providerId
                workerPoolId
                recentTasks {
                  taskId
                  run {
                    taskId
                    runId
                    started
                    resolved
                    state
                    __typename
                  }
                  __typename
                }
                latestTasks {
                  taskId
                  metadata {
                    name
                    source
                    description
                    owner
                    __typename
                  }
                  __typename
                }
                actions {
                  name
                  title
                  context
                  url
                  description
                  __typename
                }
                __typename
              }
              WorkerManagerWorker(
                workerPoolId: $workerPoolId
                workerGroup: $workerGroup
                workerId: $workerId
              ) {
                workerPoolId
                workerGroup
                workerId
                providerId
                state
                created
                expires
                capacity
                lastModified
                lastChecked
                __typename
              }
              workerTypes(provisionerId: $provisionerId, connection: $workerTypesConnection) {
                edges {
                  node {
                    workerType
                    __typename
                  }
                  __typename
                }
                __typename
              }
              provisioners {
                edges {
                  node {
                    provisionerId
                    __typename
                  }
                  __typename
                }
                __typename
              }
            }
        """,
    }
    # Remove None values from variables
    payload["variables"] = {k: v for k, v in payload["variables"].items() if v is not None}
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["data"]["worker"]["quarantineDetails"]


# main
if __name__ == "__main__":
    # Example usage:
    # result = view_workers(
    #     provisioner="proj-autophone",
    #     workerType="gecko-t-bitbar-gw-perf-a55",
    # )
    # sys.exit(0)

    provisioner = "proj-autophone"
    workerType = "gecko-t-bitbar-gw-perf-a55"
    workerPoolId = f"{provisioner}/{workerType}"
    result = view_quarantined_worker_details(
        provisionerId=provisioner,
        workerType=workerType,
        workerGroup="bitbar",
        workerId="a55-23",
        workerPoolId=workerPoolId,
    )

    print(json.dumps(result, indent=2))
