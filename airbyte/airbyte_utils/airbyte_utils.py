import logging

import requests


def api_call(endpoint: str, payload: dict = None) -> dict:
    try:
        return requests.post(
            f"http://dps-platform-airbyte-airbyte-webapp-svc/api/v1/{endpoint}",  # NOSONAR
            headers={"accept": "application/json", "content-type": "application/json"},
            json=payload,
        ).json()
    except Exception as exception:
        logging.warning(f"failed to call api endpoint `{endpoint}`: {exception}")

        return {}


def get_workspace_id() -> str | None:
    return api_call("workspaces/list").get("workspaces", [{}])[0].get("workspaceId")


def get_connections() -> list[dict]:
    return api_call("connections/list", {"workspaceId": get_workspace_id()}).get("connections", [])


def get_connections_dict() -> dict:
    return {connection["name"]: connection["connectionId"] for connection in get_connections()}


def get_refreshed_connection(connection_id: str) -> dict:
    return api_call("web_backend/connections/get", {"connectionId": connection_id, "withRefreshedCatalog": True})


def update_connection_schema(payload: dict) -> dict:
    return api_call("web_backend/connections/update", payload)


def update_connection_schemata() -> None:
    for connection in get_connections():
        try:
            logging.info(f"updating schema of connection `{connection['name']}`")

            payload = get_refreshed_connection(connection["connectionId"])

            payload.pop("catalogDiff")
            payload.pop("sourceId")
            payload.pop("source")
            payload.pop("destinationId")
            payload.pop("destination")
            payload.pop("operationIds")
            payload.pop("isSyncing")
            payload.pop("latestSyncJobCreatedAt")
            payload.pop("latestSyncJobStatus")
            payload.pop("schemaChange")
            payload.pop("notifySchemaChanges")

            payload["sourceCatalogId"] = payload.pop("catalogId")
            payload["skipReset"] = True

            update_connection_schema(payload)
        except Exception as exception:
            logging.warning(f"failed to update schema of connection `{connection['name']}`: {exception}")


connections_dict = get_connections_dict()
