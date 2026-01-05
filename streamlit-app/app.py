import logging
import os
import secrets
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import msal
import requests
import streamlit as st
from dotenv import load_dotenv

# Ensure repository root is on the path so we can import existing modules
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

# Load .env file from repository root
load_dotenv(dotenv_path=REPO_ROOT / ".env")

import config  # noqa: E402
from src.dataspot_auth import DataspotAuth  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _bool_env(var_name: str, default: bool = False) -> bool:
    value = os.getenv(var_name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        st.error(f"Missing environment variable: {var_name}")
        st.stop()
    return value


def _build_msal_client(client_id: str, client_secret: str, tenant_id: str) -> msal.ConfidentialClientApplication:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    return msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )


def _start_login_flow(cca: msal.ConfidentialClientApplication, scopes: List[str], redirect_uri: str) -> None:
    if "auth_state" not in st.session_state:
        st.session_state["auth_state"] = secrets.token_urlsafe(16)

    auth_url = cca.get_authorization_request_url(
        scopes=scopes,
        redirect_uri=redirect_uri,
        state=st.session_state["auth_state"],
        prompt="select_account",
        response_mode="query",
    )
    st.markdown("### Login required")
    st.markdown(f"[Sign in with Entra ID]({auth_url})")
    st.info("You will be redirected back here after signing in.")
    st.stop()


def _exchange_code_for_token(
    cca: msal.ConfidentialClientApplication, scopes: List[str], redirect_uri: str, code: str, state: str
) -> Dict:
    expected_state = st.session_state.get("auth_state")
    if expected_state and state and expected_state != state:
        st.error("State mismatch during authentication. Please try signing in again.")
        st.stop()

    result = cca.acquire_token_by_authorization_code(
        code,
        scopes=scopes,
        redirect_uri=redirect_uri,
    )

    if "access_token" not in result:
        error = result.get("error_description") or "Unknown error during token exchange."
        st.error(f"Authentication failed: {error}")
        st.stop()

    st.session_state["token_result"] = result
    st.query_params.clear()  # Clear code/state from URL
    return result


def _get_interactive_headers() -> Tuple[Dict[str, str], str]:
    tenant_id = _required_env("DATASPOT_TENANT_ID")
    client_id = _required_env("DATASPOT_CLIENT_ID")
    client_secret = _required_env("DATASPOT_CLIENT_SECRET")
    exposed_client_id = _required_env("DATASPOT_EXPOSED_CLIENT_ID")
    dataspot_access_key = _required_env("DATASPOT_SERVICE_USER_ACCESS_KEY")
    redirect_uri = os.getenv("DATASPOT_REDIRECT_URI", "http://localhost:8501/redirect")

    scopes = [f"api://{exposed_client_id}/.default"]
    cca = _build_msal_client(client_id, client_secret, tenant_id)

    code = st.query_params.get("code")
    state = st.query_params.get("state")

    token_result = st.session_state.get("token_result")
    if not token_result:
        if code:
            token_result = _exchange_code_for_token(cca, scopes, redirect_uri, code, state)
        else:
            _start_login_flow(cca, scopes, redirect_uri)

    access_token = token_result["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "dataspot-access-key": dataspot_access_key,
        "Content-Type": "application/json",
    }
    return headers, "interactive"


def _get_service_headers() -> Tuple[Dict[str, str], str]:
    auth = DataspotAuth()
    headers = auth.get_headers()
    return headers, "service-user"


def _fetch_public_dataset_count(headers: Dict[str, str], scheme_name: str) -> Tuple[int, int]:
    url = f"{config.base_url}/api/{config.database_name}/schemes/{scheme_name}/download?format=JSON"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    assets = response.json()
    if not isinstance(assets, list):
        raise ValueError("Unexpected response format: expected a list of assets.")

    # Filter to only datasets (exclude collections and other asset types)
    datasets = [asset for asset in assets if asset.get("_type") == "Dataset"]
    
    # Filter to only PUBLISHED datasets (status "PUBLISHED" makes them visible/public)
    published_datasets = [dataset for dataset in datasets if dataset.get("status") == "PUBLISHED"]
    
    logging.info(f"Fetched {len(assets)} total assets from {scheme_name}; {len(datasets)} datasets; {len(published_datasets)} PUBLISHED")
    return len(published_datasets), len(datasets)


def main() -> None:
    st.set_page_config(page_title="Dataspot Streamlit MVP", page_icon="📊")

    use_service_auth = _bool_env("USE_SERVICE_AUTH", default=False)
    st.sidebar.write("Authentication mode")
    st.sidebar.write("Service auth is ON" if use_service_auth else "Interactive login")

    try:
        if use_service_auth:
            headers, mode = _get_service_headers()
        else:
            headers, mode = _get_interactive_headers()
    except Exception as exc:
        st.error(f"Authentication failed: {exc}")
        return

    st.success(f"Authenticated via {mode}")

    try:
        published_count, total_datasets = _fetch_public_dataset_count(headers, config.dnk_scheme_name)
    except Exception as exc:
        st.error(f"Failed to load dataset counts: {exc}")
        return

    st.header("Datasets visible to you")
    st.metric(label="PUBLISHED datasets in Datenprodukte", value=published_count, delta=None)
    st.caption(f"Total datasets in scheme: {total_datasets}")


if __name__ == "__main__":
    main()
