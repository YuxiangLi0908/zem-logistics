from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from warehouse.utils.constants import (
    APP_ENV,
    DELIVERY_METHOD_OPTIONS,
    SP_CLIENT_ID,
    SP_DOC_LIB,
    SP_PRIVATE_KEY,
    SP_SCOPE,
    SP_TENANT,
    SP_THUMBPRINT,
    SP_URL,
    SYSTEM_FOLDER,
)

def _get_sharepoint_auth() -> ClientContext:
    ctx = ClientContext(SP_URL).with_client_certificate(
        SP_TENANT,
        SP_CLIENT_ID,
        SP_THUMBPRINT,
        private_key=SP_PRIVATE_KEY,
        scopes=[SP_SCOPE],
    )
    return ctx

if __name__ == "__main__":
    conn = _get_sharepoint_auth()
    print(conn)