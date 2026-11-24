"""Address-group scoped comparison identities. Keep v2 stable for data migrations."""
from warehouse.utils.quote_identity_v1 import profile_configuration as configuration_v1


ADDRESS_GROUPS = ("LA", "SAV", "NJ")


def scope_configuration(configuration, address_group):
    if address_group not in ADDRESS_GROUPS:
        raise ValueError("比较编号必须指定 LA、SAV 或 NJ 地址组")
    return {**configuration, "version": 2, "address_group": address_group}


def profile_configuration(parameters, address_group):
    return scope_configuration(configuration_v1(parameters), address_group)
