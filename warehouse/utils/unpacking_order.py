def unpacking_order_fields(customer_name, delivery_field="custom_delivery_method", cbm_field="cbm"):
    """Match the new unpacking sheet, including its JINYU exception."""
    if customer_name == "JINYU":
        return (f"-{cbm_field}",)
    return ("destination", delivery_field, f"-{cbm_field}")


def unpacking_delivery_order(destination, delivery_method):
    """The new sheet groups named self-pickup destinations without mark suffixes."""
    if (delivery_method or "").split("-")[0] == "客户自提" and destination != "客户自提":
        return f"客户自提-{destination or ''}"
    return delivery_method
