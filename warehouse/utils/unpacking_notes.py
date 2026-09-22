import re


WALMART_DESTINATION_SUFFIXES = (
    "mem1s", "atl3", "atl2n", "atl1", "mco1", "nj3", "phl4n",
    "ky1", "ks1", "w-ind1", "w-ind2", "w-ind3", "w-mco1",
)


def needs_walmart_height(destination):
    destination = str(destination or "").strip().casefold()
    return destination.startswith("walmart") or destination.endswith(WALMART_DESTINATION_SUFFIXES)


def append_walmart_height(remark):
    """Preserve existing instructions and add the height instruction only once."""
    remark = str(remark or "").strip()
    if re.search(r"(?<!\w)80\s+height(?!\w)", remark, flags=re.IGNORECASE):
        return remark
    return f"{remark}, 80 height" if remark else "80 height"
