from botocore.exceptions import ClientError


def is_zone_unavailable_error(exc: ClientError) -> bool:
    error = exc.response.get('Error', {})
    code = str(error.get('Code', ''))
    message = str(error.get('Message', ''))
    normalized = f"{code} {message}".lower()
    zone_markers = ['availability zone', 'subnet', 'zone']
    unavailable_markers = ['unavailable', 'not supported', 'not available']
    return any(marker in normalized for marker in zone_markers) and any(marker in normalized for marker in unavailable_markers)