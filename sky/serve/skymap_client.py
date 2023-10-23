""" Sky Map Client for SkyServe."""
import enum
import logging

import requests

logger = logging.getLogger(__name__)


class ProbeStatus(enum.Enum):
    """Process status."""
    NO_DATA = -1
    UNAVAILABLE = 0
    AVAILABLE = 1


class SkymapClient:
    """Skymap Client specification."""

    URL = 'https://bluo.short.gy/skymap/predict-data'

    def __init__(self) -> None:
        pass

    def probe(self, cloud, zone, accelerator) -> ProbeStatus:

        return_status = ProbeStatus.NO_DATA
        request_json = {
            'cloud': cloud,
            'zone': zone,
            'accelerator': accelerator
        }
        logger.info(f'SkyMap request: {request_json}')
        try:
            response = requests.post(self.URL, json=request_json, timeout=2)
            if response.json()['count'] == 0:
                return_status = ProbeStatus.UNAVAILABLE
            elif response.json()['count'] > 0:
                return_status = ProbeStatus.AVAILABLE
            elif response.json()['count'] == -1:
                return_status = ProbeStatus.NO_DATA
            else:
                assert False, 'Invalid response from SkyMap'
        except requests.Timeout:
            print('SkyMap - Timeout')
        except requests.ConnectionError:
            print('SkyMap - ConnectionError')

        logger.info(f'SkyMap response: {return_status}')
        return return_status
