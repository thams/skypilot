""" Sky Spot placer for SkyServe."""
import enum
import logging
import random
from typing import List

from sky import task as task_lib
from sky.serve import skymap_client as map_client

logger = logging.getLogger(__name__)


class SpotPolicy(enum.Enum):

    SINGLE_ZONE = 'SingleZone'
    NAIVE_SPREAD = 'NaiveSpread'
    EAGER_FAILOVER = 'EagerFailover'
    SKYMAP_FAILOVER = 'SkymapFailover'


class SpotPlacer:
    """Spot Placer specification."""

    def __init__(self, zones: List[str], spot_policy: str, use_spot: bool,
                 task_yaml_path: str,
                 skymap_client: map_client.SkymapClient) -> None:

        if spot_policy == 'NaiveSpread':
            self.spot_policy = SpotPolicy.NAIVE_SPREAD
        elif spot_policy == 'EagerFailover':
            self.spot_policy = SpotPolicy.EAGER_FAILOVER
        elif spot_policy == 'SingleZone':
            self.spot_policy = SpotPolicy.SINGLE_ZONE
        elif spot_policy == 'SkymapFailover':
            self.spot_policy = SpotPolicy.SKYMAP_FAILOVER

        self.zones = zones
        logger.info(f'SpotPlacer {zones}')

        self.current_zone_idx: int = 0
        self.single_zone: str = ''
        self.zone_timeout_cnt: int = 0
        self.zone_timeout_interval: int = 100
        self.skymap_client = skymap_client
        self.use_spot = use_spot
        self.preempted_zones: List[str] = []

        self.task_yaml_path: str = task_yaml_path
        self.task = task_lib.Task.from_yaml(self.task_yaml_path)
        task_resource = list(self.task.resources)[0]
        self.cloud = f'{task_resource.cloud}'
        if task_resource and task_resource.accelerators:
            assert isinstance(task_resource.accelerators, dict)
            self.accelerators = list(
                list(self.task.resources)[0].accelerators.keys())[0]

    def get_next_zone(self) -> str:
        assert self.zones is not None
        if self.spot_policy == SpotPolicy.NAIVE_SPREAD:
            zone = self.zones[self.current_zone_idx]
            self.current_zone_idx += 1
        elif self.spot_policy == SpotPolicy.SINGLE_ZONE:
            if not self.single_zone:
                self.single_zone = random.choice(self.zones)
            zone = self.single_zone
        elif self.spot_policy == SpotPolicy.EAGER_FAILOVER:
            zone = random.choice(self.zones)
            while zone in self.preempted_zones:
                zone = random.choice(self.zones)
            logger.info(f'self.preempted_zones: {self.preempted_zones}')
        elif self.spot_policy == SpotPolicy.SKYMAP_FAILOVER:
            zone = random.choice(self.zones)
            skymap_status = self.skymap_client.probe(self.cloud, zone,
                                                     self.accelerators,
                                                     self.use_spot)
            while zone in self.preempted_zones or skymap_status == map_client.ProbeStatus.UNAVAILABLE:  # pylint: disable=line-too-long
                zone = random.choice(self.zones)
            logger.info(f'self.preempted_zones: {self.preempted_zones}')
        logger.info(f'Chosen zone: {zone}, policy: {self.spot_policy}')
        return zone

    def get_next_use_spot(self) -> str:
        # For future, mixed spot and on-demand
        if self.use_spot:
            return '--use-spot'
        else:
            return '--no-use-spot'

    def handle_preemption(self, zone):
        logger.info(f'handle_preemption: {zone}')
        self.preempted_zones.append(zone)
        if self.zones and len(self.preempted_zones) == len(self.zones):
            self.preempted_zones.pop(0)

    def handle_heartbeat(self):
        if self.zone_timeout_cnt == self.zone_timeout_interval:
            if self.preempted_zones:
                self.preempted_zones.pop(0)
            logger.info(f'Pop self.preempted_zones: {self.preempted_zones}')
            self.zone_timeout_cnt = 0

        self.zone_timeout_cnt += 1
        logger.info(f'self.zone_timeout_cnt: {self.zone_timeout_cnt}')

    @property
    def active(self) -> bool:
        return self.zones is not None and self.use_spot
