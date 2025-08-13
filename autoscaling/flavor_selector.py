import math

from classes.config_modes import BasicMode
from classes.configuration import Configuration
from classes.flavor_availability import FlavorAvailability
from constants import FLAVOR_GPU_ONLY, FLAVOR_GPU_REMOVE, FLAVOR_HIGH_MEM
from logger import setup_custom_logger
from simplevm_api import get_usable_flavors_from_api
from utils import reduce_flavor_memory

logger = setup_custom_logger(__name__)

def translate_metrics_to_flavor(
    config_mode:BasicMode,cpu:int, mem:int, tmp_disk:int, flavors_data:list[FlavorAvailability], available_check:bool, quiet:bool
):
    """
    Select the flavor by cpu and memory.
    In the future, the server could select the flavor itself based on the required CPU and memory data.
        - "de.NBI tiny"     1 Core  , 2  GB Ram
        - "de.NBI mini"     4 Cores , 7  GB Ram
        - "de.NBI small"    8 Cores , 16 GB Ram
        - "de.NBI medium"  14 Cores , 32 GB Ram
        - "de.NBI large"   28 Cores , 64 GB Ram
        - "de.NBI large + ephemeral" 28 Cores , 64 GB Ram
    If ephemeral required, only ephemeral flavor possible.
    Check if a flavor is available, if not, test the next higher (option).
    :param cpu: required cpu value for job
    :param mem: required memory value for job (mb)
    :param tmp_disk: required ephemeral value (mb)
    :param flavors_data: flavor data (json) from cluster api
    :param available_check: test if flavor is available
    :param quiet: print messages
    :return: matched and available flavor
    """
    cpu = int(cpu)
    mem = int(mem)
    found_flavor = False
    if not flavors_data:
        logger.debug("flavors_data is empty")
        return None
    for fv_data in reversed(flavors_data):
        fv_data_flavor=fv_data.flavor
        if fv_data_flavor.ephemeral_disk == 0 and config_mode.flavor_ephemeral:
            continue

        if (
            cpu <= int(fv_data_flavor.vcpus)
            and mem <= int(fv_data["available_memory"])
            and (
                int(tmp_disk) < int(fv_data["flavor"]["temporary_disk"])
                or (
                    int(tmp_disk) == 0 and int(fv_data_flavor. >= 0)
                )
            )
        ):
            # job tmp disk must be lower than flavor tmp disk, exception if both zero
            # ex. flavor with 1TB tmp disk, jobs with 1TB are not scheduled, only jobs with 999M tmp disk
            found_flavor = True

            if not available_check:
                return fv_data
            if fv_data["usable_count"] > 0:
                logger.debug(
                    "-> match found %s for cpu %d mem %d",
                    fv_data["flavor"]["name"],
                    cpu,
                    mem,
                )
                return fv_data
            if not quiet:
                logger.info(
                    "flavor %s found, but not available - searching for cpu %s memory %s tmp_disk %s",
                    fv_data["flavor"]["name"],
                    cpu,
                    mem,
                    tmp_disk,
                )
            break
    if not quiet and not found_flavor:
        logger.warning(
            "unable to find a suitable flavor - searching for cpu %s mem %s tmp_disk %s",
            cpu,
            mem,
            tmp_disk,
        )

    return None


def __get_flavor_available_count(flavor_data:list[FlavorAvailability], flavor_name:str):
    """
    Return the number of available workers with this flavor.
    :param flavor_data:
    :param flavor_name:
    :return: maximum of new possible workers with this flavor
    """
    flavor_tmp = __get_flavor_by_name(flavor_data, flavor_name)
    if flavor_tmp:
        return flavor_tmp.available_count
    return 0


def __get_flavor_by_name(flavor_data:list[FlavorAvailability], flavor_name:str) -> FlavorAvailability:
    """
    Return flavor object by flavor name.
    :param flavor_data:
    :param flavor_name:
    :return: flavor data
    """
    for flavor_tmp in flavor_data:
        if flavor_tmp.flavor.name == flavor_name:
            return flavor_tmp
    return None

def usable_flavor_data(flavor_data:list[FlavorAvailability],config:Configuration) -> int:
    """
    Calculate the number of usable flavors, based on configuration.
    :param flavor_data:
    :return: number of usable flavors
    """
    fv_cut = int(config.active_mode.flavor_restriction)
    # modify flavor count by current version
    flavors_available = len(flavor_data)
    logger.debug("flavors_available %s fv_cut %s", flavors_available, fv_cut)
    if 0 < fv_cut < 1:
        fv_cut = math.ceil(flavors_available * fv_cut)
    if 1 <= fv_cut < flavors_available:
        flavors_usable = fv_cut
    else:
        flavors_usable = flavors_available
        if fv_cut != 0:
            logger.error("wrong flavor cut value")
    logger.debug("found %s flavors %s usable ", flavors_available, flavors_usable)
    if flavors_usable == 0 and flavors_available > 0:
        flavors_usable = 1
    return flavors_usable

def flavor_mod_gpu(flavors_data:list[FlavorAvailability],config:Configuration) -> list[FlavorAvailability]:
    """
    Modify flavor data according to gpu option.
    :param flavors_data: flavor data
    :return: changed flavor data
    """
    flavors_data_mod = []
    removed_flavors = []
    flavor_gpu = config.active_mode.flavor_gpu
    # modify flavors by gpu
    for fv_data in flavors_data:
        # use only gpu flavors
        if flavor_gpu == FLAVOR_GPU_ONLY and fv_data.flavor.gpu == 0:
            removed_flavors.append(fv_data.flavor.name)
        # remove all gpu flavors
        elif flavor_gpu == FLAVOR_GPU_REMOVE and fv_data.flavor.gpu != 0:
            removed_flavors.append(fv_data.flavor.name])
        else:
            flavors_data_mod.append(fv_data)

    if removed_flavors:
        logger.debug("unlisted flavors by GPU config option: %s", removed_flavors)
    return flavors_data_mod

def get_usable_flavors(config:Configuration,quiet:bool, cut:bool):
    """
    Receive flavor information from portal.
        - the largest flavor on top
        - memory in GB (real_memory/1024)
    :param quiet: boolean - print flavor data
    :param cut: boolean - cut flavor information according to configuration
    :return: available flavors as json
    """
    flavors_data=get_usable_flavors_from_api()
    flavors_info=[FlavorAvailability(flavor=data["flavor"],used_count=data["used_count"],available_count=data["available_count"],real_available_count_openstack=data["real_available_count_openstack"]] for data in flavors_data)]
    flavors_info=sorted(flavors_info,key=lambda fl:fl.flavor.credits_costs_per_hour,fl.flavor.ram_gib,fl.flavor.vcpus,fl.flavor.ephemeral_disk,reverse=True)
    counter = 0
    flavors_data:list[FlavorAvailability] = flavor_mod_gpu(flavors_data=flavors_info)
    flavors_data_mod:list[FlavorAvailability] = []
    if cut:
        flavors_usable:int = usable_flavor_data(flavors_data)
    else:
        flavors_usable:int = len(flavors_data)

    for fd in flavors_data:

                if (
                    config.active_mode.limit_flavor_usage
                    and fd.flavor.type.shortcut == FLAVOR_HIGH_MEM
                    and fd.flavor.name in  config.active_mode.limit_flavor_usage
                ):
                    #TODO could happen directly via init with config injection
                    fd.set_custom_limit(custom_limit=config.active_mode.limit_flavor_usage[fd.flavor.name])

                # --------------
                if counter <= flavors_usable:
                    flavors_data_mod.append(fd)
                    counter +=1
                else:
                    break


                if not quiet:
                    fd.log_info()

    return flavors_data_mod
