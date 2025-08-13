from classes.config_modes import BasicMode
from classes.flavor_availability import FlavorAvailability
from logger import setup_custom_logger

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
