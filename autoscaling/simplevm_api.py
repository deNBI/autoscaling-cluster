
from constants import SCALE_DATA_VERSION

from utils import
def get_cluster_data():
    """
    Receive worker information from portal.
    request example:
    :return:
        cluster data dictionary
        if api error: None
    """
    try:
        json_data = {
            "password": __get_cluster_password(),
            "version": SCALE_DATA_VERSION,
        }
        response = requests.post(url=get_url_info_cluster(), json=json_data)
        # logger.debug("response code %s, send json_data %s", response.status_code, json_data)

        if response.status_code == HTTP_CODE_OK:
            res = response.json()
            version_check_scale_data(res["VERSION"])
            logger.debug(pformat(res))
            return res
        else:
            handle_code_unauthorized(res=res)

    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        logger.error("unable to receive cluster data")
        if res.status_code == HTTP_CODE_UNAUTHORIZED:
            handle_code_unauthorized(res=res)
        else:
            __sleep_on_server_error()
    except OSError as error:
        logger.error(error)
    except Exception as e:
        logger.error("error by accessing cluster data %s", e)
    return None
