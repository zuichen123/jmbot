from .postman_api import *
from .postman_impl import *
from .postman_proxy import *

ComponentRegistry.register_component(Postman, 'postman_key', globals().values(), False)
ComponentRegistry.get_all_impl(Postman)['cffi'] = CurlCffiPostman
