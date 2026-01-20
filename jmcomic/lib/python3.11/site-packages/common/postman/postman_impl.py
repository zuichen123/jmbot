from typing import Type, Dict, Union

from ..base import ComponentRegistry
from .postman_api import *


class RequestsPostman(AbstractPostman):
    postman_key = 'requests'

    def __get__(self):
        from requests import get
        return get

    def __post__(self):
        from requests import post
        return post

    def before_request(self, kwargs):
        kwargs = super().before_request(kwargs)
        kwargs.pop('impersonate', None)
        return kwargs


class RequestsSessionPostman(AbstractSessionPostman):
    postman_key = 'requests-session'

    def create_session(self, kwargs):
        import requests
        session = requests.Session()
        if 'cookies' in kwargs:
            session.cookies = requests.sessions.cookiejar_from_dict(kwargs.pop('cookies'))
        return session

    def before_request(self, kwargs):
        kwargs = super().before_request(kwargs)
        kwargs.pop('impersonate', None)
        return kwargs


class CurlCffiPostman(AbstractPostman):
    postman_key = 'curl_cffi'

    def __init__(self, kwargs) -> None:
        super().__init__(kwargs)

    def __get__(self):
        from curl_cffi.requests import get
        return get

    def __post__(self):
        from curl_cffi.requests import post
        return post


class CurlCffiSessionPostman(AbstractSessionPostman):
    postman_key = 'curl_cffi_session'

    def create_session(self, kwargs):
        return self.new_cffi_session(kwargs)

    # noinspection PyMethodMayBeStatic
    def new_cffi_session(self, kwargs: dict):
        from curl_cffi import requests
        return requests.Session(**kwargs)

    def before_request(self, kwargs):
        return kwargs


# help typing
PostmanImplClazz = Type[Postman]


class Postmans:

    @classmethod
    def get_impl_clazz(cls, key):
        return ComponentRegistry.get_impl_clazz(Postman, key)

    @classmethod
    def new_session(cls, **kwargs):
        return cls.get_impl_clazz('curl_cffi_session').create(**kwargs)

    @classmethod
    def new_postman(cls, **kwargs):
        return cls.get_impl_clazz('curl_cffi').create(**kwargs)

    @classmethod
    def create(cls,
               filepath='./postman.yml',
               data=None,
               ) -> Postman:
        if data is None:
            from common import PackerUtil
            data = PackerUtil.unpack(filepath)[0]
            if data is None:
                raise AssertionError(f'空配置文件: {filepath}')

        return cls.PostmanDslBuilder().build_postman(data)

    # noinspection PyMethodMayBeStatic
    class PostmanDslBuilder:

        def __init__(self) -> None:
            self.dsl_handler_list: list[Callable[[Dict], Union[Postman, None]]] = [
                self.proxy_handler,
                self.impltype_handler,
            ]

        def proxy_handler(self, data: dict, key='proxies'):
            meta_data = data.get('meta_data', {})

            from common import ProxyBuilder

            proxies = meta_data.get(key, None)

            # 无代理，或代理已配置好好的
            if proxies is None or isinstance(proxies, dict):
                return

            meta_data[key] = ProxyBuilder.build_by_str(proxies)

        def impltype_handler(self,
                             dsl_data: dict,
                             __default_cname__='requests',
                             ):
            meta_data = dsl_data.setdefault('meta_data', {})
            impl_type = dsl_data.get('type', __default_cname__)

            # fix wrong config, but not a good solution
            if impl_type.startswith(__default_cname__) and 'impersonate' in meta_data:
                meta_data.pop('impersonate')

            clazz = Postmans.get_impl_clazz(impl_type)
            return clazz(meta_data)

        def build_postman(self, data: dict):
            # key alias
            if 'metadata' in data:
                data.setdefault('meta_data', data['metadata'])

            for handler in self.dsl_handler_list:
                postman = handler(data)
                if postman is not None:
                    return postman

            raise NotImplementedError
