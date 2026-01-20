from typing import Any, Callable


class Postman:

    def get(self,
            url: str,
            data: Any = None,
            json: Any = None,
            params: Any = None,
            headers: Any = None,
            cookies: Any = None,
            files: Any = None,
            auth: Any = None,
            timeout: Any = None,
            allow_redirects: bool = None,
            proxies: Any = None,
            hooks: Any = None,
            stream: bool = None,
            verify: Any = None,
            cert: Any = None,
            ):
        raise NotImplementedError

    def post(self,
             url: str,
             data: Any = None,
             json: Any = None,
             params: Any = None,
             headers: Any = None,
             cookies: Any = None,
             files: Any = None,
             auth: Any = None,
             timeout: Any = None,
             allow_redirects: bool = None,
             proxies: Any = None,
             hooks: Any = None,
             stream: bool = None,
             verify: Any = None,
             cert: Any = None,
             ):
        raise NotImplementedError

    def copy(self) -> 'Postman':
        raise NotImplementedError

    def get_meta_data(self, key=None, dv=None) -> dict:
        """
        获取Postman的元信息，调用示例：
        # 1.
        headers = postman.get_meta_data().get('headers', {})
        # 2.
        headers = postman.get_meta_data('headers', {})
        # 3.
        headers = postman['headers'] or {}

        :param key: 键
        :param dv: default-value，当键不存在时的默认值
        """
        raise NotImplementedError

    def __getitem__(self, item):
        return self.get_meta_data()[item]

    def __setitem__(self, key, value):
        self.get_meta_data()[key] = value

    @classmethod
    def create(cls, clazz=None, **kwargs):
        if clazz is None:
            from .postman_impl import RequestsPostman
            clazz = RequestsPostman

        return clazz(kwargs)

    def get_root_postman(self):
        from .postman_proxy import PostmanProxy
        if isinstance(self, PostmanProxy):
            return self.postman.get_root_postman()
        return self

    # proxy method

    def with_fix_url(self, url: str):
        from .postman_proxy import FixUrlPostman
        return FixUrlPostman(self, url)

    def with_retry(self, retry_times, clazz=None):
        if clazz is None:
            from .postman_proxy import RetryPostman
            clazz = RetryPostman

        return clazz(self, retry_times)

    def with_multi_part(self):
        from .postman_proxy import MultiPartPostman
        return MultiPartPostman(self)

    def with_wrap_resp(self, clazz=None):
        from .postman_proxy import WrapRespPostman
        return WrapRespPostman(self, clazz)

    def with_redirect_catching(self, clazz=None):
        if clazz is None:
            from .postman_proxy import RedirectPostman
            clazz = RedirectPostman

        return clazz(self)


class AbstractPostman(Postman):
    postman_key = 'abstract'

    @classmethod
    def create(cls, **kwargs):
        return cls(kwargs)

    def __init__(self, kwargs) -> None:
        if not isinstance(kwargs, dict):
            raise AssertionError('kwargs is not a dict')

        self.meta_data: dict = kwargs

    def get(self, url, **kwargs):
        kwargs = self.before_request(kwargs)
        return self.__get__()(url, **kwargs)

    def post(self, url, **kwargs):
        kwargs = self.before_request(kwargs)
        return self.__post__()(url, **kwargs)

    def __get__(self) -> Callable:
        raise NotImplementedError

    def __post__(self) -> Callable:
        raise NotImplementedError

    def before_request(self, kwargs):
        return self.merge_kwargs(kwargs)

    def merge_kwargs(self, kwargs: dict):
        """
        把 kwargs 合并到 self.meta_data.copy()
        """
        ret = self.meta_data.copy()
        for k, v in kwargs.items():
            if v is None:
                continue
            ret[k] = v
        return ret

    def get_meta_data(self, key=None, dv=None) -> dict:
        if key is None:
            return self.meta_data
        else:
            return self.meta_data.get(key, dv)

    def copy(self):
        return self.__class__(self.meta_data.copy())


class AbstractSessionPostman(AbstractPostman):
    postman_key = 'abstract-session'

    def __init__(self, kwargs: dict) -> None:
        super().__init__(kwargs)

        self.session = self.create_session(kwargs)

    def create_session(self, kwargs):
        raise NotImplementedError

    def __get__(self):
        return self.session.get

    def __post__(self):
        return self.session.post

    def __getitem__(self, item):
        return getattr(self.session, item)

    def __setitem__(self, key, value):
        setattr(self.session, key, value)
