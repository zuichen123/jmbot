from .postman_api import *


class PostmanProxy(Postman):

    def __init__(self, postman: Postman):
        self.postman = postman

    def get(self, *args, **kwargs):
        return self.postman.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        return self.postman.post(*args, **kwargs)

    def get_meta_data(self, key=None, dv=None) -> dict:
        return self.postman.get_meta_data(key, dv)

    def __getitem__(self, item):
        return self.postman.__getitem__(item)

    def __setitem__(self, key, value):
        self.postman.__setitem__(key, value)

    def copy(self):
        return self.__class__(self.postman.copy())

    def get_root_postman(self):
        return self.postman.get_root_postman()


class FixUrlPostman(PostmanProxy):

    def __init__(self,
                 postman: Postman,
                 fix_url: str,
                 ):
        super().__init__(postman)
        self.fix_url = fix_url

    def get(self, url=None, **kwargs):
        return super().get(url or self.fix_url, **kwargs)

    def post(self, url=None, **kwargs):
        return super().post(url or self.fix_url, **kwargs)

    def copy(self):
        return self.__class__(self.postman.copy(), self.fix_url)


class RetryPostman(PostmanProxy):

    def __init__(self,
                 postman: Postman,
                 retry_times: int,
                 ):
        super().__init__(postman)
        self.retry_times = retry_times

    def retry_request(self, request, url, **kwargs):
        retry_times = self.retry_times
        if retry_times <= 0:
            return request(url, **kwargs)

        for i in range(retry_times):
            try:
                resp = request(url, **kwargs)
                self.accept_resp_or_raise(resp)
                return resp
            except KeyboardInterrupt as e:
                raise e
            except Exception as e:
                self.excp_handle(e)
                self.tip_retrying(i, request, url, kwargs)

        return self.fallback(url, kwargs)

    def get(self, *args, **kwargs):
        return self.retry_request(self.postman.get, *args, **kwargs)

    def post(self, *args, **kwargs):
        return self.retry_request(self.postman.post, *args, **kwargs)

    def fallback(self, url, kwargs):
        raise Exception(f"请求失败，重试了{self.retry_times}次后依然失败: {url}，携带参数: {kwargs}")

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def excp_handle(self, e):
        from common import traceback_print_exec
        traceback_print_exec()

    def tip_retrying(self, time, _request, url, kwargs):
        pass

    def copy(self):
        return self.__class__(self.postman.copy(), self.retry_times)

    def accept_resp_or_raise(self, resp):
        pass


class MultiPartPostman(PostmanProxy):

    def build_headers(self, data, kwargs):
        headers = kwargs.get('headers', None)
        if headers is None:
            headers = self.get_meta_data().get('headers', {})
        headers['Content-Type'] = data.content_type
        return headers

    def post(self, *args, **kwargs):
        data = kwargs.get('data', None)
        from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
        if isinstance(data, (MultipartEncoder, MultipartEncoderMonitor)):
            kwargs['headers'] = self.build_headers(data, kwargs)
        return super().post(*args, **kwargs)


class WrapRespPostman(PostmanProxy):

    def __init__(self, postman: Postman, wrap_resp_class=None):
        super().__init__(postman)
        if wrap_resp_class is None:
            from common import CommonResp
            wrap_resp_class = CommonResp
        self.WrapResp = wrap_resp_class

    def get(self, *args, **kwargs):
        return self.WrapResp(super().get(*args, **kwargs))

    def post(self, *args, **kwargs):
        return self.WrapResp(super().post(*args, **kwargs))

    def copy(self):
        return self.__class__(self.postman.copy(), self.WrapResp)


# noinspection PyMethodMayBeStatic
class RedirectPostman(PostmanProxy):

    def request(self, url, kwargs, method):
        kwargs.setdefault("allow_redirects", False)

        while True:
            resp = method(url, **kwargs)
            status_code = resp.status_code

            if status_code == 301:
                # 永久移除
                url = self.get_redirect_url_from_resp(resp)
                continue
            if status_code == 302:
                # 暂时重定向
                break
            else:
                raise NotImplementedError(f"status_code = {status_code}")

        return self.get_redirect_url_from_resp(resp)

    def get_redirect_url_from_resp(self, resp):
        return resp.headers['Location']

    def get(self, url, **kwargs):
        return self.request(url, kwargs, super().get)

    def post(self, url, **kwargs):
        return self.request(url, kwargs, super().post)
