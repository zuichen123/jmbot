from .typing_util import *
from .json_util import DictModel


class ProxyBuilder:
    proxy_protocols = ['https', 'http']
    addr_f = '{}:{}'

    @classmethod
    def build_proxy(cls, address):
        proxies = {protocol: address for protocol in cls.proxy_protocols}
        return proxies

    @classmethod
    def clash_proxy(cls, ip='127.0.0.1', port='7890'):
        return cls.build_proxy(cls.addr_f.format(ip, port))

    @classmethod
    def v2ray_proxy(cls, ip='127.0.0.1', port='10809'):
        return cls.build_proxy(cls.addr_f.format(ip, port))

    @classmethod
    def fiddler_proxy(cls, ip='127.0.0.1', port='8888'):
        return cls.build_proxy(cls.addr_f.format(ip, port))

    @classmethod
    def system_proxy(cls):
        import urllib.request
        proxies: dict = urllib.request.getproxies()
        if len(proxies) == 0:
            return {}

        # extract ip and port from proxies dict
        addr: str = proxies.popitem()[1]
        prot = '://'
        index = addr.find(prot)
        if prot in addr:
            addr = addr[index + len(prot):]

        return cls.build_proxy(addr)

    @classmethod
    def build_by_str(cls, text):
        """
        :param text: 127.0.0.1:1234 / clash / v2ray
        """
        proxy = getattr(cls, text + '_proxy', None)
        if proxy is None:
            # 127.0.0.1:1234
            return cls.build_proxy(text)
        else:
            # clash_proxy
            return proxy()

    v2Ray_proxy = v2ray_proxy


def save_resp_content(resp, filepath: str):
    from .file_util import of_dir_path
    of_dir_path(filepath, mkdir=True)
    with open(filepath, 'wb') as f:
        f.write(resp.content)


def set_global_proxy(status='off'):
    """
    全局性的关闭或者启用系统代理。
    测试时发现，如果设置了系统代理，在访问 https 网站时发生错误 requests.exceptions.ProxyError
    原因是 SSL: self._sslobj.do_handshake() -> OSError: [Errno 0] Error
    进一步，是因为 urllib3 1.26.0 以上版本支持 https协议，但是代理软件不支持，导致连接错误。
    所以使用 { 'https': 'http://127.0.0.1:1080' }，http的协议访问 https 的网址（本地通信），即可解决。
    或者在 requests.get 函数中增加 proxies={'https': None} 参数来解决，但每次访问都需加这个参数，太麻烦，
    此处通过修改 requests 库中的 get_environ_proxies 函数的方法来全局性地关闭系统代理，或者仅关闭 https 的代理。

    注意：仅影响本 Python程序的 requests包，不影响其他 Python程序，
    不影响 Windows系统的代理设置，也不影响浏览器的代理设置。

    :param status: status - 'off', 关闭系统代理；'on', 打开系统代理；'toggle', 切换关闭或者打开状态；
    """
    from requests import sessions, utils
    init_func = sessions.get_environ_proxies
    if status == 'off':
        # 关闭系统代理
        if init_func.__name__ == '<lambda>':
            # 已经替换了原始函数，即已经是关闭状态，无需设置
            return
        # 修改函数，也可以是 lambda *x, **y: {'https': None}
        sessions.get_environ_proxies = lambda *x, **y: {}
    elif status == 'on':
        # 打开系统代理，如果设置了代理的话
        # 对高版本的 urllib3(>1.26.0) 打补丁，修正 https代理 BUG: OSError: [Errno 0] Error
        # noinspection PyUnresolvedReferences
        proxies = utils.getproxies()
        if 'https' in proxies:
            proxies['https'] = proxies.get('http')  # None 或者 'http://127.0.0.1:1080'
        sessions.get_environ_proxies = lambda *x, **y: proxies
        sessions.get_environ_proxies.__name__ = 'get_environ_proxies'
    else:
        # 切换开关状态
        if init_func.__name__ == '<lambda>':
            # 已经是关闭状态
            set_global_proxy('on')
        else:
            # 已经是打开状态
            set_global_proxy('off')


def print_resp_json(resp, indent=2):
    from .json_util import json_dumps
    json_str = json_dumps(resp.json(), indent=indent)
    from .sys_util import parse_unicode_escape_text
    print(parse_unicode_escape_text(json_str))


class IResp:

    @property
    def http_code(self) -> str:
        raise NotImplementedError

    @property
    def is_success(self) -> bool:
        raise NotImplementedError

    @property
    def is_not_success(self) -> bool:
        return not self.is_success

    def require_success(self):
        if self.is_not_success:
            raise AssertionError('response is failed')

    @property
    def text(self) -> str:
        raise NotImplementedError

    @property
    def url(self) -> str:
        raise NotImplementedError

    @property
    def headers(self) -> dict:
        raise NotImplementedError

    @property
    def content(self):
        raise NotImplementedError

    @property
    def redirect_url(self) -> str:
        return self.headers['Location']

    @property
    def res_data(self) -> dict:
        raise NotImplementedError

    @property
    def model_data(self) -> DictModel:
        raise NotImplementedError

    def json(self, **kwargs) -> dict:
        raise NotImplementedError

    def model(self) -> DictModel:
        raise NotImplementedError


class CommonResp(IResp):

    def __init__(self, resp):
        self.resp = resp

    @property
    def http_code(self):
        return self.resp.status_code

    @property
    def is_success(self) -> bool:
        return self.resp.status_code == 200

    @property
    def text(self) -> str:
        return self.resp.text

    @property
    def url(self) -> str:
        return self.resp.url

    @property
    def headers(self) -> dict:
        return self.resp.headers

    @property
    def content(self):
        return self.resp.content

    @property
    def code(self):
        return self.json()['code']

    @property
    def msg(self):
        return self.json()['msg']

    @property
    def res_data(self) -> Any:
        self.require_success()
        return self.json()['data']

    @property
    def model_data(self) -> DictModel:
        self.require_success()
        return self.model().data

    def json(self, **kwargs) -> Dict:
        raise NotImplementedError

    def model(self) -> DictModel:
        raise NotImplementedError


def get_browser_cookies(browser: str, domain, safe=False):
    if not browser:
        raise ValueError('browser参数不能为空')

    try:
        import browser_cookie3

        cookies = getattr(browser_cookie3, browser)(domain_name=domain)
        cdict = {c.name: c.value for c in cookies}
        return cdict, cookies
    except BaseException as e:
        if safe:
            return None, e
        else:
            raise e
