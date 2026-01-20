from common import Dict, Optional, Set


class CookieParser:

    def check_cookies_str_is_valid(self, cookies_str: str) -> bool:
        raise NotImplementedError

    def parse_to_cookies(self, cookies_str: str) -> dict:
        raise NotImplementedError

    def apply(self, cookies_str: str = None, when_valid_message=None) -> Optional[dict]:
        if cookies_str is None:
            from pyperclip import paste
            cookies_str = paste()
        if self.check_cookies_str_is_valid(cookies_str):
            if when_valid_message is not None:
                print(when_valid_message)

            return self.parse_to_cookies(cookies_str)


# suit for the chrome plugin: [cookie-editor]
# the format of cookie data exported by this plugin is as follow :
# [
#     {
#         "domain": ".bilibili.com",
#         "expirationDate": 1693404645,
#         "hostOnly": false,
#         "httpOnly": false,
#         "name": "buvid_fp_plain",
#         "path": "/",
#         "sameSite": null,
#         "secure": false,
#         "session": false,
#         "storeId": null,
#         "value": "undefined"
#     },
#     ...
# ]

class ChromePluginCookieParser(CookieParser):
    default_required = {
        "domain",
        "hostOnly",
        "httpOnly",
        "name",
        "path",
        "sameSite",
        "secure",
        "session",
        "storeId",
        "value",
    }

    def __init__(self, required_word_for_cookies_str: Set[str] = None):
        self.required_word = self.default_required.union(required_word_for_cookies_str or set())

    def copy_cookies_to_clip(self, cookies):
        import_cookies = []
        for k, v in cookies.items():
            import_cookies.append({
                "name": k,
                "value": v,
                'session': False,
            })
        from common import json_dumps, copy_to_clip

        imports = json_dumps(import_cookies, indent=2)
        copy_to_clip(imports)

    def check_cookies_str_is_valid(self, cookies_str: str) -> bool:
        if cookies_str == '':
            return False
        if not cookies_str.startswith('['):
            return False

        for word in self.required_word:
            if word not in cookies_str:
                return False

        return True

    def parse_to_cookies(self, cookies_str: str) -> Dict[str, str]:
        from json import loads
        dic: dict = loads(cookies_str)
        return {cookie['name']: cookie['value'] for cookie in dic}
