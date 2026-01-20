import inspect

from .typing_util import *
from .args_util import process_args_kwargs
from .file_util import fix_filepath, mkdir_if_not_exists

_workspace = ""


def workspace(sub_file=None, mode=None, encoding='utf-8', is_dir=False) -> Union[str, IO]:
    global _workspace

    if len(_workspace) != 0:
        workspace = _workspace
    else:
        import os
        workspace = fix_filepath(os.getcwd())

    if sub_file is None:
        return workspace

    sub_file = fix_filepath(workspace + sub_file)
    if is_dir:
        if mode is not None:
            raise AssertionError(f'不可以读取文件夹！args=({sub_file}, {mode}, {encoding}, {is_dir})')

        mkdir_if_not_exists(sub_file)
        return sub_file
    else:
        if mode is None:
            return sub_file

        return open(sub_file,
                    mode=mode,
                    encoding=encoding,
                    )


def set_application_workspace(workspace: str):
    global _workspace
    _workspace = fix_filepath(workspace)


def current_thread():
    from threading import current_thread
    return current_thread()


def pause_console():
    import os
    os.system("pause")


def atexit_register(func, args=None, kwargs=None, ensure_only_once=False):
    if ensure_only_once is True:
        atexit_unregister(func)
    args, kwargs = process_args_kwargs(args, kwargs)
    import atexit
    atexit.register(func, *args, **kwargs)


def atexit_unregister(func):
    import atexit
    atexit.unregister(func)


def atexit_clear():
    # noinspection PyProtectedMember
    from atexit import _clear
    _clear()


def atexit_ncallbacks():
    # noinspection PyProtectedMember
    from atexit import _ncallbacks
    return _ncallbacks()


ismethod = inspect.ismethod
is_function = inspect.isfunction


class ApplicationMessageCenter:
    msg_need_to_confirm: Dict[Any, List[str]] = {}

    @staticmethod
    def __ofKey():
        # from threading import current_thread
        # return current_thread()
        return 1

    @classmethod
    def append_msg(cls, msg: str, key=None):
        key = key or cls.__ofKey()
        msgs = cls.msg_need_to_confirm
        if key not in msgs:
            msgs[key] = [msg]
        else:
            msgs[key].append(msg)

    @classmethod
    def of_msgs_need_to_confirm(cls) -> List[str]:
        msgs = cls.msg_need_to_confirm.get(cls.__ofKey(), None)
        return msgs if msgs is not None else []


def print_sep(text='-', length=70):
    print(''.center(length, text))


def print_eye_catching(text, need_to_confirm=False, surround='\n', multiple=1, key=None):
    if need_to_confirm is True:
        ApplicationMessageCenter.append_msg(text, key)
    margin = surround * multiple
    print(f"{margin}{text}{margin}")


def print_list(data: Iterable, sep='\n', **kwargs):
    print(*data, sep=sep, **kwargs)


def print_obj_dict(obj: object, sep='\n', **kwargs):
    dic = obj if isinstance(obj, dict) else obj.__dict__
    print_list(list(dic.items()), sep=sep, **kwargs)


def paste_from_clip() -> str:
    from pyperclip import paste
    return paste()


def copy_to_clip(text: str, do_print=False):
    from pyperclip import copy
    copy(text)
    if do_print is True:
        print(text)


def ask_open_sys_explorer(message: str, dirpath):
    import tkinter
    from tkinter.messagebox import askyesno
    window = tkinter.Tk()
    window.attributes('-topmost', True)
    window.withdraw()  # 退出默认 tk 窗口
    if askyesno('打开文件夹', message):
        import os
        r = "\\"
        os.system(f"explorer {dirpath.replace('/', r)}")


def parse_unicode_escape_text(text: str) -> str:
    def decode(raw: re.Match):
        return raw.group(0).encode('utf-8').decode('unicode_escape')

    return re.sub(r"\\u\w{4}", decode, text).replace(r"\/", "/")


def traceback_print_exec():
    import traceback
    traceback.print_exc()


def str_to_line_iter(lines: str):
    for line in lines.strip().splitlines():
        line = line.strip()
        if line != '':
            yield line


def str_to_list(lines: str):
    return list(str_to_line_iter(lines))


def str_to_set(lines: str):
    return set(str_to_line_iter(lines))


def show_bytecodes():
    from dis import dis
    try:
        dis(paste_from_clip())
    except:
        return


class ConfigTemplate:
    default_provider = {}

    def __init__(self, metadata: dict, filepath=None):
        self.metadata: dict = metadata
        self._filepath = filepath

    @property
    def filepath(self):
        return self._filepath

    def __getattr__(self, item):
        if item in self.metadata:
            return self.metadata[item]

        if self is self.__class__.default_provider:
            raise KeyError(item)

        return self.from_default(item)

    def __setattr__(self, key: str, value):
        if key == 'metadata' or key == 'default_provider' or key.startswith('_'):
            return super().__setattr__(key, value)
        self.metadata[key] = value

    __getitem__ = __getattr__
    __setitem__ = __setattr__

    def to_full_metadata(self):
        return {**self.default_provider, **self.metadata}

    def remove_self_default(self):
        for k in list(self.metadata.keys()):
            try:
                value = self.from_default(k)
                if self.metadata[k] == value:
                    self.metadata.pop(k)
            except (KeyError, AttributeError):
                continue

    def from_default(self, item):
        dp = self.default_provider
        if isinstance(dp, dict):
            return dp[item]

        return getattr(dp, item)

    def to_file(self, filepath=None):
        filepath = filepath or self._filepath
        if not filepath:
            raise Exception('filepath is None')
        from common import PackerUtil
        PackerUtil.pack(self.metadata, self._filepath)
        self._filepath = filepath

    @classmethod
    def from_file(cls, filepath):
        from common import PackerUtil
        metadata, _ = PackerUtil.unpack(filepath)
        config = cls(metadata, filepath)
        return config
