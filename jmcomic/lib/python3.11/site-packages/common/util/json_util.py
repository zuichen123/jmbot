from json import load, loads, dump, dumps

from .typing_util import *

json_loads = loads
json_load = load

json_dumps = dumps
json_dump = dump

missing_sentinel = object()


class AdvancedDict:
    """
    功能增强版Dict

    1. get增强，支持使用如下方式获取字典的值：
        object.key
        object[key]
    2. set增强，支持使用如下方式设置字典的值：
        object.key = value
        object[key] = value
    3. 代理dict的方法
    4. 添加常用函数
    """

    def __init__(self, data: dict):
        if not isinstance(data, dict):
            raise AssertionError('初始化参数必须是dict类型')

        self._data = data

    @classmethod
    def wrap(cls, data):
        if isinstance(data, dict):
            return cls(data)
        if isinstance(data, cls):
            return cls(data.src_dict)

        raise NotImplementedError(f'unsupported type: {type(data)}')

    #  get增强

    def __getattr__(self, item):
        return self.wrap_value(self._data[item])

    # set增强
    def __setattr__(self, key, value):
        if key == '_data':
            return super().__setattr__(key, value)
        self._data[key] = value

    def __str__(self):
        return self._data.__str__()

    def __repr__(self):
        return self._data.__repr__()

    def __setitem__(self, key, value):
        self._data[key] = value

    @classmethod
    def wrap_value(cls, v):
        if isinstance(v, (list, tuple)):
            v = [cls(e) if isinstance(e, dict) else e for e in v]
        elif isinstance(v, dict):
            v = cls(v)
        return v

    # 原始dict

    @property
    def src_dict(self):
        return self._data

    # 代理dict的方法

    def __contains__(self, item):
        return item in self._data

    def __getitem__(self, item):
        return self.wrap_value(self._data[item])

    def get(self, *args, **kwargs):
        return self._data.get(*args, **kwargs)

    def pairs(self):
        for k, v in self._data.items():
            yield k, self.wrap_value(v)

    def items(self):
        return self._data.items()

    def values(self):
        return self._data.values()

    def keys(self):
        return self._data.keys()

    def __iter__(self):
        return iter(self._data)

    def __delitem__(self, key):
        del self._data[key]

    def setdefault(self, k, v):
        return self._data.setdefault(k, v)

    def pop(self, k, dv):
        return self._data.pop(k, dv)

    # 添加常用函数
    def to_json(self, **kwargs):
        import json
        return json.dumps(self._data, **kwargs)

    def to_file(self, filepath):
        from common import PackerUtil
        PackerUtil.pack(self._data, filepath)

    def __copy__(self):
        import copy
        return self.__class__(copy.deepcopy(self._data))

    def copy(self):
        return self.__copy__()

    def get_from_any_key(self, *keys, missing=missing_sentinel):
        """
        从字典中获取指定键对应的值，如果不存在，则返回指定的默认值

        :param keys: 可变数量的键
        :return: 第一个存在的键对应的值，如果没有找到任何键，则返回None
        :param missing: 当没有找到任何键对应的值时，返回的值
        """
        for key in keys:
            try:
                return self[key]
            except KeyError:
                pass

        if missing is missing_sentinel:
            raise KeyError(keys)

        return missing


class EasyAccessDict:
    """
    更方便读取dict的类
    dic['a']['b']['c'] -> obj.a.b.c
    """

    def __init__(self, data: dict):
        self.init_data(data)

    def init_data(self, data: dict):
        for k, v in data.items():
            if k is None:
                continue
            if isinstance(v, (list, tuple)):
                setattr(self, k, [self.__class__(e) if isinstance(e, dict) else e for e in v])
            else:
                setattr(self, k, self.__class__(v) if isinstance(v, dict) else v)

    def __getitem__(self, item):
        """
        model['aaa'] ---> model.aaa
        """
        if not isinstance(item, str):
            raise NotImplementedError(f"item: {item} ({type(item)})")

        return getattr(self, item)


class AdvancedEasyAccessDict:
    """
    对EasyAccessDict功能的增强
    1. 支持懒加载，性能更好开销更小（相比EAD初始化即加载所有键值的方式）
    2. 支持当作dict来用，可以达到无感知的兼容效果
    3. 支持拿到原始dict
    """

    def __init__(self, data: dict):
        if data is None:
            raise AssertionError(f"data is None")

        self._data = data

    def __getattr__(self, item):
        v = self._data[item]
        if isinstance(v, (list, tuple)):
            v = [self.__class__(e) if isinstance(e, dict) else e for e in v]
        elif isinstance(v, dict):
            v = self.__class__(v)

        setattr(self, item, v)
        return v

    # 原始dict

    @property
    def src_dict(self):
        return self._data

    def get_from_anyone(self, *keys):
        for key in keys:
            try:
                return self.__getitem__(key)
            except KeyError:
                pass
        raise KeyError(keys, self._data)

    # 模拟dict的方法

    def __contains__(self, item):
        return item in self._data

    def __getitem__(self, item):
        return self._data[item]

    def get(self, *args, **kwargs):
        return self._data.get(*args, **kwargs)

    def items(self):
        return self._data.items()


DictModel = AdvancedDict


def json_loadf(filepath,
               encoding='utf-8',
               decode_unicode=False,
               ):
    from .file_util import file_not_exists
    if file_not_exists(filepath):
        raise AssertionError(f"不存在的json文件路径：{filepath}")

    with open(filepath, 'r', encoding=encoding) as f:
        if decode_unicode is False:
            return json_load(f)
        else:
            from .sys_util import parse_unicode_escape_text
            return json_loads(parse_unicode_escape_text(f.read()))


def json_dumpf(obj, fp, encoding='utf-8', indent=2):
    with open(fp, 'w', encoding=encoding) as f:
        json_dump(obj, f, indent=indent)


def accpet_json(fp, accept_v: Callable[[Any], None] = None, accpet_k: Callable[[str], None] = None):
    def __accept_json_keys_values(data):
        if isinstance(data, list):
            for each in data:
                __accept_json_keys_values(each)

        elif isinstance(data, dict):
            for k, v in data.items():
                if accpet_k is not None:
                    accpet_k(k)
                __accept_json_keys_values(v)
        else:
            if accept_v is not None:
                accept_v(data)

    data = json_load(fp)

    if accpet_k is not None or accept_v is not None:
        __accept_json_keys_values(data)

    return data


def keys_of_json(data) -> Generator:
    if isinstance(data, list):
        for each in data:
            keys_of_json(each)

    elif isinstance(data, dict):
        for k, v in data.items():
            yield k
            keys_of_json(v)


def values_of_json(data) -> Generator:
    if isinstance(data, list):
        for each in data:
            yield from values_of_json(each)

    elif isinstance(data, dict):
        for v in data.values():
            yield from values_of_json(v)

    else:
        yield data
