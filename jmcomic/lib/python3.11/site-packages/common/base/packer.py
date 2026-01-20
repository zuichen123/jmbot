from typing import Callable, Type, Dict, Optional, Any, Tuple, TypeVar


class Packer:

    def pack(self, obj: object, filepath: str):
        raise NotImplementedError

    def unpack(self, filepath: str, clazz=None) -> Any:
        """
        反序列化得到对象

        :param filepath: 文件路径
        :param clazz: 预期的class对象，Packer不一定会使用此参数（如YmlPacker就不会，而JsonPacker会）。
        此参数需要是callable，并且能返回一个对象，即：obj = clazz()
        :return:
        """
        raise NotImplementedError

    def unpack_by_str(self, text: str, clazz=None) -> Any:
        raise NotImplementedError


class AbstractPacker(Packer):
    pack_mode = 'w'
    unpack_mode = 'r'
    encoding = 'utf-8'

    def pack(self, obj: object, filepath: str):
        self.with_file(filepath,
                       self.pack_mode,
                       self.dump,
                       obj=obj,
                       )

    def unpack(self, filepath: str, clazz=None) -> Any:
        return self.with_file(filepath,
                              self.unpack_mode,
                              self.load,
                              clazz=clazz,
                              )

    def dump(self, fp, obj: object):
        raise NotImplementedError

    def load(self, fp, clazz: Optional[Type]) -> Any:
        raise NotImplementedError

    @classmethod
    def with_file(cls,
                  filepath: str,
                  mode: str,
                  visitor: Callable,
                  **kwargs
                  ):
        with open(filepath, mode, encoding=cls.encoding) as f:
            return visitor(f, **kwargs)


class YmlPacker(AbstractPacker):

    def unpack_by_str(self, text: str, clazz=None) -> Any:
        import yaml
        return yaml.load(text, yaml.SafeLoader)

    def dump(self, fp, obj):
        import yaml
        yaml.dump(obj,
                  fp,
                  allow_unicode=True,
                  indent=2,
                  )

    def load(self, fp, clazz) -> Any:
        import yaml
        return yaml.load(fp, yaml.SafeLoader)

    @staticmethod
    def add_constructor(tag, constructor: Callable):
        from yaml import add_constructor
        add_constructor(tag, constructor)


class JsonPacker(AbstractPacker):

    def unpack_by_str(self, text: str, clazz=None) -> Any:
        from json import loads
        return loads(text)

    def dump(self, fp, obj, indent=2):
        from json import dump
        dump(self.to_dict(obj),
             fp,
             ensure_ascii=False,
             indent=indent,
             )

    def load(self, fp, clazz) -> Any:
        from json import load
        dic: dict = load(fp)
        if clazz is None:
            return dic

        obj: object = clazz()
        obj.__dict__.update(dic)
        return obj

    @classmethod
    def to_dict(cls, obj):
        """
        将对象转换为字典
        """
        if hasattr(obj, "__dict__"):
            d = dict(obj.__dict__)
            for key, value in d.items():
                if hasattr(value, "__dict__"):
                    d[key] = cls.to_dict(value)
            return d
        elif isinstance(obj, list):
            return [cls.to_dict(item) for item in obj]
        else:
            return obj


class PicklePacker(AbstractPacker):
    pack_mode = 'wb'
    unpack_mode = 'rb'
    encoding = None

    def dump(self, fp, obj: object):
        import pickle
        pickle.dump(obj, fp)

    def load(self, fp, clazz: Optional[Type]) -> Any:
        import pickle
        return pickle.load(fp)

    def unpack_by_str(self, text: str, clazz=None) -> Any:
        raise NotImplementedError('unsupported')


class PackerUtil:
    mode_yml = 'yml'
    mode_json = 'json'
    mode_py_pickle = 'pickle'

    mode_mapping: Dict[str, Type[Packer]] = {
        mode_yml: YmlPacker,
        mode_json: JsonPacker,
        mode_py_pickle: PicklePacker,

    }

    @classmethod
    def get_packer(cls, mode: str) -> Packer:
        packer: Type[Packer] = cls.mode_mapping.get(mode, None)

        if packer is None:
            raise AssertionError(f"unknown mode: '{mode}', acceptable modes={list(cls.mode_mapping.keys())}")

        return packer()

    @classmethod
    def decide_packer(cls, filepath: str) -> Packer:
        from common import of_file_suffix
        return cls.get_packer(of_file_suffix(filepath, trim_comma=True))

    @classmethod
    def pack(cls, obj: object, filepath: str, packer=None):
        packer = packer or cls.decide_packer(filepath)
        packer.pack(obj, filepath)

    __T = TypeVar('__T')

    @classmethod
    def unpack(cls, filepath: str, clazz: Type[__T] = None, packer=None) -> Tuple[__T, Packer]:
        packer = packer or cls.decide_packer(filepath)
        return packer.unpack(filepath, clazz), packer

    @classmethod
    def unpack_by_str(cls, text, mode, clazz=None) -> Tuple[Any, Packer]:
        packer = cls.get_packer(mode)
        return packer.unpack_by_str(text, clazz), packer
