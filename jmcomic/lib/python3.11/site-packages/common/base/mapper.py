from common import Any, Optional, Dict, Iterable
from common import file_not_exists, create_file


class Mapper:

    def set(self, key: Any, value: Any, save=False):
        raise NotImplementedError

    def get(self, key: Any, default_v: Any = None) -> Any:
        raise NotImplementedError

    def has_not_key(self, key: Any) -> bool:
        return not self.__contains__(key)

    def remove(self, obj, save=False):
        raise NotImplementedError

    def size(self) -> int:
        raise NotImplementedError

    def set_data(self, data: dict):
        raise NotImplementedError

    def get_data(self) -> dict:
        raise NotImplementedError

    def clear_data(self):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError

    def save(self, callback=None):
        raise NotImplementedError

    def make_sure_loaded(self):
        raise NotImplementedError

    def items(self):
        raise NotImplementedError

    def keys(self) -> Iterable[str]:
        raise NotImplementedError

    def values(self) -> Iterable[str]:
        raise NotImplementedError

    def get_separator(self) -> str:
        raise NotImplementedError

    def set_separator(self, separator: str):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __getitem__(self, key):
        obj = self.get(key)
        if obj is None:
            raise KeyError(key)
        return obj

    def __setitem__(self, key, value):
        self.set(key, value)

    def __contains__(self, key):
        raise NotImplementedError

    def __len__(self):
        return self.size()


# noinspection PyAbstractClass
class AbstractDictMapper(Mapper):

    def __init__(self):
        self.data: Optional[Dict] = None

    def set(self, key, value, save=False):
        self.make_sure_loaded()
        self.data[key] = value
        if save is True:
            self.save()

    def get(self, key, default_v=None) -> Any:
        self.make_sure_loaded()
        return self.data.get(key, default_v)

    def __contains__(self, key):
        self.make_sure_loaded()
        return self.data.__contains__(key)

    def remove(self, key, save=False):
        self.make_sure_loaded()
        if key in self.data:
            del self.data[key]
            if save is True:
                self.save()

    def size(self) -> int:
        self.make_sure_loaded()
        return len(self.data)

    def set_data(self, data):
        self.data = data

    def get_data(self):
        self.make_sure_loaded()
        return self.data

    def clear_data(self):
        self.data = {}

    def make_sure_loaded(self):
        if self.data is None:
            self.load()

    def __iter__(self):
        self.make_sure_loaded()
        return iter(self.data.items())

    def __len__(self):
        self.make_sure_loaded()
        return len(self.data)

    def items(self):
        self.make_sure_loaded()
        return self.data.items()

    def keys(self):
        self.make_sure_loaded()
        return self.data.keys()

    def values(self):
        self.make_sure_loaded()
        return self.data.values()


class MapperImpl(AbstractDictMapper):

    def __init__(self, map_file_path: str, separator: str):
        super().__init__()
        self.map_file_path = map_file_path
        self.separator = separator

    def load(self):
        self.clear_data()

        # check file
        if file_not_exists(self.map_file_path):
            create_file(self.map_file_path)
            return

        # read file
        with open(self.map_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    continue

                split_word_index = line.find(self.separator)
                if split_word_index == -1:
                    continue

                key = line[:split_word_index].strip()
                value = line[split_word_index + 1:].strip()
                self.data[key] = value

    def save(self, callback=None):
        if self.data is None and callback is None:
            return

        self.make_sure_loaded()
        data = self.data if callback is None else callback(self.data)

        # write file
        with open(self.map_file_path, 'w', encoding='utf-8') as f:
            if isinstance(data, (tuple, list)):
                for (k, v) in data:
                    f.write(f"{k}{self.separator}{v}\n")
            elif isinstance(data, dict):
                for k, v in data.items():
                    f.write(f"{k}{self.separator}{v}\n")
            else:
                raise AssertionError(f"无法序列化data, type={type(data)}")

        return data

    def get_separator(self) -> str:
        return self.separator

    def set_separator(self, separator: str):
        self.separator = separator

    def __str__(self) -> str:
        return self.map_file_path


class MapperFactory:

    @classmethod
    def get_mapper(cls,
                   filepath: str,
                   separator=',',
                   load_after_created=False,
                   save_at_exit=False
                   ) -> Mapper:
        mapper = MapperImpl(filepath, separator)

        if load_after_created:
            mapper.load()

        if save_at_exit:
            from common import atexit_register
            atexit_register(mapper.save)

        return mapper
