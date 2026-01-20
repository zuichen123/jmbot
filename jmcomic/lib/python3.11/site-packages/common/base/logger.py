from common import Optional, Iterable, file_not_exists, create_file


class Logger:

    def is_done(self, obj) -> bool:
        raise NotImplementedError

    def is_not_done(self, obj) -> bool:
        return not self.is_done(obj)

    def add(self, obj, save=False):
        raise NotImplementedError

    def add_all(self, data: Iterable, save=False):
        for each in data:
            self.add(each, save)

    def size(self) -> int:
        raise NotImplementedError

    def remove(self, obj, save=False):
        raise NotImplementedError

    def set_data(self, data):
        raise NotImplementedError

    def get_data(self) -> set:
        raise NotImplementedError

    def clear_data(self):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError

    def save(self, callback=None):
        raise NotImplementedError

    def __len__(self):
        return self.size()

    def __contains__(self, item):
        return self.is_done(item)

    def __iter__(self):
        raise NotImplementedError

    def make_sure_loaded(self):
        raise NotImplementedError


# noinspection PyAbstractClass
class AbstractSetLogger(Logger):

    def __init__(self):
        self.data: Optional[set] = None

    def is_done(self, obj) -> bool:
        self.make_sure_loaded()
        return obj in self.data

    def add(self, obj, save=False):
        self.make_sure_loaded()
        self.data.add(obj)
        if save is True:
            self.save()

    def set_data(self, data):
        self.data = data

    def get_data(self) -> set:
        self.make_sure_loaded()
        return self.data

    def remove(self, obj, save=False):
        if self.is_done(obj):
            self.data.remove(obj)
            if save is True:
                self.save()

    def size(self):
        self.make_sure_loaded()
        return len(self.data)

    def clear_data(self):
        self.data = set()

    def make_sure_loaded(self):
        if self.data is None:
            self.load()

    def __iter__(self):
        self.make_sure_loaded()
        return self.data.__iter__()


class LoggerImpl(AbstractSetLogger):

    def __init__(self, log_file_path: str):
        super().__init__()
        self.log_file_path = log_file_path

    def load(self):
        self.clear_data()
        # check file
        if file_not_exists(self.log_file_path):
            create_file(self.log_file_path)
            return
        # read file
        with open(self.log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if len(line) != 0:
                    self.data.add(line)

    def save(self, callback=None):
        # no load, no save
        if self.data is None and callback is None:
            return

        self.make_sure_loaded()
        data = self.data if callback is None else callback(self.data)

        with open(self.log_file_path, 'w', encoding='utf-8') as f:
            for obj in data:
                f.write(f"{obj}\n")

    def __str__(self):
        return self.log_file_path


class LoggerFactory:

    @classmethod
    def get_logger(cls,
                   filepath: str,
                   load_after_created=False,
                   save_at_exit=False,
                   ) -> Logger:
        """
        工厂模式，暴露获取logger的接口

        :param filepath: logger管理的文件路径
        :param load_after_created: 是否创建后就立即加载数据，多线程下建议传True以保证数据一致
        :param save_at_exit: atexit_register(logger.save)
        :return: Logger实现类
        """
        logger = LoggerImpl(filepath)

        # load after created
        if load_after_created:
            logger.load()

        if save_at_exit:
            from common import atexit_register
            atexit_register(logger.save)

        return logger
