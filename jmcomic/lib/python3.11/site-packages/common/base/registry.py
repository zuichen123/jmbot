from typing import Dict, TypeVar, Type, Optional, Iterable

from common import atexit_register, atexit_unregister, current_thread


class AtexitRegistry:

    def __init__(self, atexit_hooks) -> None:
        self.atexit_hooks = atexit_hooks

    def register(self):
        for func in self.atexit_hooks:
            func, args = func if isinstance(func, tuple) else (func, None)
            atexit_register(func, args)

    def unregister(self):
        for func in self.atexit_hooks:
            atexit_unregister(func if not isinstance(func, tuple) else func[0])


# 注册组件
class ComponentRegistry:
    registry: Dict[Type, Dict[str, Type]] = {}

    @classmethod
    def register_component(cls,
                           interface: type,
                           key_name: str,
                           variables: Iterable,
                           require_key=True,
                           ):
        cls.registry.setdefault(interface, {})
        for clazz in variables:
            if isinstance(clazz, type) and clazz != interface and issubclass(clazz, interface):
                try:
                    key = getattr(clazz, key_name)
                    cls.registry[interface][key] = clazz
                except AttributeError:
                    if require_key:
                        raise AssertionError(f'register failed, {clazz} must have a "{key_name}" attribute')

        return cls.registry[interface]

    __T = TypeVar('__T')

    @classmethod
    def get_impl_clazz(cls, interface: Type[__T], key: str) -> Type[__T]:
        if interface not in cls.registry:
            raise AssertionError(f'interface {interface} not found in registry')

        clazz: Optional[Type[cls.__T]] = cls.registry[interface].get(key, None)
        if clazz is None:
            raise AssertionError(f'key "{key}" not found in registry of {interface}')

        return clazz

    @classmethod
    def get_all_impl(cls, interface: type) -> Dict[str, Type]:
        return cls.registry[interface]


class StopThreadFlag:
    RUN = True
    STOP = False

    def __init__(self, key):
        self.key = key
        self._marked = set()
        self._all_stop = False

    @property
    def marked_thread_set(self):
        return self._marked

    def should_stop(self, thread=None):
        thread = thread or current_thread()
        try:
            return self._all_stop or self.STOP == getattr(thread, self.key)
        except AttributeError:
            self.mark_run()

    def mark_run(self, thread=None):
        thread = thread or current_thread()
        if self._all_stop or getattr(thread, self.key, None) == self.STOP:
            import sys
            sys.stderr.write(
                f"{self.key} is already set STOP for thread: [{thread}], "
                f"current thread: [{current_thread()}]\n"
            )
            return

        setattr(thread, self.key, self.RUN)
        self._marked.add(thread)

    def mark_stop(self, thread):
        thread = thread or current_thread()
        setattr(thread, self.key, self.STOP)
        self._marked.add(thread)

    def mark_stop_for_all(self):
        for thread in self._marked:
            self.mark_stop(thread)
            self._all_stop = True

    def wait_for_all(self):
        for thread in self.marked_thread_set:
            if thread is current_thread():
                continue

            while thread.is_alive():
                thread.join(timeout=1)

    def raise_stop_exception(self):
        raise KeyboardInterrupt(f'thread: {current_thread()}')

    def sleep_or_raise(self, duration: float):
        from time import sleep
        if duration < 0.1:
            sleep(duration)

        times = int(duration / 0.1)
        for _ in range(times):
            self.if_stop_raise()
            sleep(0.1)

        self.if_stop_raise()
        sleep(duration - 0.1 * times)

    def if_stop_raise(self):
        if self.should_stop():
            self.raise_stop_exception()

    def sleep_or_return(self, duration: float, part=0.5) -> bool:
        """
        :return: should stop
        """
        from time import sleep
        if duration < part:
            sleep(duration)

        times = int(duration / part)
        for _ in range(times):
            if self.should_stop():
                return True
            sleep(part)

        if self.should_stop():
            return True
        sleep(duration - part * times)
        return False


class ThreadFlagManager:

    def __init__(self, flag):
        self.flag: StopThreadFlag = flag

    def notice_stop_callback(self):
        pass

    def sleep_or_raise(self, duration: float):
        from time import sleep
        if duration < 0.1:
            sleep(duration)

        times = int(duration / 0.1)
        for _ in range(times):
            self.if_stop_raise()
            sleep(0.1)

        self.if_stop_raise()
        sleep(duration - 0.1 * times)

    def if_stop_raise(self):
        if self.is_stop():
            self.flag.raise_stop_exception()

    def sleep_or_return(self, duration: float, part=0.5) -> bool:
        """
        :return: should stop
        """
        from time import sleep
        if duration < part:
            sleep(duration)

        times = int(duration / part)
        for _ in range(times):
            if self.is_stop():
                return True
            sleep(part)

        if self.is_stop():
            return True
        sleep(duration - part * times)
        return False

    def is_stop(self):
        if self.flag.should_stop():
            self.notice_stop_callback()

        return self.flag.should_stop()
