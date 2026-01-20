from typing import (
    List,
    Callable,
    Iterable,
    Optional,
    Any,
    Union,
    Type,
    Dict,
    Tuple,
    TypeVar
)

from ..util import Thread, Process, \
    process_args_kwargs, process_single_arg_to_args_and_kwargs, \
    current_thread
from .registry import StopThreadFlag


class MultiTaskLauncher:
    Task = Union[Thread, Process]
    TaskClass = Union[Type[Thread], Type[Process]]
    DEFAULT_FLAG = None

    def __init__(self, task_metadata: Optional[Dict[str, Any]] = None):
        if task_metadata is None:
            task_metadata = {}

        self.flag: Optional[StopThreadFlag] = self.DEFAULT_FLAG
        self.task_ls: list = []
        self.task_metadata: dict = task_metadata

    def check_stop_flag(self):
        if self.flag and self.flag.should_stop():
            self.flag.raise_stop_exception()

    def create_task(self,
                    target: Callable,
                    args: Optional[Any] = None,
                    kwargs: Optional[dict] = None,
                    clazz: TaskClass = Thread,
                    start=True,
                    **extra
                    ) -> Union[Thread, Process]:
        args, kwargs = process_args_kwargs(args, kwargs)

        metadata = self.task_metadata.copy()
        metadata.update(extra)

        task = clazz(target=target,
                     args=args,
                     kwargs=kwargs,
                     **metadata,
                     )
        self.task_ls.append(task)

        if self.flag is not None:
            self.flag.mark_run(task)

        if start:
            task.start()

        return task

    def wait_finish(self):
        try:
            for task in self.task_ls:
                self.wait_task(task)
        except KeyboardInterrupt:
            if self.flag:
                self.flag.mark_stop_for_all()
            raise

    def wait_task(self, task: Task):
        if task == current_thread():
            return

        while task.is_alive():
            self.check_stop_flag()
            task.join(timeout=0.1)

    def pause(self, duration: float):
        if duration <= 0:
            return
        from time import sleep
        if duration < 0.1:
            sleep(duration)

        times = int(duration / 0.1)
        for _ in range(times):
            self.check_stop_flag()
            sleep(0.1)

        self.check_stop_flag()
        sleep(duration - 0.1 * times)

    @classmethod
    def build_daemon(cls):
        return MultiTaskLauncher({"daemon": True})

    def __len__(self):
        return len(self.task_ls)

    def __iter__(self):
        return iter(self.task_ls)


def multi_task_launcher(clazz: Union[Type[Thread], Type[Process]],
                        iter_objs: Iterable[TypeVar('OBJ')],
                        apply_each_obj_func: Callable[[TypeVar('OBJ')], Any],
                        wait_finish=True,
                        *,
                        batch_size: Optional[int] = None,
                        pause_duration=-1,
                        flag: Optional[StopThreadFlag] = None,
                        **metadata
                        ) -> MultiTaskLauncher:
    metadata.setdefault("daemon", True)
    launcher = MultiTaskLauncher(metadata)
    if flag:
        launcher.flag = flag

    for index, obj in enumerate(iter_objs):
        args, kwargs = process_single_arg_to_args_and_kwargs(obj)

        launcher.create_task(target=apply_each_obj_func,
                             args=args,
                             kwargs=kwargs,
                             clazz=clazz,
                             )
        if batch_size is not None and (index + 1) % batch_size == 0:
            launcher.pause(pause_duration)
            launcher.wait_finish()

    if wait_finish is True:
        launcher.wait_finish()

    return launcher


def multi_thread_launcher(iter_objs: Iterable,
                          apply_each_obj_func: Callable,
                          wait_finish=True,
                          batch_size: Optional[int] = None,
                          pause_duration=-1,
                          flag=None,
                          **metadata
                          ) -> MultiTaskLauncher:
    return multi_task_launcher(Thread,
                               iter_objs,
                               apply_each_obj_func,
                               wait_finish,
                               batch_size=batch_size,
                               pause_duration=pause_duration,
                               flag=flag,
                               **metadata
                               )


def thread_pool_executor(
        iter_objs: Iterable,
        apply_each_obj_func: Callable,
        wait_finish=True,
        max_workers=None,
        flag=None,
):
    # copy from Python312\Lib\concurrent\futures\thread.py
    # 计算最大线程数
    if max_workers is None:
        import os
        max_workers = min(32, (os.cpu_count() or 1) + 4)
    if max_workers <= 0:
        raise ValueError("max_workers must be greater than 0")

    # 准备工作队列
    from queue import Queue
    q = Queue()
    launcher = MultiTaskLauncher.build_daemon()
    if flag is not None:
        launcher.flag = flag
    STOP = None

    def do_work():
        while True:
            obj = q.get()
            if obj is STOP:
                return
            args, kwargs = obj
            try:
                apply_each_obj_func(*args, **kwargs)
            except BaseException:
                from common import traceback_print_exec
                traceback_print_exec()

    def try_add_worker():
        num_threads = len(launcher)
        if num_threads < max_workers:
            launcher.create_task(do_work, name=f'{apply_each_obj_func.__name__}_thread_pool_executor_{num_threads}')

    # 向队列中添加任务并启动线程
    for obj in iter_objs:
        q.put(process_single_arg_to_args_and_kwargs(obj))
        try_add_worker()

    # 向队列中添加停止信号
    for _ in range(len(launcher)):
        q.put(STOP)

    # 等待工作线程全部完成
    if wait_finish:
        launcher.wait_finish()

    return launcher, q


def multi_call(func, iter_objs, launcher=multi_thread_launcher, wait=True):
    ret_dict = {}

    def get_ret(obj):
        ret = func(obj)
        ret_dict[obj] = ret

    task_ls = launcher(
        iter_objs=iter_objs,
        apply_each_obj_func=get_ret,
        wait_finish=wait
    )

    if wait is not True:
        return ret_dict, task_ls

    return ret_dict


"""
提供阻塞获取一个线程的target函数返回值
"""


class CacheRunner(Thread):

    def __init__(self, target, args=(), kwargs=None, flag=None):
        super().__init__(target=self.wrap_func(target, args, kwargs))
        self.daemon = True

        self._sentinel = object()
        self._cache = self._sentinel
        self._flag = flag

    def wrap_func(self, target: callable, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}

        def wrapper():
            self._flag.mark_run()
            self._cache = target(*args, **kwargs)
            return

        wrapper.__name__ = target.__name__
        return wrapper

    def get(self) -> Any:
        cache = self._cache

        if cache is not self._sentinel:
            return cache

        # noinspection PyUnresolvedReferences
        if not self._started.is_set() and not self.is_alive():
            self.start()

        while self.is_alive():
            if self._flag and self._flag.should_stop():
                self._flag.raise_stop_exception()
            self.join(0.1)

        return self._cache

    def __call__(self, *args, **kwargs):
        return self.get()


def invoke_all(args_func_list: List[Tuple], wait=True, executor=None):
    if executor is None:
        from concurrent.futures import ThreadPoolExecutor
        executor = ThreadPoolExecutor()

    future_ls = []
    for args, func in args_func_list:
        args, kwargs = process_single_arg_to_args_and_kwargs(args)
        future = executor.submit(func, *args, **kwargs)
        future_ls.append(future)

    executor.shutdown(wait)

    if wait:
        return [f.result() for f in future_ls]
    else:
        return future_ls


def cache_run(func):
    runner = CacheRunner(func)
    runner.start()
    return runner
