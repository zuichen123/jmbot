import queue
from typing import Any, Callable

from .registry import ThreadFlagManager, StopThreadFlag


class QueueGenerator(ThreadFlagManager):

    def __init__(self,
                 end_element: object,
                 accept_flag: StopThreadFlag,
                 filter_method: Callable[[Any], bool],
                 ):
        """
        total_element: 所有外界放进来的元素
        q: 被filter_method过滤的元素
        done_element: q中被取出的元素
        """
        super().__init__(accept_flag)
        self.flag.mark_run()

        self.q = queue.Queue()
        self.total_element = set()
        self.done_element = set()

        self.end_element = end_element
        self.is_giveback = False
        self.is_end = False
        self.filter_method = filter_method

    def total(self):
        return len(self.total_element)

    def __len__(self):
        return len(self.done_element) + self.q.qsize()

    def __iter__(self):
        self.flag.mark_run()
        while True:
            e = self.get_and_check_stop()
            if e == self.end_element:
                return

            self.done_element.add(e)
            yield e

            while self.is_giveback:
                self.is_giveback = False
                yield e

    def get_and_check_stop(self):
        while True:
            try:
                if self.is_stop():
                    return self.end_element
                return self.q.get(block=False)
            except queue.Empty:
                if self.is_end:
                    return self.end_element
                self.sleep_or_return(0.5)

    def remaining(self) -> int:
        return self.q.qsize()

    def giveback(self):
        self.is_giveback = True

    def put(self, obj: object):
        if self.end_element == obj:
            self.is_end = True
            return

        can_use = self.filter_method is None or self.filter_method(obj)
        if not can_use:
            return
        if obj in self.total_element:
            return
        self.total_element.add(obj)
        self.q.put(obj)

    def close(self):
        self.put(self.end_element)
