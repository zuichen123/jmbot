from .typing_util import *
from .args_util import process_args_kwargs


class Asserter:
    Ret = Union[bool, Any]

    @staticmethod
    def assert_equal(raw: Any,
                     correct: Any,
                     ret_when_false=None,
                     callback_when_true=None,
                     args=None,
                     kwargs=None
                     ) -> Ret:
        is_success = raw == correct
        return_value = True if is_success else \
            (raw if ret_when_false is None else ret_when_false)

        if callback_when_true is not None and \
                return_value is True:
            Asserter.call_func(callback_when_true, args, kwargs)

        return return_value

    @staticmethod
    def any_match(raw,
                  accepted: Iterable,
                  ret_when_false=None,
                  callback_when_true=None,
                  args=None,
                  kwargs=None
                  ) -> Ret:
        is_success = any(each in raw for each in accepted)
        return_value = True if is_success else \
            (False if ret_when_false is None else ret_when_false)

        if callback_when_true is not None and \
                return_value is True:
            Asserter.call_func(callback_when_true, args, kwargs)

        return return_value

    @staticmethod
    def require_not_empty(obj):
        if obj is None or len(obj) == 0:
            raise AssertionError('参数为空')

    @staticmethod
    def call_func(func, args, kwargs):
        args, kwargs = process_args_kwargs(args, kwargs)
        func(*args, **kwargs)


any_match = Asserter.any_match
assert_equal = Asserter.assert_equal
require_not_empty = Asserter.require_not_empty
