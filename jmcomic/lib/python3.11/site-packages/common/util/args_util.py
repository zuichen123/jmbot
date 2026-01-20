from .typing_util import *


def process_single_arg_to_args_and_kwargs(obj) -> Tuple[tuple, dict]:
    """
    obj: 'a' -> args: ('a', )
    obj: (1, 2) -> args: (1, 2)
    obj: {'1': '2'} -> kwargs -> {'1': '2'}
    obj: [1, {'1': '2'}] -> args: (1, )  kwargs: {'1': '2'}
    """
    if isinstance(obj, list):
        return process_args_kwargs(*obj)

    if isinstance(obj, dict):
        return (), obj

    return arg_to_tuple(obj), {}


def process_args_kwargs(args: Optional[Any],
                        kwargs: Optional[Dict]) -> Tuple[Tuple, Dict]:
    return arg_to_tuple(args), kwargs or {}


def arg_to_tuple(args) -> tuple:
    if args is None:
        args = ()
    elif not isinstance(args, (tuple, list)):
        args = (args,)
    return args
