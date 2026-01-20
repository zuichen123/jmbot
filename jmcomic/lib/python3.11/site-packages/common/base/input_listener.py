import threading


class InputListenerThread(threading.Thread):
    """
    InputThread('请输入内容').join_wait_input('遇到ctrl+c或其他异常，强制退出')
    """

    def __init__(self, msg):
        super().__init__()
        self.daemon = True
        self.user_input = None
        self.msg = msg

    def run(self):
        import sys
        try:
            print(self.msg)
            line = sys.stdin.readline()
            if line != '':
                self.user_input = line
        except KeyboardInterrupt:
            return

    def get_input(self, timeout):
        self.join(timeout)  # 等待指定时间
        if self.is_alive():  # 如果线程还在运行，说明没有输入
            return None
        else:
            return self.user_input

    def join_wait_input(self, exit_msg, timeout=1, flag=None):
        self.start()
        sentinel = object()
        self.user_input = sentinel

        while True:
            # 设置1秒超时
            user_input = self.get_input(timeout)

            # 如果收到停止信号
            if flag is not None and flag.should_stop():
                raise KeyboardInterrupt(exit_msg)

            # 如果线程异常退出了
            if user_input is sentinel:
                raise KeyboardInterrupt(exit_msg)

            # 已收到用户输入
            if user_input is not None:
                return user_input

            print(self.msg)
            timeout *= 1.5
