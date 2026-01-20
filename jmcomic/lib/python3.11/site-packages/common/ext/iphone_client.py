from common import Iterable, Tuple, Optional, \
    Postman, FixUrlPostman, of_file_name, multi_thread_launcher


class IphoneClient:

    def __init__(self, host: str):
        self.url = f'http://{host}:10000'
        self.postman: FixUrlPostman = self.build_postman()
        self.callback = self.default_call_back

    @staticmethod
    def default_call_back(monitor,
                          cache_field_key='lastp',
                          ):
        from requests_toolbelt import MultipartEncoderMonitor
        monitor: MultipartEncoderMonitor
        if hasattr(monitor, cache_field_key):
            lastp = getattr(monitor, cache_field_key)
        else:
            lastp = 0
        percentage = monitor.bytes_read / monitor.len
        if lastp == int(percentage * 100):
            return
        else:
            setattr(monitor, cache_field_key, int(percentage * 100))

        # 'files[]': (filename, open(filepath, 'rb'), self.decide_content_type(filepath))
        fields: dict = monitor.encoder.fields
        filename = fields['files[]'][0]
        print('{}: {:.1%}'.format(filename, percentage))

    def upload_file(self,
                    filepath: str,
                    upload_to_dir: str = '/',
                    filename=None,
                    ):
        data = self.build_data(
            filepath,
            filename or of_file_name(filepath),
            upload_to_dir
        )
        print(f'上传本地到: [{upload_to_dir}] ← [{filepath}] ({filename})')
        resp = self.postman.post(data=data)

        is_upload_success = self.check_resp(resp)
        if is_upload_success is False:
            raise AssertionError(f'文件上传失败，响应文本:"{resp.text}" ← {filepath}')

    def upload_file_batch(self,
                          file_info_iterable: Iterable[Tuple[str, str, Optional[str]]],
                          ):
        """
        多线程上传文件
        :param file_info_iterable: (filepath: str, upload_to_dir, filename)
        """
        return multi_thread_launcher(
            iter_objs=file_info_iterable,
            apply_each_obj_func=self.upload_file,
            wait_finish=True,
        )

    def build_headers(self, url=None):
        url = url or self.url
        return {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Origin': url,
            'Pragma': 'no-cache',
            'Referer': url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 '
                          'Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
        }

    def build_postman(self):
        return Postman \
            .create(headers=self.build_headers(), verify=False) \
            .with_multi_part() \
            .with_fix_url(f'{self.url}/upload')

    def build_data(self,
                   filepath: str,
                   filename: str,
                   upload_to_dir: str,
                   ):
        payload = {
            'files[]': (filename, open(filepath, 'rb'), self.decide_content_type(filepath)),
            'path': (None, upload_to_dir, None)
        }
        from requests_toolbelt import MultipartEncoderMonitor
        return MultipartEncoderMonitor.from_fields(
            fields=payload,
            callback=self.callback
        )

    @staticmethod
    def decide_content_type(_filepath) -> str:
        return 'text/plain'

    @staticmethod
    def check_resp(resp):
        if resp.status_code != 200:
            return False
        return True
