from .typing_util import *

_win_forbid_char = [char for char in '\\/:*?"<>|\n\t\r']


def fix_windir_name(dn: str, attr_char='_') -> str:
    """
    把文件夹名称变成win的合法文件夹名
    @param dn: 文件夹名称 dirname
    @param attr_char: 非法字符替换为
    @return: 合法文件夹名
    """
    name = ''.join(map(lambda c: attr_char if c in _win_forbid_char else c, dn))
    if name.endswith('.'):
        name = name.rstrip('.')
    return name


def fix_filepath(filepath: str, *args, **kwargs) -> str:
    """
    unix-style filepath
    """
    filepath = filepath.replace("\\", '/').replace("//", '/')

    if os.path.isdir(filepath):
        return filepath if filepath[-1] == '/' else filepath + '/'
    else:
        return filepath


def fix_suffix(suffix: str) -> str:
    """
    保证suffix以"."开头，如 .png
    """
    return suffix if suffix[0] == '.' else f'.{suffix}'


def mkdir_if_not_exists(dirpath: str):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath, exist_ok=True)


def file_exists(filepath: str) -> bool:
    return os.path.exists(filepath)


def file_not_exists(filepath: str) -> bool:
    return not os.path.exists(filepath)


def path_last_seperator_index(filepath):
    return max(filepath.rfind("/"), filepath.rfind("\\"))


def of_file_name(filepath: str, trim_suffix=False) -> str:
    if trim_suffix is True:
        filepath = change_file_suffix(filepath, '')

    return filepath[path_last_seperator_index(filepath) + 1::]


def of_file_suffix(filepath: str, trim_comma=False) -> str:
    return filepath[filepath.rfind(".") + trim_comma:]


def of_file_mdate(f, fmt: str = "%Y-%m-%d %H:%M:%S"):
    from .time_util import format_ts
    return format_ts(os.path.getmtime(f), fmt)


def of_file_size(f):
    return os.path.getsize(f)


def suffix_equal(fp1, fp2):
    return of_file_suffix(fp1) == of_file_suffix(fp2)


def suffix_not_equal(fp1, fp2):
    return of_file_suffix(fp1) != of_file_suffix(fp2)


def change_file_name(filepath: str, new_name: str) -> str:
    index = path_last_seperator_index(filepath)
    return filepath[:index + 1] + new_name


def rename(src: str,
           new_name: str = None,
           new_dir: str = None,
           new_suffix: str = None,
           ):
    if file_not_exists(src):
        raise AssertionError(f'重命名文件的路径不存在: {src}')

    if new_name is None:
        new_name = of_file_name(src, trim_suffix=True)

    if new_dir is None:
        new_dir = src[:path_last_seperator_index(src) + 1]
    else:
        mkdir_if_not_exists(new_dir)

    if new_suffix is None:
        new_suffix = of_file_suffix(src)

    new_path = fix_filepath(new_dir) + new_name + new_suffix

    os.rename(src, new_path)


def change_file_suffix(filepath: str, new_suffix: str) -> str:
    if new_suffix == '':
        return filepath[:filepath.rfind(".")]

    return filepath[:filepath.rfind(".") + (new_suffix[0] != '.')] + new_suffix


def of_dir_path(filepath, mkdir=False):
    dirpath = os.path.dirname(filepath)
    if mkdir is True:
        mkdir_if_not_exists(dirpath)
    return dirpath


def create_file(filepath: str):
    f = open(filepath, 'w')
    f.close()


def files_of_dir(abs_dir_path: str) -> List[str]:
    abs_dir_path = fix_filepath(os.path.abspath(abs_dir_path))
    return [f'{abs_dir_path}{f_or_d}' for f_or_d in sorted(os.listdir(abs_dir_path))]


def accept_files_of_dir(abs_dir_path: str, acceptor: Callable[[str, str, int], None]):
    abs_dir_path = fix_filepath(abs_dir_path)
    for index, filename in enumerate(os.listdir(abs_dir_path)):
        acceptor(f'{abs_dir_path}{filename}', filename, index)


def backup_dir_to_zip(base_dir: str,
                      target: str,
                      acceptor: Optional[Callable[[str], bool]] = None,
                      ):
    import zipfile
    zfile = zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED)

    with zfile:
        for root, dirs, files in os.walk(base_dir):
            root: str
            # 遍历当前目录下的文件
            for file in files:
                abspath = os.path.join(root, file)
                if acceptor is not None and not acceptor(abspath):
                    continue

                # 将相对路径添加到zip文件中，避免保存绝对路径
                relpath = os.path.relpath(abspath, base_dir)
                zfile.write(abspath, relpath)
    return zfile


def read_text(filepath, encoding='utf-8') -> str:
    with open(filepath, 'r', encoding=encoding) as f:
        return f.read()


def write_text(filepath, content, encoding='utf-8'):
    with open(filepath, 'w', encoding=encoding) as f:
        f.write(content)


# @author: ChatGPT 3.5
class ZipFolder:

    @staticmethod
    def zip_folder(src_folder_path: str, dest_zip_file_path: str):
        """
        Compress all the files and folders under a source directory into a destination zip file.

        :param src_folder_path: The path of the source folder to be compressed.
        :param dest_zip_file_path: The path of the destination zip file to be created.
        """
        # Create a ZipFile object with write permission.
        import zipfile

        with zipfile.ZipFile(dest_zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Walk through all the items under the source folder.
            for root, dirs, files in os.walk(src_folder_path):
                # Add each file and folder to the destination zip file.
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    zipf.write(file_path, arcname=os.path.relpath(file_path, src_folder_path))
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    zipf.write(dir_path, arcname=os.path.relpath(dir_path, src_folder_path))

    @staticmethod
    def unzip_file(src_zip_file_path: str, dest_folder_path: str):
        """
        Extract all the files and folders from a source zip file into a destination folder.

        :param src_zip_file_path: The path of the source zip file to be extracted.
        :param dest_folder_path: The path of the destination folder to hold the extracted files and folders.
        """
        # Create a ZipFile object with read permission.
        import zipfile
        with zipfile.ZipFile(src_zip_file_path, 'r') as zipf:
            # Extract all the contents to the destination folder.
            zipf.extractall(dest_folder_path)


def get_latest_file_name(folder_path, return_path=True):
    """
    返回指定文件夹下最新的文件名
    """
    # 获取该文件夹下所有文件的路径和修改时间
    files = [(os.path.join(folder_path, file), os.path.getmtime(os.path.join(folder_path, file))) for file in
             os.listdir(folder_path)]
    # 按文件修改时间排序
    files.sort(key=lambda x: -x[1])
    # 返回最新的文件名
    return files[0][0] if return_path is True else os.path.basename(files[0][0])
