from setuptools import setup, find_packages

def _requires_from_file(filename):
    return open(filename).read().splitlines()

setup(
    name='strdset',  # パッケージ名（pip listで表示される）
    version="0.0.1",  # バージョン
    description="Stream dset",  # 説明
    author='Yoonsoo Kim',  # 作者名
    packages=find_packages(),  # 使うモジュール一覧を指定する
    install_requires=_requires_from_file('requirements.txt'),
    license='MIT'  # ライセンス
)
