from setuptools import setup, find_packages

def _requires_from_file(filename):
    return open(filename).read().splitlines()

setup(
    name='myapp',  # パッケージ名（pip listで表示される）
    version="0.0.1",  # バージョン
    description="sample of minimum package",  # 説明
    author='haneya',  # 作者名
    packages=find_packages(),  # 使うモジュール一覧を指定する
    install_requires=_requires_from_file('requirements.txt'),
    license='MIT'  # ライセンス
)
