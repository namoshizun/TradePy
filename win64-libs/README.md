**Windows环境安装包**

环境需求: Windows 10, 64 bit

- 安装Graphviz:
    1. 运行本目录下的 Graphviz-install-7.1.0-win10-64bit.exe.
    2. 运行如下命令:
        ```
        python -m pip install --global-option=build_ext `
              --global-option="-IC:\Program Files\Graphviz\include" `
              --global-option="-LC:\Program Files\Graphviz\lib" `
              pygraphviz
        ```
- 安装TA-Lib:
  - 从[此处](https://github.com/cgohlke/talib-build/releases/tag/v0.4.26)下载并安装对应您的Python版本的TA-Lib Wheel文件。
  - 例如：Win10 64位系统，Python 3.10版本，Intel CPU，应下载`TA_Lib-0.4.26-cp310-cp310-win_amd64.whl`。然后运行`pip install TA_Lib-0.4.26-cp310-cp310-win_amd64.whl`。
