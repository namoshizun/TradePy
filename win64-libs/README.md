**Windows Installation**

Requirement: Windows 10, 64 bit

- Graphviz:
    1. Run the exe installer.
    2. Run command:
        ```
        python -m pip install --global-option=build_ext `
              --global-option="-IC:\Program Files\Graphviz\include" `
              --global-option="-LC:\Program Files\Graphviz\lib" `
              pygraphviz
        ```
- TA-Lib:
  - `pip install TA_Lib-0.4.24-cp310-cp310-win_amd64.whl`
  - Or download the wheel from here: https://github.com/cgohlke/talib-build/releases
