## General coding style

- Prefer iteration and modularization over code duplication. Implementation must be elegant, intuitive and Pythonic.
- Follow the "let it crash" principle: avoid excessive error handling and edge-case checks, especially for experimental solutions or features. Do not obscure the main intent with defensive boilerplate.
- When asked to review the code, GO BY THE BOOK! Be thoughtful, critical and brutally honest.
- Don't assume. Don't hide confusion. Surface tradeoffs.
- **Important**: 
  1. Fix problems at their root cause, not their symptoms.
  2. If a bug reveals a deeper design flaw or incomplete design, propose fixing the design instead.


## Python dev

- Your implementation must be elegant, intuitive and Pythonic.
- All method parameters **must** be typed, all variables **should** be typed wherever sensible.
- Adopt Python 3.10+ typing styles. Must use native collection types (e.g., list, dict) instead of importing them from the typing module (e.g., from typing import List).
- Use loguru instead of the builtin logging module
- Write all Python tests as `pytest` style functions, not `unittest` classes.
- Manage dependencies with `uv`.
