## General coding style

- Prefer iteration and modularization over code duplication.
- Follow the "let it crash" principle: avoid excessive error handling and edge case checks, especially when implementing experimental solutions or features. Don't let the main intent of functions and classes be obscured by boilerplate exception handling.
- **Important**: try to fix things at the cause, not the symptom.
- When asked to review the code, GO BY THE BOOK! Be thoughtful, critical and brutally honest.
- Don't assume. Don't hide confusion. Surface tradeoffs.

## Python dev

- Your implementation must be elegant, intuitive and Pythonic.
- All method parameters **must** be typed, all variables **should** be typed wherever sensible.
- Adopt Python 3.10+ typing styles. Must use native collection types (e.g., list, dict) instead of importing them from the typing module (e.g., from typing import List).
- Use loguru instead of the builtin logging module
- Write all Python tests as `pytest` style functions, not `unittest` classes.
- Manage dependencies with `uv`.
