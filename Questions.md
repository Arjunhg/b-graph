## 1) can you let me know why we need __init__.py in every folder?

## Answer:
    `__init__.py` files are used to tell Python: **“this directory is a package.”**
    That has a few important consequences:

    ---

    ### 1. Makes the folder importable

    Without `__init__.py`, Python (especially older versions or certain tooling) may not treat the folder as a package.

    Example:

    ```python
    from backend.db import loader
    ```

    This works reliably because:

    ```
    backend/
    __init__.py
    db/
        __init__.py
        loader.py
    ```

    ---

    ### 2. Controls package initialization

    When you import a package, the code inside `__init__.py` runs once.

    You can use it to:

    * Set up shared variables
    * Initialize connections
    * Expose a clean API

    Example:

    ```python
    # db/__init__.py
    from .engine import get_engine
    ```

    Now you can do:

    ```python
    from backend.db import get_engine
    ```

    instead of:

    ```python
    from backend.db.engine import get_engine
    ```

    ---

    ### 3. Organizes your public API

    You can decide what your package “exports”:

    ```python
    __all__ = ["get_engine", "loader"]
    ```

    ---

    ### 4. Avoids import ambiguity

    It helps Python clearly distinguish between:

    * a folder of scripts
    * a proper module/package

    ---

    ### 5. Modern note (Python 3.3+)

    Technically, Python now supports **implicit namespace packages**, meaning you *can* omit `__init__.py`.

    However, you still usually include it because:

    * Some tools (linters, IDEs, frameworks) expect it
    * It keeps behavior explicit and predictable
    * You may want initialization logic later

    ---

    ### In your project

    Your structure:

    ```
    backend/
    db/
    graph/
    llm/
    routes/
    ```

    Each folder has `__init__.py` so you can do clean imports like:

    ```python
    from backend.graph.builder import build_graph
    from backend.llm.client import LLMClient
    ```

    ---

    ### TL;DR

    You don’t strictly *need* `__init__.py` anymore, but you **should use it** because it:

    * ensures reliable imports
    * lets you control package behavior
    * keeps your project clean and maintainable

    ---

    If you want, I can show you how to use `__init__.py` to simplify your imports across this project 👍





## 2) what does from __future__ import annotations do and why do we need it, in what scenarios?

## Answer:


## 3) 

## Answer:

## 4)

## Answer:

## 5)

## Answer:

## 6)

## Answer:

## 7)

## Answer:

## 8)

## Answer:

## 9)

## Answer:

## 10)

## Answer:

## 11)

## Answer:

## 12)

## Answer:

## 13)

## Answer:

## 14)

## Answer:

## 15)

## Answer:

## 16)

## Answer:

## 17)

## Answer:

## 18)

## Answer:

## 19)

## Answer: