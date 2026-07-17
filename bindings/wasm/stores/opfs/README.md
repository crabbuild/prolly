# Browser OPFS store

This package persists the store protocol in an Origin Private File System file.
Each logical transaction is committed through one writable-file close, while
an in-process queue preserves CAS and transaction isolation. The directory
handle remains caller-owned.
