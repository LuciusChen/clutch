# ob-clutch Org-Babel Guide

# Org-Babel Integration

Execute SQL directly in org files via the separate [ob-clutch](https://github.com/LuciusChen/ob-clutch) package. It bridges clutch connections into Org-Babel block types for MySQL, PostgreSQL, SQLite, and the generic `clutch` backend used for JDBC databases.

Configuration:

```emacs-lisp
(require 'ob-clutch)

(with-eval-after-load 'org
  (org-babel-do-load-languages
   'org-babel-load-languages
   '((mysql . t)
     (postgresql . t)
     (sqlite . t)
     (clutch . t))))   ; generic block — needed for JDBC backends
```

Using a saved connection (works for any backend including JDBC):

```
#+begin_src mysql :connection dev
SELECT * FROM users LIMIT 5;
#+end_src
```

```
#+begin_src clutch :connection prod-oracle
SELECT * FROM employees WHERE ROWNUM <= 10;
#+end_src
```

The generic `clutch` block does not need an inline `:backend` when `:connection` is present; the backend is taken from the saved connection entry. An explicit `:backend` is only required for inline connection parameters.

Or with inline connection parameters:

```
#+begin_src mysql :host 127.0.0.1 :port 3306 :user root :password test :database mydb
SHOW TABLES;
#+end_src
```

PostgreSQL and SQLite blocks:

```
#+begin_src postgresql :connection dev-pg
SELECT now();
#+end_src
```

```
#+begin_src sqlite :database /tmp/demo.db
SELECT name FROM sqlite_master WHERE type = 'table';
#+end_src
```

Generic form with explicit backend — required for JDBC databases:

```
#+begin_src clutch :backend pg :host 127.0.0.1 :port 5432 :user postgres :password test :database mydb
SELECT version();
#+end_src
```

```
#+begin_src clutch :backend oracle :host db.corp.com :port 1521 :user scott :password tiger :database ORCL
SELECT table_name FROM user_tables;
#+end_src
```

```
#+begin_src clutch :backend sqlserver :host sqlserver.corp.com :port 1433 :user sa :password secret :database mydb
SELECT TOP 10 * FROM sys.tables;
#+end_src
```

Compatibility entry files live in the [ob-clutch](https://github.com/LuciusChen/ob-clutch) package:

- `ob-mysql.el`, `ob-postgresql.el`, `ob-sqlite.el` — thin language entry points for the named block types.
- `ob-clutch.el` — core execution logic; also registers the generic `clutch` block type.

`C-c C-c` executes the source block; results are inserted as an org table.

## Header Arguments

A block displays up to 1000 data rows by default. Use `:max-rows N` to set a limit for the block or `:max-rows nil` to request all rows.

Use `:async yes` on native MySQL, PostgreSQL or JDBC connections to keep editing while a block runs. With point on the block, `M-x ob-clutch-cancel-block-at-point` cancels its latest run. SQLite blocks run synchronously.

See the [ob-clutch header arguments](https://github.com/LuciusChen/ob-clutch#header-arguments) for connection parameters and backend-specific options.

## Connection Caching

Connections are cached by parameter hash and source TRAMP origin, then reused across source block evaluations in the same Emacs session. All cached connections are cleanly disconnected when Emacs exits (via `kill-emacs-hook`).

Local SQLite filenames are resolved against the command's source directory during connection preparation. Relative `:database app.db` headers in different directories open different databases.
