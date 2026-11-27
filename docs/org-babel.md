# ob-clutch Org-Babel Guide

Run SQL directly inside Org source blocks using the separate [ob-clutch](https://github.com/LuciusChen/ob-clutch) package. It supports `mysql`, `postgresql`, `sqlite`, and generic `clutch` (including JDBC) blocks.

## Configuration

```emacs-lisp
(require 'ob-clutch)

(with-eval-after-load 'org
  (org-babel-do-load-languages
   'org-babel-load-languages
   '((mysql . t)
     (postgresql . t)
     (sqlite . t)
     (clutch . t))))   ; generic block (required for JDBC)
```

`ob-clutch` provides `ob-mysql.el`, `ob-postgresql.el`, and `ob-sqlite.el` as thin language shims over `ob-clutch.el`. Press `C-c C-c` on a block to run it and insert the result as an Org table.

## Examples

Saved connection (works for any backend; generic `clutch` blocks read `:backend` from the saved entry automatically):

```org
#+begin_src mysql :connection dev
SELECT * FROM users LIMIT 5;
#+end_src

#+begin_src clutch :connection prod-oracle
SELECT * FROM employees WHERE ROWNUM <= 10;
#+end_src
```

Inline parameters:

```org
#+begin_src mysql :host 127.0.0.1 :port 3306 :user root :password test :database mydb
SHOW TABLES;
#+end_src

#+begin_src postgresql :connection dev-pg
SELECT now();
#+end_src

#+begin_src sqlite :database /tmp/demo.db
SELECT name FROM sqlite_master WHERE type = 'table';
#+end_src
```

Generic `clutch` blocks with inline parameters require an explicit `:backend`:

```org
#+begin_src clutch :backend pg :host 127.0.0.1 :port 5432 :user postgres :password test :database mydb
SELECT version();
#+end_src

#+begin_src clutch :backend oracle :host db.corp.com :port 1521 :user scott :password tiger :database ORCL
SELECT table_name FROM user_tables;
#+end_src

#+begin_src clutch :backend sqlserver :host sqlserver.corp.com :port 1433 :user sa :password secret :database mydb
SELECT TOP 10 * FROM sys.tables;
#+end_src
```

## Header Arguments and Async Execution

- `:max-rows` caps inserted table rows (default 1000; pass `:max-rows nil` for all rows).
- `:async yes` runs native MySQL, PostgreSQL, or JDBC blocks without blocking Emacs; run `M-x ob-clutch-cancel-block-at-point` to cancel. SQLite blocks run synchronously.
- See the [ob-clutch header arguments](https://github.com/LuciusChen/ob-clutch#header-arguments) for the full list of connection and backend-specific headers.

## Connection Caching

Connections are cached by parameter hash and TRAMP origin for the Emacs session and closed automatically on exit (`kill-emacs-hook`). Relative SQLite `:database` paths resolve against the Org file's directory, so blocks in different directories with `:database app.db` open separate files.
