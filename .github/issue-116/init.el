;;; init.el --- Emacs setup for the issue 116 reproduction  -*- lexical-binding: t -*-
(setq inhibit-startup-screen t)
(require 'package)
(setq package-archives '(("melpa" . "https://melpa.org/packages/")))
(package-initialize)
(add-to-list 'load-path (getenv "CLUTCH_DIR"))
(add-to-list 'load-path (getenv "PGSQL_DIR"))
(require 'clutch)
(setq clutch-connection-alist
      `(("shop" . (:backend pg :host "127.0.0.1"
                   :port ,(string-to-number (getenv "PGPORT_T116"))
                   :user "demo" :database "postgres"))))
(require 'server)
(server-start)

(defun t116-console ()
  "Open the console buffer for the saved connection."
  (clutch-query-console "shop")
  nil)

(defun t116-buffer ()
  (cl-find-if (lambda (b) (string-prefix-p "*clutch: shop*" (buffer-name b)))
              (buffer-list)))

(defun t116-ready ()
  "Return t when the console connection is live."
  (let ((b (t116-buffer)))
    (and b (buffer-local-value 'clutch-connection b)
         (clutch--connection-alive-p (buffer-local-value 'clutch-connection b))
         t)))

(defun t116-prep (sql-index line-numbers)
  "Put statement SQL-INDEX in the console, with LINE-NUMBERS 1 or 0."
  (dolist (b (buffer-list))
    (when (string-prefix-p "*clutch-result" (buffer-name b)) (kill-buffer b)))
  (switch-to-buffer (t116-buffer))
  (delete-other-windows)
  (erase-buffer)
  (insert (nth sql-index '("SELECT 1;" "SELECT pg_sleep(20);")))
  (goto-char (point-min))
  (display-line-numbers-mode (if (= line-numbers 1) 1 -1))
  (select-frame-set-input-focus (selected-frame))
  nil)

(defun t116-state ()
  (if (cl-some (lambda (b) (string-prefix-p "*clutch-result" (buffer-name b)))
               (buffer-list))
      "result"
    (format "running=%d" (hash-table-count clutch--running-queries))))

(defun t116-info ()
  (format "emacs=%s ln=%s" emacs-version
          (and (t116-buffer)
               (buffer-local-value 'display-line-numbers-mode (t116-buffer)))))
