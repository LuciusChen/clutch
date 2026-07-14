;;; clutch-query.el --- SQL execution and result workflow -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later


;; This file is part of clutch.

;; clutch is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; clutch is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with clutch.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; SQL editing mode, query console, REPL, dispatch menu, SQL literal
;; conversion, pagination, execution, and indirect edit buffers for clutch.
;;
;; This module is required by `clutch.el' — do not require `clutch' here.

;;; Code:

(require 'clutch-backend)
(require 'clutch-connection)
(require 'clutch-diagnostics)
(require 'clutch-schema)
(require 'clutch-sql)
(require 'clutch-ui)
(require 'cl-lib)
(require 'comint)
(require 'sql)
(require 'transient)
(require 'xref)

;;;; Configuration

(defcustom clutch-console-directory
  (expand-file-name "clutch" user-emacs-directory)
  "Directory for persisting query console buffer content."
  :type 'directory
  :group 'clutch)

(defcustom clutch-console-yank-cleanup t
  "When non-nil, clean whitespace in pasted text in query consoles.
After `yank', `yank-pop', or `clipboard-yank' in a query console buffer,
trailing whitespace, mixed indentation, and CRLF line endings are
cleaned up in the pasted region only."
  :type 'boolean
  :group 'clutch)

;; Direct workflow loads must not reverse-load the composition root.
(defvar clutch-result-max-rows 500
  "Direct-load fallback for the shared result row budget defined in clutch.el.")

;; Forward declarations — variables defined in sibling workflow modules
(defvar clutch--query-buffer-local-p)
(defvar clutch--query-mode-line-name)

(defvar-local clutch--executing-p nil
  "Non-nil while a query is executing in this buffer.
Used to update the mode-line with a spinner during execution.")

(defvar-local clutch--last-query nil
  "Last executed SQL query string.")

(defvar-local clutch--last-result-buffer nil
  "Latest result buffer produced from this query source buffer.")

(defvar clutch--source-window nil
  "Window that initiated the current query execution.
Dynamically bound by `clutch--execute' so result buffers open
adjacent to the correct console window.")

(defvar clutch--executing-sql-start nil
  "Buffer position where the currently executing SQL begins, or nil.
Dynamically bound by `clutch--execute-and-mark'.")

(defvar clutch--executing-sql-end nil
  "Buffer position where the currently executing SQL ends, or nil.
Dynamically bound by `clutch--execute-and-mark'.")

(declare-function clutch-result--display-error
                  "clutch-result"
                  (connection sql summary message &optional elapsed hint))
(declare-function clutch-result--display "clutch-result" (result sql elapsed))
(declare-function clutch-result--check-pending-changes "clutch-result" ())
(declare-function clutch-result--display-select
                  "clutch-result"
                  (connection sql result elapsed row-identity-prep
                              server-pageable result-context source-buffer))
(declare-function clutch-result--preview-execution-sql "clutch-result" ())
(declare-function clutch-act-dwim "clutch-object" (&optional entry))
(declare-function clutch-jump "clutch-object" (&optional entry))
(declare-function clutch-describe-dwim "clutch-object" (&optional entry))
(declare-function clutch-copy-context-for-agent "clutch-result" ())

;;;; Query console

(defvar-local clutch--console-name nil
  "Display name if this buffer is a query console, nil otherwise.
Set by `clutch-query-console'; used for buffer display and persistence.")

(defvar-local clutch--console-storage-name nil
  "Stable storage identity for this query console, or nil.
When nil, console persistence falls back to `clutch--console-name'.")

(defvar-local clutch--console-ad-hoc-params nil
  "Connection params for a query console not backed by a saved profile.")

(defun clutch--install-query-keybindings (map)
  "Install common query-console key bindings into MAP."
  (define-key map (kbd "C-c C-c") #'clutch-execute-dwim)
  (define-key map (kbd "C-c C-r") #'clutch-execute-region)
  (define-key map (kbd "C-c C-b") #'clutch-execute-buffer)
  (define-key map (kbd "C-c C-e") #'clutch-connect)
  (define-key map (kbd "C-c C-m") #'clutch-commit)
  (define-key map (kbd "C-c C-u") #'clutch-rollback)
  (define-key map (kbd "C-c C-a") #'clutch-toggle-auto-commit)
  (define-key map (kbd "C-c C-j") #'clutch-jump)
  (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
  (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
  (define-key map (kbd "C-c C-l") #'clutch-switch-schema)
  (define-key map (kbd "C-c C-p") #'clutch-preview-execution-sql)
  (define-key map (kbd "C-c C-s") #'clutch-refresh-schema)
  (define-key map (kbd "C-c ?") #'clutch-dispatch)
  map)

(defun clutch--query-mode-common-setup (&optional mode-line-name)
  "Install common local state for clutch query editing modes.
MODE-LINE-NAME is the base name shown while the buffer is idle."
  (setq-local clutch--query-buffer-local-p t)
  (setq-local clutch--query-mode-line-name (or mode-line-name "clutch"))
  (set-buffer-file-coding-system 'utf-8-unix nil t)
  (add-hook 'kill-emacs-hook #'clutch--save-all-consoles)
  (add-hook 'kill-buffer-hook #'clutch--disconnect-on-kill nil t)
  (add-hook 'kill-buffer-hook #'clutch--save-console nil t)
  (clutch--update-mode-line))

(defun clutch--console-buffer-storage-match-p (storage-name)
  "Return non-nil when the current console buffer matches STORAGE-NAME."
  (and storage-name
       (or (equal clutch--console-storage-name storage-name)
           (and (not clutch--console-storage-name)
                clutch--connection-params
                (equal (clutch--console-persistence-name
                        clutch--console-name
                        clutch--connection-params)
                       storage-name)))))

(defun clutch--find-console-buffer (name &optional storage-name)
  "Return the live console buffer for NAME or STORAGE-NAME, or nil."
  (or (and storage-name
           (cl-find-if
            (lambda (buf)
              (and (buffer-live-p buf)
                   (with-current-buffer buf
                     (and (clutch--query-buffer-p)
                          (clutch--console-buffer-storage-match-p
                           storage-name)))))
            (buffer-list)))
      (cl-find-if
       (lambda (buf)
         (and (buffer-live-p buf)
              (with-current-buffer buf
                (and (clutch--query-buffer-p)
                     (equal clutch--console-name name)
                     (or (not storage-name)
                         (and (not clutch--console-storage-name)
                              (not clutch--connection-params)))))))
       (buffer-list))))

(defun clutch--query-console-major-mode (params)
  "Return the major mode function for query console PARAMS."
  (or (clutch-backend-query-mode
       (clutch--backend-key-from-params params)
       params)
      #'clutch-mode))

(defun clutch--ensure-query-console-major-mode (params)
  "Set the current buffer to the query console mode for PARAMS."
  (let ((mode (clutch--query-console-major-mode params)))
    (unless (eq major-mode mode)
      (funcall mode))))

(defconst clutch--console-url-secret-param-regexp
  (concat "\\([?&;]"
          (regexp-opt '("access_token" "pass" "password" "passwd"
                        "private_key" "private-key" "pwd" "secret" "token"))
          "=\\)[^&;]*")
  "Regexp matching URL parameters that must not affect console identity.")

(defconst clutch--console-identity-param-keys
  '(:user :host :port :database :schema :sid :ssh-host
    :tramp-default-directory)
  "Connection params that distinguish query console identity.")

(defun clutch--console-redacted-url (url)
  "Return URL with obvious password parameters redacted."
  (when url
    (let ((case-fold-search t))
      (replace-regexp-in-string
       clutch--console-url-secret-param-regexp
       "\\1REDACTED"
       url))))

(defun clutch--console-identity-pairs (params)
  "Return canonical non-secret identity pairs for connection PARAMS."
  (when params
    (let ((backend (or (plist-get params :backend)
                       (plist-get params :driver)))
          (url (clutch--console-redacted-url (plist-get params :url))))
      (append
       (and backend (list (cons :backend backend)))
       (cl-loop for key in clutch--console-identity-param-keys
                when (plist-member params key)
                collect (cons key (plist-get params key)))
       (and url (list (cons :url url)))))))

(defun clutch--console-identity-from-params (params)
  "Return a stable query-console persistence identity from PARAMS."
  (when-let* ((pairs (clutch--console-identity-pairs params)))
    (concat "console-"
            (secure-hash 'sha256 (prin1-to-string pairs)))))

(defun clutch--console-persistence-name (name &optional params)
  "Return storage identity for console NAME and PARAMS."
  (or (clutch--console-identity-from-params params)
      name))

(defun clutch--console-file (name)
  "Return the persistence file path for console NAME."
  (expand-file-name
   (concat (replace-regexp-in-string "[/:\\*?\"<>|]" "_" name) ".sql")
   clutch-console-directory))

(defun clutch--save-console ()
  "Save console buffer content to its persistence file."
  (when clutch--console-name
    (condition-case err
        (progn
          (make-directory clutch-console-directory t)
          (let ((coding-system-for-write 'utf-8-unix)
                (storage-name (or clutch--console-storage-name
                                  clutch--console-name)))
            (write-region (point-min) (point-max)
                          (clutch--console-file storage-name)
                          nil 'silent)))
      (error
       (message "Failed to save console %s: %s"
                clutch--console-name
                (error-message-string err))))))

(defun clutch--save-all-consoles ()
  "Save content of all open query console buffers.
Run from `kill-emacs-hook' to persist consoles on Emacs exit."
  (dolist (buf (buffer-list))
    (with-current-buffer buf
      (clutch--save-console))))

(defun clutch--console-window-for (buf)
  "Return the best window to display BUF.
Priority: (1) window already showing BUF; (2) any visible clutch
console window; (3) nil, meaning use the selected window."
  (or (get-buffer-window buf)
      (cl-find-if (lambda (w)
                    (string-prefix-p "*clutch: "
                                     (buffer-name (window-buffer w))))
                  (window-list))))

(defun clutch--sqlite-file-console-target (&optional params)
  "Return an ad hoc SQLite query console target for PARAMS."
  (let ((params (or params (clutch--read-sqlite-file-params))))
    (list :name (clutch--ad-hoc-console-name params)
          :params params
          :ad-hoc t)))

(defun clutch--ad-hoc-console-name (params)
  "Return a display name for an ad hoc console using PARAMS."
  (if (eq (plist-get params :backend) 'sqlite)
      (format "SQLite: %s" (abbreviate-file-name (plist-get params :database)))
    (let* ((backend (or (clutch--backend-display-name-from-params params)
                        (format "%s" (plist-get params :backend))))
           (user (or (plist-get params :user) "?"))
           (host (or (plist-get params :host) "?"))
           (port (plist-get params :port))
           (database (or (plist-get params :database)
                         (plist-get params :sid))))
      (format "%s: %s@%s%s%s"
              backend
              user
              host
              (if port (format ":%s" port) "")
              (if database (format "/%s" database) "")))))

(defun clutch--ad-hoc-console-target (&optional params)
  "Return an ad hoc query console target for PARAMS."
  (let ((params (or params (clutch--read-manual-connection-params t))))
    (list :name (clutch--ad-hoc-console-name params)
          :params params
          :ad-hoc t)))

(defun clutch--read-query-console-choice (names)
  "Read a query console choice from NAMES; no match means new."
  (clutch--read-saved-connection-choice "Console: " names))

(defun clutch--read-query-console-target ()
  "Read a saved connection name or an ad hoc connection target."
  (let* ((names (mapcar #'car clutch-connection-alist))
         (choice (if names
                     (clutch--read-query-console-choice names)
                   "")))
    (cond
     ((string= choice "")
      (clutch--ad-hoc-console-target))
     ((member choice names)
      choice)
     (t
      (clutch--ad-hoc-console-target)))))

(defun clutch--console-yank-cleanup ()
  "Clean whitespace in the just-pasted region of a query console.
Only runs when `clutch-console-yank-cleanup' is non-nil, the current
buffer is a query console, and the last command was a yank variant."
  (when (and clutch-console-yank-cleanup
             clutch--console-name
             (memq this-command '(yank yank-pop clipboard-yank)))
    (let ((beg (region-beginning))
          (end (region-end)))
      (when (< beg end)
        (whitespace-cleanup-region beg end)))))

(defun clutch--open-query-console
    (name params &optional ad-hoc-params source-default-directory)
  "Open or switch to query console NAME using PARAMS.
AD-HOC-PARAMS, when non-nil, are stored for console-local reconnects.
SOURCE-DEFAULT-DIRECTORY is the buffer directory that initiated the command."
  (let* ((params (clutch--prepare-connection-origin-params
                  params source-default-directory))
         (ad-hoc-params (and ad-hoc-params params))
         (product (clutch--effective-sql-product params))
         (storage-name (clutch--console-persistence-name name params))
         (existing (clutch--find-console-buffer name storage-name)))
    (if (and existing
             (buffer-local-value 'clutch-connection existing)
             (clutch--connection-alive-p
              (buffer-local-value 'clutch-connection existing)))
        (progn
          (with-current-buffer existing
            (clutch--ensure-query-console-major-mode params)
            (let ((existing-params (or clutch--connection-params params)))
              (setq-local clutch--console-name name)
              (setq-local clutch--console-storage-name storage-name)
              (setq-local clutch--console-ad-hoc-params
                          (and ad-hoc-params existing-params))
              (clutch--bind-connection-context
               clutch-connection existing-params product))
            (clutch--update-console-buffer-name))
          (select-window
           (or (clutch--console-window-for existing) (selected-window)))
          (switch-to-buffer existing))
      (let* ((conn (if (and existing
                            (buffer-local-value 'clutch-connection existing)
                            (not (clutch--connection-alive-p
                                  (buffer-local-value 'clutch-connection existing))))
                       (with-current-buffer existing
                         (clutch--build-conn params))
                     (clutch--build-conn params)))
             (buf (or existing
                      (generate-new-buffer " *clutch-console*")))
             (is-new (zerop (buffer-size buf))))
        (select-window (or (clutch--console-window-for buf) (selected-window)))
        (switch-to-buffer buf)
        (clutch--ensure-query-console-major-mode params)
        (setq-local clutch--console-name name)
        (setq-local clutch--console-storage-name storage-name)
        (setq-local clutch--console-ad-hoc-params ad-hoc-params)
        (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
        (when is-new
          (let* ((coding-system-for-read 'utf-8)
                 (file (clutch--console-file storage-name))
                 (legacy-file (clutch--console-file name))
                 (read-file (if (or (file-readable-p file)
                                    (equal file legacy-file))
                                file
                              legacy-file)))
            (when (file-readable-p read-file)
              (insert-file-contents read-file))))
        (clutch--activate-current-buffer-connection conn params product)
        (clutch--update-console-buffer-name)))))

;;;###autoload (autoload 'clutch-query-sqlite-file "clutch" nil t)
(defun clutch-query-sqlite-file (file)
  "Open a query console for SQLite database FILE."
  (interactive
   (list (plist-get (clutch--read-sqlite-file-params) :database)))
  (let ((source-default-directory default-directory)
        (params (list :backend 'sqlite
                      :database (clutch--normalize-sqlite-database-file file))))
    (pcase-let ((`(:name ,name :params ,target-params :ad-hoc t)
                 (clutch--sqlite-file-console-target params)))
      (clutch--open-query-console
       name target-params target-params source-default-directory))))

;;;###autoload (autoload 'clutch-query-console "clutch" nil t)
(defun clutch-query-console (target)
  "Open or switch to a query console for TARGET.
Creates a dedicated buffer *clutch: TARGET* with `clutch-mode' enabled
and connects automatically if not already connected.
Repeated calls with the same saved connection or ad hoc target switch to the
existing buffer.
When called from outside a clutch buffer, reuses any visible clutch
window rather than replacing the current window."
  (interactive (list (clutch--read-query-console-target)))
  (let ((source-default-directory default-directory))
    (if (stringp target)
        (let ((params (or (clutch--saved-connection-params target)
                          (user-error "No saved connection named %s" target))))
          (clutch--open-query-console
           target params nil source-default-directory))
      (let ((name (plist-get target :name))
            (params (plist-get target :params)))
        (clutch--open-query-console
         name params params source-default-directory)))))

;;;###autoload (autoload 'clutch-switch-console "clutch" nil t)
(defun clutch-switch-console ()
  "Switch to an open clutch query console using `completing-read'."
  (interactive)
  (let ((consoles (cl-loop for buf in (buffer-list)
                            when (string-prefix-p "*clutch: " (buffer-name buf))
                            collect (buffer-name buf))))
    (if consoles
        (switch-to-buffer (completing-read "Switch to console: " consoles nil t))
      (user-error "No clutch consoles open.  Use M-x clutch-query-console"))))

(defconst clutch--row-identity-hidden-prefix "clutch__rid_"
  "Prefix used for hidden row identity result columns.")

(defun clutch--row-identity-hidden-aliases (count)
  "Return COUNT hidden row identity column aliases."
  (cl-loop for i below count
           collect (format "%s%d" clutch--row-identity-hidden-prefix i)))

(defun clutch--row-identity-key-expressions (conn candidate)
  "Return SELECT expressions for key CANDIDATE on CONN."
  (mapcar (lambda (column)
            (clutch-db-escape-identifier conn column))
          (plist-get candidate :columns)))

(defun clutch--row-identity-select-expressions (conn candidate)
  "Return hidden SELECT expressions for CANDIDATE on CONN."
  (or (plist-get candidate :select-expressions)
      (clutch--row-identity-key-expressions conn candidate)))

(defconst clutch--row-identity-aggregate-functions
  '("ARRAY_AGG" "AVG" "BIT_AND" "BIT_OR" "BIT_XOR" "BOOL_AND" "BOOL_OR"
    "COLLECT" "CORR" "COUNT" "COVAR_POP" "COVAR_SAMP" "EVERY"
    "GROUP_CONCAT" "JSON_AGG" "JSON_OBJECT_AGG" "JSONB_AGG"
    "JSONB_OBJECT_AGG" "LISTAGG" "MAX" "MEDIAN" "MIN"
    "PERCENTILE_CONT" "PERCENTILE_DISC" "REGR_COUNT" "STDDEV"
    "STDDEV_POP" "STDDEV_SAMP" "STRING_AGG" "SUM" "VAR_POP"
    "VAR_SAMP" "VARIANCE" "XMLAGG")
  "Aggregate function names that make row identity injection unsafe.")

(defun clutch--row-identity-ident-char-p (char)
  "Return non-nil when CHAR can be part of a SQL identifier."
  (and char
       (or (and (>= char ?A) (<= char ?Z))
           (and (>= char ?a) (<= char ?z))
           (and (>= char ?0) (<= char ?9))
           (= char ?_)
           (= char ?$))))

(defun clutch--row-identity-skip-space (sql pos)
  "Return position after whitespace in SQL starting at POS."
  (let ((len (length sql)))
    (while (and (< pos len)
                (memq (aref sql pos) '(?\s ?\t ?\r ?\n)))
      (cl-incf pos))
    pos))

(defun clutch--row-identity-looking-at-keyword-p (sql pos keyword)
  "Return non-nil for KEYWORD as a token at POS in SQL."
  (let* ((len (length sql))
         (end (+ pos (length keyword)))
         (case-fold-search t))
    (and (<= end len)
         (string= (upcase (substring sql pos end)) keyword)
         (not (clutch--row-identity-ident-char-p
               (and (< end len) (aref sql end)))))))

(defun clutch--row-identity-window-aggregate-call-p (sql open-pos)
  "Return non-nil when SQL has a window aggregate call at OPEN-POS."
  (when-let* ((close-pos (clutch-db-sql-matching-paren-position sql open-pos)))
    (let ((pos (clutch--row-identity-skip-space sql (1+ close-pos))))
      (when (clutch--row-identity-looking-at-keyword-p sql pos "FILTER")
        (setq pos (clutch--row-identity-skip-space
                   sql (+ pos (length "FILTER"))))
        (when (and (< pos (length sql))
                   (= (aref sql pos) ?\())
          (when-let* ((filter-close
                       (clutch-db-sql-matching-paren-position sql pos)))
            (setq pos (clutch--row-identity-skip-space
                       sql (1+ filter-close))))))
      (clutch--row-identity-looking-at-keyword-p sql pos "OVER"))))

(defun clutch--row-identity-select-list-has-aggregate-p (sql)
  "Return non-nil for a non-window aggregate in SQL's outer SELECT list."
  (let ((case-fold-search t))
    (when (string-match "\\`\\s-*select\\b" sql)
      (let* ((start (match-end 0))
             (end (clutch-db-sql-find-top-level-clause sql "FROM")))
        (when end
          (let ((select-list (substring sql start end))
                (pos 0)
                (len (- end start)))
            (catch 'aggregate
              (while (< pos len)
                (if-let* ((skip (clutch-db-sql-skip-literal-or-comment
                                 select-list pos t)))
                    (setq pos skip)
                  (let ((char (aref select-list pos)))
                    (cond
                     ((= char ?\()
                      (let ((inner (clutch--row-identity-skip-space
                                    select-list (1+ pos))))
                        (if (or (clutch--row-identity-looking-at-keyword-p
                                 select-list inner "SELECT")
                                (clutch--row-identity-looking-at-keyword-p
                                 select-list inner "WITH"))
                            (setq pos (if-let* ((close (clutch-db-sql-matching-paren-position
                                                        select-list pos)))
                                          (1+ close)
                                        len))
                          (cl-incf pos))))
                     ((clutch--row-identity-ident-char-p char)
                      (let ((ident-start pos))
                        (while (and (< pos len)
                                    (clutch--row-identity-ident-char-p
                                     (aref select-list pos)))
                          (cl-incf pos))
                        (let* ((ident (upcase (substring select-list ident-start pos)))
                               (call-pos (clutch--row-identity-skip-space
                                          select-list pos)))
                          (when (and (member ident clutch--row-identity-aggregate-functions)
                                     (< call-pos len)
                                     (= (aref select-list call-pos) ?\()
                                     (not (clutch--row-identity-window-aggregate-call-p
                                           select-list call-pos)))
                            (throw 'aggregate t)))))
                     (t
                      (cl-incf pos))))))
              nil)))))))

(defun clutch--row-identity-augmentable-sql-p (sql table)
  "Return non-nil when SQL for TABLE may receive hidden identity columns."
  (and table
       (clutch-db-sql-source-table sql t)
       (not (clutch--row-identity-select-list-has-aggregate-p sql))
       (not (clutch-db-sql-next-top-level-clause-position
             sql 0 '("DISTINCT" "GROUP" "HAVING")))))

(defun clutch--row-identity-star-qualifier (sql from-pos)
  "Return TABLE.* qualifier for simple SELECT * SQL before FROM-POS."
  (let ((select-list (string-trim (substring sql 0 from-pos))))
    (when (string-match-p "\\`\\s-*SELECT\\s-+\\*\\s-*\\'" select-list)
      (pcase-let* ((`(,body-start ,body-end)
                    (clutch-db-sql-from-body-range sql from-pos))
                   (body (substring sql body-start body-end))
                   (parts (clutch-db-sql-from-body-parts body)))
        (when parts
          (let* ((table (car parts))
                 (alias (cadr parts))
                 (qualifier (or alias
                                (clutch-db-sql-table-qualifier table))))
            (format "%s.*" qualifier)))))))

(defun clutch--row-identity-inject-select-list (conn sql expressions aliases)
  "Return SQL with hidden identity EXPRESSIONS inserted using ALIASES.
CONN supplies identifier escaping for the hidden aliases."
  (let ((sql (string-trim-right
              (replace-regexp-in-string ";\\s-*\\'" "" sql))))
    (if-let* ((from-pos (clutch-db-sql-find-top-level-clause sql "FROM")))
      (let* ((star-qualifier
              (clutch--row-identity-star-qualifier sql from-pos))
             (select-head
              (if star-qualifier
                  (format "SELECT %s" star-qualifier)
                (string-trim-right (substring sql 0 from-pos))))
             (hidden (mapconcat
                      #'identity
                      (cl-mapcar
                       (lambda (expr alias)
                         (format "%s AS %s"
                                 expr
                                 (clutch-db-escape-identifier conn alias)))
                       expressions aliases)
                      ", ")))
        (concat select-head
                ", " hidden " "
                (string-trim-left (substring sql from-pos))))
      sql)))

(defun clutch--prepare-row-identity-query (conn sql &optional candidate table)
  "Return a row identity preparation plist for executing SQL on CONN.
The returned plist contains :sql, :table, :candidate, :hidden-aliases,
:augmented, and :identity-status.  If no identity candidate is available, :sql
is the original SQL.
CANDIDATE and TABLE reuse row identity already established by a result buffer."
  (let* ((analysis-sql (clutch-db-sql-normalize sql))
         (source-token (or (plist-get candidate :source-token)
                           (and (not table)
                                (clutch-db-sql--source-table-token analysis-sql))))
         (table (or table
                    (and source-token
                         (clutch-db--source-table-name conn source-token))))
         (source-schema (and source-token
                             (clutch-db--source-table-schema conn source-token)))
         (source-catalog (and source-token
                              (clutch-db--source-table-catalog conn source-token)))
         candidates
         identity-error)
    (cond
     (candidate
      (setq candidates (list candidate)))
     (table
      (condition-case err
          (setq candidates
                (if (or source-schema source-catalog)
                    (clutch-db-row-identity-candidates
                     conn table source-schema source-catalog)
                  (clutch-db-row-identity-candidates conn table)))
        (clutch-db-error
         (setq identity-error err)))))
    (let* ((candidate (car candidates))
           (expressions (and candidate
                             (clutch--row-identity-select-expressions
                              conn candidate)))
           (aliases (and expressions
                         (clutch--row-identity-hidden-aliases
                          (length expressions))))
           (augment-p (and candidate expressions
                           (clutch--row-identity-augmentable-sql-p
                            analysis-sql table)))
           (identity-status (cond
                             (identity-error 'error)
                             (candidate 'candidate)
                             (table 'unsupported))))
      (list :sql (if augment-p
                     (clutch--row-identity-inject-select-list
                      conn analysis-sql expressions aliases)
                   sql)
            :table table
            :source-token source-token
            :source-schema source-schema
            :source-catalog source-catalog
            :candidate candidate
            :candidates candidates
            :hidden-aliases (and augment-p aliases)
            :augmented (and augment-p t)
            :identity-status identity-status
            :identity-error-message
            (and identity-error
                 (or (and (stringp (cadr identity-error))
                          (cadr identity-error))
                     (error-message-string identity-error)))
            :identity-error identity-error))))

(defun clutch--row-identity-column-indices (columns names)
  "Return column indices in COLUMNS for NAMES, or nil if any is absent."
  (let ((col-names (clutch-db-result-column-names columns))
        indices)
    (catch 'missing
      (dolist (name names (nreverse indices))
        (if-let* ((idx (cl-position name col-names :test #'string=)))
            (push idx indices)
          (throw 'missing nil))))))

(defun clutch--finalize-row-identity (prep columns)
  "Return finalized row identity metadata for PREP and result COLUMNS."
  (when-let* ((candidate (plist-get prep :candidate)))
    (let* ((aliases (plist-get prep :hidden-aliases))
           (indices (if aliases
                        (clutch--row-identity-column-indices columns aliases)
                      (clutch--row-identity-column-indices
                       columns (plist-get candidate :columns))))
           (source-indices
            (and (eq (plist-get candidate :kind) 'primary-key)
                 (clutch--row-identity-column-indices
                  columns (plist-get candidate :columns)))))
      (when indices
        (append
         (list :table (plist-get prep :table)
               :source-token (plist-get prep :source-token)
               :source-schema (plist-get prep :source-schema)
               :source-catalog (plist-get prep :source-catalog)
               :indices indices
               :source-indices source-indices
               :hidden-aliases aliases)
         candidate)))))

(defun clutch--apply-row-identity-column-metadata (columns prep)
  "Return COLUMNS with hidden identity aliases from PREP marked hidden."
  (let ((aliases (plist-get prep :hidden-aliases)))
    (if (not aliases)
        columns
      (mapcar
       (lambda (column)
         (let ((name (plist-get column :name)))
           (if (member name aliases)
               (plist-put (copy-sequence column) :hidden t)
             column)))
       columns))))

;;;; SQL pagination helpers

(defun clutch--sql-has-page-tail-p (sql)
  "Return non-nil if SQL has a top-level LIMIT or OFFSET clause."
  (or (clutch-db-sql-has-top-level-limit-p sql)
      (clutch-db-sql-has-top-level-offset-p sql)))

(defun clutch--server-rewritable-result-p (sql columns)
  "Return non-nil when SQL result COLUMNS are safe for derived-table rewrites.
The check is intentionally conservative: the SQL must be a simple single-table
SELECT without its own LIMIT/OFFSET, and the actual result labels must be
unique.  Arbitrary query results are displayed as result sets instead."
  (let* ((analysis-sql (clutch-db-sql-normalize sql))
         (table (clutch-db-sql-source-table analysis-sql))
         (seen (make-hash-table :test 'equal)))
    (and table
         (not (clutch--sql-has-page-tail-p analysis-sql))
         (clutch--row-identity-augmentable-sql-p analysis-sql table)
         (cl-loop for name in (clutch-db-result-column-names columns)
                  for key = (downcase name)
                  if (gethash key seen)
                  return nil
                  else do (puthash key t seen)
                  finally return t))))

;;;; Query execution engine

(defun clutch--risky-dml-where-condition (sql)
  "Return top-level WHERE condition expression from normalized SQL, or nil."
  (when-let* ((where-pos (clutch-db-sql-find-top-level-clause sql "WHERE")))
    (let* ((cond-start (+ where-pos 5))
           (cond-end (or (clutch-db-sql-next-top-level-clause-position
                          sql cond-start
                          '("RETURNING" "ORDER\\s-+BY" "LIMIT" "OFFSET"
                            "FETCH" "GROUP\\s-+BY" "HAVING" "UNION"
                            "INTERSECT" "EXCEPT" "FOR\\s-+UPDATE"
                            "OPTION"))
                         (length sql))))
      (string-trim (substring sql cond-start cond-end)))))

(defun clutch--risky-dml-trivially-true-expression-p (expr)
  "Return non-nil when EXPR is visibly true without row data."
  (cl-labels
      ((code (expr)
         (string-trim (clutch-db-sql-mask-literal-or-comment expr)))
       (strip-parens (expr)
         (let ((expr (code expr)))
           (while (and (> (length expr) 1)
                       (= (aref expr 0) ?\()
                       (let ((close
                              (clutch-db-sql-matching-paren-position expr 0)))
                         (and close (= close (1- (length expr))))))
             (setq expr (code (substring expr 1 (1- (length expr))))))
           expr))
       (split-keyword (expr keyword)
         (let ((case-fold-search t)
               (pattern (format "\\b%s\\b" keyword))
               (start 0)
               parts)
           (clutch-db-sql-scan-code
            expr 0 nil
            (lambda (pos _ch depth)
              (when (and (zerop depth)
                         (string-match pattern expr pos)
                         (= (match-beginning 0) pos))
                (push (string-trim (substring expr start pos)) parts)
                (setq start (match-end 0)))
              nil))
           (nreverse (cons (string-trim (substring expr start)) parts))))
       (literal-true-p (expr)
         (let ((expr (strip-parens expr))
               (case-fold-search t))
           (or (string-match-p "\\`\\(?:TRUE\\|1\\)\\'" expr)
               (and (string-match
                     "\\`\\([0-9]+\\)\\s-*=\\s-*\\([0-9]+\\)\\'" expr)
                    (string= (match-string 1 expr)
                             (match-string 2 expr))))))
       (true-p (expr)
         (let ((expr (strip-parens expr)))
           (or (literal-true-p expr)
               (let ((parts (split-keyword expr "OR")))
                 (and (cdr parts)
                      (cl-some #'true-p parts)))
               (let ((parts (split-keyword expr "AND")))
                 (and (cdr parts)
                      (cl-every #'true-p parts)))))))
    (true-p expr)))

(defun clutch--risky-dml-reason (sql)
  "Return a confirmation reason for risky DML SQL, or nil."
  (let ((normalized (clutch-db-sql-normalize sql))
        (main-op (clutch-db-sql-main-op-keyword sql)))
    (when (member main-op '("UPDATE" "DELETE"))
      (if-let* ((where (clutch--risky-dml-where-condition normalized)))
          (and (clutch--risky-dml-trivially-true-expression-p where)
               "WHERE is always true")
        "no WHERE"))))

(defun clutch--require-risky-dml-confirmation (sql)
  "Require explicit typed confirmation for risky DML SQL."
  (when-let* ((reason (clutch--risky-dml-reason sql)))
    (let ((token (read-string
                  (format "High-risk DML (%s). Type YES to continue: " reason))))
      (unless (string= token "YES")
        (user-error "Query cancelled")))))

(defun clutch--confirm-query-execution (sql)
  "Prompt for any confirmation required before executing SQL."
  (when (clutch-db-sql-destructive-p sql)
    (unless (yes-or-no-p
             (format "Execute destructive query?\n  %s\n\nProceed? "
                     (truncate-string-to-width (string-trim sql) 80)))
      (user-error "Query cancelled")))
  (clutch--require-risky-dml-confirmation sql))

(defun clutch--note-schema-affecting-query (sql connection)
  "Refresh or invalidate cached schema metadata after SQL on CONNECTION."
  (when (clutch-db-sql-schema-affecting-p sql)
    (if (clutch-db-eager-schema-refresh-p connection)
        (clutch--set-schema-status connection 'stale)
      (clutch--refresh-schema-cache-async connection))))

(defun clutch--execute-nonselect-statement (sql connection)
  "Execute non-SELECT SQL on CONNECTION without opening a result buffer."
  (clutch--confirm-query-execution sql)
  (prog1 (clutch--run-db-query connection sql)
    (clutch--note-schema-affecting-query sql connection)))

(defun clutch--query-debug-summary (result)
  "Return a compact summary string for RESULT."
  (if-let* ((rows (clutch-db-result-rows result)))
      (format "Returned %d row(s)" (length rows))
    (format "Affected %s row(s)"
            (or (clutch-db-result-affected-rows result) 0))))

(defun clutch--execute-source-buffer ()
  "Return the source BUFFER for the current execution."
  (if (window-live-p clutch--source-window)
      (window-buffer clutch--source-window)
    (current-buffer)))

(defun clutch--remember-execute-debug-event (connection phase sql
                                                        &optional buffer summary
                                                        elapsed context)
  "Record an execute debug event.
CONNECTION, PHASE, SQL, BUFFER, SUMMARY, ELAPSED, and CONTEXT describe it."
  (when clutch-debug-mode
    (apply #'clutch--remember-debug-event
           (append (when buffer (list :buffer buffer))
                   (list :connection connection
                         :op "execute"
                         :phase phase
                         :backend (and connection (clutch-db-backend-key connection))
                         :sql sql)
                   (when summary (list :summary summary))
                   (when elapsed (list :elapsed elapsed))
                   (when context (list :context context))))))

(defun clutch--abort-execution-on-db-error (source-buffer connection sql err
                                                        &optional elapsed)
  "Record ERR for SQL on CONNECTION and abort execution from SOURCE-BUFFER.
ELAPSED, when non-nil, is the failed execution duration in seconds."
  (clutch--show-execution-error source-buffer connection sql err elapsed)
  (unless (clutch--connection-alive-p connection)
    (clutch--abandon-query-connection connection))
  (throw 'clutch--execution-aborted nil))

(defun clutch--execute-select (sql connection &optional result-context)
  "Execute a SELECT SQL query with pagination on CONNECTION.
RESULT-CONTEXT, when non-nil, is an internal plist carrying source metadata
for SQL generated from an already verified result.
Returns the query result."
  (let* ((result-context (append result-context
                                 (clutch-db-query-result-context
                                  connection sql)))
         (page-size clutch-result-max-rows)
         (fetch-size (1+ page-size))
         (row-identity-prep
          (or (plist-get result-context :row-identity-prep)
              (clutch--prepare-row-identity-query connection sql)))
         (identity-sql (plist-get row-identity-prep :sql))
         (server-pageable
          (if (plist-member result-context :server-pageable)
              (plist-get result-context :server-pageable)
            (not (clutch--sql-has-page-tail-p identity-sql))))
         (execution-sql (if server-pageable
                            (clutch-db-build-paged-sql
                             connection identity-sql 0 fetch-size nil 0)
                          identity-sql))
         (start (float-time))
         (source-buffer (clutch--execute-source-buffer))
         (_debug-start
          (clutch--remember-execute-debug-event
           connection "start" sql source-buffer nil nil
           (list :execution-sql (clutch--debug-sql-preview execution-sql)
                 :server-pageable server-pageable)))
         (result (condition-case err
                     (clutch--run-db-query connection execution-sql)
                   (clutch-db-error
                    (clutch--abort-execution-on-db-error
                     source-buffer connection sql err
                     (- (float-time) start)))))
         (elapsed (- (float-time) start)))
    (clutch--remember-execute-debug-event
     connection "success" sql source-buffer
     (clutch--query-debug-summary result) elapsed)
    (clutch-result--display-select
     connection sql result elapsed row-identity-prep
     server-pageable result-context source-buffer)
    result))

(defun clutch--execute-dml (sql connection)
  "Execute a DML SQL query on CONNECTION and display results.
Returns the query result."
  (setq clutch--last-query sql)
  (let* ((start (float-time))
         (_debug-start
          (clutch--remember-execute-debug-event connection "start" sql))
         (result (condition-case err
                     (clutch--run-db-query connection sql)
                   (clutch-db-error
                    (clutch--abort-execution-on-db-error
                     (clutch--execute-source-buffer) connection sql err
                     (- (float-time) start)))))
         (elapsed (- (float-time) start)))
    (clutch--remember-execute-debug-event
     connection "success" sql nil (clutch--query-debug-summary result) elapsed)
    (clutch--note-schema-affecting-query sql connection)
    (clutch-result--display result sql elapsed)
    result))

(defun clutch--abandon-query-connection (connection)
  "Drop CONNECTION after an unrecoverable query interruption."
  (unwind-protect
      (when (clutch--connection-alive-p connection)
        (clutch-db-disconnect connection))
    (clutch--cleanup-dead-connection connection)
    (when (eq connection clutch-connection)
      (setq clutch-connection nil))))

(defun clutch--handle-query-quit (connection)
  "Convert a raw quit on CONNECTION into an interrupt or disconnect."
  (let* ((source-buffer (current-buffer))
         (interrupted
          (condition-case err
              (and (clutch--connection-alive-p connection)
                   (clutch-db-interrupt-query connection))
            (clutch-db-error
             (let* ((msg (error-message-string err))
                    (summary (clutch--humanize-db-error msg)))
               (clutch--remember-buffer-query-error-details
                source-buffer connection nil err)
               (when clutch-debug-mode
                 (clutch--remember-debug-event
                  :buffer source-buffer
                  :connection connection
                  :op "cancel"
                  :phase "error"
                  :backend (and connection (clutch-db-backend-key connection))
                  :summary summary))
               (message "Interrupt failed: %s"
                        (clutch--debug-workflow-message summary))
               nil)))))
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :connection connection
       :op "interrupt"
       :phase (if interrupted "success" "disconnect")
       :backend (and connection (clutch-db-backend-key connection))
       :summary (if interrupted
                    "Interrupted running query without disconnecting"
                  "Interrupt recovery failed; connection abandoned")))
    (unless interrupted
      (clutch--abandon-query-connection connection))
    (signal 'clutch-query-interrupted nil)))

(defun clutch--execute (sql &optional conn result-context)
  "Execute SQL on CONN (or current buffer connection).
Times execution and displays results.
For SELECT queries, applies pagination (LIMIT/OFFSET).
RESULT-CONTEXT carries internal result metadata for generated SELECT SQL.
Prompts for confirmation on destructive operations."
  (if (and conn (not (eq conn clutch-connection)))
      (unless (clutch--connection-alive-p conn)
        (user-error
         "Connection closed.  Reconnect from the SQL buffer or REPL"))
    (clutch--ensure-connection))
  (let ((connection (or conn clutch-connection))
        (source-win (selected-window)))
    (clutch--forget-problem-record (current-buffer) connection)
    (clutch-result--check-pending-changes)
    (clutch-db-with-foreground-connection connection
      (clutch--confirm-query-execution sql)
      (setq clutch--executing-p t)
      (clutch--spinner-start)
      (clutch--update-mode-line)
      (redisplay t)
      (unwind-protect
          (condition-case nil
              (catch 'clutch--execution-aborted
                (let ((clutch--source-window source-win))
                  (if (clutch-db-result-query-p connection sql)
                      (clutch--execute-select sql connection result-context)
                    (clutch--execute-dml sql connection))))
            (quit
             (clutch--handle-query-quit connection)))
        (when (window-live-p source-win)
          (select-window source-win))
        (setq clutch--executing-p nil)
        (clutch--update-mode-line)))))


(defun clutch--show-execution-error
    (source-buffer connection sql err &optional elapsed context region)
  "Record ERR for SQL on CONNECTION and render the execution error result.
SOURCE-BUFFER is updated with the failed SQL marker and last result buffer
when live.  ELAPSED records the failed duration.  CONTEXT is merged into
stored diagnostics.  REGION is an optional (BEG . END) source range to mark.
Return a plist with :message, :summary, and :display-summary."
  (let* ((failure (clutch--remember-execute-error
                   source-buffer connection sql err context))
         (message (car failure))
         (summary (cdr failure))
         (display (clutch--humanize-db-error-parts message))
         (display-summary (or (plist-get display :summary) summary))
         (hint (plist-get display :hint))
         (region (or region
                     (and clutch--executing-sql-start
                          clutch--executing-sql-end
                          (cons clutch--executing-sql-start
                                clutch--executing-sql-end)))))
    (when (and (buffer-live-p source-buffer) region)
      (with-current-buffer source-buffer
        (clutch--mark-failed-sql-region
         (car region) (cdr region) display-summary)))
    (let ((buf (clutch-result--display-error
                connection sql display-summary message elapsed hint)))
      (when (and (buffer-live-p source-buffer)
                 (buffer-live-p buf))
        (with-current-buffer source-buffer
          (setq-local clutch--last-result-buffer buf))))
    (list :message message
          :summary summary
          :display-summary display-summary)))

(defun clutch--execute-and-mark (sql beg end &optional conn)
  "Execute SQL on CONN and mark BEG..END on success."
  (pcase-let* ((`(,trim-beg . ,trim-end)
                 (or (clutch--trim-sql-bounds beg end)
                     (cons beg end))))
    (clutch--clear-executed-sql-overlay)
    (redisplay t)
    (when (let ((clutch--executing-sql-start trim-beg)
                (clutch--executing-sql-end trim-end))
            (clutch--execute sql conn))
      (clutch--mark-executed-sql-region beg end))))

;;;; Query-at-point detection

(defun clutch--query-bounds-at-point ()
  "Return the SQL statement bounds around point as (BEG . END)."
  (let* ((text (buffer-substring-no-properties (point-min) (point-max)))
         (offset (- (point) (point-min)))
         (bounds (clutch-db-sql-blank-line-statement-bounds text offset)))
    (cons (+ (point-min) (car bounds))
          (+ (point-min) (cdr bounds)))))

(defun clutch--statement-delimited-buffer-p ()
  "Return non-nil when the current buffer has a top-level semicolon."
  (let ((text (buffer-substring-no-properties (point-min) (point-max))))
    (consp (clutch-db-sql-statement-breaks text))))

(defun clutch--preview-sql-buffer (sql)
  "Display SQL in the *clutch-preview* buffer."
  (let ((buf (get-buffer-create "*clutch-preview*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert sql)
        (goto-char (point-min))
        (if (derived-mode-p 'sql-mode)
            (setq buffer-read-only t)
          (when (fboundp 'sql-mode)
            (sql-mode))
          (setq buffer-read-only t))))
    (pop-to-buffer buf)))

;;;###autoload (autoload 'clutch-preview-execution-sql "clutch" nil t)
(defun clutch-preview-execution-sql ()
  "Preview the execution payload for the current workflow."
  (interactive)
  (let ((sql
         (cond
          ((derived-mode-p 'clutch-result-mode)
           (clutch-result--preview-execution-sql))
          ((use-region-p)
           (string-trim (buffer-substring-no-properties
                         (region-beginning) (region-end))))
          (t
           (pcase-let* ((`(,beg . ,end) (clutch--dwim-bounds-at-point)))
             (string-trim (buffer-substring-no-properties beg end)))))))
    (when (or (null sql) (string-empty-p sql))
      (user-error "No SQL to preview"))
    (clutch--preview-sql-buffer sql)))

;;;; Interactive commands

(defun clutch--statement-bounds-at-point ()
  "Return the SQL statement bounds using only semicolons as delimiters.
Unlike `clutch--query-bounds-at-point', blank lines are ignored so that
long statements spanning multiple paragraphs are captured whole.
Semicolons inside strings, line comments, and block comments are skipped."
  (let* ((text (buffer-substring-no-properties (point-min) (point-max)))
         (offset (- (point) (point-min)))
         (bounds (clutch-db-sql-semicolon-statement-bounds-at-offset
                  text offset t)))
    (cons (+ (point-min) (car bounds))
          (+ (point-min) (cdr bounds)))))

(defun clutch--dwim-bounds-at-point ()
  "Return point-local SQL bounds for DWIM execute and preview workflows.
Prefer semicolon-delimited statement bounds when the current buffer
contains any top-level semicolon, but drop detached leading paragraphs made
only of line comments.  Otherwise fall back to query-at-point bounds, which
also split on blank lines."
  (if (clutch--statement-delimited-buffer-p)
      (let* ((statement-bounds (clutch--statement-bounds-at-point))
             (query-bounds (clutch--query-bounds-at-point))
             (statement-beg (car statement-bounds))
             (query-beg (car query-bounds)))
        (if (and (< statement-beg query-beg)
                 (cl-every
                  (lambda (line)
                    (let ((trimmed (string-trim-left line)))
                      (or (string-empty-p trimmed)
                          (string-prefix-p "--" trimmed))))
                  (split-string
                   (buffer-substring-no-properties statement-beg query-beg)
                   "\n")))
            (cons query-beg (cdr statement-bounds))
          statement-bounds))
    (clutch--query-bounds-at-point)))

(defun clutch--split-statement-specs (sql &optional base-position)
  "Split SQL into `(STATEMENT BEG END)' specs on unquoted semicolons.
BEG and END are buffer positions when BASE-POSITION is non-nil.  Semicolons
inside single-quoted strings, -- line comments, and /* */ block comments are
skipped."
  (let ((stmts nil)
        (start 0)
        (len (length sql)))
    (cl-labels
        ((blank-char-p (ch)
           (memq ch '(?\s ?\t ?\r ?\n)))
         (emit (end)
           (let ((tbeg start)
                 (tend end))
             (while (and (< tbeg tend)
                         (blank-char-p (aref sql tbeg)))
               (cl-incf tbeg))
             (while (and (< tbeg tend)
                         (blank-char-p (aref sql (1- tend))))
               (cl-decf tend))
             (when (< tbeg tend)
               (push (list (substring sql tbeg tend)
                           (and base-position (+ base-position tbeg))
                           (and base-position (+ base-position tend)))
                     stmts)))))
      (dolist (break (clutch-db-sql-statement-breaks sql))
        (emit break)
        (setq start (1+ break)))
      (emit len))
    (nreverse stmts)))

(defun clutch--execute-statements (stmts)
  "Execute STMTS sequentially.
DML/DDL statements run silently; the final SELECT (if any) opens a
result buffer.  Stops and reports on the first error."
  (let* ((specs (mapcar (lambda (stmt)
                          (if (consp stmt)
                              stmt
                            (list stmt nil nil)))
                        stmts))
         (last-spec (car (last specs)))
         (before-last (butlast specs))
         (done 0)
         (source-buffer (current-buffer)))
    (let ((mark-success
           (lambda (spec)
             (pcase-let ((`(,_sql ,beg ,end) spec))
               (when (and beg end)
                 (clutch--mark-executed-sql-region beg end)
                 (redisplay t)))))
          (clear-status
           (lambda (spec)
             (when (nth 1 spec)
               (clutch--clear-executed-sql-overlay)
               (redisplay t))))
          (signal-statement-error
           (lambda (err spec)
             (pcase-let ((`(,stmt ,beg ,end) spec))
               (let* ((failure
                       (clutch--show-execution-error
                        source-buffer clutch-connection stmt err nil
                        (list :statement-index (1+ done))
                        (and beg end (cons beg end))))
                      (summary (plist-get failure :summary)))
                 (user-error "Statement %d failed: %s"
                             (1+ done)
                             (clutch--debug-workflow-message summary)))))))
      (dolist (spec before-last)
        (pcase-let ((`(,stmt ,_beg ,_end) spec))
          (funcall clear-status spec)
          (condition-case err
              (progn
                (clutch--execute-nonselect-statement stmt clutch-connection)
                (cl-incf done)
                (funcall mark-success spec))
            (quit
             (clutch--handle-query-quit clutch-connection))
            (clutch-db-error
             (funcall signal-statement-error err spec)))))
      (pcase-let ((`(,last ,beg ,end) last-spec))
        (cond
         ((clutch-db-result-query-p clutch-connection last)
          (when (> done 0)
            (message "%s statement%s %s"
                     (clutch--message-count done)
                     (if (= done 1) "" "s")
                     (clutch--message-keyword "executed")))
          (if (and beg end)
              (clutch--execute-and-mark last beg end)
            (clutch--execute last)))
         (t
          (funcall clear-status last-spec)
          (condition-case err
              (progn
                (clutch--execute-nonselect-statement last clutch-connection)
                (cl-incf done)
                (funcall mark-success last-spec)
                (message "%s statement%s %s"
                         (clutch--message-count done)
                         (if (= done 1) "" "s")
                         (clutch--message-keyword "executed")))
            (quit
             (clutch--handle-query-quit clutch-connection))
            (clutch-db-error
             (funcall signal-statement-error err last-spec)))))))))

;;;###autoload (autoload 'clutch-execute-dwim "clutch" nil t)
(defun clutch-execute-dwim (beg end)
  "Execute SQL from BEG to END using the most useful local boundary.
With an active region, execute that region.  Otherwise, prefer the
semicolon-delimited statement at point when the current buffer contains
top-level semicolons; fall back to the current query-at-point when it does
not.  When the region contains multiple semicolon-separated statements, they
are executed sequentially."
  (interactive
   (if (use-region-p)
       (list (region-beginning) (region-end))
     (list (point) (point))))
  (clutch--ensure-connection)
  (if (use-region-p)
      (clutch--execute-sql-range beg end "region")
    (pcase-let* ((`(,qb . ,qe) (clutch--dwim-bounds-at-point))
                 (sql (string-trim (buffer-substring-no-properties qb qe))))
      (when (string-empty-p sql)
        (user-error "No SQL at point"))
      (clutch--execute-and-mark sql qb qe))))

(defun clutch--execute-sql-range (beg end scope)
  "Execute trimmed SQL between BEG and END for SCOPE.
Semicolon-delimited multi-statement ranges run sequentially."
  (let* ((raw-sql (buffer-substring-no-properties beg end))
         (sql (string-trim raw-sql))
         (stmts (clutch--split-statement-specs raw-sql beg)))
    (when (or (string-empty-p sql) (null stmts))
      (user-error "No SQL in %s" scope))
    (if (cdr stmts)
        (clutch--execute-statements stmts)
      (clutch--execute-and-mark sql beg end))))

;;;###autoload (autoload 'clutch-execute-region "clutch" nil t)
(defun clutch-execute-region (beg end)
  "Execute SQL in the region from BEG to END.
Semicolon-delimited multi-statement regions run sequentially."
  (interactive "r")
  (clutch--ensure-connection)
  (clutch--execute-sql-range beg end "region"))

;;;###autoload (autoload 'clutch-execute-buffer "clutch" nil t)
(defun clutch-execute-buffer ()
  "Execute SQL in the current buffer.
Semicolon-delimited multi-statement buffers run sequentially."
  (interactive)
  (clutch--ensure-connection)
  (clutch--execute-sql-range (point-min) (point-max) "buffer"))

(defun clutch--find-connection ()
  "Find a live database connection from any `clutch-mode' buffer.
Returns the connection or nil."
  (cl-loop for buf in (buffer-list)
           for conn = (buffer-local-value 'clutch-connection buf)
           when (clutch--connection-alive-p conn)
           return conn))

;;;###autoload (autoload 'clutch-execute "clutch" nil t)
(defun clutch-execute (sql)
  "Execute SQL from any buffer.
With an active region, execute the region.  Otherwise execute the
current line.  Uses the connection from any `clutch-mode' buffer."
  (interactive
   (list (string-trim
          (if (use-region-p)
              (buffer-substring-no-properties (region-beginning) (region-end))
            (buffer-substring-no-properties
             (line-beginning-position) (line-end-position))))))
  (when (string-empty-p sql)
    (user-error "No SQL to execute"))
  (let* ((conn (or clutch-connection
                   (clutch--find-connection)
                   (user-error "No active connection.  Use M-x clutch-mode then C-c C-e to connect")))
         (beg (if (use-region-p) (region-beginning) (line-beginning-position)))
         (end (if (use-region-p) (region-end) (line-end-position))))
    (clutch--execute-and-mark sql beg end conn)))

;;;; Indirect edit buffer

(defun clutch--string-at-point ()
  "Return the string literal content at point, or nil.
Uses `syntax-ppss' to detect string boundaries, so it works in
any mode that has a proper syntax table (Java, Kotlin, Python,
Go, Ruby, etc.)."
  (let ((ppss (syntax-ppss)))
    (when (nth 3 ppss)
      (let ((str-start (nth 8 ppss)))
        (save-excursion
          (goto-char str-start)
          (forward-sexp 1)
          (buffer-substring-no-properties (1+ str-start) (1- (point))))))))

(defvar clutch--indirect-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c '") #'clutch-indirect-execute)
    (define-key map (kbd "C-c C-k") #'clutch-indirect-abort)
    map)
  "Keymap for `clutch--indirect-mode'.")

(define-minor-mode clutch--indirect-mode
  "Minor mode active in indirect SQL edit buffers.
\\<clutch--indirect-mode-map>
Key bindings:
  \\[clutch-indirect-execute]	Execute and close
  \\[clutch-indirect-abort]	Abort and close"
  :lighter " Indirect")

;;;###autoload (autoload 'clutch-indirect-execute "clutch" nil t)
(defun clutch-indirect-execute ()
  "Execute the SQL in the indirect buffer, then close it."
  (interactive)
  (let ((sql (string-trim
              (buffer-substring-no-properties (point-min) (point-max))))
        (conn (or clutch-connection
                  (clutch--find-connection))))
    (when (string-empty-p sql)
      (user-error "No SQL to execute"))
    (unless conn
      (user-error "No active connection"))
    (quit-window 'kill)
    ;; `quit-window' kills the indirect buffer, leaving the Lisp execution
    ;; context in a dead buffer.  Any subsequent `with-current-buffer' call
    ;; would fail when `save-current-buffer' tries to restore that dead buffer.
    ;; Explicitly switch to the live buffer now selected after the kill.
    (with-current-buffer (window-buffer (selected-window))
      (clutch--execute sql conn))))

;;;###autoload (autoload 'clutch-indirect-abort "clutch" nil t)
(defun clutch-indirect-abort ()
  "Abort the indirect edit buffer."
  (interactive)
  (quit-window 'kill))

(defun clutch--extract-indirect-sql-text ()
  "Return SQL text to populate an indirect edit buffer.
Uses region if active, string literal at point if inside one,
or the current line otherwise."
  (string-trim
   (cond
    ((use-region-p)
     (buffer-substring-no-properties (region-beginning) (region-end)))
    ((clutch--string-at-point))
    (t
     (buffer-substring-no-properties
      (line-beginning-position) (line-end-position))))))

;;;###autoload (autoload 'clutch-edit-indirect "clutch" nil t)
(defun clutch-edit-indirect ()
  "Open an indirect `clutch-mode' buffer with SQL extracted from context.
With an active region, use the region.  When point is inside a
string literal (DAO code, etc.), extract the string content.
Otherwise use the current line.

The indirect buffer inherits the connection from any live
`clutch-mode' buffer.  Edit the SQL freely, then press
\\<clutch--indirect-mode-map>\\[clutch-indirect-execute] \
to execute or \\[clutch-indirect-abort] to abort."
  (interactive)
  (let* ((text (clutch--extract-indirect-sql-text))
         (conn (or (bound-and-true-p clutch-connection)
                   (clutch--find-connection)))
         (params clutch--connection-params)
         (product clutch--conn-sql-product)
         (buf  (generate-new-buffer "*clutch: indirect*")))
    (pop-to-buffer buf)
    (clutch-mode)
    (when conn
      (clutch--bind-connection-context conn params product)
      (clutch--update-mode-line))
    (clutch--indirect-mode 1)
    (insert text)
    (goto-char (point-min))
    (message "Edit SQL, then C-c ' to execute, C-c C-k to abort")))

;;;; REPL mode

(defvar clutch-repl-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "RET") #'clutch-repl-send-input)
    (define-key map (kbd "<return>") #'clutch-repl-send-input)
    (define-key map (kbd "C-c C-e") #'clutch-connect)
    (define-key map (kbd "C-c C-m") #'clutch-commit)
    (define-key map (kbd "C-c C-u") #'clutch-rollback)
    (define-key map (kbd "C-c C-a") #'clutch-toggle-auto-commit)
    (define-key map (kbd "C-c C-j") #'clutch-jump)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "C-c C-l") #'clutch-switch-schema)
    map)
  "Keymap for `clutch-repl-mode'.")

(defvar-local clutch-repl--pending-input ""
  "Accumulated partial SQL input waiting for a semicolon.")

;;;###autoload (autoload 'clutch-repl-mode "clutch" nil t)
(define-derived-mode clutch-repl-mode comint-mode "clutch-repl"
  "Major mode for database REPL.

\\<clutch-repl-mode-map>
  \\[clutch-connect]\tConnect to server
  \\[clutch-jump]\tObject jump
  \\[clutch-describe-dwim]\tDescribe object
  \\[clutch-act-dwim]\tObject actions
  \\[clutch-switch-schema]\tSwitch schema/database"
  (setq comint-prompt-regexp "^db> \\|^    -> ")
  (setq comint-input-sender #'clutch-repl--input-sender)
  (clutch--install-completion-capfs)
  (add-hook 'xref-backend-functions #'clutch--xref-backend nil t)
  (add-hook 'kill-buffer-hook #'clutch--disconnect-on-kill nil t))

(defun clutch-repl--prompt (&optional continuation)
  "Return the REPL prompt string.
When CONTINUATION is non-nil, return the continuation prompt."
  (propertize (if continuation "    -> " "db> ")
              'face 'minibuffer-prompt))

(defun clutch-repl--input-start-position ()
  "Return the best input-start position for the current REPL line."
  (save-excursion
    (beginning-of-line)
    (if (looking-at comint-prompt-regexp)
        (match-end 0)
      (point))))

(defun clutch-repl--ensure-process ()
  "Ensure the current REPL buffer has a live dummy comint process."
  (let ((proc (get-buffer-process (current-buffer))))
    (if (and proc (process-live-p proc))
        proc
      (let ((mark-pos (and proc
                           (markerp (process-mark proc))
                           (marker-position (process-mark proc)))))
        (when proc
          (delete-process proc))
        (setq proc (start-process "clutch-repl" (current-buffer) "cat"))
        (set-process-query-on-exit-flag proc nil)
        (set-marker (process-mark proc)
                    (or mark-pos (clutch-repl--input-start-position))
                    (current-buffer))
        proc))))

;;;###autoload (autoload 'clutch-repl-send-input "clutch" nil t)
(defun clutch-repl-send-input ()
  "Send REPL input, recreating the dummy comint process when needed."
  (interactive)
  (clutch-repl--ensure-process)
  (comint-send-input))

(defun clutch-repl--input-sender (_proc input)
  "Process INPUT from comint.
Accumulates input until a semicolon is found, then executes."
  (let ((combined (concat clutch-repl--pending-input
                          (unless (string-empty-p clutch-repl--pending-input) "\n")
                          input)))
    (if (string-match-p ";\\s-*$" combined)
        (progn
          (setq clutch-repl--pending-input "")
          (clutch-repl--execute-and-print (string-trim combined)))
      (setq clutch-repl--pending-input combined)
      (clutch-repl--output (clutch-repl--prompt t)))))

(defun clutch-repl--font-lock-output (text)
  "Return TEXT with display faces preserved in a font-locked REPL buffer."
  (let ((copy (copy-sequence text))
        (pos 0)
        next face)
    (while (< pos (length copy))
      (setq next (or (next-single-property-change pos 'face copy)
                     (length copy))
            face (get-text-property pos 'face copy))
      (when face
        (add-text-properties pos next `(font-lock-face ,face) copy))
      (setq pos next))
    copy))

(defun clutch-repl--output (text)
  "Insert TEXT into the REPL buffer at the process mark."
  (let ((inhibit-read-only t)
        (proc (clutch-repl--ensure-process)))
    (goto-char (process-mark proc))
    (insert (clutch-repl--font-lock-output text))
    (set-marker (process-mark proc) (point))))

(defun clutch-repl--format-dml-result (result elapsed)
  "Format a DML RESULT with ELAPSED time as a string for the REPL."
  (let ((msg (concat "\n"
                     (clutch--message-keyword "Affected rows")
                     ": "
                     (clutch--message-count
                      (or (clutch-db-result-affected-rows result) 0)))))
    (when-let* ((id (clutch-db-result-last-insert-id result))
                ((> id 0)))
      (setq msg (concat msg
                        ", "
                        (clutch--message-keyword "Last insert ID")
                        ": "
                        (clutch--message-count id))))
    (when-let* ((w (clutch-db-result-warnings result))
                ((> w 0)))
      (setq msg (concat msg
                        ", "
                        (clutch--message-keyword "Warnings")
                        ": "
                        (clutch--message-count w))))
    (concat msg
            " ("
            (clutch--message-literal (format "%.3fs" elapsed))
            ")\n\n"
            (clutch-repl--prompt))))

(defun clutch-repl--format-error (message)
  "Format MESSAGE as a REPL error."
  (concat "\n"
          (propertize "ERROR" 'face 'error)
          ": "
          (propertize message 'face 'clutch-error-summary-face)
          "\n\n"
          (clutch-repl--prompt)))

(defun clutch-repl--execute-and-print (sql)
  "Execute SQL from the REPL and display the result."
  (let ((repl-buffer (current-buffer))
        (repl-window (selected-window)))
    (cl-labels ((output (text)
                  (when (buffer-live-p repl-buffer)
                    (with-current-buffer repl-buffer
                      (clutch-repl--output text)))))
      (clutch--forget-problem-record repl-buffer clutch-connection)
      (condition-case err
          (progn
            (clutch--ensure-connection)
            (setq clutch--last-query sql)
            (let* ((start (float-time))
                   (result (clutch--run-db-query clutch-connection sql))
                   (elapsed (- (float-time) start))
                   (columns (clutch-db-result-columns result))
                   (rows (clutch-db-result-rows result)))
              (if columns
                  (progn
                    (unless (clutch-db-result-connection result)
                      (setf (clutch-db-result-connection result)
                            clutch-connection))
                    (unwind-protect
                        (clutch-result--display result sql elapsed)
                      (when (window-live-p repl-window)
                        (select-window repl-window)))
                    (output
                     (let ((row-count (length rows)))
                       (concat "\n"
                               (clutch--message-count row-count)
                               " "
                               (if (= row-count 1) "row" "rows")
                               " shown in "
                               (clutch--message-ident "result buffer")
                               " in "
                               (clutch--message-literal
                                (format "%.3fs" elapsed))
                               "\n\n"
                               (clutch-repl--prompt)))))
                (output (clutch-repl--format-dml-result result elapsed)))))
        (quit
         (condition-case nil
             (clutch--handle-query-quit clutch-connection)
           (clutch-query-interrupted
            (output (clutch-repl--format-error "Query interrupted")))))
        (error
         (clutch--remember-buffer-query-error-details
          repl-buffer clutch-connection sql err)
         (output
          (clutch-repl--format-error
           (clutch--humanize-db-error (error-message-string err)))))))))

;;;###autoload (autoload 'clutch-repl "clutch" nil t)
(defun clutch-repl ()
  "Start a database REPL buffer."
  (interactive)
  (let* ((buf-name "*clutch REPL*")
         (buf (get-buffer-create buf-name)))
    (unless (comint-check-proc buf)
      (with-current-buffer buf
        (unless (derived-mode-p 'clutch-repl-mode)
          (clutch-repl-mode))
        (clutch-repl--ensure-process)
        (clutch-repl--output (clutch-repl--prompt))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

;;;; Query editing major mode

(defvar clutch-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (clutch--install-query-keybindings map)
    (define-key map (kbd "C-c TAB") #'clutch-complete-at-point)
    (define-key map (kbd "C-c <tab>") #'clutch-complete-at-point)
    (define-key map (kbd "TAB") #'clutch-complete-qualified-or-indent)
    (define-key map (kbd "<tab>") #'clutch-complete-qualified-or-indent)
    map)
  "Keymap for `clutch-mode'.")

;;;###autoload (autoload 'clutch-mode "clutch" nil t)
(define-derived-mode clutch-mode sql-mode "clutch"
  "Major mode for editing and executing SQL queries.

\\<clutch-mode-map>
Key bindings:
  \\[clutch-execute-dwim]	Execute region or statement/query at point
  \\[clutch-execute-region]	Execute region
  \\[clutch-execute-buffer]	Execute buffer
  \\[clutch-connect]	Connect to server
  \\[clutch-jump]	Object jump
  \\[clutch-describe-dwim]	Describe object
  \\[clutch-act-dwim]	Object actions
  \\[clutch-switch-schema]	Switch schema/database
  \\[clutch-complete-at-point]	Complete SQL identifier at point
  \\[clutch-complete-qualified-or-indent]	Complete qualified column or indent
  \\[clutch-preview-execution-sql]	Preview execution"
  (clutch--query-mode-common-setup)
  (clutch--install-completion-capfs)
  (add-hook 'eldoc-documentation-functions
            #'clutch--eldoc-function nil t)
  (add-hook 'xref-backend-functions #'clutch--xref-backend nil t))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mysql\\'" . clutch-mode))

;;;; Transient dispatch menu

(defun clutch--dispatch-transaction-controls-inapt-p ()
  "Return non-nil when current connection has no transaction controls."
  (not (and clutch-connection
            (clutch--manual-commit-supported-p clutch-connection))))

(transient-define-suffix clutch--dispatch-commit ()
  "Transient suffix for `clutch-commit'."
  :inapt-if #'clutch--dispatch-transaction-controls-inapt-p
  (interactive)
  (call-interactively #'clutch-commit))

(transient-define-suffix clutch--dispatch-rollback ()
  "Transient suffix for `clutch-rollback'."
  :inapt-if #'clutch--dispatch-transaction-controls-inapt-p
  (interactive)
  (call-interactively #'clutch-rollback))

(defun clutch--dispatch-auto-commit-description ()
  "Return the transient description for the current auto-commit state."
  (if (clutch--dispatch-transaction-controls-inapt-p)
      (concat "Auto-commit "
              (propertize "(unavailable)" 'face 'transient-inactive-value))
    (concat "Auto-commit "
            (clutch--transient-state-display
             (if (clutch-db-manual-commit-p clutch-connection) 'manual 'auto)
             '((manual . "manual") (auto . "auto"))))))

(transient-define-suffix clutch--dispatch-toggle-auto-commit ()
  "Transient suffix for `clutch-toggle-auto-commit' with a dynamic label."
  :description #'clutch--dispatch-auto-commit-description
  :inapt-if (lambda ()
              (or (clutch--dispatch-transaction-controls-inapt-p)
                  (and clutch-connection
                       (clutch-db-manual-commit-p clutch-connection)
                       (clutch--tx-dirty-p clutch-connection))))
  (interactive)
  (call-interactively #'clutch-toggle-auto-commit))

;;;###autoload (autoload 'clutch-dispatch "clutch" nil t)
(transient-define-prefix clutch-dispatch ()
  "Main dispatch menu for clutch."
  [:pad-keys t
   ["Connection"
    ("c" "Connect" clutch-connect)
    ("q" "Query console" clutch-query-console)
    ("f" "SQLite file" clutch-query-sqlite-file)
    ("S" "Prepare SSH" clutch-prepare-ssh-host)
    ("d" "Disconnect" clutch-disconnect)
    ("m" "Commit" clutch--dispatch-commit)
    ("u" "Rollback" clutch--dispatch-rollback)
    ("a" clutch--dispatch-toggle-auto-commit)
    ("R" "REPL" clutch-repl)]
   ["Execute"
    ("x" "DWIM" clutch-execute-dwim)
    ("r" "Region" clutch-execute-region)
    ("b" "Buffer" clutch-execute-buffer)
    ("p" "Preview execution" clutch-preview-execution-sql)
    ("k" "Copy agent context" clutch-copy-context-for-agent)]
   ["Edit"
    ("'" "Indirect edit" clutch-edit-indirect)]
   ["Objects"
    ("j" "Jump to object" clutch-jump)
    ("D" "Describe object" clutch-describe-dwim)
    ("o" "Object actions" clutch-act-dwim)
    ("l" "Switch schema/db" clutch-switch-schema)
    ("s" "Refresh schema" clutch-refresh-schema)]])

(provide 'clutch-query)
;;; clutch-query.el ends here
