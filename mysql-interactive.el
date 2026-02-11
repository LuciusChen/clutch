;;; mysql-interactive.el --- Interactive MySQL client -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

;; This file is part of mysql.el.

;; mysql.el is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; mysql.el is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with mysql.el.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Interactive SQL client built on mysql.el.
;;
;; Provides:
;; - `mysql-mode': SQL editing major mode (derived from `sql-mode')
;; - `mysql-repl': REPL via `comint-mode'
;; - Query execution with column-paged result tables
;; - Schema browsing and completion
;;
;; Entry points:
;;   M-x mysql-mode      — open a SQL editing buffer
;;   M-x mysql-repl      — open a REPL
;;   Open a .mysql file   — activates mysql-mode automatically

;;; Code:

(require 'mysql)
(require 'sql)
(require 'comint)
(require 'org-table)
(require 'cl-lib)
(require 'ring)

(declare-function mysql-dispatch "mysql-transient")
(declare-function mysql-result-dispatch "mysql-transient")

;;;; Customization

(defgroup mysql-interactive nil
  "Interactive MySQL client."
  :group 'mysql
  :prefix "mysql-interactive-")

(defface mysql-header-face
  '((t :inherit bold))
  "Face for column headers in result tables."
  :group 'mysql-interactive)

(defface mysql-header-active-face
  '((t :inherit (highlight bold)))
  "Face for the column header under the cursor."
  :group 'mysql-interactive)

(defface mysql-border-face
  '((t :inherit shadow))
  "Face for table borders (pipes and separators)."
  :group 'mysql-interactive)

(defface mysql-null-face
  '((t :inherit shadow :slant italic))
  "Face for NULL values."
  :group 'mysql-interactive)

(defface mysql-modified-face
  '((t :inherit warning))
  "Face for modified cell values."
  :group 'mysql-interactive)

(defface mysql-fk-face
  '((t :inherit font-lock-type-face :underline t))
  "Face for foreign key column values.
Underlined to indicate clickable (RET to follow)."
  :group 'mysql-interactive)

(defcustom mysql-connection-alist nil
  "Alist of saved MySQL connections.
Each entry has the form:
  (NAME . (:host H :port P :user U :password P :database D))
NAME is a string used for `completing-read'."
  :type '(alist :key-type string
                :value-type (plist :options
                                   ((:host string)
                                    (:port integer)
                                    (:user string)
                                    (:password string)
                                    (:database string))))
  :group 'mysql-interactive)

(defcustom mysql-interactive-history-file
  (expand-file-name "mysql-history" user-emacs-directory)
  "File for persisting SQL query history."
  :type 'file
  :group 'mysql-interactive)

(defcustom mysql-interactive-history-length 500
  "Maximum number of history entries to keep."
  :type 'natnum
  :group 'mysql-interactive)

(defcustom mysql-interactive-result-max-rows 1000
  "Maximum number of rows to display in result tables."
  :type 'natnum
  :group 'mysql-interactive)

(defcustom mysql-interactive-column-width-max 30
  "Maximum display width for a single column in the result table."
  :type 'natnum
  :group 'mysql-interactive)

(defcustom mysql-interactive-column-width-step 5
  "Step size for widening/narrowing columns with +/-."
  :type 'natnum
  :group 'mysql-interactive)

(defcustom mysql-interactive-column-padding 1
  "Number of padding spaces on each side of a cell."
  :type 'natnum
  :group 'mysql-interactive)

;;;; Buffer-local variables

(defvar-local mysql-interactive-connection nil
  "Current `mysql-conn' for this buffer.")

(defvar-local mysql-interactive--last-query nil
  "Last executed SQL query string.")

(defvar-local mysql-interactive--result-columns nil
  "Column names from the last result, as a list of strings.")

(defvar-local mysql-interactive--result-rows nil
  "Row data from the last result, as a list of lists.")

(defvar-local mysql-interactive--column-widths nil
  "Vector of integers — display width for each column.")

(defvar-local mysql-interactive--column-pages nil
  "Vector of vectors — each page contains non-pinned column indices.")

(defvar-local mysql-interactive--current-col-page 0
  "Current column page index.")

(defvar-local mysql-interactive--pinned-columns nil
  "List of column indices that are pinned (visible on all pages).")

(defvar-local mysql-interactive--last-window-width nil
  "Last known window body width, to avoid redundant refreshes.")

(defvar-local mysql-interactive--header-active-col nil
  "Col-idx currently highlighted in the header, or nil.")

(defvar-local mysql-interactive--header-overlay nil
  "Overlay used to highlight the active column header.")

(defvar-local mysql-interactive--display-offset 0
  "Number of rows currently displayed (for load-more paging).")

(defvar-local mysql-interactive--result-column-defs nil
  "Full column definition plists from the last result.")

(defvar-local mysql-interactive--pending-edits nil
  "Alist of pending edits: ((ROW-IDX . COL-IDX) . NEW-VALUE).")

(defvar-local mysql-interactive--fk-info nil
  "Foreign key info for the current result.
Alist of (COL-IDX . (:ref-table TABLE :ref-column COLUMN)).")

(defvar-local mysql-interactive--where-filter nil
  "Current WHERE filter string, or nil if no filter active.")

(defvar-local mysql-interactive--base-query nil
  "The original unfiltered SQL query, used by WHERE filtering.")

(defvar-local mysql-record--result-buffer nil
  "Reference to the parent result buffer (Record buffer local).")

(defvar-local mysql-record--row-idx nil
  "Current row index being displayed (Record buffer local).")

(defvar-local mysql-record--expanded-fields nil
  "List of column indices with expanded long fields (Record buffer local).")

;;;; History

(defvar mysql-interactive--history (make-ring 500)
  "Ring buffer of executed SQL queries.")

(defvar mysql-interactive--history-loaded nil
  "Non-nil if history has been loaded from disk.")

(defun mysql-interactive--load-history ()
  "Load history from `mysql-interactive-history-file'."
  (unless mysql-interactive--history-loaded
    (setq mysql-interactive--history (make-ring mysql-interactive-history-length))
    (when (file-readable-p mysql-interactive-history-file)
      (let ((entries (split-string
                      (with-temp-buffer
                        (insert-file-contents mysql-interactive-history-file)
                        (buffer-string))
                      "\0" t)))
        (dolist (entry (nreverse entries))
          (ring-insert mysql-interactive--history entry))))
    (setq mysql-interactive--history-loaded t)))

(defun mysql-interactive--save-history ()
  "Save history to `mysql-interactive-history-file'."
  (let ((entries nil)
        (len (ring-length mysql-interactive--history)))
    (dotimes (i (min len mysql-interactive-history-length))
      (push (ring-ref mysql-interactive--history i) entries))
    (with-temp-file mysql-interactive-history-file
      (insert (mapconcat #'identity entries "\0")))))

(defun mysql-interactive--add-history (sql)
  "Add SQL to history ring, avoiding duplicates at head."
  (mysql-interactive--load-history)
  (let ((trimmed (string-trim sql)))
    (unless (string-empty-p trimmed)
      (when (or (ring-empty-p mysql-interactive--history)
                (not (string= trimmed (ring-ref mysql-interactive--history 0))))
        (ring-insert mysql-interactive--history trimmed))
      (mysql-interactive--save-history))))

(defun mysql-interactive-show-history ()
  "Select a query from history and insert it at point."
  (interactive)
  (mysql-interactive--load-history)
  (when (ring-empty-p mysql-interactive--history)
    (user-error "No history entries"))
  (let* ((entries (ring-elements mysql-interactive--history))
         (choice (completing-read "SQL history: " entries nil t)))
    (insert choice)))

;;;; Connection management

(defun mysql-interactive--connection-key (conn)
  "Return a descriptive string for CONN like \"user@host:port/db\"."
  (format "%s@%s:%s/%s"
          (or (mysql-conn-user conn) "?")
          (or (mysql-conn-host conn) "?")
          (or (mysql-conn-port conn) 3306)
          (or (mysql-conn-database conn) "")))

(defun mysql-interactive--connection-alive-p (conn)
  "Return non-nil if CONN is live."
  (and conn
       (mysql-conn-p conn)
       (process-live-p (mysql-conn-process conn))))

(defun mysql-interactive--ensure-connection ()
  "Ensure current buffer has a live connection.  Signal error if not."
  (unless (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (user-error "Not connected.  Use C-c C-e to connect")))

(defun mysql-interactive--update-mode-line ()
  "Update mode-line lighter with connection status."
  (setq mode-name
        (if (mysql-interactive--connection-alive-p mysql-interactive-connection)
            (format "MySQL[%s]"
                    (mysql-interactive--connection-key
                     mysql-interactive-connection))
          "MySQL[disconnected]"))
  (force-mode-line-update))

(defun mysql-connect-interactive ()
  "Connect to a MySQL server interactively.
If `mysql-connection-alist' is non-empty, offer saved connections via
`completing-read'.  Otherwise prompt for each parameter."
  (interactive)
  (when (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (mysql-disconnect mysql-interactive-connection)
    (setq mysql-interactive-connection nil))
  (let* ((conn-params
          (if mysql-connection-alist
              (cdr (assoc (completing-read "Connection: "
                                          (mapcar #'car mysql-connection-alist)
                                          nil t)
                          mysql-connection-alist))
            (list :host (read-string "Host (127.0.0.1): " nil nil "127.0.0.1")
                  :port (read-number "Port (3306): " 3306)
                  :user (read-string "User: ")
                  :password (read-passwd "Password: ")
                  :database (let ((db (read-string "Database (optional): ")))
                              (unless (string-empty-p db) db)))))
         (conn (condition-case err
                   (apply #'mysql-connect conn-params)
                 (mysql-error
                  (user-error "Connection failed: %s"
                              (error-message-string err))))))
    (mysql-query conn "SET NAMES utf8mb4")
    (setq mysql-interactive-connection conn)
    (mysql-interactive--update-mode-line)
    (mysql-interactive--refresh-schema-cache conn)
    (message "Connected to %s" (mysql-interactive--connection-key conn))))

(defun mysql-interactive-disconnect ()
  "Disconnect from the current MySQL server."
  (interactive)
  (when (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (mysql-disconnect mysql-interactive-connection)
    (message "Disconnected"))
  (setq mysql-interactive-connection nil)
  (mysql-interactive--update-mode-line))

;;;; Value formatting

(defun mysql-interactive--format-value (val)
  "Format VAL for display in a result table.
nil → \"NULL\", plists → formatted date/time strings."
  (cond
   ((null val) "NULL")
   ((stringp val) val)
   ((numberp val) (number-to-string val))
   ;; datetime plist: has :year and :hours
   ((and (listp val) (plist-get val :year) (plist-get val :hours))
    (format "%04d-%02d-%02d %02d:%02d:%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   ;; date plist: has :year but no :hours
   ((and (listp val) (plist-get val :year))
    (format "%04d-%02d-%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)))
   ;; time plist: has :hours but no :year
   ((and (listp val) (plist-get val :hours))
    (format "%s%02d:%02d:%02d"
            (if (plist-get val :negative) "-" "")
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   (t (format "%S" val))))

(defun mysql-interactive--truncate-cell (str max-width)
  "Truncate STR to MAX-WIDTH, replacing embedded pipes to protect org tables."
  (let ((clean (replace-regexp-in-string "|" "¦" (replace-regexp-in-string "\n" "↵" str))))
    (if (> (length clean) max-width)
        (concat (substring clean 0 (- max-width 1)) "…")
      clean)))

(defun mysql-interactive--column-names (columns)
  "Extract column names from COLUMNS as a list of strings.
Handles the case where the driver returns non-string names
\(e.g., SELECT 1 produces an integer column name)."
  (mapcar (lambda (c)
            (let ((name (plist-get c :name)))
              (if (stringp name) name (format "%s" name))))
          columns))

(defun mysql-interactive--value-to-literal (val)
  "Convert Elisp VAL to a SQL literal string.
nil → \"NULL\", numbers unquoted, strings escaped."
  (cond
   ((null val) "NULL")
   ((numberp val) (number-to-string val))
   ((stringp val) (mysql-escape-literal val))
   (t (mysql-escape-literal (mysql-interactive--format-value val)))))

;;;; Column width computation and paging

(defun mysql-interactive--long-field-type-p (col-def)
  "Return non-nil if COL-DEF is a long field type (JSON/BLOB/TEXT)."
  (memq (plist-get col-def :type)
        (list mysql--type-json
              mysql--type-blob
              mysql--type-tiny-blob
              mysql--type-medium-blob
              mysql--type-long-blob)))

(defun mysql-interactive--long-field-placeholder (col-def)
  "Return a placeholder string for a long field type COL-DEF."
  (pcase (plist-get col-def :type)
    (245 "<JSON>")
    (_ "<BLOB>")))

(defun mysql-interactive--compute-column-widths (col-names rows column-defs)
  "Compute display width for each column.
COL-NAMES is a list of header strings, ROWS is the data,
COLUMN-DEFS is the column metadata list.
Returns a vector of integers."
  (let* ((ncols (length col-names))
         (max-w mysql-interactive-column-width-max)
         (widths (make-vector ncols 0))
         (sample (seq-take rows 50)))
    (dotimes (i ncols)
      (if (mysql-interactive--long-field-type-p (nth i column-defs))
          (aset widths i 10)
        (let ((header-w (string-width (nth i col-names)))
              (data-w 0))
          (dolist (row sample)
            (let ((formatted (mysql-interactive--format-value (nth i row))))
              (setq data-w (max data-w (string-width formatted)))))
          (aset widths i (max 5 (min max-w (max header-w data-w)))))))
    widths))

(defun mysql-interactive--compute-column-pages (widths pinned window-width)
  "Compute column pages based on WIDTHS, PINNED columns, and WINDOW-WIDTH.
Each column occupies width + 2*padding + 1 (pipe separator).
PINNED columns are always shown and their width is deducted first.
Returns a vector of vectors, each containing non-pinned column indices."
  (let* ((padding mysql-interactive-column-padding)
         (ncols (length widths))
         (pinned-total (+ 1 ;; leading pipe
                          (cl-reduce #'+ (mapcar (lambda (i)
                                                   (+ (aref widths i) (* 2 padding) 1))
                                                 pinned)
                                     :initial-value 0)))
         (available (max 10 (- window-width pinned-total)))
         (pages nil)
         (current-page nil)
         (used 0))
    (dotimes (i ncols)
      (unless (memq i pinned)
        (let ((col-w (+ (aref widths i) (* 2 padding) 1)))
          (when (and current-page (> (+ used col-w) available))
            (push (vconcat (nreverse current-page)) pages)
            (setq current-page nil
                  used 0))
          (push i current-page)
          (cl-incf used col-w))))
    (when current-page
      (push (vconcat (nreverse current-page)) pages))
    (if pages
        (vconcat (nreverse pages))
      (vector (vector)))))

(defun mysql-interactive--visible-columns ()
  "Return list of column indices visible on the current page.
Pinned columns come first, followed by the current page's columns."
  (let ((page-cols (when (and mysql-interactive--column-pages
                              (< mysql-interactive--current-col-page
                                 (length mysql-interactive--column-pages)))
                     (append (aref mysql-interactive--column-pages
                                   mysql-interactive--current-col-page)
                             nil))))
    (append mysql-interactive--pinned-columns page-cols)))

;;;; Result display

(defun mysql-interactive--result-buffer-name ()
  "Return the result buffer name based on current connection."
  (if (mysql-interactive--connection-alive-p mysql-interactive-connection)
      (format "*mysql: %s*"
              (or (mysql-conn-database mysql-interactive-connection) "results"))
    "*mysql: results*"))

(defun mysql-interactive--insert-simple-table (col-names rows &optional row-offset)
  "Insert an org-table with COL-NAMES and ROWS.
ROW-OFFSET is the starting row index for text properties (default 0).
Each cell carries text properties `mysql-row-idx', `mysql-col-idx',
and `mysql-full-value' for edit support.
This function is used by schema browser and REPL only."
  (let ((max-w mysql-interactive-column-width-max)
        (ridx (or row-offset 0))
        (table-start (point)))
    ;; header
    (insert "| "
            (mapconcat #'identity col-names " | ")
            " |\n")
    (insert "|-\n")
    ;; rows
    (dolist (row rows)
      (insert "| ")
      (cl-loop for val in row
               for cidx from 0
               for formatted = (mysql-interactive--format-value val)
               for display = (mysql-interactive--truncate-cell formatted max-w)
               do (insert (propertize
                           display
                           'mysql-row-idx ridx
                           'mysql-col-idx cidx
                           'mysql-full-value val))
               do (insert " | "))
      ;; fix trailing: replace last " | " with " |"
      (delete-char -2)
      (insert "|\n")
      (cl-incf ridx))
    (org-table-align)
    ;; Extract aligned header into header-line-format
    (goto-char table-start)
    (let ((header-text (buffer-substring (line-beginning-position)
                                         (line-end-position))))
      ;; Delete header row and separator from buffer
      (delete-region (line-beginning-position)
                     (progn (forward-line 2) (point)))
      (setq header-line-format
            (propertize (concat " " header-text) 'face 'mysql-header-face)))))

;;;; Column-paged renderer

(defun mysql-interactive--render-separator (visible-cols widths)
  "Render a separator line for VISIBLE-COLS with WIDTHS."
  (let ((padding mysql-interactive-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (push (concat "+" (make-string (+ (aref widths cidx) (* 2 padding)) ?-))
            parts))
    (concat (mapconcat #'identity (nreverse parts) "") "+")))

(defun mysql-interactive--render-header (visible-cols widths)
  "Render the header row string for VISIBLE-COLS with WIDTHS.
Each column name carries a `mysql-header-col' text property
so the active-column overlay can find it."
  (let ((padding mysql-interactive-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (let* ((name (nth cidx mysql-interactive--result-columns))
             (w (aref widths cidx))
             (padded (string-pad
                      (if (> (string-width name) w)
                          (concat (truncate-string-to-width name (1- w)) "…")
                        name)
                      w))
             (pad-str (make-string padding ?\s)))
        (push (concat (propertize "|" 'face 'mysql-border-face)
                      pad-str
                      (propertize padded
                                  'face 'mysql-header-face
                                  'mysql-header-col cidx)
                      pad-str)
              parts)))
    (concat (mapconcat #'identity (nreverse parts) "")
            (propertize "|" 'face 'mysql-border-face))))

(defun mysql-interactive--render-row (row ridx visible-cols widths)
  "Render a single data ROW at row index RIDX.
VISIBLE-COLS is a list of column indices, WIDTHS is the width vector.
Returns a propertized string."
  (let ((padding mysql-interactive-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (let* ((val (nth cidx row))
             (col-def (nth cidx mysql-interactive--result-column-defs))
             (edited (assoc (cons ridx cidx) mysql-interactive--pending-edits))
             (display-val (if edited (cdr edited) val))
             (w (aref widths cidx))
             (formatted (if (and (not edited)
                                 (mysql-interactive--long-field-type-p col-def))
                            (mysql-interactive--long-field-placeholder col-def)
                          (replace-regexp-in-string
                           "\n" "↵"
                           (mysql-interactive--format-value display-val))))
             (truncated (if (> (string-width formatted) w)
                            (concat (truncate-string-to-width formatted (1- w)) "…")
                          formatted))
             (padded (string-pad truncated w))
             (face (cond (edited 'mysql-modified-face)
                         ((null val) 'mysql-null-face)
                         ((assq cidx mysql-interactive--fk-info) 'mysql-fk-face)
                         (t nil)))
             (cell (propertize padded
                               'mysql-row-idx ridx
                               'mysql-col-idx cidx
                               'mysql-full-value (if edited (cdr edited) val)
                               'face face))
             (pad-str (make-string padding ?\s)))
        (push (concat (propertize "|" 'face 'mysql-border-face)
                      pad-str cell pad-str)
              parts)))
    (concat (mapconcat #'identity (nreverse parts) "")
            (propertize "|" 'face 'mysql-border-face))))

(defun mysql-interactive--render-footer (total offset num-pages cur-page)
  "Return the footer string for TOTAL rows, OFFSET shown, page CUR-PAGE/NUM-PAGES."
  (let ((parts (list (propertize
                      (format "-- %d row%s" total (if (= total 1) "" "s"))
                      'face 'font-lock-comment-face))))
    (when (< offset total)
      (push (propertize (format "  (showing %d, n: load more)" offset)
                        'face 'font-lock-comment-face)
            parts))
    (when (> num-pages 1)
      (push (propertize (format "  Col page %d/%d  []/[] prev/next" cur-page num-pages)
                        'face 'font-lock-comment-face)
            parts))
    (when mysql-interactive--where-filter
      (push (propertize (format "  [W: %s]" mysql-interactive--where-filter)
                        'face 'font-lock-warning-face)
            parts))
    (apply #'concat (nreverse parts))))

(defun mysql-interactive--render-result ()
  "Render the result buffer content using column paging."
  (let* ((inhibit-read-only t)
         (visible-cols (mysql-interactive--visible-columns))
         (widths mysql-interactive--column-widths)
         (page (seq-take mysql-interactive--result-rows
                         mysql-interactive--display-offset))
         (total (length mysql-interactive--result-rows))
         (sep (propertize (mysql-interactive--render-separator visible-cols widths)
                          'face 'mysql-border-face)))
    (erase-buffer)
    (setq header-line-format nil)
    (when mysql-interactive--pending-edits
      (insert (propertize
               (format "-- %d pending edit%s\n"
                       (length mysql-interactive--pending-edits)
                       (if (= (length mysql-interactive--pending-edits) 1) "" "s"))
               'face 'mysql-modified-face)))
    (insert sep "\n")
    (insert (mysql-interactive--render-header visible-cols widths) "\n")
    (insert sep "\n")
    (let ((ridx 0))
      (dolist (row page)
        (insert (mysql-interactive--render-row row ridx visible-cols widths) "\n")
        (cl-incf ridx)))
    (insert sep "\n")
    (insert (mysql-interactive--render-footer
             total mysql-interactive--display-offset
             (length mysql-interactive--column-pages)
             (1+ mysql-interactive--current-col-page))
            "\n")
    (goto-char (point-min))))

(defun mysql-interactive--goto-cell (ridx cidx)
  "Move point to the cell at ROW-IDX RIDX and COL-IDX CIDX.
Falls back to the same row (any column), then point-min."
  (goto-char (point-min))
  (let ((found nil))
    (while (and (not found)
                (setq found (text-property-search-forward
                             'mysql-row-idx ridx #'eq)))
      (let ((beg (prop-match-beginning found)))
        (if (eq (get-text-property beg 'mysql-col-idx) cidx)
            (goto-char beg)
          (setq found nil))))
    (unless found
      ;; Fall back: find the same row, any column
      (goto-char (point-min))
      (if-let* ((m (text-property-search-forward 'mysql-row-idx ridx #'eq)))
          (goto-char (prop-match-beginning m))
        (goto-char (point-min))))))

(defun mysql-interactive--refresh-display ()
  "Recompute column pages for current window width and re-render.
Preserves cursor position (row + column) across the refresh."
  (when mysql-interactive--column-widths
    (let* ((save-ridx (get-text-property (point) 'mysql-row-idx))
           (save-cidx (get-text-property (point) 'mysql-col-idx))
           (win (get-buffer-window (current-buffer)))
           (width (if win (window-body-width win) 80)))
      (setq mysql-interactive--column-pages
            (mysql-interactive--compute-column-pages
             mysql-interactive--column-widths
             mysql-interactive--pinned-columns
             width))
      (let ((max-page (1- (length mysql-interactive--column-pages))))
        (setq mysql-interactive--current-col-page
              (max 0 (min mysql-interactive--current-col-page max-page))))
      (setq mysql-interactive--last-window-width width)
      (setq mysql-interactive--header-active-col nil)
      (when mysql-interactive--header-overlay
        (delete-overlay mysql-interactive--header-overlay)
        (setq mysql-interactive--header-overlay nil))
      (mysql-interactive--render-result)
      (when save-ridx
        (mysql-interactive--goto-cell save-ridx save-cidx)))))

(defun mysql-interactive--window-size-change (frame)
  "Handle window size changes for result buffers in FRAME."
  (dolist (win (window-list frame 'no-mini))
    (let ((buf (window-buffer win)))
      (when (buffer-local-value 'mysql-interactive--column-widths buf)
        (let ((new-width (window-body-width win)))
          (unless (eq new-width
                      (buffer-local-value 'mysql-interactive--last-window-width buf))
            (with-current-buffer buf
              (mysql-interactive--refresh-display))))))))

(defun mysql-interactive--display-select-result (col-names rows columns)
  "Render a SELECT result with COL-NAMES, ROWS, and COLUMNS metadata."
  (let* ((inhibit-read-only t)
         (page-size mysql-interactive-result-max-rows)
         (total (length rows)))
    (setq-local mysql-interactive--column-widths
                (mysql-interactive--compute-column-widths
                 col-names rows columns))
    (setq-local mysql-interactive--display-offset
                (min page-size total))
    (mysql-interactive--refresh-display)
    (add-hook 'window-size-change-functions
              #'mysql-interactive--window-size-change)))

(defun mysql-interactive--display-dml-result (result sql elapsed)
  "Render a DML RESULT (INSERT/UPDATE/DELETE) with SQL and ELAPSED time."
  (let ((inhibit-read-only t))
    (erase-buffer)
    (setq-local mysql-interactive--column-widths nil)
    (insert (propertize (format "-- %s\n" (string-trim sql))
                        'face 'font-lock-comment-face))
    (insert (format "Affected rows: %s\n"
                    (or (mysql-result-affected-rows result) 0)))
    (when-let* ((id (mysql-result-last-insert-id result))
                ((> id 0)))
      (insert (format "Last insert ID: %s\n" id)))
    (when-let* ((w (mysql-result-warnings result))
                ((> w 0)))
      (insert (format "Warnings: %s\n" w)))
    (insert (propertize (format "\nCompleted in %.3fs\n" elapsed)
                        'face 'font-lock-comment-face))
    (goto-char (point-min))))

(defun mysql-interactive--display-result (result sql elapsed)
  "Display RESULT in the result buffer.
SQL is the query text, ELAPSED the time in seconds."
  (let* ((buf-name (mysql-interactive--result-buffer-name))
         (buf (get-buffer-create buf-name))
         (columns (mysql-result-columns result))
         (rows (mysql-result-rows result))
         (col-names (mysql-interactive--column-names columns)))
    (with-current-buffer buf
      (mysql-result-mode)
      (setq-local mysql-interactive--last-query sql)
      (setq-local mysql-interactive-connection
                  (mysql-result-connection result))
      (setq-local mysql-interactive--result-columns col-names)
      (setq-local mysql-interactive--result-column-defs columns)
      (setq-local mysql-interactive--result-rows rows)
      (setq-local mysql-interactive--display-offset 0)
      (setq-local mysql-interactive--pending-edits nil)
      (setq-local mysql-interactive--current-col-page 0)
      (setq-local mysql-interactive--pinned-columns nil)
      (if col-names
          (mysql-interactive--display-select-result col-names rows columns)
        (mysql-interactive--display-dml-result result sql elapsed))
      (mysql-interactive--load-fk-info))
    (display-buffer buf '(display-buffer-at-bottom))))

;;;; Query execution engine

(defun mysql-interactive--execute (sql &optional conn)
  "Execute SQL on CONN (or current buffer connection).
Records history, times execution, and displays results."
  (let ((connection (or conn mysql-interactive-connection)))
    (unless (mysql-interactive--connection-alive-p connection)
      (user-error "Not connected.  Use C-c C-e to connect"))
    (setq mysql-interactive--last-query sql)
    (mysql-interactive--add-history sql)
    (let* ((start (float-time))
           (result (condition-case err
                       (mysql-query connection sql)
                     (mysql-error
                      (user-error "Query error: %s" (error-message-string err)))))
           (elapsed (- (float-time) start)))
      (mysql-interactive--display-result result sql elapsed)
      result)))

;;;; Query-at-point detection

(defun mysql-interactive--query-at-point ()
  "Return the SQL query around point.
Queries are delimited by semicolons or blank lines."
  (let* ((delimiter "\\(;\\|^[[:space:]]*$\\)")
         (beg (save-excursion
                (if (re-search-backward delimiter nil t)
                    (match-end 0)
                  (point-min))))
         (end (save-excursion
                (if (re-search-forward delimiter nil t)
                    (match-beginning 0)
                  (point-max))))
         (query (string-trim (buffer-substring-no-properties beg end))))
    (when (string-empty-p query)
      (user-error "No query at point"))
    query))

;;;; Interactive commands

(defun mysql-execute-query-at-point ()
  "Execute the SQL query at point."
  (interactive)
  (mysql-interactive--ensure-connection)
  (mysql-interactive--execute (mysql-interactive--query-at-point)))

(defun mysql-execute-region (beg end)
  "Execute SQL in the region from BEG to END."
  (interactive "r")
  (mysql-interactive--ensure-connection)
  (mysql-interactive--execute
   (string-trim (buffer-substring-no-properties beg end))))

(defun mysql-execute-buffer ()
  "Execute the entire buffer as a SQL query."
  (interactive)
  (mysql-interactive--ensure-connection)
  (mysql-interactive--execute
   (string-trim (buffer-substring-no-properties (point-min) (point-max)))))

(defun mysql-interactive--find-connection ()
  "Find a live MySQL connection from any mysql-mode buffer.
Returns the connection or nil."
  (cl-loop for buf in (buffer-list)
           for conn = (buffer-local-value 'mysql-interactive-connection buf)
           when (mysql-interactive--connection-alive-p conn)
           return conn))

;;;###autoload
(defun mysql-execute (sql)
  "Execute SQL from any buffer.
With an active region, execute the region.  Otherwise execute the
current line.  Uses the connection from any mysql-mode buffer."
  (interactive
   (list (string-trim
          (if (use-region-p)
              (buffer-substring-no-properties (region-beginning) (region-end))
            (buffer-substring-no-properties
             (line-beginning-position) (line-end-position))))))
  (when (string-empty-p sql)
    (user-error "No SQL to execute"))
  (let ((conn (or mysql-interactive-connection
                  (mysql-interactive--find-connection)
                  (user-error "No active MySQL connection.  Use M-x mysql-mode then C-c C-e to connect"))))
    (mysql-interactive--execute sql conn)))

;;;; Schema cache + completion

(defvar mysql-interactive--schema-cache (make-hash-table :test 'equal)
  "Global schema cache.  Keys are connection-key strings.
Values are hash-tables mapping table-name → list of column-name strings.")

(defun mysql-interactive--refresh-schema-cache (conn)
  "Refresh schema cache for CONN.
Only loads table names (fast).  Column info is loaded lazily."
  (condition-case nil
      (let* ((key (mysql-interactive--connection-key conn))
             (table-result (mysql-query conn "SHOW TABLES"))
             (table-names (mapcar #'car (mysql-result-rows table-result)))
             (schema (make-hash-table :test 'equal)))
        (dolist (tbl table-names)
          (puthash tbl nil schema))
        (puthash key schema mysql-interactive--schema-cache)
        (message "Connected — %d tables" (hash-table-count schema)))
    (mysql-error nil)))

(defun mysql-interactive--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA.
Fetches via SHOW COLUMNS if not yet cached.  Returns column list."
  (let ((cols (gethash table schema 'missing)))
    (unless (eq cols 'missing)
      (or cols
          (condition-case nil
              (let* ((result (mysql-query
                              conn
                              (format "SHOW COLUMNS FROM %s"
                                      (mysql-escape-identifier table))))
                     (col-names (mapcar #'car (mysql-result-rows result))))
                (puthash table col-names schema)
                col-names)
            (mysql-error nil))))))

(defun mysql-interactive--schema-for-connection ()
  "Return the schema hash-table for the current connection, or nil."
  (when (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (gethash (mysql-interactive--connection-key mysql-interactive-connection)
             mysql-interactive--schema-cache)))

(defun mysql-interactive-completion-at-point ()
  "Completion-at-point function for SQL identifiers."
  (when-let* ((schema (mysql-interactive--schema-for-connection))
              (conn mysql-interactive-connection)
              (bounds (bounds-of-thing-at-point 'symbol)))
    (let* ((beg (car bounds))
           (end (cdr bounds))
           (line-before (buffer-substring-no-properties
                         (line-beginning-position) beg))
           ;; After FROM/JOIN/INTO/UPDATE → table names only
           (table-context-p
            (string-match-p
             "\\b\\(FROM\\|JOIN\\|INTO\\|UPDATE\\|TABLE\\|DESCRIBE\\|DESC\\)\\s-+\\S-*\\'"
             (upcase line-before)))
           (candidates
            (if table-context-p
                (hash-table-keys schema)
              ;; Combine table names and lazily-loaded column names
              (let ((all (copy-sequence (hash-table-keys schema))))
                (dolist (tbl (hash-table-keys schema))
                  (when-let* ((cols (mysql-interactive--ensure-columns
                                     conn schema tbl)))
                    (setq all (nconc all (copy-sequence cols)))))
                (delete-dups all)))))
      (list beg end candidates :exclusive 'no))))

;;;; Schema browser

(defvar mysql-schema-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "g" #'mysql-schema-refresh)
    (define-key map (kbd "RET") #'mysql-schema-describe-at-point)
    map)
  "Keymap for `mysql-schema-mode'.")

(define-derived-mode mysql-schema-mode special-mode "MySQL-Schema"
  "Mode for browsing MySQL schema."
  (setq truncate-lines t)
  (setq-local revert-buffer-function #'mysql-schema--revert))

(defun mysql-schema--revert (_ignore-auto _noconfirm)
  "Revert function for schema browser."
  (mysql-schema-refresh))

(defun mysql-list-tables ()
  "Show a list of tables in the current database."
  (interactive)
  (mysql-interactive--ensure-connection)
  (let* ((conn mysql-interactive-connection)
         (result (mysql-query conn "SHOW TABLES"))
         (tables (mapcar #'car (mysql-result-rows result)))
         (buf (get-buffer-create
               (format "*mysql: %s tables*"
                       (or (mysql-conn-database conn) "?")))))
    (with-current-buffer buf
      (mysql-schema-mode)
      (setq-local mysql-interactive-connection conn)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize
                 (format "-- Tables in %s (%d)\n\n"
                         (or (mysql-conn-database conn) "?")
                         (length tables))
                 'face 'font-lock-comment-face))
        (dolist (tbl tables)
          (insert-text-button tbl
                              'action (lambda (btn)
                                        (mysql-describe-table
                                         (button-label btn)))
                              'follow-link t)
          (insert "\n"))
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun mysql-describe-table (table)
  "Show the structure of TABLE using DESCRIBE."
  (interactive
   (list (if-let* ((schema (mysql-interactive--schema-for-connection)))
             (completing-read "Table: " (hash-table-keys schema) nil t)
           (read-string "Table: "))))
  (mysql-interactive--ensure-connection)
  (let* ((conn mysql-interactive-connection)
         (result (mysql-query conn (format "DESCRIBE %s"
                                           (mysql-escape-identifier table))))
         (col-names (mysql-interactive--column-names
                     (mysql-result-columns result)))
         (rows (mysql-result-rows result))
         (buf (get-buffer-create (format "*mysql: %s*" table))))
    (with-current-buffer buf
      (mysql-schema-mode)
      (setq-local mysql-interactive-connection conn)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "-- Structure of %s\n\n" table)
                            'face 'font-lock-comment-face))
        (mysql-interactive--insert-simple-table col-names rows)
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun mysql-describe-table-at-point ()
  "Describe the table name at point."
  (interactive)
  (if-let* ((table (thing-at-point 'symbol t)))
      (mysql-describe-table table)
    (call-interactively #'mysql-describe-table)))

(defun mysql-schema-describe-at-point ()
  "In schema browser, describe the table on the current line."
  (interactive)
  (if-let* ((btn (button-at (point))))
      (mysql-describe-table (button-label btn))
    (if-let* ((table (thing-at-point 'symbol t)))
        (mysql-describe-table table)
      (user-error "No table at point"))))

(defun mysql-schema-refresh ()
  "Refresh the schema browser and cache."
  (interactive)
  (mysql-interactive--ensure-connection)
  (mysql-interactive--refresh-schema-cache mysql-interactive-connection)
  (mysql-list-tables))

;;;; mysql-mode (SQL editing major mode)

(defvar mysql-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-c") #'mysql-execute-query-at-point)
    (define-key map (kbd "C-c C-r") #'mysql-execute-region)
    (define-key map (kbd "C-c C-b") #'mysql-execute-buffer)
    (define-key map (kbd "C-c C-e") #'mysql-connect-interactive)
    (define-key map (kbd "C-c C-t") #'mysql-list-tables)
    (define-key map (kbd "C-c C-d") #'mysql-describe-table-at-point)
    (define-key map (kbd "C-c C-l") #'mysql-interactive-show-history)
    (define-key map (kbd "C-c C-o") #'mysql-dispatch)
    map)
  "Keymap for `mysql-mode'.")

;;;###autoload
(define-derived-mode mysql-mode sql-mode "MySQL"
  "Major mode for editing and executing MySQL queries.

\\<mysql-mode-map>
Key bindings:
  \\[mysql-execute-query-at-point]	Execute query at point
  \\[mysql-execute-region]	Execute region
  \\[mysql-execute-buffer]	Execute buffer
  \\[mysql-connect-interactive]	Connect to server
  \\[mysql-list-tables]	List tables
  \\[mysql-describe-table-at-point]	Describe table at point
  \\[mysql-interactive-show-history]	Show query history"
  (add-hook 'completion-at-point-functions
            #'mysql-interactive-completion-at-point nil t)
  (mysql-interactive--update-mode-line))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mysql\\'" . mysql-mode))

;;;; mysql-result-mode

(defvar mysql-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "C-c '") #'mysql-result-edit-cell)
    (define-key map (kbd "C-c C-c") #'mysql-result-commit)
    (define-key map "g" #'mysql-result-rerun)
    (define-key map "e" #'mysql-result-export)
    (define-key map "c" #'mysql-result-goto-column)
    (define-key map "n" #'mysql-result-load-more)
    (define-key map "s" #'mysql-result-sort-by-column)
    (define-key map "S" #'mysql-result-sort-by-column-desc)
    (define-key map "y" #'mysql-result-yank-cell)
    (define-key map "w" #'mysql-result-copy-row-as-insert)
    (define-key map "Y" #'mysql-result-copy-as-csv)
    (define-key map "W" #'mysql-result-apply-filter)
    (define-key map (kbd "RET") #'mysql-result-open-record)
    (define-key map "]" #'mysql-result-next-col-page)
    (define-key map "[" #'mysql-result-prev-col-page)
    (define-key map "+" #'mysql-result-widen-column)
    (define-key map "-" #'mysql-result-narrow-column)
    (define-key map "p" #'mysql-result-pin-column)
    (define-key map "P" #'mysql-result-unpin-column)
    (define-key map "?" #'mysql-result-dispatch)
    map)
  "Keymap for `mysql-result-mode'.")

(defun mysql-interactive--update-header-highlight ()
  "Highlight the header cell for the column under the cursor.
Uses an overlay so the buffer text is not modified."
  (when mysql-interactive--column-widths
    (let ((cidx (get-text-property (point) 'mysql-col-idx)))
      (unless (eql cidx mysql-interactive--header-active-col)
        (setq mysql-interactive--header-active-col cidx)
        (when mysql-interactive--header-overlay
          (delete-overlay mysql-interactive--header-overlay))
        (when cidx
          (save-excursion
            (goto-char (point-min))
            (when-let* ((m (text-property-search-forward
                            'mysql-header-col cidx #'eq)))
              (let ((ov (make-overlay (prop-match-beginning m)
                                      (prop-match-end m))))
                (overlay-put ov 'face 'mysql-header-active-face)
                (setq mysql-interactive--header-overlay ov)))))))))

(define-derived-mode mysql-result-mode special-mode "MySQL-Result"
  "Mode for displaying MySQL query results.

\\<mysql-result-mode-map>
  \\[mysql-result-open-record]	Open record view for row
  \\[mysql-result-apply-filter]	Apply WHERE filter
  \\[mysql-result-edit-cell]	Edit cell value
  \\[mysql-result-commit]	Commit edits as UPDATE
  \\[mysql-result-goto-column]	Jump to column by name
  \\[mysql-result-next-col-page]	Next column page
  \\[mysql-result-prev-col-page]	Previous column page
  \\[mysql-result-sort-by-column]	Sort ascending
  \\[mysql-result-sort-by-column-desc]	Sort descending
  \\[mysql-result-widen-column]	Widen column
  \\[mysql-result-narrow-column]	Narrow column
  \\[mysql-result-pin-column]	Pin column
  \\[mysql-result-unpin-column]	Unpin column
  \\[mysql-result-yank-cell]	Copy cell value
  \\[mysql-result-copy-row-as-insert]	Copy row(s) as INSERT
  \\[mysql-result-copy-as-csv]	Copy row(s) as CSV
  \\[mysql-result-load-more]	Load more rows
  \\[mysql-result-rerun]	Re-execute the query
  \\[mysql-result-export]	Export results"
  (setq truncate-lines t)
  (hl-line-mode 1)
  (add-hook 'post-command-hook
            #'mysql-interactive--update-header-highlight nil t))

(defun mysql-result-load-more ()
  "Load the next page of rows into the result display."
  (interactive)
  (let* ((total (length mysql-interactive--result-rows))
         (offset mysql-interactive--display-offset)
         (page-size mysql-interactive-result-max-rows))
    (when (>= offset total)
      (user-error "All %d rows already displayed" total))
    (setq-local mysql-interactive--display-offset
                (min (+ offset page-size) total))
    (mysql-interactive--refresh-display)))

(defun mysql-result-rerun ()
  "Re-execute the last query that produced this result buffer."
  (interactive)
  (if-let* ((sql mysql-interactive--last-query))
      (mysql-interactive--execute sql mysql-interactive-connection)
    (user-error "No query to re-execute")))

;;;; Cell editing (C-c ')

(defun mysql-result--cell-at (pos)
  "Return (ROW-IDX COL-IDX FULL-VALUE) at buffer position POS, or nil."
  (when-let* ((ridx (get-text-property pos 'mysql-row-idx)))
    (list ridx
          (get-text-property pos 'mysql-col-idx)
          (get-text-property pos 'mysql-full-value))))

(defun mysql-result--cell-at-point ()
  "Return (ROW-IDX COL-IDX FULL-VALUE) for the cell at or near point.
If point is on a pipe separator or padding space, scans left then
right on the current line to find the nearest cell."
  (or (mysql-result--cell-at (point))
      (let ((bol (line-beginning-position))
            (eol (line-end-position)))
        (or (cl-loop for p downfrom (1- (point)) to bol
                     thereis (mysql-result--cell-at p))
            (cl-loop for p from (1+ (point)) to eol
                     thereis (mysql-result--cell-at p))))))

(defun mysql-result--row-idx-at-line ()
  "Return the row index for the current line, or nil.
Scans text properties across the line."
  (cl-loop for p from (line-beginning-position) to (line-end-position)
           thereis (get-text-property p 'mysql-row-idx)))

(defun mysql-result--rows-in-region (beg end)
  "Return sorted list of unique row indices in the region BEG..END."
  (let (indices)
    (save-excursion
      (goto-char beg)
      (while (< (point) end)
        (when-let* ((ridx (mysql-result--row-idx-at-line)))
          (cl-pushnew ridx indices))
        (forward-line 1)))
    (sort indices #'<)))

(defvar mysql-result--edit-callback nil
  "Callback for the cell edit buffer: (lambda (new-value) ...).")

(defvar mysql-result-edit-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") #'mysql-result-edit-finish)
    (define-key map (kbd "C-c C-k") #'mysql-result-edit-cancel)
    map)
  "Keymap for the cell edit buffer.")

(define-minor-mode mysql-result-edit-mode
  "Minor mode for editing a MySQL cell value.
\\<mysql-result-edit-mode-map>
  \\[mysql-result-edit-finish]	Accept edit
  \\[mysql-result-edit-cancel]	Cancel"
  :lighter " MySQL-Edit"
  :keymap mysql-result-edit-mode-map)

(defun mysql-result-edit-cell ()
  "Edit the cell at point in a dedicated buffer (like `C-c \\='` in org)."
  (interactive)
  (let* ((cell (or (mysql-result--cell-at-point)
                   (user-error "No cell at point")))
         (ridx (nth 0 cell))
         (cidx (nth 1 cell))
         (val  (nth 2 cell))
         (col-name (nth cidx mysql-interactive--result-columns))
         (result-buf (current-buffer))
         (edit-buf (get-buffer-create
                    (format "*mysql-edit: [%d].%s*" ridx col-name))))
    (with-current-buffer edit-buf
      (erase-buffer)
      (insert (mysql-interactive--format-value val))
      (goto-char (point-min))
      (mysql-result-edit-mode 1)
      (setq-local header-line-format
                  (format " Editing row %d, column \"%s\"  |  C-c C-c: accept  C-c C-k: cancel"
                          ridx col-name))
      (setq-local mysql-result--edit-callback
                  (lambda (new-value)
                    (with-current-buffer result-buf
                      (mysql-result--apply-edit ridx cidx new-value)))))
    (pop-to-buffer edit-buf)))

(defun mysql-result-edit-finish ()
  "Accept the edit and return to the result buffer."
  (interactive)
  (let ((new-value (string-trim-right (buffer-string)))
        (cb mysql-result--edit-callback))
    (quit-window 'kill)
    (when cb
      (funcall cb (if (string= new-value "NULL") nil new-value)))))

(defun mysql-result-edit-cancel ()
  "Cancel the edit and return to the result buffer."
  (interactive)
  (quit-window 'kill))

(defun mysql-result--apply-edit (ridx cidx new-value)
  "Record edit for row RIDX, column CIDX with NEW-VALUE and refresh display."
  (let ((key (cons ridx cidx))
        (original (nth cidx (nth ridx mysql-interactive--result-rows))))
    ;; If edited back to original, remove the pending edit
    (if (equal new-value original)
        (setq mysql-interactive--pending-edits
              (assoc-delete-all key mysql-interactive--pending-edits))
      (let ((existing (assoc key mysql-interactive--pending-edits)))
        (if existing
            (setcdr existing new-value)
          (push (cons key new-value) mysql-interactive--pending-edits)))))
  (mysql-interactive--refresh-display))

;;;; Commit edits

(defun mysql-result--detect-table ()
  "Try to detect the source table from query or column metadata.
Returns table name string or nil."
  ;; Try column metadata first — :org-table is the physical table
  (when-let* ((defs mysql-interactive--result-column-defs)
              (tables (delete-dups
                       (delq nil (mapcar (lambda (c) (plist-get c :org-table))
                                         defs))))
              ;; single table only
              ((= (length tables) 1))
              ((not (string-empty-p (car tables)))))
    (car tables)))

(defun mysql-result--detect-primary-key ()
  "Return a list of column indices that form the primary key, or nil."
  (when-let* ((conn mysql-interactive-connection)
              (table (mysql-result--detect-table)))
    (condition-case nil
        (let* ((result (mysql-query
                        conn
                        (format "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'"
                                (mysql-escape-identifier table))))
               (pk-cols (mapcar (lambda (row)
                                  (let ((name (nth 4 row)))
                                    (if (stringp name) name (format "%s" name))))
                                (mysql-result-rows result)))
               (col-names mysql-interactive--result-columns))
          (delq nil (mapcar (lambda (pk)
                              (cl-position pk col-names :test #'string=))
                            pk-cols)))
      (mysql-error nil))))

(defun mysql-interactive--load-fk-info ()
  "Load foreign key info for the current result's source table.
Populates `mysql-interactive--fk-info' with an alist mapping
column indices to their referenced table and column."
  (setq mysql-interactive--fk-info nil)
  (when-let* ((conn mysql-interactive-connection)
              (table (mysql-result--detect-table))
              (col-names mysql-interactive--result-columns))
    (condition-case nil
        (let* ((sql (format
                     "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s \
AND REFERENCED_TABLE_NAME IS NOT NULL"
                     (mysql-escape-literal table)))
               (result (mysql-query conn sql))
               (rows (mysql-result-rows result)))
          (dolist (row rows)
            (let* ((col-name (let ((n (nth 0 row)))
                               (if (stringp n) n (format "%s" n))))
                   (ref-table (nth 1 row))
                   (ref-col (nth 2 row))
                   (idx (cl-position col-name col-names :test #'string=)))
              (when idx
                (push (cons idx (list :ref-table ref-table
                                      :ref-column ref-col))
                      mysql-interactive--fk-info)))))
      (error nil))))

(defun mysql-result--group-edits-by-row (edits)
  "Group EDITS alist by row index into a hash-table.
Returns hash-table mapping ridx → list of (cidx . value)."
  (let ((ht (make-hash-table :test 'eql)))
    (pcase-dolist (`((,ridx . ,cidx) . ,val) edits)
      (push (cons cidx val) (gethash ridx ht)))
    ht))

(defun mysql-result--build-update-stmt (table row _ridx edits col-names pk-indices)
  "Build an UPDATE statement for TABLE.
ROW is the original row data at _RIDX, EDITS is a list of (cidx . value),
COL-NAMES are column names, PK-INDICES are primary key column indices."
  (let ((set-parts
         (mapcar (lambda (e)
                   (format "%s = %s"
                           (mysql-escape-identifier (nth (car e) col-names))
                           (mysql-interactive--value-to-literal (cdr e))))
                 edits))
        (where-parts
         (mapcar (lambda (pki)
                   (let ((v (nth pki row)))
                     (format "%s %s"
                             (mysql-escape-identifier (nth pki col-names))
                             (if (null v) "IS NULL"
                               (format "= %s"
                                       (mysql-interactive--value-to-literal v))))))
                 pk-indices)))
    (format "UPDATE %s SET %s WHERE %s"
            (mysql-escape-identifier table)
            (mapconcat #'identity set-parts ", ")
            (mapconcat #'identity where-parts " AND "))))

(defun mysql-result-commit ()
  "Generate and execute UPDATE statements for pending edits."
  (interactive)
  (unless mysql-interactive--pending-edits
    (user-error "No pending edits"))
  (let* ((table (or (mysql-result--detect-table)
                    (user-error "Cannot detect source table (multi-table query?)")))
         (pk-indices (or (mysql-result--detect-primary-key)
                         (user-error "Cannot detect primary key for table %s" table)))
         (col-names mysql-interactive--result-columns)
         (rows mysql-interactive--result-rows)
         (by-row (mysql-result--group-edits-by-row mysql-interactive--pending-edits))
         (statements nil))
    (maphash
     (lambda (ridx edits)
       (push (mysql-result--build-update-stmt
              table (nth ridx rows) ridx edits col-names pk-indices)
             statements))
     by-row)
    (let ((sql-text (mapconcat (lambda (s) (concat s ";"))
                               (nreverse statements) "\n")))
      (when (yes-or-no-p
             (format "Execute %d UPDATE statement%s?\n\n%s\n\n"
                     (length statements)
                     (if (= (length statements) 1) "" "s")
                     sql-text))
        (dolist (stmt statements)
          (condition-case err
              (mysql-query mysql-interactive-connection stmt)
            (mysql-error
             (user-error "UPDATE failed: %s" (error-message-string err)))))
        (setq mysql-interactive--pending-edits nil)
        (message "%d row%s updated"
                 (length statements)
                 (if (= (length statements) 1) "" "s"))
        (mysql-interactive--execute mysql-interactive--last-query
                                    mysql-interactive-connection)))))

;;;; Sort

(defun mysql-result--sort-key (val)
  "Return a comparison key for VAL.
Numbers sort numerically, nil sorts last, everything else as string."
  (cond
   ((null val) nil)
   ((numberp val) val)
   ((stringp val) val)
   ;; date/time plists — format to string for comparison
   (t (mysql-interactive--format-value val))))

(defun mysql-result--compare (a b)
  "Compare two sort keys A and B.  nils sort last."
  (cond
   ((and (null a) (null b)) nil)
   ((null a) nil)  ; a (nil) goes after b
   ((null b) t)    ; b (nil) goes after a
   ((and (numberp a) (numberp b)) (< a b))
   (t (string< (format "%s" a) (format "%s" b)))))

(defun mysql-result--sort (col-name descending)
  "Sort result rows by COL-NAME.  If DESCENDING, reverse order."
  (unless mysql-interactive--result-columns
    (user-error "No result data"))
  (let* ((col-names mysql-interactive--result-columns)
         (idx (cl-position col-name col-names :test #'string=)))
    (unless idx
      (user-error "Column %s not found" col-name))
    (setq mysql-interactive--result-rows
          (sort mysql-interactive--result-rows
                (lambda (a b)
                  (let ((va (mysql-result--sort-key (nth idx a)))
                        (vb (mysql-result--sort-key (nth idx b))))
                    (if descending
                        (mysql-result--compare vb va)
                      (mysql-result--compare va vb))))))
    (setq mysql-interactive--display-offset
          (min (length mysql-interactive--result-rows)
               mysql-interactive-result-max-rows))
    (mysql-interactive--refresh-display)
    (message "Sorted by %s %s" col-name (if descending "DESC" "ASC"))))

(defun mysql-result--read-column ()
  "Read a column name, defaulting to column at point."
  (let* ((col-names mysql-interactive--result-columns)
         (cidx (get-text-property (point) 'mysql-col-idx))
         (default (when cidx (nth cidx col-names))))
    (completing-read (if default
                         (format "Sort by column (default %s): " default)
                       "Sort by column: ")
                     col-names nil t nil nil default)))

(defun mysql-result-sort-by-column ()
  "Sort results ascending by a column."
  (interactive)
  (mysql-result--sort (mysql-result--read-column) nil))

(defun mysql-result-sort-by-column-desc ()
  "Sort results descending by a column."
  (interactive)
  (mysql-result--sort (mysql-result--read-column) t))

;;;; WHERE filtering

(defun mysql-interactive--apply-where (sql filter)
  "Apply WHERE FILTER to SQL query string.
If SQL already has a WHERE clause, appends FILTER with AND.
Otherwise inserts WHERE before ORDER BY/GROUP BY/HAVING/LIMIT or at end."
  (let* ((trimmed (string-trim-right
                   (replace-regexp-in-string ";\\s-*\\'" "" sql)))
         (case-fold-search t)
         (has-where (string-match-p "\\bWHERE\\b" trimmed))
         (clause (if has-where
                     (format "AND (%s) " filter)
                   (format "WHERE %s " filter)))
         (tail-re "\\b\\(ORDER[[:space:]]+BY\\|GROUP[[:space:]]+BY\\|HAVING\\|LIMIT\\)\\b"))
    (if (string-match tail-re trimmed)
        (let ((pos (match-beginning 0)))
          (concat (substring trimmed 0 pos) clause (substring trimmed pos)))
      (concat trimmed " " (string-trim-right clause)))))

(defun mysql-result-apply-filter ()
  "Apply or clear a WHERE filter on the current result query.
Prompts for a WHERE condition.  Enter empty string to clear."
  (interactive)
  (unless mysql-interactive--last-query
    (user-error "No query to filter"))
  (let* ((base (or mysql-interactive--base-query
                   mysql-interactive--last-query))
         (current mysql-interactive--where-filter)
         (input (string-trim
                 (read-string
                  (if current
                      (format "WHERE filter (current: %s, empty to clear): "
                              current)
                    "WHERE filter (e.g., age > 18): ")
                  nil nil current)))
         (filtered-sql (unless (string-empty-p input)
                         (mysql-interactive--apply-where base input))))
    (mysql-interactive--execute (or filtered-sql base)
                                mysql-interactive-connection)
    (setq mysql-interactive--base-query (when filtered-sql base))
    (setq mysql-interactive--where-filter (when filtered-sql input))
    (message (if filtered-sql
                 (format "Filter applied: WHERE %s" input)
               "Filter cleared"))))

;;;; Yank cell / Copy row as INSERT

(defun mysql-result-yank-cell ()
  "Copy the full value of the cell at point to the kill ring."
  (interactive)
  (let* ((cell (or (mysql-result--cell-at-point)
                   (user-error "No cell at point")))
         (text (mysql-interactive--format-value (nth 2 cell))))
    (kill-new text)
    (message "Copied: %s" (truncate-string-to-width text 60 nil nil "…"))))

(defun mysql-result-copy-row-as-insert ()
  "Copy the current row as an INSERT statement to the kill ring.
With an active region, copies all rows in the region."
  (interactive)
  (let* ((indices (if (use-region-p)
                      (mysql-result--rows-in-region (region-beginning) (region-end))
                    (list (or (mysql-result--row-idx-at-line)
                              (user-error "No row at point")))))
         (table (or (mysql-result--detect-table) "TABLE"))
         (col-names mysql-interactive--result-columns)
         (rows mysql-interactive--result-rows)
         (cols (mapconcat #'mysql-escape-identifier col-names ", "))
         (stmts (mapcar
                 (lambda (ridx)
                   (let ((row (nth ridx rows)))
                     (format "INSERT INTO %s (%s) VALUES (%s);"
                             (mysql-escape-identifier table) cols
                             (mapconcat #'mysql-interactive--value-to-literal row ", "))))
                 indices)))
    (kill-new (mapconcat #'identity stmts "\n"))
    (message "Copied %d INSERT statement%s"
             (length stmts) (if (= (length stmts) 1) "" "s"))))

(defun mysql-result-copy-as-csv ()
  "Copy the current row as CSV to the kill ring.
With an active region, copies all rows in the region.
Includes a header row with column names."
  (interactive)
  (let* ((indices (if (use-region-p)
                      (mysql-result--rows-in-region (region-beginning) (region-end))
                    (list (or (mysql-result--row-idx-at-line)
                              (user-error "No row at point")))))
         (col-names mysql-interactive--result-columns)
         (rows mysql-interactive--result-rows)
         (csv-escape (lambda (val)
                       (let ((s (mysql-interactive--format-value val)))
                         (if (string-match-p "[,\"\n]" s)
                             (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" s))
                           s))))
         (lines (cons (mapconcat #'identity col-names ",")
                      (mapcar (lambda (ridx)
                                (mapconcat csv-escape (nth ridx rows) ","))
                              indices))))
    (kill-new (mapconcat #'identity lines "\n"))
    (message "Copied %d row%s as CSV"
             (length indices) (if (= (length indices) 1) "" "s"))))

(defun mysql-result--goto-col-idx (col-idx)
  "Move point to the first data cell matching COL-IDX in the buffer."
  (goto-char (point-min))
  (when-let* ((found (text-property-search-forward 'mysql-col-idx col-idx #'eq)))
    (goto-char (prop-match-beginning found))))

(defun mysql-result-goto-column ()
  "Jump to the column page containing a specific column."
  (interactive)
  (unless mysql-interactive--result-columns
    (user-error "No result columns"))
  (let* ((col-names mysql-interactive--result-columns)
         (choice (completing-read "Go to column: " col-names nil t))
         (idx (cl-position choice col-names :test #'string=)))
    (when idx
      (let ((target-page nil))
        (if (memq idx mysql-interactive--pinned-columns)
            (setq target-page mysql-interactive--current-col-page)
          (cl-loop for pi from 0 below (length mysql-interactive--column-pages)
                   when (cl-find idx (aref mysql-interactive--column-pages pi))
                   do (setq target-page pi)))
        (if (null target-page)
            (user-error "Column %s not found in pages" choice)
          (unless (= target-page mysql-interactive--current-col-page)
            (setq mysql-interactive--current-col-page target-page)
            (mysql-interactive--render-result))
          (mysql-result--goto-col-idx idx))))))

(defun mysql-result-export ()
  "Export the current result.
Prompts for format: csv (new buffer) or copy (kill ring)."
  (interactive)
  (let ((fmt (completing-read "Export format: " '("csv" "copy") nil t)))
    (pcase fmt
      ("csv" (mysql-interactive--export-csv))
      ("copy"
       (kill-ring-save (point-min) (point-max))
       (message "Buffer content copied to kill ring")))))

(defun mysql-interactive--export-csv ()
  "Export the current result as CSV into a new buffer.
Generates CSV directly from cached data."
  (let* ((col-names mysql-interactive--result-columns)
         (rows mysql-interactive--result-rows)
         (csv-escape (lambda (val)
                       (let ((s (mysql-interactive--format-value val)))
                         (if (string-match-p "[,\"\n]" s)
                             (format "\"%s\""
                                     (replace-regexp-in-string "\"" "\"\"" s))
                           s))))
         (csv-buf (generate-new-buffer "*mysql: export.csv*")))
    (with-current-buffer csv-buf
      (insert (mapconcat #'identity col-names ",") "\n")
      (dolist (row rows)
        (insert (mapconcat csv-escape row ",") "\n"))
      (goto-char (point-min)))
    (pop-to-buffer csv-buf)))

;;;; Column page navigation and width adjustment

(defun mysql-result-next-col-page ()
  "Switch to the next column page."
  (interactive)
  (let ((max-page (1- (length mysql-interactive--column-pages))))
    (if (>= mysql-interactive--current-col-page max-page)
        (user-error "Already on last column page")
      (cl-incf mysql-interactive--current-col-page)
      (mysql-interactive--render-result))))

(defun mysql-result-prev-col-page ()
  "Switch to the previous column page."
  (interactive)
  (if (<= mysql-interactive--current-col-page 0)
      (user-error "Already on first column page")
    (cl-decf mysql-interactive--current-col-page)
    (mysql-interactive--render-result)))

(defun mysql-result-widen-column ()
  "Widen the column at point by `mysql-interactive-column-width-step'."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'mysql-col-idx)))
      (progn
        (cl-incf (aref mysql-interactive--column-widths cidx)
                 mysql-interactive-column-width-step)
        (mysql-interactive--refresh-display))
    (user-error "No column at point")))

(defun mysql-result-narrow-column ()
  "Narrow the column at point by `mysql-interactive-column-width-step'."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'mysql-col-idx)))
      (let ((new-w (max 5 (- (aref mysql-interactive--column-widths cidx)
                              mysql-interactive-column-width-step))))
        (aset mysql-interactive--column-widths cidx new-w)
        (mysql-interactive--refresh-display))
    (user-error "No column at point")))

(defun mysql-result-pin-column ()
  "Pin the column at point so it appears on all column pages."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'mysql-col-idx)))
      (progn
        (cl-pushnew cidx mysql-interactive--pinned-columns)
        (mysql-interactive--refresh-display)
        (message "Pinned column %s" (nth cidx mysql-interactive--result-columns)))
    (user-error "No column at point")))

(defun mysql-result-unpin-column ()
  "Unpin the column at point."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'mysql-col-idx)))
      (progn
        (setq mysql-interactive--pinned-columns
              (delq cidx mysql-interactive--pinned-columns))
        (mysql-interactive--refresh-display)
        (message "Unpinned column %s" (nth cidx mysql-interactive--result-columns)))
    (user-error "No column at point")))

;;;; Record buffer

(defvar mysql-record-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "RET") #'mysql-record-toggle-expand)
    (define-key map (kbd "C-c '") #'mysql-record-edit-field)
    (define-key map "n" #'mysql-record-next-row)
    (define-key map "p" #'mysql-record-prev-row)
    (define-key map "y" #'mysql-record-yank-field)
    (define-key map "w" #'mysql-record-copy-as-insert)
    (define-key map "q" #'quit-window)
    (define-key map "g" #'mysql-record-refresh)
    (define-key map "?" #'mysql-record-dispatch)
    map)
  "Keymap for `mysql-record-mode'.")

(declare-function mysql-record-dispatch "mysql-transient")

(define-derived-mode mysql-record-mode special-mode "MySQL-Record"
  "Mode for displaying a single MySQL row in detail.

\\<mysql-record-mode-map>
  \\[mysql-record-toggle-expand]	Expand/collapse field or follow FK
  \\[mysql-record-edit-field]	Edit field
  \\[mysql-record-next-row]	Next row
  \\[mysql-record-prev-row]	Previous row
  \\[mysql-record-yank-field]	Copy field value
  \\[mysql-record-copy-as-insert]	Copy row as INSERT
  \\[mysql-record-refresh]	Refresh"
  (setq truncate-lines nil))

(defun mysql-result-open-record ()
  "Open a Record buffer showing the row at point."
  (interactive)
  (let* ((ridx (or (mysql-result--row-idx-at-line)
                    (user-error "No row at point")))
         (result-buf (current-buffer))
         (buf-name (format "*mysql-record: row %d*" ridx))
         (buf (get-buffer-create buf-name)))
    (with-current-buffer buf
      (mysql-record-mode)
      (setq-local mysql-record--result-buffer result-buf)
      (setq-local mysql-record--row-idx ridx)
      (setq-local mysql-record--expanded-fields nil)
      (mysql-record--render))
    (display-buffer buf '(display-buffer-at-bottom))))

(defun mysql-record--render-field (name cidx val col-def ridx edits fk-info
                                        expanded-fields max-name-w)
  "Insert one field line for column NAME at CIDX.
VAL is the cell value, COL-DEF the column metadata, RIDX the row index.
EDITS, FK-INFO, EXPANDED-FIELDS provide edit/FK/expand state.
MAX-NAME-W is the label column width."
  (let* ((edited (assoc (cons ridx cidx) edits))
         (display-val (if edited (cdr edited) val))
         (long-p (mysql-interactive--long-field-type-p col-def))
         (expanded-p (memq cidx expanded-fields))
         (fk (cdr (assq cidx fk-info)))
         (formatted (mysql-interactive--format-value display-val))
         (display (if (and long-p (not expanded-p) (> (length formatted) 80))
                      (concat (substring formatted 0 80) "…")
                    formatted))
         (face (cond (edited 'mysql-modified-face)
                     ((null val) 'mysql-null-face)
                     (fk 'mysql-fk-face)
                     (t nil))))
    (insert (propertize (string-pad name max-name-w)
                        'face 'mysql-header-face)
            (propertize " : " 'face 'mysql-border-face)
            (propertize display
                        'mysql-row-idx ridx
                        'mysql-col-idx cidx
                        'mysql-full-value (if edited (cdr edited) val)
                        'face face)
            "\n")))

(defun mysql-record--render ()
  "Render the current row in the Record buffer."
  (unless (buffer-live-p mysql-record--result-buffer)
    (user-error "Result buffer no longer exists"))
  (let* ((result-buf mysql-record--result-buffer)
         (ridx mysql-record--row-idx)
         (col-names (buffer-local-value 'mysql-interactive--result-columns result-buf))
         (col-defs (buffer-local-value 'mysql-interactive--result-column-defs result-buf))
         (rows (buffer-local-value 'mysql-interactive--result-rows result-buf))
         (fk-info (buffer-local-value 'mysql-interactive--fk-info result-buf))
         (edits (buffer-local-value 'mysql-interactive--pending-edits result-buf))
         (inhibit-read-only t))
    (unless (< ridx (length rows))
      (user-error "Row %d no longer exists" ridx))
    (erase-buffer)
    (setq header-line-format
          (propertize (format " Record: row %d/%d" (1+ ridx) (length rows))
                      'face 'mysql-header-face))
    (let* ((row (nth ridx rows))
           (max-name-w (apply #'max (mapcar #'string-width col-names))))
      (cl-loop for name in col-names
               for cidx from 0
               do (mysql-record--render-field
                   name cidx (nth cidx row) (nth cidx col-defs)
                   ridx edits fk-info mysql-record--expanded-fields
                   max-name-w)))
    (goto-char (point-min))))

(defun mysql-record-toggle-expand ()
  "Toggle expand/collapse for long fields, or follow FK."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'mysql-col-idx))
            (ridx (get-text-property (point) 'mysql-row-idx)))
      (let* ((result-buf mysql-record--result-buffer)
             (fk-info (buffer-local-value 'mysql-interactive--fk-info result-buf))
             (fk (cdr (assq cidx fk-info)))
             (col-defs (buffer-local-value 'mysql-interactive--result-column-defs result-buf))
             (col-def (nth cidx col-defs))
             (val (get-text-property (point) 'mysql-full-value)))
        (cond
         (fk
          (when (null val)
            (user-error "NULL value — cannot follow"))
          (with-current-buffer result-buf
            (mysql-interactive--execute
             (format "SELECT * FROM %s WHERE %s = %s"
                     (mysql-escape-identifier (plist-get fk :ref-table))
                     (mysql-escape-identifier (plist-get fk :ref-column))
                     (mysql-interactive--value-to-literal val))
             mysql-interactive-connection)))
         ((mysql-interactive--long-field-type-p col-def)
          (if (memq cidx mysql-record--expanded-fields)
              (setq mysql-record--expanded-fields
                    (delq cidx mysql-record--expanded-fields))
            (push cidx mysql-record--expanded-fields))
          (mysql-record--render))
         (t
          (message "%s" (mysql-interactive--format-value val)))))
    (user-error "No field at point")))

(defun mysql-record-edit-field ()
  "Edit the field at point in a dedicated buffer."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'mysql-col-idx)))
      (let* ((ridx mysql-record--row-idx)
             (result-buf mysql-record--result-buffer)
             (col-names (buffer-local-value 'mysql-interactive--result-columns result-buf))
             (col-name (nth cidx col-names))
             (val (get-text-property (point) 'mysql-full-value))
             (record-buf (current-buffer))
             (edit-buf (get-buffer-create
                        (format "*mysql-edit: [%d].%s*" ridx col-name))))
        (with-current-buffer edit-buf
          (erase-buffer)
          (insert (mysql-interactive--format-value val))
          (goto-char (point-min))
          (mysql-result-edit-mode 1)
          (setq-local header-line-format
                      (format " Editing row %d, column \"%s\"  |  C-c C-c: accept  C-c C-k: cancel"
                              ridx col-name))
          (setq-local mysql-result--edit-callback
                      (lambda (new-value)
                        (with-current-buffer result-buf
                          (mysql-result--apply-edit ridx cidx new-value))
                        (when (buffer-live-p record-buf)
                          (with-current-buffer record-buf
                            (mysql-record--render))))))
        (pop-to-buffer edit-buf))
    (user-error "No field at point")))

(defun mysql-record-next-row ()
  "Show the next row in the Record buffer."
  (interactive)
  (let ((total (with-current-buffer mysql-record--result-buffer
                 (length mysql-interactive--result-rows))))
    (if (>= (1+ mysql-record--row-idx) total)
        (user-error "Already at last row")
      (cl-incf mysql-record--row-idx)
      (setq mysql-record--expanded-fields nil)
      (mysql-record--render))))

(defun mysql-record-prev-row ()
  "Show the previous row in the Record buffer."
  (interactive)
  (if (<= mysql-record--row-idx 0)
      (user-error "Already at first row")
    (cl-decf mysql-record--row-idx)
    (setq mysql-record--expanded-fields nil)
    (mysql-record--render)))

(defun mysql-record-yank-field ()
  "Copy the field value at point to the kill ring."
  (interactive)
  (if-let* ((val (get-text-property (point) 'mysql-full-value)))
      (let ((text (mysql-interactive--format-value val)))
        (kill-new text)
        (message "Copied: %s" (truncate-string-to-width text 60 nil nil "…")))
    (user-error "No field at point")))

(defun mysql-record-copy-as-insert ()
  "Copy the current record row as an INSERT statement."
  (interactive)
  (let ((ridx mysql-record--row-idx)
        (result-buf mysql-record--result-buffer))
    (with-current-buffer result-buf
      (let* ((table (or (mysql-result--detect-table) "TABLE"))
             (col-names mysql-interactive--result-columns)
             (row (nth ridx mysql-interactive--result-rows))
             (cols (mapconcat #'mysql-escape-identifier col-names ", "))
             (vals (mapconcat #'mysql-interactive--value-to-literal row ", ")))
        (kill-new (format "INSERT INTO %s (%s) VALUES (%s);"
                          (mysql-escape-identifier table) cols vals))
        (message "Copied INSERT statement")))))

(defun mysql-record-refresh ()
  "Refresh the Record buffer."
  (interactive)
  (mysql-record--render))

;;;; REPL mode

(defvar mysql-repl-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "C-c C-e") #'mysql-connect-interactive)
    (define-key map (kbd "C-c C-t") #'mysql-list-tables)
    (define-key map (kbd "C-c C-d") #'mysql-describe-table-at-point)
    map)
  "Keymap for `mysql-repl-mode'.")

(defvar-local mysql-repl--pending-input ""
  "Accumulated partial SQL input waiting for a semicolon.")

(define-derived-mode mysql-repl-mode comint-mode "MySQL-REPL"
  "Major mode for MySQL REPL.

\\<mysql-repl-mode-map>
  \\[mysql-connect-interactive]	Connect to server
  \\[mysql-list-tables]	List tables
  \\[mysql-describe-table-at-point]	Describe table at point"
  (setq comint-prompt-regexp "^mysql> \\|^    -> ")
  (setq comint-input-sender #'mysql-repl--input-sender)
  (add-hook 'completion-at-point-functions
            #'mysql-interactive-completion-at-point nil t))

(defun mysql-repl--input-sender (_proc input)
  "Process INPUT from comint.
Accumulates input until a semicolon is found, then executes."
  (let ((combined (concat mysql-repl--pending-input
                          (unless (string-empty-p mysql-repl--pending-input) "\n")
                          input)))
    (if (string-match-p ";\\s-*$" combined)
        ;; Complete statement — execute
        (progn
          (setq mysql-repl--pending-input "")
          (mysql-repl--execute-and-print (string-trim combined)))
      ;; Incomplete — accumulate and show continuation prompt
      (setq mysql-repl--pending-input combined)
      (mysql-repl--output "    -> "))))

(defun mysql-repl--output (text)
  "Insert TEXT into the REPL buffer at the process mark."
  (let ((inhibit-read-only t)
        (proc (get-buffer-process (current-buffer))))
    (goto-char (process-mark proc))
    (insert text)
    (set-marker (process-mark proc) (point))))

(defun mysql-repl--format-dml-result (result elapsed)
  "Format a DML RESULT with ELAPSED time as a string for the REPL."
  (let ((msg (format "\nAffected rows: %s"
                     (or (mysql-result-affected-rows result) 0))))
    (when-let* ((id (mysql-result-last-insert-id result))
                ((> id 0)))
      (setq msg (concat msg (format ", Last insert ID: %s" id))))
    (when-let* ((w (mysql-result-warnings result))
                ((> w 0)))
      (setq msg (concat msg (format ", Warnings: %s" w))))
    (format "%s (%.3fs)\n\nmysql> " msg elapsed)))

(defun mysql-repl--execute-and-print (sql)
  "Execute SQL and print results inline in the REPL."
  (if (not (mysql-interactive--connection-alive-p mysql-interactive-connection))
      (mysql-repl--output "ERROR: Not connected.  Use C-c C-e to connect.\nmysql> ")
    (mysql-interactive--add-history sql)
    (setq mysql-interactive--last-query sql)
    (condition-case err
        (let* ((start (float-time))
               (result (mysql-query mysql-interactive-connection sql))
               (elapsed (- (float-time) start))
               (columns (mysql-result-columns result))
               (rows (mysql-result-rows result)))
          (if columns
              (let* ((col-names (mysql-interactive--column-names columns))
                     (table-str (with-temp-buffer
                                  (mysql-interactive--insert-simple-table col-names rows)
                                  (buffer-string))))
                (mysql-repl--output
                 (format "\n%s%d row%s in %.3fs\n\nmysql> "
                         table-str (length rows)
                         (if (= (length rows) 1) "" "s")
                         elapsed)))
            (mysql-repl--output (mysql-repl--format-dml-result result elapsed))))
      (mysql-error
       (mysql-repl--output
        (format "\nERROR: %s\n\nmysql> " (error-message-string err)))))))

;;;###autoload
(defun mysql-repl ()
  "Start a MySQL REPL buffer."
  (interactive)
  (let* ((buf-name "*MySQL REPL*")
         (buf (get-buffer-create buf-name)))
    (unless (comint-check-proc buf)
      (with-current-buffer buf
        ;; Start a dummy process for comint
        (let ((proc (start-process "mysql-repl" buf "cat")))
          (set-process-query-on-exit-flag proc nil)
          (mysql-repl-mode)
          (mysql-repl--output "mysql> "))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(provide 'mysql-interactive)
;;; mysql-interactive.el ends here
