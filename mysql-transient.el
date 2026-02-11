;;; mysql-transient.el --- Transient menus for mysql-interactive -*- lexical-binding: t; -*-

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

;; Transient-based dispatch menus (Magit-style) for mysql-interactive.
;; Provides `mysql-dispatch' as the main entry point.
;;
;; Usage:
;;   (require 'mysql-transient)
;;   ;; Then: M-x mysql-dispatch
;;   ;; Or bind in mysql-mode:
;;   ;;   (define-key mysql-mode-map (kbd "C-c C-c") #'mysql-dispatch)

;;; Code:

(require 'transient)
(require 'mysql-interactive)

;;;###autoload (autoload 'mysql-dispatch "mysql-transient" nil t)
(transient-define-prefix mysql-dispatch ()
  "Main dispatch menu for MySQL."
  ["Connection"
   ("c" "Connect"    mysql-connect-interactive)
   ("d" "Disconnect" mysql-interactive-disconnect)
   ("R" "REPL"       mysql-repl)]
  ["Execute"
   ("x" "Query at point" mysql-execute-query-at-point)
   ("r" "Region"         mysql-execute-region)
   ("b" "Buffer"         mysql-execute-buffer)
   ("l" "History"        mysql-interactive-show-history)]
  ["Schema"
   ("t" "List tables"    mysql-list-tables)
   ("D" "Describe table" mysql-describe-table-at-point)])

;;;###autoload (autoload 'mysql-result-dispatch "mysql-transient" nil t)
(transient-define-prefix mysql-result-dispatch ()
  "Dispatch menu for MySQL result buffer."
  ["Navigate"
   ("RET" "Open record" mysql-result-open-record)
   ("c" "Go to column" mysql-result-goto-column)
   ("n" "Load more"    mysql-result-load-more)]
  ["Column Pages"
   ("]" "Next page"     mysql-result-next-col-page)
   ("[" "Prev page"     mysql-result-prev-col-page)
   ("+" "Widen column"  mysql-result-widen-column)
   ("-" "Narrow column" mysql-result-narrow-column)
   ("p" "Pin column"    mysql-result-pin-column)
   ("P" "Unpin column"  mysql-result-unpin-column)]
  ["Filter / Sort"
   ("W" "WHERE filter" mysql-result-apply-filter)
   ("s" "Sort ASC"  mysql-result-sort-by-column)
   ("S" "Sort DESC" mysql-result-sort-by-column-desc)]
  ["Edit"
   ("e" "Edit cell"  mysql-result-edit-cell)
   ("C" "Commit"     mysql-result-commit)]
  ["Copy / Export"
   ("y" "Yank cell"       mysql-result-yank-cell)
   ("w" "Row(s) as INSERT" mysql-result-copy-row-as-insert)
   ("Y" "Row(s) as CSV"   mysql-result-copy-as-csv)
   ("E" "Export"           mysql-result-export)]
  ["Other"
   ("g" "Re-execute" mysql-result-rerun)])

;;;###autoload (autoload 'mysql-record-dispatch "mysql-transient" nil t)
(transient-define-prefix mysql-record-dispatch ()
  "Dispatch menu for MySQL record buffer."
  ["Navigate"
   ("n" "Next row"     mysql-record-next-row)
   ("p" "Prev row"     mysql-record-prev-row)
   ("RET" "Expand/FK"  mysql-record-toggle-expand)]
  ["Edit"
   ("C-c '" "Edit field" mysql-record-edit-field)]
  ["Copy"
   ("y" "Yank field"      mysql-record-yank-field)
   ("w" "Row as INSERT"   mysql-record-copy-as-insert)]
  ["Other"
   ("g" "Refresh" mysql-record-refresh)
   ("q" "Quit"    quit-window)])

(provide 'mysql-transient)
;;; mysql-transient.el ends here
