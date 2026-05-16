;;; clutch-result.el --- Result buffer workflows -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; Result buffer navigation, selection, copy/export, value viewing,
;; and record-buffer workflows for Clutch.

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'clutch-connection)
(require 'clutch-query)
(require 'clutch-object)
(require 'clutch-schema)
(require 'clutch-sql)
(require 'clutch-ui)
(require 'clutch-edit)
(require 'subr-x)
(require 'transient)

(defvar clutch-connection)
(defvar clutch-agent-context-max-cell-width)
(defvar clutch-agent-context-max-result-rows)
(defvar clutch-csv-export-default-coding-system)
(defvar clutch-column-width-step)
(defvar clutch-result-max-rows)
(defvar clutch--base-query)
(defvar clutch--buffer-error-details)
(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar clutch--last-query)
(defvar clutch--last-result-buffer)
(defvar clutch--pre-fullscreen-config)

(defvar-local clutch--live-view-buffer nil
  "Live value viewer buffer attached to the current source buffer, or nil.")

(defvar-local clutch--live-view-source-buffer nil
  "Source buffer followed by the current live value viewer.")
(defvar-local clutch--live-view-frozen nil
  "Non-nil when the live value viewer is frozen.")
(defvar-local clutch--live-view-source-cell-id nil
  "Last source cell identity rendered by a live value viewer.")

(declare-function clutch--column-border-position "clutch-ui" (cidx))
(declare-function clutch--column-info-message-string "clutch-ui" (info))
(declare-function clutch--column-info-string "clutch-ui" (cidx))
(declare-function clutch--debug-workflow-message "clutch-query" (message))
(declare-function clutch--dwim-bounds-at-point "clutch-query" ())
(declare-function clutch--ensure-point-visible-horizontally "clutch-ui" ())
(declare-function clutch--format-value "clutch-ui" (val))
(declare-function clutch--header-with-disconnect-badge "clutch-ui" (base))
(declare-function clutch--message-count "clutch-ui" (value))
(declare-function clutch--message-ident "clutch-ui" (value))
(declare-function clutch--message-keyword "clutch-ui" (value))
(declare-function clutch--message-literal "clutch-ui" (value))
(declare-function clutch--refresh-display "clutch-ui" ())
(declare-function clutch--refresh-footer-cursor "clutch-ui" ())
(declare-function clutch--refresh-footer-line "clutch-ui" ())
(declare-function clutch--refresh-header-line "clutch-ui" ())
(declare-function clutch--remember-query-error
                  "clutch-query"
                  (buffer connection op sql err &optional context diag))
(declare-function clutch--resolve-result-column-details "clutch-ui" (conn sql col-names))
(declare-function clutch--status-separator "clutch-ui" ())
(declare-function clutch--update-result-line-formats "clutch-ui" (rows visible-cols widths nw))
(declare-function clutch--value-to-literal "clutch-query" (val))
(declare-function clutch-preview-execution-sql "clutch-query" ())

;;;; Cell navigation

;;;###autoload
(defun clutch-result-next-cell ()
  "Move point to the next cell (right, then wrap to next row)."
  (interactive)
  (let ((start (point)))
    (goto-char (next-single-property-change (point) 'clutch-col-idx
                                            nil (point-max)))
    (if-let* ((m (text-property-search-forward 'clutch-col-idx nil
                                               (lambda (_val cur) cur))))
        (goto-char (prop-match-beginning m))
      (goto-char start)))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-prev-cell ()
  "Move point to the previous cell (left, then wrap to prev row)."
  (interactive)
  (let ((start (point)))
    (when-let* ((beg (previous-single-property-change
                      (1+ (point)) 'clutch-col-idx nil (point-min))))
      (goto-char beg))
    (if-let* ((m (text-property-search-backward 'clutch-col-idx nil
                                                (lambda (_val cur) cur))))
        (goto-char (prop-match-beginning m))
      (goto-char start)))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-down-cell ()
  "Move to the same column in the next row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx)))
    (clutch--goto-cell (1+ ridx) cidx))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-up-cell ()
  "Move to the same column in the previous row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx))
              ((> ridx 0)))
    (clutch--goto-cell (1- ridx) cidx))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;; Row selection (region-based)

(defun clutch-result--selected-row-indices ()
  "Return row indices for row-oriented batch operations.
Priority: region rows > current row."
  (or (when (use-region-p)
        (clutch-result--rows-in-region (region-beginning) (region-end)))
      (when-let* ((ridx (clutch-result--row-idx-at-line)))
        (list ridx))))

;;;###autoload
(defun clutch-result-discard-pending-at-point ()
  "Discard the staged change at point."
  (interactive)
  (let ((ridx (or (clutch-result--row-idx-at-line)
                  (clutch--user-error "No row at point")))
        (nrows (length clutch--result-rows)))
    (cond
     ((>= ridx nrows)
      (let ((iidx (- ridx nrows)))
        (setq clutch--pending-inserts
              (delq (nth iidx clutch--pending-inserts) clutch--pending-inserts))
        (clutch--refresh-display)
        (message "Staged insert discarded")))
     (t
      (let* ((table (clutch-result--detect-table))
             (row-identity (clutch-result--current-row-identity table))
             (display-rows (or clutch--filtered-rows clutch--result-rows))
             (row (nth ridx display-rows))
             (cidx (clutch--col-idx-at-point))
             (identity-vec (when row-identity
                             (clutch-result--extract-row-identity-vec
                              row row-identity)))
             (edit-key (and identity-vec cidx (cons identity-vec cidx)))
             (was-edit (and edit-key
                            (cl-assoc edit-key clutch--pending-edits :test #'equal)))
             (was-delete (and identity-vec
                              (cl-find identity-vec clutch--pending-deletes
                                       :test #'equal))))
        (cond
         (was-edit
          (setq clutch--pending-edits
                (cl-remove edit-key clutch--pending-edits :test #'equal :key #'car))
          (clutch--replace-row-at-index ridx)
          (clutch--refresh-footer-line)
          (force-mode-line-update)
          (message "Staged edit discarded"))
         (was-delete
          (setq clutch--pending-deletes
                (cl-remove identity-vec clutch--pending-deletes :test #'equal))
          (clutch--replace-row-at-index ridx)
          (clutch--refresh-footer-line)
          (force-mode-line-update)
          (message "Staged deletion discarded"))
         (t
          (clutch--user-error "No staged change at point"))))))))

;;;; clutch-result-mode

(defvar clutch-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "C-c '") #'clutch-result-edit-cell)
    (define-key map (kbd "C-c C-c") #'clutch-result-commit)
    (define-key map "g" #'clutch-result-rerun)
    (define-key map "e" #'clutch-result-export)
    (define-key map "C" #'clutch-result-goto-column)
    (define-key map "n" #'clutch-result-down-cell)
    (define-key map "p" #'clutch-result-up-cell)
    (define-key map "N" #'clutch-result-next-page)
    (define-key map "P" #'clutch-result-prev-page)
    (define-key map (kbd "M->") #'clutch-result-last-page)
    (define-key map (kbd "M-<") #'clutch-result-first-page)
    (define-key map "#" #'clutch-result-count-total)
    (define-key map "A" #'clutch-result-aggregate)
    (define-key map "s" #'clutch-result-sort-by-column)
    (define-key map "S" #'clutch-result-sort-by-column-desc)
    (define-key map "c" #'clutch-result-copy-dispatch)
    (define-key map "v" #'clutch-result-view-value)
    (define-key map "V" #'clutch-result-live-view-value)
    (define-key map "|" #'clutch-result-shell-command-on-cell)
    (define-key map "?" #'clutch-result-column-info)
    (define-key map "W" #'clutch-result-apply-filter)
    (define-key map (kbd "RET") #'clutch-result-open-record)
    (define-key map "]" #'clutch-result-scroll-right)
    (define-key map "[" #'clutch-result-scroll-left)
    (define-key map "=" #'clutch-result-widen-column)
    (define-key map "-" #'clutch-result-narrow-column)
    (define-key map (kbd "C-c C-p") #'clutch-preview-execution-sql)
    (define-key map "f" #'clutch-result-fullscreen-toggle)
    (define-key map (kbd "C-c ?") #'clutch-result-dispatch)
    ;; Cell navigation
    (define-key map (kbd "TAB") #'clutch-result-next-cell)
    (define-key map (kbd "<backtab>") #'clutch-result-prev-cell)
    (define-key map (kbd "M-n") #'clutch-result-down-cell)
    (define-key map (kbd "M-p") #'clutch-result-up-cell)
    ;; n/p are down/up cell (special-mode convention); M-n/M-p are aliases
    ;; Client-side filter
    (define-key map "/" #'clutch-result-filter)
    ;; Delete / Insert
    (define-key map "d" #'clutch-result-delete-rows)
    (define-key map "i" #'clutch-result-insert-row)
    (define-key map "I" #'clutch-clone-row-to-insert)
    (define-key map (kbd "C-c C-k") #'clutch-result-discard-pending-at-point)
    map)
  "Keymap for `clutch-result-mode'.")

(defun clutch--position-indicator-parts (ridx cidx)
  "Return a formatted mode-line position string for RIDX and CIDX."
  (let* ((page-offset (or clutch--page-offset
                          (* clutch--page-current clutch-result-max-rows)))
         (global-row  (+ page-offset ridx))
         (rows        (or clutch--filtered-rows clutch--result-rows))
         (row-count   (length rows))
         (ncols       (length clutch--result-columns))
         (col-name    (when cidx (nth cidx clutch--result-columns)))
         (parts       nil))
    (push (format "R%d/%s C%d/%d"
                  (1+ global-row)
                  (if clutch--page-total-rows
                      (number-to-string clutch--page-total-rows)
                    (number-to-string row-count))
                  (if cidx (1+ cidx) 0) ncols)
          parts)
    (when col-name  (push (format "[%s]" col-name) parts))
    (push (format "pg %d" (1+ clutch--page-current)) parts)
    (when clutch--query-elapsed
      (push (clutch--format-elapsed clutch--query-elapsed) parts))
    (when clutch--filter-pattern
      (push (format "/:%s" clutch--filter-pattern) parts))
    (when clutch--where-filter
      (push (format "W:%s" clutch--where-filter) parts))
    (format " %s" (string-join parts (clutch--status-separator)))))

(defun clutch--update-position-indicator ()
  "Update mode-line with current cursor position in the result grid."
  (let ((cidx (clutch--col-idx-at-point))
        (ridx (get-text-property (point) 'clutch-row-idx)))
    (setq mode-line-position
          (when ridx (clutch--position-indicator-parts ridx cidx)))))

(defun clutch--update-row-highlight ()
  "Highlight the entire row under the cursor.
Reuses the existing overlay via `move-overlay' when possible."
  (let ((beg (line-beginning-position))
        (end (line-end-position)))
    (if (get-text-property (point) 'clutch-row-idx)
        (if (and clutch--row-overlay (overlay-buffer clutch--row-overlay))
            (move-overlay clutch--row-overlay beg end)
          (when clutch--row-overlay
            (delete-overlay clutch--row-overlay))
          (let ((ov (make-overlay beg end)))
            (overlay-put ov 'face 'hl-line)
            (overlay-put ov 'priority -1)
            (setq clutch--row-overlay ov)))
      (when clutch--row-overlay
        (delete-overlay clutch--row-overlay)
        (setq clutch--row-overlay nil)))))

(defun clutch--update-header-highlight ()
  "Highlight the header cell for the column under the cursor.
Rebuilds `header-line-format' with the active column highlighted.
Skips work for scroll commands that do not move point."
  (when (and clutch--column-widths
             (not (memq this-command
                        '(scroll-down-line scroll-up-line
                          scroll-down scroll-up
                          scroll-down-command scroll-up-command
                          mwheel-scroll))))
    (clutch--update-position-indicator)
    (clutch--update-row-highlight)
    (clutch--refresh-footer-cursor)
    (force-mode-line-update)
    (let ((cidx (clutch--col-idx-at-point)))
      (unless (eql cidx clutch--header-active-col)
        (setq clutch--header-active-col cidx)
        (clutch--refresh-header-line)))))

;;;###autoload
(define-derived-mode clutch-result-mode special-mode "clutch-result"
  "Mode for displaying database query results as one scrollable table.

\\<clutch-result-mode-map>
Navigate:
  \\[clutch-result-next-cell]	Next cell (Tab)
  \\[clutch-result-prev-cell]	Previous cell (S-Tab)
  \\[clutch-result-down-cell]	Down in same column
  \\[clutch-result-up-cell]	Up in same column
  \\[clutch-result-open-record]	Open record view for row
  \\[clutch-result-goto-column]	Jump to column by name
Pages:
  \\[clutch-result-next-page]	Next data page
  \\[clutch-result-prev-page]	Previous data page
Navigate (row):
  \\[clutch-result-down-cell]	Next row (same column)
  \\[clutch-result-up-cell]	Previous row (same column)
  \\[clutch-result-first-page]	First data page
  \\[clutch-result-last-page]	Last data page
  \\[clutch-result-count-total]	Query total row count
  \\[clutch-result-aggregate]	Aggregate current/selected column values
  \\[clutch-result-scroll-right]	Page right (snap to next column border)
  \\[clutch-result-scroll-left]	Page left (snap to previous column border)
Copy:
  \\[clutch-result-copy-dispatch]	Copy… (transient: choose format, -r to refine)
  \\[clutch-result-export]	Export all rows (copy/file)
  \\[clutch-preview-execution-sql]	Preview execution
Inspect:
  \\[clutch-result-view-value]	View current cell once
  \\[clutch-result-live-view-value]	Open live viewer that follows point
Edit:
  \\[clutch-result-edit-cell]	Edit / re-edit at point
  \\[clutch-result-commit]	Commit staged changes
  \\[clutch-result-apply-filter]	Apply WHERE filter
  \\[clutch-result-sort-by-column]	Sort ascending (SQL ORDER BY)
  \\[clutch-result-sort-by-column-desc]	Sort descending (SQL ORDER BY)
  \\[clutch-result-widen-column]	Widen column
  \\[clutch-result-narrow-column]	Narrow column
  \\[clutch-result-rerun]	Re-execute the query"
  (setq truncate-lines t)
  (hl-line-mode 1)
  (setq-local scroll-step 1)
  (setq-local hscroll-step 1)
  ;; Make mode-line use default background so footer renders cleanly
  (face-remap-add-relative 'mode-line :inherit 'default)
  (face-remap-add-relative 'mode-line-inactive :inherit 'default)
  (setq-local revert-buffer-function #'clutch-result--revert)
  (add-hook 'post-command-hook
            #'clutch--update-header-highlight nil t)
  (add-hook 'kill-buffer-hook #'clutch--result-buffer-cleanup nil t)
  (add-hook 'change-major-mode-hook #'clutch--result-buffer-cleanup nil t)
  (clutch--enable-window-size-hook))

;;;###autoload
(defun clutch-result-next-page ()
  "Go to the next data page."
  (interactive)
  (unless clutch--page-has-more
    (clutch--user-error "Already on last page"))
  (clutch--execute-page (1+ clutch--page-current)))

;;;###autoload
(defun clutch-result-prev-page ()
  "Go to the previous data page."
  (interactive)
  (when (<= clutch--page-current 0)
    (clutch--user-error "Already on first page"))
  (clutch--execute-page (1- clutch--page-current)))

;;;###autoload
(defun clutch-result-first-page ()
  "Go to the first data page."
  (interactive)
  (when (= clutch--page-current 0)
    (clutch--user-error "Already on first page"))
  (clutch--execute-page 0))

;;;###autoload
(defun clutch-result-last-page ()
  "Go to the last data page.
Triggers a COUNT(*) query if total rows are not yet known."
  (interactive)
  (unless clutch--page-total-rows
    (clutch-result-count-total))
  (when clutch--page-total-rows
    (let* ((page-size clutch-result-max-rows)
           (last-page (max 0 (1- (ceiling clutch--page-total-rows
                                           (float page-size)))))
           (last-offset (max 0 (- clutch--page-total-rows page-size))))
      (if (and (= clutch--page-current (truncate last-page))
               (= (or clutch--page-offset
                      (* clutch--page-current page-size))
                  last-offset))
          (clutch--user-error "Already on last page")
        (clutch--execute-page-at-offset last-offset (truncate last-page))))))

;;;###autoload
(defun clutch-result-count-total ()
  "Query the total row count for the current base query."
  (interactive)
  (let* ((conn clutch-connection)
         (base (clutch-result--effective-query)))
    (clutch--ensure-connection)
    (setq conn clutch-connection)
    (let* ((count-sql (clutch--build-count-sql base))
           (result (condition-case err
                       (clutch--run-db-query conn count-sql)
                     (clutch-db-error
                      (pcase-let ((`(,_message . ,summary)
                                   (clutch--remember-query-error
                                    (current-buffer) conn "count" count-sql err
                                    (list :generated-sql count-sql)
                                    (list :category "query" :op "count"))))
                        (clutch--user-error "%s"
                                            (clutch--debug-workflow-message
                                             (format "COUNT query error: %s"
                                                     summary)))))))
           (count-val (caar (clutch-db-result-rows result))))
      (setq-local clutch--page-total-rows
                  (if (numberp count-val) count-val
                    (string-to-number (format "%s" count-val))))
      (clutch--refresh-footer-line)
      (force-mode-line-update)
      (message "Total rows: %s"
               (clutch--message-count clutch--page-total-rows)))))

;;;###autoload
(defun clutch-result-rerun ()
  "Re-execute the last query that produced this result buffer."
  (interactive)
  (if-let* ((sql (clutch-result--effective-query)))
      (clutch--execute sql)
    (clutch--user-error "No query to re-execute")))

(defun clutch-result--revert (_ignore-auto _noconfirm)
  "Revert function for result buffer — re-executes the query."
  (clutch-result-rerun))


;;;; Sort

(defun clutch-result--sort (col-name descending)
  "Sort result rows by COL-NAME using SQL ORDER BY.
If DESCENDING, sort in descending order.
Re-executes from the first page."
  (unless clutch--result-columns
    (clutch--user-error "No result data"))
  (let* ((col-names clutch--result-columns)
         (idx (cl-position col-name col-names :test #'string=)))
    (unless idx
      (clutch--user-error "Column %s not found" col-name))
    (let ((direction (if descending "DESC" "ASC")))
      (setq clutch--sort-column col-name)
      (setq clutch--sort-descending descending)
      (setq clutch--order-by (cons col-name direction))
      (setq clutch--page-current 0)
      (clutch--execute-page 0)
      (message "Sorted by %s %s"
               (clutch--message-ident col-name)
               (clutch--message-keyword direction)))))

(defun clutch-result--read-column ()
  "Read a column name, defaulting to column at point."
  (let* ((col-names clutch--result-columns)
         (cidx (get-text-property (point) 'clutch-col-idx))
         (default (when cidx (nth cidx col-names))))
    (completing-read (if default
                         (format "Sort by column (default %s): " default)
                       "Sort by column: ")
                     col-names nil t nil nil default)))

;;;###autoload
(defun clutch-result-sort-by-column ()
  "Sort results by a column.
If the column is already sorted, toggle the direction."
  (interactive)
  (let* ((col-name (clutch-result--read-column))
         (descending (if (and clutch--sort-column
                              (string= col-name clutch--sort-column))
                         (not clutch--sort-descending)
                       nil)))
    (clutch-result--sort col-name descending)))

;;;###autoload
(defun clutch-result-sort-by-column-desc ()
  "Sort results descending by a column."
  (interactive)
  (clutch-result--sort (clutch-result--read-column) t))

;;;; WHERE filtering

(defun clutch--where-filter-column-expression (column condition)
  "Return a WHERE fragment for COLUMN and user-entered CONDITION."
  (let ((expr (if (string-match-p
                  "\\`\\(?:[=<>!]\\|IN\\b\\|IS\\b\\|NOT\\b\\|LIKE\\b\\|BETWEEN\\b\\)"
                  (upcase condition))
                  condition
                (concat "= " condition))))
    (format "%s %s" column expr)))

(defun clutch--read-where-filter (current columns default-col)
  "Read a WHERE filter string from CURRENT state, COLUMNS, and DEFAULT-COL."
  (if (and columns (not current))
      (let* ((col (completing-read
                   (if default-col
                       (format "Filter column (default %s, empty for raw): "
                               default-col)
                     "Filter column (empty for raw): ")
                   columns nil nil nil nil default-col))
             (condition
              (string-trim
               (read-string
                (if (string-empty-p col)
                    "WHERE filter (e.g., age > 18): "
                  (format "%s (e.g., 42, 'foo', > 18, IS NULL): " col))))))
        (cond
         ((string-empty-p condition) "")
         ((string-empty-p col) condition)
         (t (clutch--where-filter-column-expression col condition))))
    (string-trim
     (read-string
      (if current
          (format "WHERE filter (current: %s, empty to clear): " current)
        "WHERE filter (e.g., age > 18): ")
      nil nil current))))

;;;###autoload
(defun clutch-result-apply-filter ()
  "Apply or clear a WHERE filter on the current result query.
When columns are available, prompts to pick a column first (defaulting
to the column at point), then asks for the condition.  Enter an empty
string at the column prompt to write a raw WHERE clause; enter an
empty string at the condition prompt to clear the filter."
  (interactive)
  (unless clutch--last-query
    (clutch--user-error "No query to filter"))
  (let* ((base (or clutch--base-query
                   clutch--last-query))
         (current clutch--where-filter)
         (columns (mapcar (lambda (i) (nth i clutch--result-columns))
                          (clutch--visible-columns)))
         (default-col (and columns clutch--header-active-col
                           (nth clutch--header-active-col columns)))
         (input (clutch--read-where-filter current columns default-col))
         (filtered-sql (unless (string-empty-p input)
                         (clutch--apply-where base input))))
    (clutch--execute (or filtered-sql base)
                     clutch-connection)
    (setq clutch--base-query (when filtered-sql base))
    (setq clutch--where-filter (when filtered-sql input))
    (message (if filtered-sql
                 (format "Filter applied: WHERE %s" input)
               "Filter cleared"))))

;;;; Client-side filter

(defun clutch-result--apply-filter (input)
  "Apply INPUT as a client-side substring filter and re-render."
  (let* ((pattern  (downcase input))
         (col-indices (clutch--visible-columns))
         (matching (cl-loop for row in clutch--result-rows
                            when (cl-some
                                  (lambda (val)
                                    (and val
                                         (string-match-p
                                          (regexp-quote pattern)
                                          (downcase (clutch--format-value val)))))
                                  (mapcar (lambda (i) (nth i row))
                                          col-indices))
                            collect row)))
    (setq clutch--filter-pattern input
          clutch--filtered-rows matching
          clutch--marked-rows nil)
    (clutch--render-result)
    (message "Filter: %s/%s rows match %s"
             (clutch--message-count (length matching))
             (clutch--message-count (length clutch--result-rows))
             (clutch--message-literal (format "\"%s\"" input)))))

;;;###autoload
(defun clutch-result-filter ()
  "Filter visible rows by substring match (client-side).
Prompts for a pattern; enter empty string to clear."
  (interactive)
  (let ((input (string-trim
                (read-string
                 (if clutch--filter-pattern
                     (format "Filter (current: %s, empty to clear): "
                             clutch--filter-pattern)
                   "Filter (empty to clear): ")))))
    (if (string-empty-p input)
        (progn
          (setq clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--marked-rows nil)
          (clutch--render-result)
          (message "Filter cleared"))
      (clutch-result--apply-filter input))))

;;;; Yank cell / Copy row as INSERT

;;;; Refine minor mode

(defvar clutch-refine-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "m") #'clutch-refine-toggle-row)
    (define-key map (kbd "x") #'clutch-refine-toggle-col)
    (define-key map (kbd "RET") #'clutch-refine-confirm)
    (define-key map (kbd "C-g") #'clutch-refine-cancel)
    map)
  "Keymap for `clutch-refine-mode'.")

;;;###autoload
(define-minor-mode clutch-refine-mode
  "Transient minor mode for visually refining a rectangular selection.
\\<clutch-refine-mode-map>
\\[clutch-refine-toggle-row]: toggle row exclusion at point
\\[clutch-refine-toggle-col]: toggle column exclusion at point
\\[clutch-refine-confirm]: confirm and execute
\\[clutch-refine-cancel]: cancel"
  :keymap clutch-refine-mode-map
  :lighter " [REFINE: m=row x=col RET=ok C-g=cancel]"
  (unless clutch-refine-mode
    (clutch-refine--clear-overlays)))

(defun clutch-refine--clear-overlays ()
  "Delete all overlays created during refine mode."
  (mapc #'delete-overlay clutch--refine-overlays)
  (setq clutch--refine-overlays nil))

(defun clutch-refine--make-overlay (beg end face priority &optional tag-prop tag-val)
  "Create a refine overlay from BEG to END with FACE and PRIORITY.
Optionally tag with TAG-PROP = TAG-VAL for incremental removal."
  (let ((ov (make-overlay beg end)))
    (overlay-put ov 'face face)
    (overlay-put ov 'priority priority)
    (when tag-prop (overlay-put ov tag-prop tag-val))
    (push ov clutch--refine-overlays)))

(defun clutch-refine--init-overlays ()
  "Apply layer-1 selection overlays for the rect.  Called once on refine start."
  (clutch-refine--clear-overlays)
  (save-excursion
    (pcase-let ((`(,row-indices . ,col-indices) clutch--refine-rect))
      (dolist (cidx col-indices)
        (goto-char (point-min))
        (cl-loop for match = (text-property-search-forward 'clutch-col-idx cidx #'eql)
                 while match
                 do (let ((beg (prop-match-beginning match))
                          (end (prop-match-end match)))
                      (when (memq (get-text-property beg 'clutch-row-idx) row-indices)
                        (clutch-refine--make-overlay beg end 'secondary-selection 0))))))))

(defun clutch-refine--add-row-exclusion (ridx)
  "Add exclusion overlays for row RIDX within the rect's columns.
Finds the row's line first, then scans only that line — O(buffer-to-row + line)."
  (save-excursion
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-row-idx ridx #'eql)))
      (goto-char (prop-match-beginning match))
      (let ((bol (line-beginning-position))
            (eol (line-end-position))
            (col-set (cdr clutch--refine-rect)))
        (cl-loop with p = bol
                 while (< p eol)
                 do (let ((cidx (get-text-property p 'clutch-col-idx)))
                      (if (and cidx (memq cidx col-set))
                          (let ((end (or (next-single-property-change
                                         p 'clutch-col-idx nil eol)
                                        eol)))
                            (clutch-refine--make-overlay
                             p end '(:inherit shadow :strike-through t) 1
                             'clutch-refine-row ridx)
                            (setq p end))
                        (setq p (1+ p)))))))))

(defun clutch-refine--remove-row-exclusion (ridx)
  "Remove exclusion overlays tagged with RIDX."
  (setq clutch--refine-overlays
        (cl-loop for ov in clutch--refine-overlays
                 if (eql (overlay-get ov 'clutch-refine-row) ridx)
                 do (delete-overlay ov)
                 else collect ov)))

(defun clutch-refine--add-col-exclusion (cidx)
  "Add exclusion overlays for column CIDX (header + rect rows).
Scans buffer once for this column — O(buffer)."
  (save-excursion
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-header-col cidx #'eql)))
      (clutch-refine--make-overlay (prop-match-beginning match) (prop-match-end match)
                                   '(:inherit shadow :strike-through t) 1
                                   'clutch-refine-col cidx))
    (goto-char (point-min))
    (cl-loop for match = (text-property-search-forward 'clutch-col-idx cidx #'eql)
             while match
             do (let ((beg (prop-match-beginning match))
                      (end (prop-match-end match)))
                  (when (memq (get-text-property beg 'clutch-row-idx)
                              (car clutch--refine-rect))
                    (clutch-refine--make-overlay beg end
                                                 '(:inherit shadow :strike-through t) 1
                                                 'clutch-refine-col cidx))))))

(defun clutch-refine--remove-col-exclusion (cidx)
  "Remove exclusion overlays tagged with CIDX."
  (setq clutch--refine-overlays
        (cl-loop for ov in clutch--refine-overlays
                 if (eql (overlay-get ov 'clutch-refine-col) cidx)
                 do (delete-overlay ov)
                 else collect ov)))

;;;###autoload
(defun clutch-refine-toggle-row ()
  "Toggle exclusion of the row at point."
  (interactive)
  (if-let* ((ridx (clutch-result--row-idx-at-line)))
      (if (memq ridx (car clutch--refine-rect))
          (if (memq ridx clutch--refine-excluded-rows)
              (progn
                (setq clutch--refine-excluded-rows
                      (delq ridx clutch--refine-excluded-rows))
                (clutch-refine--remove-row-exclusion ridx)
                (message "Row %s %s"
                         (clutch--message-count (1+ ridx))
                         (clutch--message-keyword "included")))
            (push ridx clutch--refine-excluded-rows)
            (clutch-refine--add-row-exclusion ridx)
            (message "Row %s %s"
                     (clutch--message-count (1+ ridx))
                     (clutch--message-keyword "excluded")))
        (clutch--user-error "Row not in selection"))
    (clutch--user-error "No row at point")))

;;;###autoload
(defun clutch-refine-toggle-col ()
  "Toggle exclusion of the column at point."
  (interactive)
  (if-let* ((cidx (or (get-text-property (point) 'clutch-col-idx)
                      (get-text-property (point) 'clutch-header-col))))
      (if (memq cidx (cdr clutch--refine-rect))
          (if (memq cidx clutch--refine-excluded-cols)
              (progn
                (setq clutch--refine-excluded-cols
                      (delq cidx clutch--refine-excluded-cols))
                (clutch-refine--remove-col-exclusion cidx)
                (message "Column %s %s"
                         (clutch--message-ident
                          (format "\"%s\"" (nth cidx clutch--result-columns)))
                         (clutch--message-keyword "included")))
            (push cidx clutch--refine-excluded-cols)
            (clutch-refine--add-col-exclusion cidx)
            (message "Column %s %s"
                     (clutch--message-ident
                      (format "\"%s\"" (nth cidx clutch--result-columns)))
                     (clutch--message-keyword "excluded")))
        (clutch--user-error "Column not in selection"))
    (clutch--user-error "No column at point")))

;;;###autoload
(defun clutch-refine-confirm ()
  "Confirm the current refine selection and execute the callback."
  (interactive)
  (let* ((row-indices (cl-loop for ridx in (car clutch--refine-rect)
                               unless (memq ridx clutch--refine-excluded-rows)
                               collect ridx))
         (col-indices (cl-loop for cidx in (cdr clutch--refine-rect)
                               unless (memq cidx clutch--refine-excluded-cols)
                               collect cidx)))
    (unless row-indices
      (clutch--user-error "No rows left after exclusion"))
    (unless col-indices
      (clutch--user-error "No columns left after exclusion"))
    (let ((cb clutch--refine-callback)
          (final-rect (cons row-indices col-indices)))
      (clutch-refine--clear-overlays)
      (clutch-refine-mode -1)
      (setq mode-line-format clutch--refine-saved-mode-line
            clutch--refine-rect nil
            clutch--refine-excluded-rows nil
            clutch--refine-excluded-cols nil
            clutch--refine-callback nil
            clutch--refine-saved-mode-line nil)
      (funcall cb final-rect))))

;;;###autoload
(defun clutch-refine-cancel ()
  "Cancel refine mode without executing the callback."
  (interactive)
  (clutch-refine--clear-overlays)
  (clutch-refine-mode -1)
  (setq mode-line-format clutch--refine-saved-mode-line
        clutch--refine-rect nil
        clutch--refine-excluded-rows nil
        clutch--refine-excluded-cols nil
        clutch--refine-callback nil
        clutch--refine-saved-mode-line nil)
  (message "Refine cancelled"))

(defun clutch-result--start-refine (rect callback)
  "Enter refine mode for RECT with CALLBACK called with final rect on confirm.
RECT is (ROW-INDICES . COL-INDICES)."
  (deactivate-mark)
  (setq-local clutch--refine-rect rect
              clutch--refine-excluded-rows nil
              clutch--refine-excluded-cols nil
              clutch--refine-callback callback
              clutch--refine-saved-mode-line mode-line-format
              mode-line-format
              (concat
               (propertize " " 'display '(space :align-to 0))
               (propertize "REFINE  " 'face 'font-lock-warning-face)
               (propertize "m" 'face 'font-lock-keyword-face)
               (propertize " row   " 'face 'font-lock-comment-face)
               (propertize "x" 'face 'font-lock-keyword-face)
               (propertize " col   " 'face 'font-lock-comment-face)
               (propertize "RET" 'face 'font-lock-keyword-face)
               (propertize " confirm   " 'face 'font-lock-comment-face)
               (propertize "C-g" 'face 'font-lock-keyword-face)
               (propertize " cancel" 'face 'font-lock-comment-face)))
  (clutch-refine-mode 1)
  (clutch-refine--init-overlays))

(defun clutch-result-copy (format &optional rect)
  "Unified copy entry point for result buffer.
FORMAT is one of symbols: `tsv', `csv', `insert', `update'.
When RECT is non-nil, use it as precomputed rectangle bounds.  If region
is active, copy rectangle bounds from region endpoints.
Otherwise, copy the current cell."
  (pcase format
    ('tsv
     (if rect
         (clutch-result--yank-rectangle-cells rect)
       (if (use-region-p)
           (clutch-result--yank-region-cells)
         (pcase-let* ((`(,_ridx ,_cidx ,val) (or (clutch-result--cell-at-point)
                                               (clutch--user-error "No cell at point"))))
           (clutch-result--yank-cell-value val)))))
    ('csv
     (clutch-result--copy-rows-as-csv rect))
    ('insert
     (clutch-result--copy-rows-as-insert rect))
    ('update
     (clutch-result--copy-rows-as-update rect))
    (_
     (clutch--user-error "Unsupported copy format: %s" format))))

(defun clutch-result--copy-fmt (fmt)
  "Copy in FMT, entering refine mode first if --refine switch is set."
  (if (transient-arg-value "--refine" (transient-args 'clutch-result-copy-dispatch))
      (progn
        (unless (use-region-p)
          (clutch--user-error "Set a region before using refine mode"))
        (clutch-result--start-refine
         (clutch-result--region-rectangle-indices)
         (lambda (final-rect) (clutch-result-copy fmt final-rect))))
    (clutch-result-copy fmt)))

;;;###autoload
(defun clutch-result-copy-tsv ()
  "Copy as TSV."
  (interactive)
  (clutch-result--copy-fmt 'tsv))

;;;###autoload
(defun clutch-result-copy-csv ()
  "Copy as CSV with header."
  (interactive)
  (clutch-result--copy-fmt 'csv))

;;;###autoload
(defun clutch-result-copy-insert ()
  "Copy as INSERT statements."
  (interactive)
  (clutch-result--copy-fmt 'insert))

;;;###autoload
(defun clutch-result-copy-update ()
  "Copy as UPDATE statements."
  (interactive)
  (clutch-result--copy-fmt 'update))

(transient-define-prefix clutch-result-copy-dispatch ()
  "Copy result buffer data.
Enable --refine to exclude rows/columns interactively before copying
\(requires an active region set with C-x SPC or mouse)."
  ["Options"
   :pad-keys t
   ("-r" "Exclude rows/cols interactively (needs region)" "--refine")]
  ["Copy as"
   :pad-keys t
   ("t" "TSV"             clutch-result-copy-tsv)
   ("c" "CSV with header" clutch-result-copy-csv)
   ("i" "INSERT"          clutch-result-copy-insert)
   ("u" "UPDATE"          clutch-result-copy-update)])


(defun clutch--agent-context-sql-from-bounds (bounds)
  "Return trimmed SQL text from BOUNDS."
  (pcase-let ((`(,beg . ,end) bounds))
    (string-trim (buffer-substring-no-properties beg end))))

(defun clutch--agent-context-dwim-sql ()
  "Return current SQL for agent context from `clutch-mode'."
  (let ((sql (clutch--agent-context-sql-from-bounds
              (clutch--dwim-bounds-at-point))))
    (if (not (string-empty-p sql))
        sql
      (save-excursion
        (skip-chars-backward " \t\n\r;")
        (when (and (not (bobp))
                   (eq (char-after) ?\;))
          (backward-char))
        (when (not (eobp))
          (clutch--agent-context-sql-from-bounds
           (clutch--dwim-bounds-at-point)))))))

(defun clutch--agent-context-current-sql ()
  "Return SQL text that should anchor an external agent context export."
  (let ((sql
         (cond
          ((derived-mode-p 'clutch-result-mode)
           (clutch-result--effective-query))
          ((use-region-p)
           (buffer-substring-no-properties (region-beginning) (region-end)))
          ((derived-mode-p 'clutch-mode)
           (clutch--agent-context-dwim-sql))
          (t clutch--last-query))))
    (when (stringp sql)
      (string-trim sql))))

(defun clutch--agent-context-tables (sql)
  "Return table names that should be documented for SQL."
  (let ((tables (clutch--statement-table-identifiers-in-sql sql)))
    (if (and (derived-mode-p 'clutch-result-mode)
             clutch--result-source-table
             (not (clutch--identifier-match clutch--result-source-table
                                            tables)))
        (append tables (list clutch--result-source-table))
      tables)))

(defun clutch--agent-context-inline-value (value)
  "Format VALUE as one compact line for copied agent context."
  (let ((text (replace-regexp-in-string
               "[\n\r\t ]+" " "
               (clutch--format-value value))))
    (setq text (string-trim text))
    (if (> (string-width text) clutch-agent-context-max-cell-width)
        (truncate-string-to-width text clutch-agent-context-max-cell-width
                                  nil nil "...")
      text)))

(defun clutch--agent-context-table-entry (table)
  "Return a table object entry plist for TABLE."
  (list :name table :type "TABLE"))

(defun clutch--agent-context-table-section (conn table)
  "Return the Markdown context section for TABLE on CONN."
  (concat
   "## Table: " table "\n\n"
   (condition-case err
       (concat "```text\n"
               (clutch--object-describe-text
                conn
                (clutch--agent-context-table-entry table))
               "\n```\n\n")
     (error
      (format "- Table metadata unavailable: %s\n\n"
              (error-message-string err))))))

(defun clutch--agent-context-row-list (row)
  "Return ROW as a list."
  (cond
   ((vectorp row)
    (cl-loop for i below (length row) collect (aref row i)))
   ((listp row) row)
   (t (list row))))

(defun clutch--agent-context-format-tsv-line (values)
  "Format VALUES as one TSV line for copied agent context."
  (mapconcat #'clutch--agent-context-inline-value values "\t"))

(defun clutch--agent-context-row-values (row col-indices)
  "Return ROW values for COL-INDICES."
  (let ((values (clutch--agent-context-row-list row)))
    (mapcar (lambda (i) (nth i values)) col-indices)))

(defun clutch--agent-context-sql-match-p (a b)
  "Return non-nil when SQL strings A and B refer to the same exported query."
  (let ((a (and (stringp a) (clutch--sql-normalize-for-rewrite a)))
        (b (and (stringp b) (clutch--sql-normalize-for-rewrite b))))
    (and a b (string= a b))))

(defun clutch--agent-context-result-buffer (sql)
  "Return the result buffer whose rows should accompany SQL, or nil."
  (cond
   ((derived-mode-p 'clutch-result-mode)
    (current-buffer))
   ((and (buffer-live-p clutch--last-result-buffer)
         (with-current-buffer clutch--last-result-buffer
           (and (derived-mode-p 'clutch-result-mode)
                clutch--result-columns
                (clutch--agent-context-sql-match-p
                 sql (clutch-result--effective-query)))))
    clutch--last-result-buffer)))

(defun clutch--agent-context-result-sample (result-buffer)
  "Return a Markdown section with the sample rows from RESULT-BUFFER, or nil."
  (when (and (buffer-live-p result-buffer)
             (with-current-buffer result-buffer
               (and (derived-mode-p 'clutch-result-mode)
                    clutch--result-columns)))
    (with-current-buffer result-buffer
      (let* ((rows (or clutch--filtered-rows clutch--result-rows))
             (col-indices (clutch--visible-columns))
             (columns (mapcar (lambda (i) (nth i clutch--result-columns))
                              col-indices))
             (max-rows (max 0 clutch-agent-context-max-result-rows))
             (sample (cl-subseq rows 0 (min max-rows (length rows)))))
        (when columns
          (concat
           "## Result sample\n\n"
           (format "Showing %d of %d visible rows from the latest matching result buffer.\n\n"
                   (length sample) (length rows))
           "```text\n"
           (clutch--agent-context-format-tsv-line columns)
           "\n"
           (mapconcat
            (lambda (row)
              (clutch--agent-context-format-tsv-line
               (clutch--agent-context-row-values row col-indices)))
            sample
            "\n")
           (when sample "\n")
           "```\n\n"))))))

(defun clutch--agent-context-connection-section (conn)
  "Return a Markdown connection section for CONN."
  (let ((schema (clutch-db-current-schema conn))
        (database (clutch-db-database conn)))
    (concat "## Connection\n\n"
            (format "- Backend: %s\n" (clutch-db-display-name conn))
            (format "- Connection: %s\n" (clutch--connection-key conn))
            (format "- Database: %s\n" (or database "none"))
            (format "- Current schema/database: %s\n\n" (or schema database "none")))))

(defun clutch--agent-context-text (conn sql &optional tables)
  "Return Markdown context text for CONN, SQL, and optional TABLES."
  (let* ((tables (or tables (clutch--agent-context-tables sql)))
         (result-buffer (clutch--agent-context-result-buffer sql))
         (sample (clutch--agent-context-result-sample result-buffer)))
    (with-temp-buffer
      (insert "# Clutch database context\n\n")
      (insert (clutch--agent-context-connection-section conn))
      (insert "## SQL\n\n```sql\n" sql "\n```\n\n")
      (insert "## Referenced tables\n\n")
      (if tables
          (insert (mapconcat (lambda (table) (concat "- " table)) tables "\n")
                  "\n\n")
        (insert "- None detected\n\n"))
      (when sample
        (insert sample))
      (dolist (table tables)
        (insert (clutch--agent-context-table-section conn table)))
      (string-trim-right (buffer-string)))))

;;;###autoload
(defun clutch-copy-context-for-agent ()
  "Copy current SQL, result sample, and related metadata for an external agent.
The copied Markdown is intended for tools such as ChatGPT, Claude, or
DeepSeek.  The command uses the current connection's metadata APIs and the
latest matching result buffer; it does not execute the SQL being copied."
  (interactive)
  (clutch--ensure-connection)
  (let ((sql (clutch--agent-context-current-sql)))
    (when (or (null sql) (string-empty-p sql))
      (clutch--user-error "No SQL context to copy"))
    (let* ((tables (clutch--agent-context-tables sql))
           (text (clutch--agent-context-text clutch-connection sql tables)))
      (kill-new text)
      (message "Copied context for %s table%s"
               (clutch--message-count (length tables))
               (if (= (length tables) 1) "" "s")))))

(defun clutch-result--yank-cell-value (val)
  "Copy VAL to kill ring and show a compact preview message."
  (let ((text (clutch--format-value val)))
    (kill-new text)
    (message "Copied: %s"
             (clutch--message-literal
              (truncate-string-to-width text 60 nil nil "…")))))

(defun clutch-result--cell-at-or-near (pos)
  "Return cell triple at POS, or nearest cell on the same line."
  (or (clutch-result--cell-at pos)
      (save-excursion
        (goto-char pos)
        (let ((bol (line-beginning-position))
              (eol (line-end-position)))
          (or (cl-loop for p downfrom (max bol (1- pos)) to bol
                       thereis (clutch-result--cell-at p))
              (cl-loop for p from (min eol (1+ pos)) to eol
                       thereis (clutch-result--cell-at p)))))))

(defun clutch-result--region-rectangle-bounds ()
  "Return active region bounds as (ROW-INDICES . COL-INDICES)."
  (pcase-let* ((`(,r1 ,c1 ,_v1) (or (clutch-result--cell-at-or-near
                                     (region-beginning))
                                    (clutch--user-error "No cell at region start")))
               (`(,r2 ,c2 ,_v2) (or (clutch-result--cell-at-or-near
                                      (max (point-min) (1- (region-end))))
                                    (clutch--user-error "No cell at region end")))
               (row-min (min r1 r2))
               (row-max (max r1 r2))
               (col-min (min c1 c2))
               (col-max (max c1 c2)))
    (cons (cl-loop for ridx from row-min to row-max collect ridx)
          (cl-loop for cidx from col-min to col-max collect cidx))))

(defun clutch-result--cells-for-indices (row-indices col-indices)
  "Return cell triples for ROW-INDICES and COL-INDICES."
  (cl-loop for ridx in row-indices
           append
           (let ((row (nth ridx clutch--result-rows)))
             (cl-loop for cidx in col-indices
                      collect (list ridx cidx (nth cidx row))))))

(defun clutch-result--region-cells ()
  "Return cells in active region as a rectangle of (ROW-IDX COL-IDX VALUE)."
  (pcase-let ((`(,row-indices . ,col-indices)
               (clutch-result--region-rectangle-bounds)))
    (clutch-result--cells-for-indices row-indices col-indices)))

(defun clutch-result--region-rectangle-indices ()
  "Return rectangle row/column indices from active region.
Result is a cons cell (ROW-INDICES . COL-INDICES)."
  (unless (use-region-p)
    (clutch--user-error "Set a region to select rows and columns"))
  (clutch-result--region-rectangle-bounds))

(defun clutch-result--cells-tsv-text (cells)
  "Return TSV text for CELLS grouped by row index."
  (let (lines
        current-row
        current-values)
    (dolist (cell cells)
      (pcase-let ((`(,ridx ,_cidx ,val) cell))
        (if (or (null current-row) (= ridx current-row))
            (progn
              (setq current-row ridx)
              (push (clutch--format-value val) current-values))
          (push (string-join (nreverse current-values) "\t") lines)
          (setq current-row ridx
                current-values (list (clutch--format-value val))))))
    (when current-values
      (push (string-join (nreverse current-values) "\t") lines))
    (string-join (nreverse lines) "\n")))

(defun clutch-result--copy-cells-as-tsv (cells &optional deactivate)
  "Copy CELLS as TSV and report the copied cell count.
When DEACTIVATE is non-nil, deactivate the active region after copying."
  (unless cells
    (clutch--user-error "No cells in region"))
  (kill-new (clutch-result--cells-tsv-text cells))
  (when deactivate
    (deactivate-mark))
  (message "Copied %s cell%s from region"
           (clutch--message-count (length cells))
           (if (= (length cells) 1) "" "s")))

(defun clutch-result--yank-region-cells ()
  "Copy cell values from region as TSV-like text."
  (unless (use-region-p)
    (clutch--user-error "Set a region to copy multiple cells"))
  (clutch-result--copy-cells-as-tsv (clutch-result--region-cells) t))

(defun clutch-result--yank-rectangle-cells (rect)
  "Copy cells from RECT as TSV-like text."
  (pcase-let* ((`(,row-indices . ,col-indices) rect)
               (cells (clutch-result--cells-for-indices
                       row-indices col-indices)))
    (clutch-result--copy-cells-as-tsv cells)))

(defun clutch-result--aggregate-target (&optional rect)
  "Return aggregate target as (ROW-INDICES COL-INDICES).
When RECT is non-nil, use it directly.  With region: use all selected columns.
Without region: use current cell."
  (if (or rect (use-region-p))
      (pcase-let* ((`(,row-indices . ,col-indices)
                    (or rect (clutch-result--region-rectangle-indices))))
        (unless col-indices
          (clutch--user-error "No columns selected for aggregate"))
        (list row-indices col-indices))
    (pcase-let* ((`(,ridx ,cidx ,_val) (or (clutch-result--cell-at-point)
                                           (clutch--user-error "No cell at point"))))
      (list (list ridx) (list cidx)))))

(defun clutch-result--parse-number (val)
  "Parse VAL into a number or return nil."
  (cond
   ((numberp val) val)
   ((stringp val)
    (let ((s (string-trim val)))
      (when (and (not (string-empty-p s))
                 (string-match-p
                  "\\`[+-]?\\(?:[0-9]+\\(?:\\.[0-9]*\\)?\\|\\.[0-9]+\\)\\'" s))
        (string-to-number s))))
   (t nil)))

(defun clutch-result--compute-aggregate (row-indices col-indices)
  "Compute aggregate stats for ROW-INDICES across COL-INDICES."
  (let* ((rows (length row-indices))
         (cells (* rows (length col-indices)))
         (count 0)
        (sum 0.0)
        min-val
        max-val)
    (dolist (ridx row-indices)
      (let ((row (nth ridx clutch--result-rows)))
        (dolist (cidx col-indices)
          (let ((num (clutch-result--parse-number (nth cidx row))))
            (when num
              (setq count (1+ count)
                    sum (+ sum num))
              (setq min-val (if min-val (min min-val num) num))
              (setq max-val (if max-val (max max-val num) num)))))))
    (list :rows rows
          :cells cells
          :count count
          :skipped (- cells count)
          :sum sum
          :avg (if (> count 0) (/ sum count) 0)
          :min min-val
          :max max-val)))

(defun clutch-result--format-aggregate-summary (label stats)
  "Return aggregate summary string for LABEL with STATS."
  (let ((count (plist-get stats :count)))
    (if (> count 0)
        (format "Aggregate [%s]: sum=%g avg=%g min=%g max=%g [rows=%d cells=%d skipped=%d]"
                label
                (plist-get stats :sum)
                (plist-get stats :avg)
                (plist-get stats :min)
                (plist-get stats :max)
                (plist-get stats :rows)
                (plist-get stats :cells)
                (plist-get stats :skipped))
      (format "Aggregate [%s]: n/a [rows=%d cells=%d skipped=%d]"
              label
              (plist-get stats :rows)
              (plist-get stats :cells)
              (plist-get stats :skipped)))))

(defun clutch-result--do-aggregate (rect)
  "Perform aggregate on RECT (ROW-INDICES . COL-INDICES) and update display."
  (pcase-let* ((`(,row-indices . ,col-indices) rect)
               (label (if (= (length col-indices) 1)
                          (nth (car col-indices) clutch--result-columns)
                        "selection"))
               (stats (clutch-result--compute-aggregate row-indices col-indices))
               (summary (clutch-result--format-aggregate-summary label stats)))
    (setq-local clutch--aggregate-summary
                (list :label label
                      :rows (plist-get stats :rows)
                      :cells (plist-get stats :cells)
                      :skipped (plist-get stats :skipped)
                      :sum (plist-get stats :sum)
                      :avg (plist-get stats :avg)
                      :min (plist-get stats :min)
                      :max (plist-get stats :max)
                      :count (plist-get stats :count)))
    (clutch--refresh-display)
    (kill-new summary)))

;;;###autoload
(defun clutch-result-aggregate (&optional refine)
  "Aggregate numeric values from selected columns or current cell.
With prefix arg REFINE and an active region, enter visual refine mode."
  (interactive "P")
  (if (and refine (use-region-p))
      (clutch-result--start-refine
       (clutch-result--region-rectangle-indices)
       #'clutch-result--do-aggregate)
    (pcase-let* ((`(,row-indices ,col-indices)
                  (clutch-result--aggregate-target nil)))
      (clutch-result--do-aggregate (cons row-indices col-indices)))))

(defun clutch--render-view-buffer (buffer val setup-fn)
  "Render string VAL into BUFFER, then call SETUP-FN there.
SETUP-FN is called with no args; it should activate a mode and may also
reformat the current buffer."
  (with-current-buffer buffer
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert val)
      (funcall setup-fn)
      (goto-char (point-min))
      (setq buffer-read-only t)))
  buffer)

(defun clutch--view-in-buffer (val buf-name setup-fn)
  "Insert string VAL into BUF-NAME, call SETUP-FN, then pop to it."
  (pop-to-buffer
   (clutch--render-view-buffer (get-buffer-create buf-name) val setup-fn)))

(defun clutch--setup-json-view-buffer ()
  "Enable JSON display mode for the current buffer."
  (condition-case nil (json-pretty-print-buffer) (error nil))
  (unless (and (fboundp 'json-ts-mode)
               (condition-case nil
                   (progn (json-ts-mode) t)
                 (error nil)))
    (cond ((fboundp 'json-mode) (json-mode))
          ((fboundp 'js-mode)   (js-mode))
          (t                    (special-mode)))))

(defun clutch--decode-xml-char-refs-string (text)
  "Return TEXT with numeric XML character references decoded for display."
  (replace-regexp-in-string
   "&#\\(x[[:xdigit:]]+\\|X[[:xdigit:]]+\\|[[:digit:]]+\\);"
   (lambda (ref)
     (let* ((body (substring ref 2 -1))
            (hex (memq (aref body 0) '(?x ?X)))
            (code (string-to-number (if hex (substring body 1) body)
                                    (if hex 16 10)))
            (char (and (> code 0) (decode-char 'ucs code))))
       (if char
           (char-to-string char)
         ref)))
   text t t))

(defun clutch--decode-xml-char-refs-in-buffer ()
  "Decode numeric XML character references in the current buffer for display."
  (let ((decoded (clutch--decode-xml-char-refs-string (buffer-string))))
    (unless (equal decoded (buffer-string))
      (erase-buffer)
      (insert decoded))))

(defun clutch--setup-xml-view-buffer (val &optional quiet)
  "Pretty-print XML VAL in the current buffer and enable XML mode.
When QUIET is non-nil, suppress informational fallback messages."
  (if (executable-find "xmllint")
      (let ((raw (buffer-string))
            (err-file (make-temp-file "clutch-xmllint-")))
        (unwind-protect
            (unless (eq 0 (call-process-region
                           (point-min) (point-max)
                           "xmllint" t (list t err-file) nil "--format" "-"))
              (erase-buffer)
              (insert raw)
              (unless quiet
                (message "xmllint: %s"
                         (string-trim (with-temp-buffer
                                        (insert-file-contents err-file)
                                        (buffer-string))))))
          (delete-file err-file)))
    (unless quiet
      (message "xmllint not found — showing raw XML without formatting")))
  ;; Readability matters more than preserving numeric character references in
  ;; the transient viewer buffer; keep the raw XML value unchanged elsewhere.
  (clutch--decode-xml-char-refs-in-buffer)
  (cond ((fboundp 'nxml-mode) (nxml-mode))
        ((fboundp 'xml-mode) (xml-mode))
        (t (special-mode)))
  (setq-local header-line-format
              (format " XML%s%d bytes"
                      (clutch--status-separator)
                      (string-bytes val)))
  ;; Force fontification so XML is highlighted immediately in popup buffers.
  (when (fboundp 'font-lock-ensure)
    (font-lock-ensure (point-min) (point-max)))
  (when (fboundp 'jit-lock-fontify-now)
    (jit-lock-fontify-now (point-min) (point-max))))

(defun clutch--setup-plain-view-buffer ()
  "Enable plain text view mode for the current buffer."
  (special-mode))

(defun clutch--view-json-value (val)
  "Display VAL as formatted JSON in a pop-up buffer."
  (unless (and (stringp val) (not (string-empty-p val)))
    (clutch--user-error "No JSON value at point"))
  (clutch--view-in-buffer val "*clutch-json*" #'clutch--setup-json-view-buffer))

(defun clutch--view-xml-value (val)
  "Display VAL as formatted XML in a pop-up buffer.
Uses xmllint for pretty-printing when available; shows a message otherwise."
  (unless (and (stringp val) (not (string-empty-p val)))
    (clutch--user-error "No XML value at point"))
  (clutch--view-in-buffer val "*clutch-xml*"
    (lambda () (clutch--setup-xml-view-buffer val))))

(defun clutch--blob-bytes (val)
  "Return a unibyte string for blob-like VAL."
  (cond
   ((stringp val) (encode-coding-string val 'binary))
   ((vectorp val) (apply #'unibyte-string (append val nil)))
   (t (encode-coding-string (clutch--format-value val) 'binary))))

(defun clutch--blob-hexdump-lines (bytes &optional max-bytes)
  "Return hex dump lines for BYTES, up to MAX-BYTES bytes."
  (let* ((total (length bytes))
         (limit (min total (or max-bytes total)))
         (offset 0)
         lines)
    (while (< offset limit)
      (let* ((line-len (min 16 (- limit offset)))
             (hex-parts nil)
             (ascii-parts nil))
        (dotimes (i line-len)
          (let* ((b (aref bytes (+ offset i)))
                 (ch (if (and (>= b 32) (<= b 126)) b ?.)))
            (push (format "%02x" b) hex-parts)
            (push (char-to-string ch) ascii-parts)))
        (push (format "%08x  %-47s  |%s|"
                      offset
                      (mapconcat #'identity (nreverse hex-parts) " ")
                      (mapconcat #'identity (nreverse ascii-parts) ""))
              lines)
        (setq offset (+ offset line-len))))
    (nreverse lines)))

(defun clutch--blob-likely-text-p (bytes &optional sample-size)
  "Return non-nil when BYTES appears mostly text-like within SAMPLE-SIZE bytes."
  (let* ((n (min (length bytes) (or sample-size 512)))
         (printable 0))
    (if (= n 0)
        t
      (dotimes (i n)
        (let ((b (aref bytes i)))
          (when (or (and (>= b 32) (<= b 126))
                    (memq b '(9 10 13)))
            (setq printable (1+ printable)))))
      (>= (/ (float printable) n) 0.85))))

(defun clutch--blob-view-string (val)
  "Build a concise DataGrip-like display string for blob VAL."
  (let* ((bytes (clutch--blob-bytes val))
         (size (length bytes))
         (text-like (clutch--blob-likely-text-p bytes))
         (max-bytes (if text-like 1024 256))
         (shown (min size max-bytes))
         (truncated (> size max-bytes)))
    (concat
     (format "BLOB size: %d bytes\n\n" size)
     (if text-like
         (let ((preview (condition-case nil
                            (decode-coding-string (substring bytes 0 shown) 'utf-8 t)
                          (error ""))))
           (concat "Text preview:\n"
                   (if (string-empty-p preview) "<empty>" preview)))
       (concat "Hex preview:\n"
               (mapconcat #'identity
                          (clutch--blob-hexdump-lines bytes max-bytes)
                          "\n")))
     (if truncated
         (format "\n\n... truncated, showing first %d bytes" max-bytes)
       ""))))

(defun clutch--view-binary-as-string (val)
  "Display blob-like VAL in a DataGrip-style preview buffer."
  (let ((s (clutch--blob-view-string val)))
    (when (string-empty-p s)
      (clutch--user-error "No value at point"))
    (clutch--view-in-buffer s "*clutch-blob*"
      (lambda () (special-mode)))))

(defun clutch--view-plain-value (val)
  "Display VAL as plain text in a pop-up buffer."
  (let ((s (clutch--format-value val)))
    (clutch--view-in-buffer s "*clutch-value*" #'clutch--setup-plain-view-buffer)))

(defun clutch--view-spec (val col-def &optional quiet)
  "Return rendering spec for VAL with column metadata COL-DEF.
When QUIET is non-nil, suppress nonessential viewer messages."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((or (eq cat 'json)
          (and (stringp val) (string-match-p "\\`\\s-*[{\\[]" val)))
      (list :kind "JSON"
            :content (if (stringp val) val
                       (clutch--json-value-to-string val))
            :setup #'clutch--setup-json-view-buffer))
     ((clutch--xml-like-string-p val)
      (list :kind "XML"
            :content val
            :setup (lambda () (clutch--setup-xml-view-buffer val quiet))))
     ((eq cat 'blob)
      (list :kind "BLOB"
            :content (clutch--blob-view-string val)
            :setup #'clutch--setup-plain-view-buffer))
     (t
      (list :kind "Value"
            :content (clutch--format-value val)
            :setup #'clutch--setup-plain-view-buffer)))))

(defun clutch--dispatch-view (val col-def)
  "Open the appropriate viewer for VAL given column metadata COL-DEF.
Dispatch order: JSON content → JSON viewer; XML content → XML viewer;
blob type with non-text value → binary string; otherwise plain text."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((or (eq cat 'json)
          (and (stringp val) (string-match-p "\\`\\s-*[{\\[]" val)))
      ;; Pass raw string directly when available — avoids json-serialize
      ;; escaping non-ASCII characters (e.g. CJK) as \uXXXX.
      (clutch--view-json-value (if (stringp val) val
                                 (clutch--json-value-to-string val))))
     ((clutch--xml-like-string-p val)
      (clutch--view-xml-value val))
     ((eq cat 'blob)
      (clutch--view-binary-as-string val))
     (t
      (clutch--view-plain-value val)))))

(defvar clutch--live-view-follow-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "f" #'clutch--live-view-toggle-freeze)
    (define-key map "g" #'clutch--live-view-refresh)
    (define-key map "q" #'clutch--live-view-quit)
    map)
  "Keymap for `clutch--live-view-follow-mode'.")

(define-minor-mode clutch--live-view-follow-mode
  "Minor mode for a clutch live-follow value viewer."
  :init-value nil
  :lighter " LiveView"
  :keymap clutch--live-view-follow-mode-map)

(defun clutch--live-view-current-context ()
  "Return the current live-view source context, or nil when none is available."
  (cond
   ((derived-mode-p 'clutch-result-mode)
    (when-let* ((ridx (get-text-property (point) 'clutch-row-idx))
                (cidx (get-text-property (point) 'clutch-col-idx)))
      (list :source-buffer (current-buffer)
            :source-kind 'result
            :cell-id (list 'result (buffer-chars-modified-tick) ridx cidx)
            :ridx ridx
            :cidx cidx
            :row-count (length (or clutch--filtered-rows clutch--result-rows))
            :table clutch--result-source-table
            :column (nth cidx clutch--result-columns)
            :col-def (nth cidx clutch--result-column-defs)
            :value (get-text-property (point) 'clutch-full-value))))
   ((derived-mode-p 'clutch-record-mode)
    (when-let* ((result-buf clutch-record--result-buffer)
                ((buffer-live-p result-buf))
                (ridx (get-text-property (point) 'clutch-row-idx))
                (cidx (get-text-property (point) 'clutch-col-idx)))
      (list :source-buffer (current-buffer)
            :source-kind 'record
            :cell-id (list 'record (buffer-chars-modified-tick) ridx cidx)
            :ridx ridx
            :cidx cidx
            :row-count (with-current-buffer result-buf
                         (length clutch--result-rows))
            :table (with-current-buffer result-buf clutch--result-source-table)
            :column (with-current-buffer result-buf
                      (nth cidx clutch--result-columns))
            :col-def (with-current-buffer result-buf
                       (nth cidx clutch--result-column-defs))
            :value (get-text-property (point) 'clutch-full-value))))))

(defun clutch--live-view-header (kind context frozen)
  "Return live viewer header for KIND using CONTEXT and FROZEN state."
  (let* ((table (plist-get context :table))
         (column (or (plist-get context :column) "?"))
         (label (if table (format "%s.%s" table column) column))
         (ridx (plist-get context :ridx))
         (cidx (plist-get context :cidx))
         (row-count (or (plist-get context :row-count) 0)))
    (string-join
     (list (format " %s" kind)
           (if frozen "FROZEN" "FOLLOW")
           label
           (format "R%d/%d C%d" (1+ ridx) row-count (1+ cidx))
           "f freeze  g refresh  q quit")
     (clutch--status-separator))))

(defun clutch--live-view-detach-source (source-buf &optional viewer-buf)
  "Detach live viewer state from SOURCE-BUF.
When VIEWER-BUF is non-nil, only detach if SOURCE-BUF points at VIEWER-BUF."
  (when (buffer-live-p source-buf)
    (with-current-buffer source-buf
      (when (or (null viewer-buf)
                (eq clutch--live-view-buffer viewer-buf))
        (setq-local clutch--live-view-buffer nil)
        (remove-hook 'post-command-hook #'clutch--live-view-source-post-command t)
        (remove-hook 'kill-buffer-hook #'clutch--live-view-source-killed t)
        (remove-hook 'change-major-mode-hook #'clutch--live-view-source-killed t)))))

(defun clutch--live-view-buffer-killed ()
  "Clean up source-buffer hooks when the live viewer is killed."
  (let ((viewer (current-buffer))
        (source clutch--live-view-source-buffer))
    (setq-local clutch--live-view-source-buffer nil)
    (clutch--live-view-detach-source source viewer)))

(defun clutch--live-view-source-killed ()
  "Dispose of any live viewer attached to the current source buffer."
  (when (buffer-live-p clutch--live-view-buffer)
    (let ((viewer clutch--live-view-buffer))
      (setq-local clutch--live-view-buffer nil)
      (when (buffer-live-p viewer)
        (with-current-buffer viewer
          (setq-local clutch--live-view-source-buffer nil))
        (kill-buffer viewer))))
  (remove-hook 'post-command-hook #'clutch--live-view-source-post-command t)
  (remove-hook 'kill-buffer-hook #'clutch--live-view-source-killed t)
  (remove-hook 'change-major-mode-hook #'clutch--live-view-source-killed t))

(defun clutch--render-live-view (viewer-buf context &optional force)
  "Render CONTEXT into VIEWER-BUF.
When FORCE is non-nil, refresh even if the source cell has not changed."
  (with-current-buffer viewer-buf
    (let ((frozen clutch--live-view-frozen)
          (source (plist-get context :source-buffer))
          (cell-id (plist-get context :cell-id)))
      (unless (and (not force)
                   (equal cell-id clutch--live-view-source-cell-id))
        (pcase-let* ((`(:kind ,kind :content ,content :setup ,setup)
                      (clutch--view-spec (plist-get context :value)
                                         (plist-get context :col-def)
                                         t)))
          (clutch--render-view-buffer viewer-buf content setup)
          (setq-local clutch--live-view-source-buffer source)
          (setq-local clutch--live-view-source-cell-id cell-id)
          (setq-local clutch--live-view-frozen frozen)
          (setq-local header-line-format
                      (clutch--live-view-header kind context frozen))
          (clutch--live-view-follow-mode 1))))))

(defun clutch--live-view-source-post-command ()
  "Refresh the attached live viewer after point moves in a source buffer."
  (if (not (buffer-live-p clutch--live-view-buffer))
      (clutch--live-view-detach-source (current-buffer))
    (let ((viewer clutch--live-view-buffer)
          (context (clutch--live-view-current-context)))
      (with-current-buffer viewer
        (unless clutch--live-view-frozen
          (when context
            (clutch--render-live-view viewer context)))))))

(defun clutch--live-view-refresh ()
  "Refresh the current clutch live viewer from its source point."
  (interactive)
  (unless (buffer-live-p clutch--live-view-source-buffer)
    (clutch--user-error "Live viewer source buffer is no longer available"))
  (when-let* ((context (with-current-buffer clutch--live-view-source-buffer
                         (clutch--live-view-current-context))))
    (clutch--render-live-view (current-buffer) context t)
    (message "Live viewer refreshed")))

(defun clutch--live-view-toggle-freeze ()
  "Toggle whether the live viewer follows source-buffer point changes."
  (interactive)
  (setq-local clutch--live-view-frozen (not clutch--live-view-frozen))
  (setq-local header-line-format
              (replace-regexp-in-string
               (if clutch--live-view-frozen "FOLLOW" "FROZEN")
               (if clutch--live-view-frozen "FROZEN" "FOLLOW")
               (format "%s" header-line-format)
               t t))
  (unless clutch--live-view-frozen
    (clutch--live-view-refresh))
  (message "Live viewer %s"
           (if clutch--live-view-frozen "frozen" "following point")))

(defun clutch--live-view-quit ()
  "Close the current clutch live viewer."
  (interactive)
  (kill-buffer (current-buffer)))

(defun clutch--open-live-view ()
  "Open or refresh a live-follow viewer for the current clutch cell."
  (let* ((source (current-buffer))
         (context (or (clutch--live-view-current-context)
                      (clutch--user-error "No cell at point")))
         (viewer (get-buffer-create "*clutch-live-view*")))
    (with-current-buffer viewer
      (when (and (buffer-live-p clutch--live-view-source-buffer)
                 (not (eq clutch--live-view-source-buffer source)))
        (clutch--live-view-detach-source clutch--live-view-source-buffer viewer))
      (add-hook 'kill-buffer-hook #'clutch--live-view-buffer-killed nil t))
    (setq-local clutch--live-view-buffer viewer)
    (add-hook 'post-command-hook #'clutch--live-view-source-post-command nil t)
    (add-hook 'kill-buffer-hook #'clutch--live-view-source-killed nil t)
    (add-hook 'change-major-mode-hook #'clutch--live-view-source-killed nil t)
    (with-current-buffer viewer
      (setq-local clutch--live-view-frozen nil)
      (setq-local clutch--live-view-source-buffer source))
    (clutch--render-live-view viewer context t)
    (display-buffer viewer '(display-buffer-at-bottom . ((window-height . 0.33))))
    viewer))

;;;###autoload
(defun clutch-result-view-value ()
  "Display the cell value at point in an appropriate pop-up buffer.
Selects JSON, XML, or binary string view based on column type and content."
  (interactive)
  (pcase-let ((`(,_ridx ,cidx ,val) (or (clutch-result--cell-at-point)
                                         (clutch--user-error "No cell at point"))))
    (clutch--dispatch-view val (nth cidx clutch--result-column-defs))))

;;;###autoload
(defun clutch-result-live-view-value ()
  "Open a live-follow viewer for the result cell at point."
  (interactive)
  (clutch--open-live-view))

;;;###autoload
(defun clutch-result-shell-command-on-cell (command)
  "Pipe the cell value at point through shell COMMAND and display the output."
  (interactive "sShell command on cell: ")
  (pcase-let ((`(,_ridx ,_cidx ,val) (or (clutch-result--cell-at-point)
                                          (clutch--user-error "No cell at point"))))
    (let ((input (if (stringp val)
                     val
                   (clutch--format-value val))))
      (clutch--view-in-buffer
       (with-temp-buffer
         (insert input)
         (shell-command-on-region (point-min) (point-max) command t t)
         (buffer-string))
       "*clutch-shell-output*"
       #'special-mode))))

(defun clutch-result--build-insert-statements-for-rows (rows col-indices table)
  "Return INSERT statements for ROWS using COL-INDICES into TABLE."
  (let* ((conn      clutch-connection)
         (col-names (mapcar (lambda (i) (nth i clutch--result-columns)) col-indices))
         (cols      (mapconcat (lambda (c) (clutch-db-escape-identifier conn c))
                               col-names ", ")))
    (cl-loop for row in rows
             for vals = (mapcar (lambda (i) (nth i row)) col-indices)
             collect (format "INSERT INTO %s (%s) VALUES (%s);"
                             (clutch-db-escape-identifier conn table)
                             cols
                             (mapconcat #'clutch--value-to-literal vals ", ")))))

(defun clutch-result--build-insert-statements (indices col-indices table)
  "Return INSERT statement strings for INDICES rows using COL-INDICES into TABLE."
  (clutch-result--build-insert-statements-for-rows
   (mapcar (lambda (ridx) (nth ridx clutch--result-rows)) indices)
   col-indices
   table))

(defconst clutch--insert-placeholder-table "MY_TABLE"
  "Placeholder target table used for ambiguous INSERT copy/export output.")

(defun clutch--next-top-level-clause-position (sql start patterns)
  "Return earliest top-level clause match in SQL after START for PATTERNS.
PATTERNS is a list of case-insensitive regex fragments passed to
`clutch-db-sql-find-top-level-clause'.  Return nil when none are found."
  (car (sort (delq nil
                   (mapcar (lambda (pattern)
                             (clutch-db-sql-find-top-level-clause sql pattern start))
                           patterns))
             #'<)))

(defun clutch--simple-insert-source-table (&optional sql)
  "Return the source table for simple single-table INSERT output, or nil.
SQL defaults to the current result query.  Joined, derived, UNION, and
other ambiguous result queries return nil so INSERT copy/export can fall
back to a placeholder table name instead of inventing a wrong target."
  (let* ((sql (or sql clutch--last-query))
         (normalized (and sql
                          (string-trim-right
                           (replace-regexp-in-string ";\\s-*\\'" "" sql)))))
    (when normalized
      (let* ((case-fold-search t)
             (masked (clutch-db-sql-mask-literal-or-comment normalized))
             (from-pos (clutch-db-sql-find-top-level-clause masked "FROM")))
        (when (and from-pos
                   (not (clutch-db-sql-find-top-level-clause masked "JOIN"))
                   (not (clutch-db-sql-find-top-level-clause
                         masked "UNION\\b\\(?:\\s-+ALL\\b\\)?"))
                   (not (clutch-db-sql-find-top-level-clause masked "INTERSECT"))
                   (not (clutch-db-sql-find-top-level-clause masked "EXCEPT"))
                   (string-match "\\bFROM\\b" masked from-pos))
          (let* ((from-body-start (match-end 0))
                 (from-body-end
                  (or (clutch--next-top-level-clause-position
                       masked from-body-start
                       '("WHERE" "GROUP\\s-+BY" "HAVING" "ORDER\\s-+BY"
                         "LIMIT" "OFFSET" "FETCH" "FOR"))
                      (length masked)))
                 (from-body (string-trim
                             (substring masked from-body-start from-body-end))))
            (when (and (not (string-prefix-p "(" from-body))
                       (not (string-match-p "," from-body)))
              (clutch-result--table-from-sql normalized))))))))

(defun clutch--insert-target-table ()
  "Return a safe target table name for INSERT copy/export.
Simple single-table result sets use the detected table name.  Ambiguous
results use `clutch--insert-placeholder-table' instead."
  (or (clutch--simple-insert-source-table)
      clutch--insert-placeholder-table))

(defun clutch-result--selected-update-col-indices (row-identity col-indices op)
  "Return writable update column indices from ROW-IDENTITY, COL-INDICES, and OP.
Hidden identity columns are excluded.  Primary-key source columns are excluded
to preserve the existing copy/export UPDATE behavior."
  (let* ((pk-source-indices
          (and (eq (plist-get row-identity :kind) 'primary-key)
               (or (plist-get row-identity :source-indices)
                   (plist-get row-identity :indices))))
         (set-col-indices
          (cl-loop for cidx in col-indices
                   unless (or (plist-get (nth cidx clutch--result-column-defs)
                                         :hidden)
                              (memq cidx pk-source-indices))
                   collect cidx)))
    (unless set-col-indices
      (clutch--user-error "Cannot %s: no writable source columns selected" op))
    set-col-indices))

(defun clutch-result--ensure-update-source-columns (table col-indices op)
  "Ensure COL-INDICES map to writable source columns for TABLE during OP."
  (let* ((details (or (clutch--ensure-column-details clutch-connection table t)
                      (clutch--user-error "Cannot %s: source column metadata is unavailable"
                                  op)))
         (detail-map
          (cl-loop for detail in details
                   collect (cons (plist-get detail :name) detail)))
         (invalid (cl-loop for cidx in col-indices
                           for col-name = (nth cidx clutch--result-columns)
                           for detail = (cdr (assoc col-name detail-map))
                           unless (and detail (not (plist-get detail :generated)))
                           collect col-name)))
    (when invalid
      (clutch--user-error "Cannot %s: selected columns are not writable source columns: %s"
                  op
                  (string-join invalid ", ")))))

(defun clutch-result--build-update-statements-for-rows (rows col-indices op)
  "Return UPDATE preview statements for ROWS using COL-INDICES.
OP is a short operation description used in user-facing error messages."
  (let* ((table (clutch--result-source-table-or-user-error op))
         (row-identity (clutch-result--row-identity-or-user-error table op))
         (set-col-indices (clutch-result--selected-update-col-indices
                           row-identity col-indices op))
         (col-names clutch--result-columns)
         statements)
    (clutch-result--ensure-update-source-columns table set-col-indices op)
    (dolist (row rows)
      (let* ((identity-vec (clutch-result--extract-row-identity-vec
                            row row-identity))
             (edits (cl-loop for cidx in set-col-indices
                             collect (cons cidx (nth cidx row)))))
        (push (clutch-result--build-update-stmt
               table identity-vec edits col-names row-identity)
              statements)))
    (clutch-result--render-statements (nreverse statements))))

(defconst clutch--result-export-formats
  '(("csv-copy" :kind csv :destination clipboard)
    ("csv-file" :kind csv :destination file)
    ("insert-copy" :kind insert :destination clipboard)
    ("insert-file" :kind insert :destination file)
    ("update-copy" :kind update :destination clipboard)
    ("update-file" :kind update :destination file))
  "Available result export choices and their execution targets.")

(defconst clutch--result-export-kinds
  '((csv . (:content clutch--export-csv-content
            :file-prompt "Export CSV to file: "
            :default-file "export.csv"
            :copy-message "Copied %d row%s as CSV"
            :file-message "Exported %d row%s to %s (%s)"
            :file-coding csv))
    (insert . (:content clutch--export-insert-content
               :file-prompt "Export SQL to file: "
               :default-file "export.sql"
               :copy-message "Copied %d row%s as INSERT SQL"
               :file-message "Exported %d row%s as INSERT SQL to %s"))
    (update . (:content clutch--export-update-content
               :file-prompt "Export SQL to file: "
               :default-file "export.sql"
               :copy-message "Copied %d row%s as UPDATE SQL"
               :file-message "Exported %d row%s as UPDATE SQL to %s")))
  "Result export behavior keyed by logical export kind.")

(defun clutch-result--copy-selection-indices (&optional rect)
  "Return row and column indices for result copy commands.
RECT, when non-nil, has priority.  Otherwise active regions are treated as a
rectangle and inactive regions fall back to the current cell."
  (let ((rect (or rect
                  (if (use-region-p)
                      (clutch-result--region-rectangle-indices)
                    (pcase-let ((`(,ridx ,cidx ,_v)
                                 (or (clutch-result--cell-at-point)
                                     (clutch--user-error "No cell at point"))))
                      (cons (list ridx) (list cidx)))))))
    (cons (or (car-safe rect)
              (clutch-result--selected-row-indices)
              (clutch--user-error "No row at point"))
          (or (cdr-safe rect)
              (clutch--visible-columns)))))

(defun clutch-result--copy-rows-as-insert (&optional rect)
  "Copy row(s) as INSERT statement(s) to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell."
  (let* ((selection (clutch-result--copy-selection-indices rect))
         (indices (car selection))
         (col-indices (cdr selection))
         (table (clutch--insert-target-table))
         (stmts (clutch-result--build-insert-statements indices col-indices table)))
    (kill-new (mapconcat #'identity stmts "\n"))
    (deactivate-mark)
    (message "Copied %s %s statement%s (%s col%s)"
             (clutch--message-count (length stmts))
             (clutch--message-keyword "INSERT")
             (if (= (length stmts) 1) "" "s")
             (clutch--message-count (length col-indices))
             (if (= (length col-indices) 1) "" "s"))))

(defun clutch-result--copy-rows-as-update (&optional rect)
  "Copy row(s) as UPDATE statement(s) to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell."
  (let* ((selection (clutch-result--copy-selection-indices rect))
         (indices (car selection))
         (col-indices (cdr selection))
         (rows (mapcar (lambda (ridx) (nth ridx clutch--result-rows)) indices))
         (stmts (clutch-result--build-update-statements-for-rows
                 rows col-indices "copy UPDATE SQL")))
    (kill-new (mapconcat #'identity stmts "\n"))
    (deactivate-mark)
    (message "Copied %s %s statement%s (%s col%s)"
             (clutch--message-count (length stmts))
             (clutch--message-keyword "UPDATE")
             (if (= (length stmts) 1) "" "s")
             (clutch--message-count (length col-indices))
             (if (= (length col-indices) 1) "" "s"))))

(defun clutch--csv-escape (val)
  "Return CSV-escaped string for VAL."
  (let ((s (clutch--format-value val)))
    (if (string-match-p "[,\"\n]" s)
        (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" s))
      s)))

(defun clutch--csv-lines-for-rows (rows col-indices)
  "Return CSV lines for ROWS using COL-INDICES."
  (let ((col-names (mapcar (lambda (i) (nth i clutch--result-columns))
                           col-indices)))
    (cons (mapconcat #'identity col-names ",")
          (cl-loop for row in rows
                   for vals = (mapcar (lambda (i) (nth i row)) col-indices)
                   collect (mapconcat #'clutch--csv-escape vals ",")))))

(defun clutch-result--build-csv-lines (indices col-indices)
  "Return CSV lines for INDICES rows using COL-INDICES columns."
  (clutch--csv-lines-for-rows
   (mapcar (lambda (ridx) (nth ridx clutch--result-rows)) indices)
   col-indices))

(defun clutch-result--copy-rows-as-csv (&optional rect)
  "Copy row(s) as CSV to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell.
Includes a header row with column names."
  (let* ((selection (clutch-result--copy-selection-indices rect))
         (indices (car selection))
         (col-indices (cdr selection))
         (lines (clutch-result--build-csv-lines indices col-indices)))
    (kill-new (mapconcat #'identity lines "\n"))
    (deactivate-mark)
    (message "Copied %s row%s as %s (%s col%s)"
             (clutch--message-count (length indices))
             (if (= (length indices) 1) "" "s")
             (clutch--message-keyword "CSV")
             (clutch--message-count (length col-indices))
             (if (= (length col-indices) 1) "" "s"))))

;;;###autoload
(defun clutch-result-export ()
  "Export the current result.
Prompts for format:
- csv-copy: all rows to clipboard as CSV text
- csv-file: all rows to CSV file
- insert-copy: all rows to clipboard as INSERT statements
- insert-file: all rows to a .sql file as INSERT statements
- update-copy: all rows to clipboard as UPDATE statements
- update-file: all rows to a .sql file as UPDATE statements."
  (interactive)
  (let* ((choice (completing-read
                  "Export format: "
                  (mapcar #'car clutch--result-export-formats)
                  nil t))
         (format (cdr (assoc choice clutch--result-export-formats))))
    (unless format
      (clutch--user-error "Unsupported export format: %s" choice))
    (clutch--export-result format)))

(defun clutch--export-csv-content (rows)
  "Return CSV export text for ROWS using current visible result columns."
  (let* ((lines (clutch--csv-lines-for-rows rows (clutch--visible-columns)))
         (body (mapconcat #'identity (cdr lines) "\n")))
    (if (string-empty-p body)
        (concat (car lines) "\n")
      (concat (car lines) "\n" body "\n"))))

(defun clutch--csv-export-coding-choices ()
  "Return alist of CSV export coding labels to coding systems."
  (let ((pairs '(("utf-8-bom" . utf-8-with-signature)
                 ("utf-8" . utf-8)
                 ("gbk" . gbk)
                 ("cp936" . cp936))))
    (cl-loop for (label . coding) in pairs
             when (coding-system-p coding)
             collect (cons label coding))))

(defun clutch--read-csv-export-coding-system ()
  "Read coding system for CSV file export."
  (let* ((choices (clutch--csv-export-coding-choices))
         (default (if (coding-system-p clutch-csv-export-default-coding-system)
                      clutch-csv-export-default-coding-system
                    'utf-8-with-signature))
         (default-label (car (rassoc default choices)))
         (label (completing-read
                 (format "CSV encoding (default %s): "
                         (or default-label (symbol-name default)))
                 (mapcar #'car choices) nil t nil nil default-label)))
    (or (cdr (assoc label choices)) default)))

(defun clutch-result--collect-all-export-rows ()
  "Return all rows for current result by auto-paging when needed."
  (clutch--ensure-connection)
  (let ((effective-sql (clutch-result--effective-query)))
    (if (null effective-sql)
        clutch--result-rows
      (let* ((row-identity-prep
              (clutch--prepare-row-identity-query clutch-connection effective-sql))
             (identity-sql (plist-get row-identity-prep :sql)))
        (if (or (null clutch--base-query)
                (clutch--sql-has-limit-p effective-sql))
            (clutch-db-result-rows
             (clutch--run-db-query clutch-connection identity-sql))
          (cl-loop with page-size = clutch-result-max-rows
                   for page-num from 0
                   for paged-sql = (clutch--build-paged-sql
                                    identity-sql page-num page-size
                                    clutch--order-by)
                   for result = (clutch--run-db-query
                                  clutch-connection paged-sql)
                   for batch = (clutch-db-result-rows result)
                   append batch into rows
                   until (< (length batch) page-size)
                   finally return rows))))))

(defun clutch--export-insert-content (rows)
  "Return INSERT statement export text for ROWS using current result metadata."
  (let* ((table (clutch--insert-target-table))
         (col-indices (clutch--visible-columns))
         (stmts (clutch-result--build-insert-statements-for-rows
                 rows col-indices table)))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-update-content (rows)
  "Return UPDATE statement export text for ROWS using current result metadata."
  (let* ((col-indices (clutch--visible-columns))
         (stmts (clutch-result--build-update-statements-for-rows
                 rows col-indices "export UPDATE SQL")))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-result (format)
  "Execute result export described by FORMAT."
  (let* ((kind (plist-get format :kind))
         (destination (plist-get format :destination))
         (spec (or (cdr (assq kind clutch--result-export-kinds))
                   (clutch--user-error "Unsupported export kind: %s" kind)))
         (rows (clutch-result--collect-all-export-rows))
         (coding (when (and (eq destination 'file)
                            (eq (plist-get spec :file-coding) 'csv))
                   (clutch--read-csv-export-coding-system)))
         (text (funcall (plist-get spec :content) rows))
         (row-count (length rows))
         (row-suffix (if (= (length rows) 1) "" "s")))
    (pcase destination
      ('clipboard
       (kill-new text)
       (message (plist-get spec :copy-message) row-count row-suffix))
      ('file
       (let* ((path (read-file-name (plist-get spec :file-prompt)
                                    nil nil nil
                                    (plist-get spec :default-file)))
              (coding-system-for-write (or coding coding-system-for-write)))
         (with-temp-buffer
           (insert text)
           (write-region (point-min) (point-max) path nil 'silent))
         (apply #'message (plist-get spec :file-message)
                (append (list row-count row-suffix path)
                        (when coding (list coding))))))
      (_
       (clutch--user-error "Unsupported export destination: %s" destination)))))

(defun clutch-result--goto-col-idx (col-idx)
  "Move point to COL-IDX in the current row, preserving the row position.
When point is at line-end or a border, scan backward to find the row."
  (let ((ridx (or (get-text-property (point) 'clutch-row-idx)
                   (and (not (bolp))
                        (get-text-property (1- (point)) 'clutch-row-idx))
                   (save-excursion
                     (let ((prev (previous-single-property-change
                                  (point) 'clutch-row-idx)))
                       (when prev
                         (get-text-property (max (1- prev) (point-min))
                                            'clutch-row-idx)))))))
    (if ridx
        (clutch--goto-cell ridx col-idx)
      (goto-char (point-min))
      (when-let* ((found (text-property-search-forward
                          'clutch-col-idx col-idx #'eq)))
        (goto-char (prop-match-beginning found))))
    (clutch--ensure-point-visible-horizontally)))

;;;###autoload
(defun clutch-result-column-info ()
  "Show type information for the column at point.
When details are not yet cached, attempts to load them from the database."
  (interactive)
  (let* ((cidx (or (get-text-property (point) 'clutch-col-idx)
                   (get-text-property (point) 'clutch-header-col))))
    (unless cidx
      (clutch--user-error "No column at point"))
    ;; Try to populate details on demand if missing.
    (unless clutch--result-column-details
      (when-let* ((sql (or clutch--last-query))
                  (cols clutch--result-columns))
        (setq-local clutch--result-column-details
                    (clutch--resolve-result-column-details
                     clutch-connection sql cols))))
    (if-let* ((info (clutch--column-info-string cidx)))
        (message "%s" (clutch--column-info-message-string info))
      (message "%s (no detail info)" (nth cidx clutch--result-columns)))))

;;;###autoload
(defun clutch-result-goto-column ()
  "Jump to a specific column in the current row."
  (interactive)
  (unless clutch--result-columns
    (clutch--user-error "No result columns"))
  (let* ((col-names clutch--result-columns)
         (choice (completing-read "Go to column: " col-names nil t))
         (idx (cl-position choice col-names :test #'string=)))
    (when idx
      (clutch-result--goto-col-idx idx))))

;;;; Horizontal scrolling and width adjustment

;;;###autoload
(defun clutch-result-scroll-right ()
  "Page the result window right with one-column overlap.
The last column whose border falls within the current viewport becomes
the first column of the new view, so partially visible edge columns
remain visible after paging."
  (interactive)
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let* ((hs (window-hscroll win))
           (width (window-body-width win))
           (right-edge (+ hs width))
           (ncols (length clutch--result-columns))
           (last-in-view nil)
           (first-past nil))
      (dotimes (i ncols)
        (let ((border (clutch--column-border-position i)))
          (cond
           ((and (> border hs) (< border right-edge))
            (setq last-in-view border))
           ((and (>= border right-edge) (null first-past))
            (setq first-past border)))))
      (cond
       (last-in-view (set-window-hscroll win last-in-view))
       (first-past   (set-window-hscroll win first-past))
       (t (message "Already at rightmost columns"))))))

;;;###autoload
(defun clutch-result-scroll-left ()
  "Page the result window left with one-column overlap.
The column at the current left edge remains visible near the right
edge of the new view, so partially visible edge columns stay visible
after paging."
  (interactive)
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let* ((hs (window-hscroll win))
           (width (window-body-width win))
           (ncols (length clutch--result-columns)))
      (when (> hs 0)
        ;; Column at the current left edge (largest border <= hs).
        (let ((first-border 0)
              (target nil))
          (dotimes (i ncols)
            (let ((border (clutch--column-border-position i)))
              (when (<= border hs)
                (setq first-border border))))
          ;; Smallest column border that keeps first-border in the new view:
          ;; new-hs + width > first-border  →  new-hs > first-border - width
          (let ((min-new (- first-border width)))
            (dotimes (i ncols)
              (let ((border (clutch--column-border-position i)))
                (when (and (> border min-new) (< border hs) (null target))
                  (setq target border)))))
          (set-window-hscroll win (max 0 (or target 0))))))))

;;;###autoload
(defun clutch-result-widen-column ()
  "Widen the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (progn
        (cl-incf (aref clutch--column-widths cidx)
                 clutch-column-width-step)
        (clutch--refresh-display))
    (clutch--user-error "No column at point")))

;;;###autoload
(defun clutch-result-narrow-column ()
  "Narrow the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (let ((new-w (max 5 (- (aref clutch--column-widths cidx)
                              clutch-column-width-step))))
        (aset clutch--column-widths cidx new-w)
        (clutch--refresh-display))
    (clutch--user-error "No column at point")))

;;;; Fullscreen toggle

(defvar-local clutch--pre-fullscreen-config nil
  "Window configuration saved before entering fullscreen.")

;;;###autoload
(defun clutch-result-fullscreen-toggle ()
  "Toggle fullscreen display for the result buffer.
Expands the result buffer to fill the frame, or restores the
previous window layout."
  (interactive)
  (if clutch--pre-fullscreen-config
      (progn
        (set-window-configuration clutch--pre-fullscreen-config)
        (setq clutch--pre-fullscreen-config nil)
        (message "Restored window layout"))
    (setq clutch--pre-fullscreen-config
          (current-window-configuration))
    (delete-other-windows)
    (clutch--refresh-display)
    (message "Fullscreen (press f again to restore)")))

;;;; Record buffer

(defvar clutch-record-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "RET") #'clutch-record-toggle-expand)
    (define-key map "n" #'clutch-record-next-row)
    (define-key map "p" #'clutch-record-prev-row)
    (define-key map "v" #'clutch-record-view-value)
    (define-key map "V" #'clutch-record-live-view-value)
    (define-key map "I" #'clutch-clone-row-to-insert)
    (define-key map "q" #'quit-window)
    (define-key map "g" #'clutch-record-refresh)
    (define-key map (kbd "C-c ?") #'clutch-record-dispatch)
    map)
  "Keymap for `clutch-record-mode'.")
;;;###autoload
(define-derived-mode clutch-record-mode special-mode "clutch-record"
  "Mode for displaying a single database row in detail.

\\<clutch-record-mode-map>
  \\[clutch-record-toggle-expand]	Expand/collapse field or follow FK
  \\[clutch-record-next-row]	Next row
  \\[clutch-record-prev-row]	Previous row
  \\[clutch-record-view-value]	View current field once
  \\[clutch-record-live-view-value]	Open live viewer that follows point
  \\[clutch-record-refresh]	Refresh"
  (setq truncate-lines nil))

;;;###autoload
(defun clutch-result-open-record ()
  "Open the Record buffer showing the row at point.
Reuses a single *clutch-record* buffer, updating it in place."
  (interactive)
  (let* ((ridx (or (clutch-result--row-idx-at-line)
                   (clutch--user-error "No row at point")))
         (result-buf (current-buffer))
         (buf (get-buffer-create "*clutch-record*")))
    (with-current-buffer buf
      (unless (derived-mode-p 'clutch-record-mode)
        (clutch-record-mode))
      (setq-local clutch-record--result-buffer result-buf)
      (setq-local clutch-record--row-idx ridx)
      (setq-local clutch-record--expanded-fields nil)
      (clutch-record--render))
    (pop-to-buffer buf '(display-buffer-at-bottom))))

(defun clutch-record--render-field (name cidx val col-def row ridx row-identity
                                        edits fk-info expanded-fields max-name-w)
  "Insert one field line for column NAME at CIDX.
VAL is the cell value, COL-DEF the column metadata, ROW the full row.
RIDX is the row index.  ROW-IDENTITY, EDITS, FK-INFO, and EXPANDED-FIELDS
provide edit/FK/expand state.  MAX-NAME-W is the label column width."
  (let* ((identity-vec (and row-identity row
                            (clutch-result--extract-row-identity-vec
                             row row-identity)))
         (edited (and identity-vec (assoc (cons identity-vec cidx) edits)))
         (display-val (if edited (cdr edited) val))
         (long-p (clutch--long-field-type-p col-def))
         (expanded-p (memq cidx expanded-fields))
         (fk (cdr (assq cidx fk-info)))
         (formatted (clutch--format-value display-val))
         (display (if (and long-p (not expanded-p) (> (length formatted) 80))
                      (concat (substring formatted 0 80) "…")
                    formatted))
         (face (cond (edited 'clutch-modified-face)
                     ((null val) 'clutch-null-face)
                     (fk 'clutch-fk-face)
                     (t nil))))
    (insert (propertize (clutch--string-pad name max-name-w)
                        'face 'clutch-field-name-face)
            (propertize " : " 'face 'clutch-border-face)
            (propertize display
                        'clutch-row-idx ridx
                        'clutch-col-idx cidx
                        'clutch-full-value (if edited (cdr edited) val)
                        'face face)
            "\n")))

(defun clutch-record--render ()
  "Render the current row in the Record buffer."
  (unless (buffer-live-p clutch-record--result-buffer)
    (clutch--user-error "Result buffer no longer exists"))
  (let* ((result-buf clutch-record--result-buffer)
         (ridx clutch-record--row-idx)
         (col-names (buffer-local-value 'clutch--result-columns result-buf))
         (col-defs (buffer-local-value 'clutch--result-column-defs result-buf))
         (rows (buffer-local-value 'clutch--result-rows result-buf))
         (row-identity (with-current-buffer result-buf
                         (clutch-result--current-row-identity)))
         (fk-info (buffer-local-value 'clutch--fk-info result-buf))
         (edits (buffer-local-value 'clutch--pending-edits result-buf))
         (inhibit-read-only t))
    (unless (< ridx (length rows))
      (clutch--user-error "Row %d no longer exists" ridx))
    (clutch--bind-connection-context
     (buffer-local-value 'clutch-connection result-buf)
     (buffer-local-value 'clutch--connection-params result-buf)
     (buffer-local-value 'clutch--conn-sql-product result-buf))
    (erase-buffer)
    (setq-local clutch-record--header-base
                (propertize (format " Record: row %d/%d" (1+ ridx) (length rows))
                            'face 'clutch-header-face))
    (setq header-line-format
          '(:eval (clutch--header-with-disconnect-badge clutch-record--header-base)))
    (let* ((row (nth ridx rows))
           (max-name-w (apply #'max (mapcar #'string-width col-names))))
      (cl-loop for name in col-names
               for col-def in col-defs
               for cidx from 0
               unless (plist-get col-def :hidden)
               do (clutch-record--render-field
                   name cidx (nth cidx row) col-def
                   row ridx row-identity edits fk-info clutch-record--expanded-fields
                   max-name-w)))
    (goto-char (point-min))))

(defun clutch-record--follow-fk (fk val result-buf)
  "Navigate to the FK-referenced row for VAL using FK plist, via RESULT-BUF."
  (when (null val)
    (clutch--user-error "NULL value — cannot follow"))
  (with-current-buffer result-buf
    (let ((c (buffer-local-value 'clutch-connection result-buf)))
      (clutch--execute
       (format "SELECT * FROM %s WHERE %s = %s"
               (clutch-db-escape-identifier c (plist-get fk :ref-table))
               (clutch-db-escape-identifier c (plist-get fk :ref-column))
               (clutch--value-to-literal val))
       clutch-connection))))

;;;###autoload
(defun clutch-record-toggle-expand ()
  "Toggle expand/collapse for long fields, or follow FK."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'clutch-col-idx))
            (ridx (get-text-property (point) 'clutch-row-idx)))
      (let* ((result-buf clutch-record--result-buffer)
             (fk-info  (buffer-local-value 'clutch--fk-info result-buf))
             (fk       (cdr (assq cidx fk-info)))
             (col-defs (buffer-local-value 'clutch--result-column-defs result-buf))
             (col-def  (nth cidx col-defs))
             (val      (get-text-property (point) 'clutch-full-value)))
        (cond
         (fk
          (clutch-record--follow-fk fk val result-buf))
         ((clutch--long-field-type-p col-def)
          (if (memq cidx clutch-record--expanded-fields)
              (setq clutch-record--expanded-fields
                    (delq cidx clutch-record--expanded-fields))
            (push cidx clutch-record--expanded-fields))
          (clutch-record--render))
         (t
          (message "%s" (clutch--format-value val)))))
    (clutch--user-error "No field at point")))

;;;###autoload
(defun clutch-record-next-row ()
  "Show the next row in the Record buffer."
  (interactive)
  (let ((total (with-current-buffer clutch-record--result-buffer
                 (length clutch--result-rows))))
    (if (>= (1+ clutch-record--row-idx) total)
        (clutch--user-error "Already at last row")
      (cl-incf clutch-record--row-idx)
      (setq clutch-record--expanded-fields nil)
      (clutch-record--render))))

;;;###autoload
(defun clutch-record-prev-row ()
  "Show the previous row in the Record buffer."
  (interactive)
  (if (<= clutch-record--row-idx 0)
      (clutch--user-error "Already at first row")
    (cl-decf clutch-record--row-idx)
    (setq clutch-record--expanded-fields nil)
    (clutch-record--render)))

;;;###autoload
(defun clutch-record-view-value ()
  "Display the field value at point in an appropriate pop-up buffer.
Selects JSON, XML, or binary string view based on column type and content."
  (interactive)
  (let* ((cidx (get-text-property (point) 'clutch-col-idx))
         (_ridx (get-text-property (point) 'clutch-row-idx))
         (val  (if cidx
                   (get-text-property (point) 'clutch-full-value)
                 (clutch--user-error "No field at point")))
         (col-def (when (and cidx (buffer-live-p clutch-record--result-buffer))
                    (with-current-buffer clutch-record--result-buffer
                      (nth cidx clutch--result-column-defs)))))
    (clutch--dispatch-view val col-def)))

;;;###autoload
(defun clutch-record-live-view-value ()
  "Open a live-follow viewer for the record field at point."
  (interactive)
  (clutch--open-live-view))

;;;###autoload
(defun clutch-record-refresh ()
  "Refresh the Record buffer."
  (interactive)
  (clutch-record--render))

;;;; Dispatch menus

(transient-define-prefix clutch-result-dispatch ()
  "Dispatch menu for clutch result buffer."
  [ :pad-keys t
   ["Navigate"
    ("TAB" "Next cell"       clutch-result-next-cell)
    ("<backtab>" "Prev cell" clutch-result-prev-cell)
    ("n" "Down row"          clutch-result-down-cell)
    ("p" "Up row"            clutch-result-up-cell)
    ("RET" "Open record"     clutch-result-open-record)
    ("C" "Go to column"      clutch-result-goto-column)
    ("?" "Column info"       clutch-result-column-info)]
   ["Query"
    ("g" "Re-execute"        clutch-result-rerun)
    ("x" "Preview execution" clutch-preview-execution-sql)
    ("#" "Count total"       clutch-result-count-total)
    ("A" "Aggregate"         clutch-result-aggregate)]
   ["Staged"
    ("y" "Copy staged SQL"  clutch-result-copy-pending-sql)
    ("Y" "Save staged SQL"  clutch-result-save-pending-sql)]
   ["Filter / Sort"
    ("/" "Filter rows"       clutch-result-filter)
    ("W" "WHERE filter"      clutch-result-apply-filter)
    ("s" "Sort ASC"          clutch-result-sort-by-column)
    ("S" "Sort DESC"         clutch-result-sort-by-column-desc)]]
  [ :pad-keys t
   ["Pages"
    ("N" "Next page"         clutch-result-next-page)
    ("P" "Prev page"         clutch-result-prev-page)
    ("M-<" "First page"      clutch-result-first-page)
    ("M->" "Last page"       clutch-result-last-page)
    ("]" "Page right →│"     clutch-result-scroll-right)
    ("[" "Page left │←"      clutch-result-scroll-left)]
   ["Mutate"
    ("C-c '" "Edit / re-edit" clutch-result-edit-cell)
    ("i" "Stage insert"      clutch-result-insert-row)
    ("I" "Clone row → insert" clutch-clone-row-to-insert)
    ("d" "Stage delete"      clutch-result-delete-rows)
    ("C-c C-c" "Commit staged" clutch-result-commit)
    ("C-c C-k" "Discard staged at point" clutch-result-discard-pending-at-point)]
   ["Layout"
    ("=" "Widen column"      clutch-result-widen-column)
    ("-" "Narrow column"     clutch-result-narrow-column)
    ("f" "Fullscreen"        clutch-result-fullscreen-toggle)]]
  [ :pad-keys t
   ["Inspect"
    ("v" "View value" clutch-result-view-value)
    ("V" "Live view (follow point)" clutch-result-live-view-value)]
   ["Copy / Export (region/rect: C-x SPC)"
    ("c" "Copy… (-r to refine rows/cols)" clutch-result-copy-dispatch)
    ("k" "Copy agent context" clutch-copy-context-for-agent)
    ("e" "Export" clutch-result-export)]])

(transient-define-prefix clutch-record-dispatch ()
  "Dispatch menu for clutch record buffer."
  [ :pad-keys t
   ["Navigate"
    ("n" "Next row"     clutch-record-next-row)
    ("p" "Prev row"     clutch-record-prev-row)
    ("RET" "Expand/FK"  clutch-record-toggle-expand)]
   ["Inspect"
    ("v" "View value" clutch-record-view-value)
    ("V" "Live view (follow point)" clutch-record-live-view-value)]
   ["Other"
    ("I" "Clone row → insert" clutch-clone-row-to-insert)
    ("g" "Refresh" clutch-record-refresh)
    ("q" "Quit"    quit-window)]])

(provide 'clutch-result)

;;; clutch-result.el ends here
