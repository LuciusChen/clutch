;;; clutch-ui.el --- Result rendering and display helpers -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; Internal result rendering and display helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)
(require 'seq)
(require 'subr-x)
(require 'clutch-db)

(defvar clutch--executing-p)
(declare-function clutch--spinner-string "clutch-connection" ())

(defvar-local clutch--aggregate-summary nil
  "Last aggregate summary plist for result footer, or nil.
Plist keys: :label, :rows, :cells, :skipped, :sum, :avg, :min, :max, :count.")
(defvar clutch--cached-pk-indices)
(defvar clutch--row-identity)
(defvar clutch--cell-default-placeholder)
(defvar clutch--cell-generated-placeholder)
(defvar-local clutch--column-widths nil
  "Vector of display widths for each result column.")
(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar-local clutch--dml-result nil
  "Non-nil when this result buffer shows a DML result.")
(defvar-local clutch--filter-pattern nil
  "Current client-side filter string, or nil.")
(defvar-local clutch--fk-info nil
  "Foreign key info for the current result.")
(defvar-local clutch--filtered-rows nil
  "Filtered subset of `clutch--result-rows', or nil when unfiltered.")
(defvar-local clutch--header-active-col nil
  "Column index currently highlighted in the header, or nil.")
(defvar-local clutch--footer-base-string nil
  "Static portion of mode-line footer (pagination, sort, etc.).")
(defvar-local clutch--footer-display-cache nil
  "Cached complete footer string returned by `:eval'.
Assembled from segment caches by `clutch--assemble-footer-display'.")
(defvar-local clutch--footer-timing-cache nil
  "Cached timing segment string for the footer.")
(defvar-local clutch--footer-cursor-cache nil
  "Cached cursor position segment string for the footer.")
(defvar-local clutch--footer-filters-cache nil
  "Cached filter/aggregate segment string for the footer.")
(defvar-local clutch--header-line-string nil
  "Full header-line string before hscroll adjustment.")
(defvar-local clutch--last-window-width nil
  "Last known window body width for the current result buffer.")
(defvar-local clutch--marked-rows nil
  "List of marked row indices.")
(defvar-local clutch--order-by nil
  "Current ORDER BY state as (COL-NAME . DIRECTION) or nil.")
(defvar-local clutch--page-current 0
  "Current data page number (0-based).")
(defvar-local clutch--page-has-more nil
  "Non-nil when one-row lookahead found rows after the current page.")
(defvar-local clutch--page-offset nil
  "Zero-based SQL offset for the first row in the current result page.
Nil means derive the offset from `clutch--page-current'.")
(defvar-local clutch--page-total-rows nil
  "Total row count from COUNT(*), or nil if not yet queried.")
(defvar-local clutch--pending-deletes nil
  "List of row identity vectors staged for deletion.")
(defvar-local clutch--pending-edits nil
  "Alist of staged edits: ((IDENTITY-VEC . COL-IDX) . NEW-VALUE).")
(defvar-local clutch--pending-inserts nil
  "List of field alists staged for insertion.")
(defvar-local clutch--query-elapsed nil
  "Elapsed time in seconds for the last query execution.")
(defvar-local clutch--result-source-table nil
  "Detected source table name for the current result buffer, or nil.")
(defvar-local clutch--result-server-pageable nil
  "Non-nil when server-side page navigation is safe for this result.")
(defvar-local clutch--result-server-rewritable nil
  "Non-nil when server-side sort/filter/count rewrites are safe.")
(defvar-local clutch--result-column-defs nil
  "Full column definition plists from the last result.")
(defvar-local clutch--result-columns nil
  "Column names from the last result.")
(defvar-local clutch--result-rows nil
  "Row data from the last result.")
(defvar-local clutch--row-identity nil
  "Row identity metadata for staging edits and deletes in the current result.")
(defvar-local clutch--executed-sql-overlay nil
  "Overlay marking the last SQL execution status.")
(defvar-local clutch--row-overlay nil
  "Overlay used to highlight the current row.")
(defvar-local clutch--row-start-positions nil
  "Vector mapping rendered row indices to their line start positions.")
(defvar-local clutch--sort-column nil
  "Column name currently sorted by, or nil.")
(defvar-local clutch--sort-descending nil
  "Non-nil if the current sort is descending.")
(defvar-local clutch--result-column-details nil
  "List of column detail plists aligned with `clutch--result-columns'.
Each element corresponds to the same-index column.  Nil when unavailable.")
(defvar-local clutch--where-filter nil
  "Current WHERE filter string, or nil if no filter is active.")
(defvar-local clutch--refine-rect nil
  "Rectangle (ROW-INDICES . COL-INDICES) being refined, or nil.")
(defvar-local clutch--refine-excluded-rows nil
  "Row indices (0-based) excluded during refine mode.")
(defvar-local clutch--refine-excluded-cols nil
  "Column indices (0-based) excluded during refine mode.")
(defvar-local clutch--refine-overlays nil
  "Overlays created during refine mode.")
(defvar-local clutch--refine-callback nil
  "Callback called with final rect when refine is confirmed.")
(defvar-local clutch--refine-saved-mode-line nil
  "Saved `mode-line-format' to restore after refine mode exits.")
(defvar clutch-column-padding)
(defvar clutch-column-width-max)
(defvar clutch-connection)
(defvar clutch-describe--header-base)
(defvar clutch-result-max-rows)

(declare-function clutch--bind-connection-context "clutch-connection" (conn &optional params product))
(declare-function clutch--cached-column-details "clutch-schema" (conn table))
(declare-function clutch--ensure-column-details "clutch-schema" (conn table &optional strict))
(declare-function clutch--ensure-column-details-async "clutch-schema" (conn table))
(declare-function clutch--tx-header-line-segment "clutch-connection" (conn))
(declare-function clutch--connection-key "clutch-connection" (conn))

(defcustom clutch-column-displayers nil
  "Per-table/per-column display functions for result cells.
Each entry is (TABLE-NAME . ((COLUMN-NAME . FUNCTION) ...)).
TABLE-NAME and COLUMN-NAME are matched case-insensitively.
FUNCTION receives the raw cell value and must return a string, which may
include text properties.  Return nil to fall back to default rendering."
  :type '(alist :key-type string
                :value-type (alist :key-type string :value-type function))
  :group 'clutch)

(defun clutch--case-insensitive-string= (left right)
  "Return non-nil when LEFT and RIGHT match case-insensitively."
  (and (stringp left)
       (stringp right)
       (string= (downcase left) (downcase right))))

(defun clutch--lookup-column-displayer (table column)
  "Return registered displayer for TABLE and COLUMN, or nil."
  (when-let* ((table-entry
               (cl-assoc table clutch-column-displayers
                         :test #'clutch--case-insensitive-string=))
              (column-entry
               (cl-assoc column (cdr table-entry)
                         :test #'clutch--case-insensitive-string=)))
    (cdr column-entry)))

(defun clutch-register-column-displayer (table column function)
  "Register FUNCTION as the renderer for COLUMN in TABLE.
FUNCTION receives the raw cell value and should return a string.  Return
nil from FUNCTION to fall back to default display.

Examples:

  ;; Show JSON column as a compact summary.
  (clutch-register-column-displayer \"orders\" \"metadata\"
    (lambda (value)
      (when (stringp value)
        (ignore-errors
          (format \"{%d keys}\"
                  (hash-table-count (json-parse-string value)))))))

  ;; Show a URL as a clickable button.
  (clutch-register-column-displayer \"bookmarks\" \"url\"
    (lambda (value)
      (when (stringp value)
        (propertize (truncate-string-to-width value 40 nil nil \"…\")
                    (quote face) (quote link)
                    (quote help-echo) value))))

  ;; Map numeric status codes to styled labels.
  (clutch-register-column-displayer \"tasks\" \"status\"
    (lambda (value)
      (pcase value
        (0 (propertize \"pending\" (quote face) (quote warning)))
        (1 (propertize \"active\" (quote face) (quote success)))
        (2 (propertize \"done\" (quote face) (quote shadow))))))"
  (unless (stringp table)
    (signal 'wrong-type-argument (list 'stringp table)))
  (unless (stringp column)
    (signal 'wrong-type-argument (list 'stringp column)))
  (unless (functionp function)
    (signal 'wrong-type-argument (list 'functionp function)))
  (if-let* ((table-entry
             (cl-assoc table clutch-column-displayers
                       :test #'clutch--case-insensitive-string=)))
      (progn
        (setcar table-entry table)
        (if-let* ((column-entry
                   (cl-assoc column (cdr table-entry)
                             :test #'clutch--case-insensitive-string=)))
            (progn
              (setcar column-entry column)
              (setcdr column-entry function))
          (setcdr table-entry (cons (cons column function) (cdr table-entry)))))
    (push (cons table (list (cons column function))) clutch-column-displayers))
  function)

(defun clutch-unregister-column-displayer (table column)
  "Remove any custom displayer for COLUMN in TABLE."
  (unless (stringp table)
    (signal 'wrong-type-argument (list 'stringp table)))
  (unless (stringp column)
    (signal 'wrong-type-argument (list 'stringp column)))
  (when-let* ((table-entry
               (cl-assoc table clutch-column-displayers
                         :test #'clutch--case-insensitive-string=)))
    (setcdr table-entry
            (cl-remove-if
             (lambda (column-entry)
               (clutch--case-insensitive-string=
                (car-safe column-entry) column))
             (cdr table-entry)))
    (when (null (cdr table-entry))
      (setq clutch-column-displayers
            (delq table-entry clutch-column-displayers))))
  nil)

;;;; Result value formatting

(defun clutch--format-value (val)
  "Format VAL for display in a result table.
nil → \"NULL\", :false → \"false\", plists → formatted date/time strings,
hash-tables and vectors (JSON from MySQL/PG) → JSON string."
  (cond
   ((null val) "NULL")
   ((eq val :false) "false")
   ((stringp val) val)
   ((numberp val) (number-to-string val))
   ((listp val) (or (clutch-db-format-temporal val) (format "%S" val)))
   ((or (hash-table-p val) (vectorp val))
    (clutch--json-serialize-text val "query result value"))
   (t (format "%S" val))))

(defun clutch--json-false-value-p (val)
  "Return non-nil when VAL represents a parsed JSON false sentinel."
  (or (eq val :false)
      ;; JDBC uses a private false sentinel so it can distinguish JSON false
      ;; from SQL NULL when decoding metadata payloads.
      (and (symbolp val)
           (string= (symbol-name val) "clutch-jdbc-json-false"))))

(defun clutch--json-value-to-string (val)
  "Convert VAL to valid JSON text suitable for JSON editing and viewing."
  (cond
   ((null val)
    "null")
   ((and (stringp val)
         (fboundp 'json-serialize)
         (fboundp 'json-parse-string))
    (condition-case nil
        (clutch--json-serialize-text (json-parse-string val))
      (error (clutch--json-serialize-text val))))
   ((clutch--json-false-value-p val)
    "false")
   ((and (fboundp 'json-serialize)
         (or (numberp val)
             (eq val t)
             (hash-table-p val)
             (vectorp val)
             (listp val)))
    (condition-case nil
        (clutch--json-serialize-text
         (if (clutch--json-false-value-p val) :false val))
      (error (clutch--format-value val))))
   (t (clutch--format-value val))))

(defun clutch--string-pad (str width &optional right-align)
  "Pad STR with spaces to reach display WIDTH.
Unlike `string-pad', this accounts for wide characters (CJK).
When RIGHT-ALIGN is non-nil, pad on the left instead of the right."
  (let ((sw (string-width str)))
    (if (>= sw width)
        str
      (let ((spaces (make-string (- width sw) ?\s)))
        (if right-align
            (concat spaces str)
          (concat str spaces))))))

(defun clutch--center-padding-widths (content-width width)
  "Return (LEFT . RIGHT) padding widths to center CONTENT-WIDTH in WIDTH."
  (let* ((extra (max 0 (- width content-width)))
         (left (/ extra 2)))
    (cons left (- extra left))))

(defun clutch--format-elapsed (seconds)
  "Format SECONDS as a human-readable duration."
  (if (< seconds 1.0)
      (format "%dms" (round (* seconds 1000)))
    (format "%.3fs" seconds)))

(defun clutch--numeric-type-p (col-def)
  "Return non-nil if COL-DEF is a numeric column type."
  (eq (plist-get col-def :type-category) 'numeric))

(defun clutch--long-field-type-p (col-def)
  "Return non-nil if COL-DEF is a long field type (JSON/BLOB)."
  (memq (plist-get col-def :type-category) '(json blob)))

(defun clutch--json-like-string-p (val)
  "Return non-nil when string VAL appears to contain JSON text."
  (and (stringp val) (string-match-p "\\`\\s-*[{\\[]" val)))

(defun clutch--xml-like-string-p (val)
  "Return non-nil when string VAL appears to contain XML text.
Uses a stricter heuristic to avoid misclassifying plain \"<...\" text."
  (and (stringp val)
       (let* ((s (string-trim-left val))
              (body (if (string-match "\\`<\\?xml\\(?:.\\|\n\\)*?\\?>\\s-*\\(.*\\)\\'" s)
                        (match-string 1 s)
                      s))
              (open-re "\\`<\\([[:alpha:]_][[:alnum:]_.:-]*\\)\\(?:\\s-+[^>]*\\)?\\s-*\\(/>\\|>\\)"))
         (when (string-match open-re body)
           (let ((tag (match-string 1 body))
                 (close (match-string 2 body)))
             (if (equal close "/>")
                 (string-match-p "\\`<[^>]+/>\\s-*\\'" body)
               (string-match-p (format "</%s\\s-*>" (regexp-quote tag)) body)))))))

(defun clutch--value-placeholder (val col-def)
  "Return compact placeholder text for VAL/COL-DEF in result grid."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((or (eq cat 'json) (clutch--json-like-string-p val))
      "<JSON>")
     ((clutch--xml-like-string-p val)
      "<XML>")
     ((eq cat 'blob)
      "<BLOB>")
     (t nil))))

(defun clutch--cell-placeholder-value (val)
  "Return display placeholder text for special cell VAL, or nil."
  (pcase val
    (:clutch-generated-placeholder "<generated>")
    (:clutch-default-placeholder "<default>")
    (_ nil)))

(defun clutch--compute-column-widths (col-names rows column-defs
                                                &optional max-width)
  "Compute display width for each column.
COL-NAMES is a list of header strings, ROWS is the data,
COLUMN-DEFS is the column metadata list.
MAX-WIDTH caps individual column width (default `clutch-column-width-max').
Pass a large value or nil to use the default.
Returns a vector of integers."
  (let* ((ncols (length col-names))
         (max-w (or max-width clutch-column-width-max))
         (widths (make-vector ncols 0))
         (sample (seq-take rows 50)))
    (dotimes (i ncols)
      (if (and (clutch--long-field-type-p (nth i column-defs))
               (<= max-w clutch-column-width-max))
          (aset widths i 10)
        (let ((header-w (string-width (nth i col-names)))
              (data-w 0))
          (dolist (row sample)
            (let ((formatted (clutch--format-value (nth i row))))
              (setq data-w (max data-w (string-width formatted)))))
          (aset widths i (max 5 (min max-w (max header-w data-w)))))))
    widths))

(defun clutch--visible-columns ()
  "Return the column indices rendered in the result buffer."
  (cl-loop for i below (length clutch--result-columns)
           unless (plist-get (nth i clutch--result-column-defs) :hidden)
           collect i))

(defun clutch--column-names-for-indices (indices)
  "Return result column names for INDICES."
  (mapcar (lambda (i) (nth i clutch--result-columns)) indices))

(defun clutch--visible-column-names ()
  "Return visible result column names in rendered order."
  (clutch--column-names-for-indices (clutch--visible-columns)))

(defun clutch--cell-custom-display (value col-def)
  "Return custom display string for VALUE in COL-DEF, or nil."
  (when-let* ((table clutch--result-source-table)
              (column (plist-get col-def :name))
              (displayer (clutch--lookup-column-displayer table column)))
    (condition-case err
        (let ((display (funcall displayer value)))
          (cond
           ((null display) nil)
           ((stringp display) display)
           (t
            (message "clutch column displayer %s.%s returned %S, falling back"
                     table column display)
            nil)))
      (error
       (message "clutch column displayer %s.%s failed: %s"
                table column (error-message-string err))
       nil))))

(defconst clutch--nerd-icons-function-alist
  '((codicon   . nerd-icons-codicon)
    (devicon   . nerd-icons-devicon)
    (faicon    . nerd-icons-faicon)
    (flicon    . nerd-icons-flicon)
    (ipsicon   . nerd-icons-ipsicon)
    (mdicon    . nerd-icons-mdicon)
    (octicon   . nerd-icons-octicon)
    (pomicon   . nerd-icons-pomicon)
    (powerline . nerd-icons-powerline)
    (sucicon   . nerd-icons-sucicon)
    (wicon     . nerd-icons-wicon))
  "Alist mapping nerd-icons glyph-set symbols to public render functions.")

(defvar clutch--nerd-icons-warning-families nil
  "Nerd-icons glyph-set families already reported as unavailable.")

(defun clutch--nerd-icons-available-p ()
  "Return non-nil when nerd-icons is loadable and exposes public icon functions."
  (and (require 'nerd-icons nil t)
       (cl-some (lambda (entry) (fboundp (cdr entry)))
                clutch--nerd-icons-function-alist)))

(defun clutch--nerd-icons-warn-unavailable-family (family)
  "Warn once that nerd-icons FAMILY cannot be rendered."
  (unless (memq family clutch--nerd-icons-warning-families)
    (push family clutch--nerd-icons-warning-families)
    (display-warning
     'clutch
     (format "nerd-icons does not expose a public renderer for %S; using fallback text"
             family)
     :warning)))

(defun clutch--icon (name &optional fallback &rest icon-args)
  "Return a nerd-icons icon for NAME, or FALLBACK string.
NAME is a cons (FAMILY . ICON-NAME), where FAMILY maps to a public
nerd-icons glyph-set function such as `nerd-icons-mdicon'.
ICON-ARGS are keyword arguments forwarded to the nerd-icons function
\(e.g. :height 1.2).  Falls back to FALLBACK when nerd-icons is not
installed, the family is unsupported, or the icon is unknown."
  (pcase-let ((`(,family . ,icon-name) name))
    (let ((fn (alist-get family clutch--nerd-icons-function-alist)))
      (or (and (clutch--nerd-icons-available-p)
               (if (and fn (fboundp fn))
                   (apply fn icon-name icon-args)
                 (clutch--nerd-icons-warn-unavailable-family family)
                 nil))
          fallback
          ""))))

(defun clutch--append-face (string face)
  "Return STRING with FACE appended, preserving existing properties."
  (let ((copy (copy-sequence string)))
    (unless (or (null face)
                (string-empty-p copy))
      (add-face-text-property 0 (length copy) face 'append copy))
    copy))

(defun clutch--icon-with-face (name fallback face &rest icon-args)
  "Return icon NAME/FALLBACK with FACE appended to its text properties.
Pass ICON-ARGS through to `clutch--icon'."
  (clutch--append-face (apply #'clutch--icon name fallback icon-args) face))

(defun clutch--fixed-width-icon (spec fallback &optional face)
  "Return icon with `string-width' matching actual display width.
SPEC is (FAMILY . ICON-NAME) for `clutch--icon'.
FALLBACK is the Unicode char when nerd-icons is unavailable.
Optional FACE is applied to the result.

When `string-pixel-width' is available, measures the icon glyph
pixel width and wraps it in a display property over the correct
number of space characters.  This ensures `string-width' matches
  the real rendered width, preventing column misalignment."
  (let* ((raw (clutch--icon spec fallback))
         (raw (if (string-empty-p raw) fallback raw))
         (raw (clutch--append-face raw face)))
    (if (and (fboundp 'string-pixel-width)
             (fboundp 'default-font-width)
             (display-graphic-p))
        (let* ((pw (string-pixel-width raw))
               (fw (default-font-width))
               (cells (if (> fw 0)
                          (max 1 (round (/ (float pw) fw)))
                        (string-width raw))))
          (if (= cells (string-width raw))
              raw
            (propertize (make-string cells ?\s) 'display raw)))
      raw)))

(defun clutch--footer-icon (spec fallback face)
  "Return footer icon SPEC/FALLBACK with explicit FACE."
  (concat (clutch--icon-with-face spec fallback face)
          (propertize " " 'face face)))

(defun clutch--disconnected-badge ()
  "Return a disconnected indicator string with warning face, or nil if connected."
  (unless clutch-connection
    (concat (clutch--icon-with-face '(mdicon . "nf-md-database_off") "⨯" 'warning)
            (propertize " DISCONNECTED" 'face 'warning))))

(defun clutch--header-with-disconnect-badge (base)
  "Prepend disconnected badge to BASE header string when not connected."
  (let ((badge (clutch--disconnected-badge)))
    (if badge
        (concat (propertize " " 'display '(space :align-to 0))
                badge
                (propertize "  •  " 'face 'font-lock-comment-face)
                base)
      (or base ""))))

(defun clutch--clear-executed-sql-overlay (&rest _)
  "Remove the last SQL execution status overlay in the current buffer."
  (when (overlayp clutch--executed-sql-overlay)
    (let ((overlay clutch--executed-sql-overlay))
      (setq clutch--executed-sql-overlay nil)
      (delete-overlay overlay))))

(defun clutch--trim-sql-bounds (beg end)
  "Return (BEG . END) trimmed to non-whitespace between BEG and END."
  (save-excursion
    (goto-char beg)
    (skip-chars-forward " \t\r\n" end)
    (let ((tbeg (point)))
      (goto-char end)
      (skip-chars-backward " \t\r\n" beg)
      (let ((tend (point)))
        (when (< tbeg tend)
          (cons tbeg tend))))))

(defun clutch--sql-status-marker-before-string (status)
  "Return the before-string used to mark SQL execution STATUS."
  (pcase-let* ((`(,face ,fallback)
                (if (eq status 'failed)
                    '(clutch-failed-sql-marker-face "✗ ")
                  '(clutch-executed-sql-marker-face "✓ "))))
    (if (display-graphic-p)
        (propertize " "
                    'display
                    `(left-fringe clutch-executed-sql-dot ,face))
      (propertize fallback 'face face))))

(defun clutch--mark-sql-status-region (beg end status &optional message)
  "Mark SQL region BEG..END with execution STATUS.
MESSAGE, when non-nil, is used as hover text for failed SQL."
  (when-let* ((trimmed (clutch--trim-sql-bounds beg end))
              (tbeg (car trimmed))
              (tend (cdr trimmed)))
    (clutch--clear-executed-sql-overlay)
    (setq clutch--executed-sql-overlay
          (make-overlay (save-excursion
                          (goto-char tbeg)
                          (line-beginning-position))
                        (save-excursion
                          (goto-char tbeg)
                          (line-beginning-position))
                        nil nil nil))
    (overlay-put clutch--executed-sql-overlay 'before-string
                 (clutch--sql-status-marker-before-string status))
    (overlay-put clutch--executed-sql-overlay 'help-echo
                 (if (eq status 'failed)
                     (format "Last failed SQL: %s"
                             (or message "SQL execution failed"))
                   (format "Last executed SQL (%d chars)" (- tend tbeg))))))

(defun clutch--mark-executed-sql-region (beg end)
  "Mark the last successfully executed SQL region BEG..END."
  (clutch--mark-sql-status-region beg end 'executed))

(defun clutch--mark-failed-sql-region (beg end &optional message)
  "Mark the last failed SQL region BEG..END with MESSAGE."
  (clutch--mark-sql-status-region beg end 'failed message))

(defun clutch--header-label (name)
  "Build the display label for column NAME.
Prepends the sort indicator when the column is active."
  (let* ((sort (when (and clutch--sort-column
                          (string= name clutch--sort-column))
                 (let ((s (clutch--fixed-width-icon
                           (if clutch--sort-descending
                               '(codicon . "nf-cod-arrow_down")
                             '(codicon . "nf-cod-arrow_up"))
                           (if clutch--sort-descending "▼" "▲"))))
                   (when s
                     (propertize s 'clutch-header-icon t))))))
    (if sort
        (concat sort " " name)
      name)))

(defun clutch--render-separator (visible-cols widths &optional position)
  "Render a separator line for VISIBLE-COLS with WIDTHS.
POSITION is `top', `middle', or `bottom' (default `middle')."
  (let* ((padding clutch-column-padding)
         (pos (or position 'middle))
         (left  (pcase pos ('top "┌") ('bottom "└") (_ "├")))
         (cross (pcase pos ('top "┬") ('bottom "┴") (_ "┼")))
         (right (pcase pos ('top "┐") ('bottom "┘") (_ "┤")))
         (parts nil))
    (dolist (cidx visible-cols)
      (push (concat cross (make-string (+ (aref widths cidx) (* 2 padding)) ?─))
            parts))
    ;; Replace the leading cross of the first column with the left edge.
    (let ((line (concat (mapconcat #'identity (nreverse parts) "") right)))
      (concat left (substring line 1)))))

(defun clutch--render-header (visible-cols widths)
  "Render the header row string for VISIBLE-COLS with WIDTHS.
Each header cell body carries a `clutch-header-col' text property so
column-local commands still work from padded whitespace."
  (let ((padding clutch-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (let* ((name (nth cidx clutch--result-columns))
             (w (aref widths cidx))
             (label (clutch--header-label name))
             (truncated (if (> (string-width label) w)
                            (truncate-string-to-width label w)
                          label))
             (pads (clutch--center-padding-widths (string-width truncated) w))
             (lead (make-string (car pads) ?\s))
             (trail (make-string (cdr pads) ?\s))
             (cell (concat lead truncated trail))
             (face 'clutch-field-name-face)
             (pad-str (make-string padding ?\s))
             (body nil))
        ;; Append base face so icon-specific face (e.g. pin color) is preserved.
        (add-face-text-property 0 (length cell) face 'append cell)
        (setq body (concat pad-str cell pad-str))
        (add-text-properties 0 (length body)
                             `(clutch-header-col ,cidx)
                             body)
        (push (concat (propertize "│" 'face 'clutch-border-face)
                      body)
              parts)))
    (concat (mapconcat #'identity (nreverse parts) "")
            (propertize "│" 'face 'clutch-border-face))))

(defun clutch--cell-face (val edited cidx)
  "Return the display face for a cell with VAL at CIDX, EDITED if modified."
  (cond (edited 'clutch-modified-face)
        ((clutch--cell-placeholder-value val) 'clutch-null-face)
        ((null val) 'clutch-null-face)
        ((assq cidx clutch--fk-info) 'clutch-fk-face)
        (t nil)))

(defun clutch--cell-display-content (val w col-def edited)
  "Return the unpadded display string for a cell value VAL in width W.
COL-DEF is the column definition plist, EDITED is a staged edit cons or nil."
  (let* ((display-val (if edited (cdr edited) val))
         (custom (clutch--cell-custom-display display-val col-def))
         (special-placeholder (and (not custom)
                                   (not edited)
                                   (clutch--cell-placeholder-value display-val)))
         (s (and (not custom)
                 (or special-placeholder
                     (replace-regexp-in-string "\n" "↵"
                                               (clutch--format-value display-val)))))
         (placeholder (and (not custom)
                           (not edited)
                           (not special-placeholder)
                           (> (string-width s) w)
                           (clutch--value-placeholder display-val col-def)))
         (formatted (or custom placeholder s)))
    (if (> (string-width formatted) w)
        (truncate-string-to-width formatted w)
      formatted)))

(defun clutch--pending-insert-placeholders ()
  "Return placeholder sentinels aligned with `clutch--result-columns'."
  (when-let* ((conn clutch-connection)
              (table clutch--result-source-table)
              (details (clutch--ensure-column-details conn table)))
    (mapcar (lambda (col-name)
              (when-let* ((detail (cl-find col-name details
                                           :key (lambda (d) (plist-get d :name))
                                           :test #'string=)))
                (cond
                 ((plist-get detail :generated) clutch--cell-generated-placeholder)
                 ((plist-get detail :default) clutch--cell-default-placeholder)
                 (t nil))))
            clutch--result-columns)))

(defun clutch--build-render-state ()
  "Return hash-table lookups for the current result render.
These tables avoid repeated linear scans through staged UI state while
rendering large result pages."
  (let ((edit-table (make-hash-table :test 'equal))
        (edit-row-table (make-hash-table :test 'equal))
        (marked-table (make-hash-table :test 'eql))
        (delete-table (make-hash-table :test 'equal))
        (row-identity clutch--row-identity))
    (dolist (edit clutch--pending-edits)
      (puthash (car edit) edit edit-table)
      (puthash (car (car edit)) t edit-row-table))
    (dolist (ridx clutch--marked-rows)
      (puthash ridx t marked-table))
    (dolist (identity-vec clutch--pending-deletes)
      (puthash identity-vec t delete-table))
    (list :edits edit-table
          :edit-rows edit-row-table
          :marked marked-table
          :deletes delete-table
          :insert-placeholders (clutch--pending-insert-placeholders)
          :row-identity row-identity)))

(defun clutch--render-edit-entry (row _ridx cidx render-state)
  "Return staged edit entry for ROW/CIDX from RENDER-STATE, or nil."
  (let* ((edits (plist-get render-state :edits))
         (row-identity (plist-get render-state :row-identity)))
    (and row-identity
         (gethash (cons (clutch-db-row-identity-values
                         row row-identity)
                        cidx)
                  edits))))

(defun clutch--row-pending-edit-p (row _ridx render-state)
  "Return non-nil when ROW has any staged edit in RENDER-STATE."
  (let* ((edit-rows (plist-get render-state :edit-rows))
         (row-identity (plist-get render-state :row-identity)))
    (and row-identity
         (gethash (clutch-db-row-identity-values row row-identity)
                  edit-rows))))

(defun clutch--render-cell (row ridx cidx widths render-state)
  "Render cell at column CIDX of ROW at row index RIDX.
WIDTHS is the width vector.  Returns a propertized string
including the leading border and padding."
  (let* ((val     (nth cidx row))
         (col-def (nth cidx clutch--result-column-defs))
         (edited  (clutch--render-edit-entry row ridx cidx render-state))
         (w       (aref widths cidx))
         (content (clutch--cell-display-content val w col-def edited))
         (padded  (clutch--string-pad content w (clutch--numeric-type-p col-def)))
         (face    (clutch--cell-face val edited cidx))
         (pad-str (make-string clutch-column-padding ?\s))
         (body nil))
    (when (and (eq face 'clutch-fk-face)
               (not (string-empty-p content)))
      (let ((start (if (clutch--numeric-type-p col-def)
                       (- (length padded) (length content))
                     0)))
        (add-face-text-property start (+ start (length content)) face 'append padded)
        (setq face nil)))
    (when face
      (add-face-text-property 0 (length padded) face 'append padded))
    (setq body (concat pad-str padded pad-str))
    (add-text-properties 0 (length body)
                         `(clutch-row-idx ,ridx
                           clutch-col-idx ,cidx
                           clutch-full-value ,(if edited (cdr edited) val))
                         body)
    (concat (propertize "│" 'face 'clutch-border-face)
            body)))

(defun clutch--render-row (row ridx visible-cols widths render-state)
  "Render a single data ROW at row index RIDX.
VISIBLE-COLS is a list of column indices, WIDTHS is the width vector,
and RENDER-STATE carries cached render data.
Returns a propertized string."
  (concat (mapconcat (lambda (cidx)
                       (clutch--render-cell row ridx cidx widths render-state))
                     visible-cols "")
          (propertize "│" 'face 'clutch-border-face)))

(defun clutch--render-static-table (col-names rows &optional column-defs)
  "Render a table string from COL-NAMES and ROWS.
Uses the same visual style as the result renderer.
COLUMN-DEFS, if provided, is used for long-field detection.
Returns a string (with text properties)."
  (let* ((clutch--result-columns col-names)
         (clutch--result-column-defs column-defs)
         (clutch--result-source-table nil)
         (clutch--row-identity nil)
         (clutch--pending-edits nil)
         (clutch--fk-info nil)
         (ncols (length col-names))
         (all-cols (number-sequence 0 (1- ncols)))
         (widths (clutch--compute-column-widths col-names rows column-defs 1000))
         (bface 'clutch-border-face)
         (sep-top (propertize (clutch--render-separator all-cols widths 'top)
                              'face bface))
         (sep-mid (propertize (clutch--render-separator all-cols widths 'middle)
                              'face bface))
         (sep-bot (propertize (clutch--render-separator all-cols widths 'bottom)
                              'face bface))
         (header (clutch--render-header all-cols widths))
         (render-state (clutch--build-render-state))
         (lines nil))
    (push sep-top lines)
    (push header lines)
    (push sep-mid lines)
    (cl-loop for row in rows
             for ridx from 0
             do (push (clutch--render-row row ridx all-cols widths render-state) lines))
    (push sep-bot lines)
    (mapconcat #'identity (nreverse lines) "\n")))

(defun clutch--render-row-line (ridx render-state)
  "Return the rendered buffer line string for row RIDX.
RENDER-STATE carries cached lookup tables for staged row state."
  (let* ((rows (or clutch--filtered-rows clutch--result-rows))
         (row (nth ridx rows))
         (visible-cols (clutch--visible-columns))
         (widths (clutch--effective-widths))
         (nw (clutch--row-number-digits))
         (global-first-row (or clutch--page-offset
                               (* clutch--page-current clutch-result-max-rows)))
         (bface 'clutch-border-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (marked-table (plist-get render-state :marked))
         (delete-table (plist-get render-state :deletes))
         (row-identity (plist-get render-state :row-identity))
         (deletingp (and row-identity
                         (gethash (clutch-db-row-identity-values
                                   row row-identity)
                                  delete-table)))
         (editedp (clutch--row-pending-edit-p row ridx render-state))
         (data-row (let ((rendered (clutch--render-row
                                    row ridx visible-cols widths render-state)))
                     (if deletingp
                         (propertize rendered 'face 'clutch-pending-delete-face)
                       rendered)))
         (mark-char (cond (deletingp "D")
                          (editedp "E")
                          ((gethash ridx marked-table) "*")
                          (t " ")))
         (num-label (string-pad
                     (number-to-string
                      (1+ (+ global-first-row ridx)))
                     nw nil t))
         (num-face (cond (deletingp 'clutch-pending-delete-face)
                         (editedp 'clutch-modified-face)
                         ((gethash ridx marked-table) 'clutch-marked-face)
                         (t 'shadow))))
    (concat (propertize "│" 'face bface)
            (propertize mark-char 'face num-face)
            (propertize num-label 'face num-face)
            pad-str
            data-row
            "\n")))

(defun clutch--footer-row-summary (row-count page-num page-size total-rows
                                             &optional page-offset page-has-more)
  "Build propertized row count display for the footer.
ROW-COUNT and PAGE-NUM describe the visible page, PAGE-SIZE is the configured
page length, and TOTAL-ROWS is the exact overall total when known.
PAGE-OFFSET overrides PAGE-NUM for last-window pagination, and PAGE-HAS-MORE
records one-row lookahead."
  (let ((hi 'font-lock-keyword-face)
        (dim 'font-lock-comment-face)
        (offset (or page-offset (* page-num page-size))))
    (if (zerop row-count)
        (concat (propertize "0" 'face hi)
                (propertize " of " 'face dim)
                (propertize (format "%s" (or total-rows offset))
                            'face hi)
                (propertize " rows" 'face dim))
      (let* ((start (1+ offset))
             (end (+ offset row-count))
             (inferred-total (and (not page-has-more) end))
             (total-text (cond
                          (total-rows (format "%d" total-rows))
                          (page-has-more (format "%d+" (1+ end)))
                          (inferred-total (format "%d" inferred-total))
                          (t "?"))))
        (concat (propertize (format "%d-%d" start end) 'face hi)
                (propertize " of " 'face dim)
                (propertize total-text 'face hi)
                (propertize " rows" 'face dim))))))

(defun clutch--footer-aggregate-part ()
  "Build footer part for the last aggregate summary."
  (when-let* ((stats clutch--aggregate-summary))
    (let* ((dim 'font-lock-comment-face)
           (hi 'font-lock-keyword-face)
           (label (plist-get stats :label))
           (label-part (unless (string= label "selection")
                         (propertize (format "[%s] " label) 'face dim))))
      (if (> (plist-get stats :count) 0)
          (concat
           (clutch--footer-icon '(mdicon . "nf-md-calculator_variant") "∑"
                                'font-lock-keyword-face)
           label-part
           (propertize (format " sum=%g avg=%g min=%g max=%g"
                               (plist-get stats :sum)
                               (plist-get stats :avg)
                               (plist-get stats :min)
                               (plist-get stats :max))
                       'face hi)
           (propertize (format " [r%d c%d s%d]"
                               (plist-get stats :rows)
                               (plist-get stats :cells)
                               (plist-get stats :skipped))
                       'face dim))
        (concat
         (clutch--footer-icon '(mdicon . "nf-md-calculator_variant") "∑"
                              'font-lock-keyword-face)
         label-part
         (propertize " n/a" 'face hi)
         (propertize (format " [r%d c%d s%d]"
                             (plist-get stats :rows)
                             (plist-get stats :cells)
                             (plist-get stats :skipped))
                     'face dim))))))

(defun clutch--footer-filter-parts ()
  "Build footer parts for active filters and aggregate summary.
Returns a list of propertized strings (may be empty)."
  (delq nil
        (list
         (clutch--footer-aggregate-part)
         (when clutch--where-filter
           (let ((icon (clutch--footer-icon '(codicon . "nf-cod-filter") "W:"
                                            'font-lock-warning-face)))
             (concat icon
                     (propertize clutch--where-filter
                                 'face 'font-lock-warning-face))))
         (when clutch--filter-pattern
           (let ((icon (clutch--footer-icon '(codicon . "nf-cod-search") "/:"
                                            'font-lock-string-face)))
             (concat icon
                     (propertize clutch--filter-pattern
                                 'face 'font-lock-string-face)))))))

(defun clutch--footer-sort-part ()
  "Build footer part for active SQL ORDER BY state."
  (when-let* ((order clutch--order-by))
    (pcase-let ((`(,column . ,direction) order))
      (let ((icon (if (string-match-p "\\`desc\\'" direction)
                      '(octicon . "nf-oct-sort_desc")
                    '(octicon . "nf-oct-sort_asc")))
            (hi 'font-lock-keyword-face))
        (concat (clutch--footer-icon icon "↕" hi)
                (propertize (format "%s[%s]" (upcase direction) column)
                            'face hi))))))

(defun clutch--footer-pending-part ()
  "Build footer part for staged edits, deletions, or insertions."
  (let (parts)
    (when clutch--pending-edits
      (push (format "E-%d" (length clutch--pending-edits))
            parts))
    (when clutch--pending-deletes
      (push (format "D-%d" (length clutch--pending-deletes))
            parts))
    (when clutch--pending-inserts
      (push (format "I-%d" (length clutch--pending-inserts))
            parts))
    (when parts
      (let ((commit-icon (clutch--icon-with-face '(codicon . "nf-cod-check")
                                                 "✓" 'font-lock-comment-face))
            (discard-icon (clutch--icon-with-face '(codicon . "nf-cod-discard")
                                                  "✗" 'font-lock-comment-face)))
        (concat
         (clutch--footer-icon '(codicon . "nf-cod-diff_modified") "✎"
                              'clutch-modified-face)
         (propertize (mapconcat #'identity (nreverse parts) " ")
                     'face 'clutch-modified-face)
         (propertize "  " 'face 'font-lock-comment-face)
         commit-icon
         (propertize ":C-c C-c  " 'face 'font-lock-comment-face)
         discard-icon
         (propertize ":C-c C-k" 'face 'font-lock-comment-face))))))

(defun clutch--footer-mutation-capability-part ()
  "Build footer part describing update/delete capability for the result."
  (when (and clutch--result-columns
             clutch--result-source-table)
    (unless clutch--row-identity
      (let ((warn-icon 'font-lock-warning-face)
            (warn-text '(:inherit font-lock-warning-face :weight normal)))
        (concat (clutch--footer-icon '(codicon . "nf-cod-warning") "⚠" warn-icon)
                (propertize "row identity missing" 'face warn-text)
                (propertize " E/D off" 'face warn-text))))))

(defun clutch--footer-main-parts (row-count page-num page-size total-rows
                                            &optional page-offset page-has-more)
  "Return list of main footer part strings for pagination state.
ROW-COUNT and PAGE-NUM describe the visible page, PAGE-SIZE sets the page
length, and TOTAL-ROWS describes the full result size when known.
PAGE-OFFSET overrides PAGE-NUM for last-window pagination, and PAGE-HAS-MORE
records one-row lookahead."
  (let ((hi 'font-lock-keyword-face))
    (delq nil
          (list
           (concat (clutch--footer-icon '(mdicon . "nf-md-sigma") "Σ" hi)
                   (clutch--footer-row-summary
                    row-count page-num page-size total-rows
                    page-offset page-has-more))
           (clutch--tx-header-line-segment clutch-connection)
           (clutch--footer-sort-part)
           (clutch--footer-mutation-capability-part)
           (clutch--footer-pending-part)))))

(defun clutch--footer-timing-part ()
  "Return the dynamic footer timing segment for the current result buffer."
  (let ((hi 'font-lock-keyword-face))
    (when-let* ((payload (cond
                          (clutch--executing-p
                           (and (clutch--spinner-string)
                                (propertize (clutch--spinner-string)
                                            'face 'success)))
                          (clutch--query-elapsed
                           (propertize
                            (clutch--format-elapsed clutch--query-elapsed)
                            'face hi)))))
      (concat (clutch--footer-icon '(mdicon . "nf-md-timer_outline") "⏱" hi)
              payload))))

(defun clutch--footer-cursor-part ()
  "Return a mode-line segment showing the current row and column at point."
  (let ((ridx (get-text-property (point) 'clutch-row-idx))
        (cidx (get-text-property (point) 'clutch-col-idx)))
    (when (and ridx cidx)
      (let* ((hi 'font-lock-keyword-face)
             (dim 'font-lock-comment-face)
             (total-cols (length clutch--result-columns))
             (raw-name (or (nth cidx clutch--result-columns) ""))
             (col-name (if (> (length raw-name) 20)
                           (concat (substring raw-name 0 17) "...")
                         raw-name)))
        (concat (clutch--footer-icon '(mdicon . "nf-md-cursor_default_click_outline") "⌖" hi)
                (propertize (format "R-%d" (1+ ridx)) 'face hi)
                (propertize ":" 'face dim)
                (propertize (format "%s-%d/%d" col-name (1+ cidx) total-cols)
                            'face hi))))))

(defun clutch--render-footer (row-count page-num page-size total-rows
                                        &optional page-offset page-has-more)
  "Return the static footer string for pagination state.
ROW-COUNT and PAGE-NUM describe the visible page, and PAGE-SIZE sets
the page length.  TOTAL-ROWS describes the full result size when known.
PAGE-OFFSET and PAGE-HAS-MORE carry lookahead pagination state."
  (let ((sep (propertize "  •  " 'face 'font-lock-comment-face)))
    (mapconcat #'identity
               (clutch--footer-main-parts row-count page-num page-size
                                          total-rows page-offset
                                          page-has-more)
               sep)))

(defun clutch--footer-mode-line-display ()
  "Return the cached footer string for mode-line display.
The cache is rebuilt by `clutch--refresh-footer-display'."
  (or clutch--footer-display-cache ""))

(defun clutch--assemble-footer-display ()
  "Assemble `clutch--footer-display-cache' from cached segments."
  (let ((badge (clutch--disconnected-badge))
        (base (or clutch--footer-base-string ""))
        (timing clutch--footer-timing-cache)
        (filters clutch--footer-filters-cache)
        (cursor clutch--footer-cursor-cache)
        (sep (propertize "  •  " 'face 'font-lock-comment-face)))
    (setq clutch--footer-display-cache
          (concat (propertize " " 'display '(space :align-to 0))
                  (when badge (concat badge sep))
                  base
                  (when timing (concat sep timing))
                  (when filters (concat sep (string-join filters sep)))
                  (when cursor (concat sep cursor))))))

(defun clutch--refresh-footer-display ()
  "Rebuild all footer segments and assemble the cached display string."
  (setq clutch--footer-timing-cache (clutch--footer-timing-part)
        clutch--footer-cursor-cache (clutch--footer-cursor-part)
        clutch--footer-filters-cache (clutch--footer-filter-parts))
  (clutch--assemble-footer-display))

(defun clutch--refresh-footer-timing ()
  "Rebuild only the timing segment and reassemble the footer."
  (setq clutch--footer-timing-cache (clutch--footer-timing-part))
  (clutch--assemble-footer-display))

(defun clutch--refresh-footer-cursor ()
  "Rebuild only the cursor segment and reassemble the footer."
  (setq clutch--footer-cursor-cache (clutch--footer-cursor-part))
  (clutch--assemble-footer-display))

(defun clutch--effective-widths ()
  "Return column widths adjusted for header indicator icons.
Columns with sort indicators get wider to fit the label."
  (let ((widths (copy-sequence clutch--column-widths)))
    (dotimes (cidx (length widths))
      (let* ((name (nth cidx clutch--result-columns))
             (label (clutch--header-label name))
             (label-w (string-width label)))
        (when (> label-w (aref widths cidx))
          (aset widths cidx label-w))))
    widths))

(defun clutch--message-part (value face)
  "Return VALUE formatted for echo-area display with FACE."
  (propertize (format "%s" value) 'face face))

(defun clutch--message-count (value)
  "Return VALUE formatted as a highlighted count."
  (clutch--message-part value 'font-lock-constant-face))

(defun clutch--message-ident (value)
  "Return VALUE formatted as a highlighted identifier."
  (clutch--message-part value 'clutch-field-name-face))

(defun clutch--message-keyword (value)
  "Return VALUE formatted as a highlighted operation/status keyword."
  (clutch--message-part value 'font-lock-keyword-face))

(defun clutch--message-literal (value)
  "Return VALUE formatted as a highlighted literal."
  (clutch--message-part value 'font-lock-string-face))

(defun clutch--message-path (value)
  "Return VALUE formatted as a highlighted file path."
  (clutch--message-part value 'font-lock-doc-face))

(defun clutch--status-separator ()
  "Return the standard short-status separator."
  (propertize "  •  " 'face 'font-lock-comment-face))

(defun clutch--column-info-field (label value &optional face)
  "Return a propertized column-info LABEL and VALUE using optional FACE."
  (concat (clutch--message-keyword label)
          " "
          (clutch--message-part value (or face 'font-lock-string-face))))

(defun clutch--column-info-message-string (info)
  "Return one-line minibuffer text for propertized column INFO."
  (let ((start 0)
        (parts nil))
    (while (string-match "\n" info start)
      (push (substring info start (match-beginning 0)) parts)
      (setq start (match-end 0)))
    (push (substring info start) parts)
    (string-join (nreverse parts) (clutch--status-separator))))

(defun clutch--column-info-string (cidx)
  "Build the display string for column at CIDX from cached details."
  (when-let* ((details clutch--result-column-details)
              (col (nth cidx details)))
    (let ((parts nil))
      (when-let* ((comment (plist-get col :comment)))
        (unless (string-empty-p comment)
          (push (propertize comment 'face 'font-lock-doc-face) parts)))
      (when-let* ((default (plist-get col :default)))
        (push (clutch--column-info-field "Default:" default) parts))
      (push (clutch--column-info-field
             "Nullable:"
             (if (plist-get col :nullable) "YES" "NO")
             (if (plist-get col :nullable)
                 'font-lock-comment-face
               'font-lock-warning-face))
            parts)
      (when-let* ((type (plist-get col :type)))
        (push (clutch--column-info-field "Type:" type 'font-lock-type-face) parts))
      (when-let* ((name (plist-get col :name)))
        (push (propertize name 'face 'clutch-field-name-face) parts))
      (string-join parts "\n"))))

(defun clutch--result-column-details (conn table col-names &optional load)
  "Return detail plists aligned with result columns COL-NAMES.
Uses cached metadata for CONN/TABLE.  When LOAD is non-nil, synchronously load
missing table metadata."
  (when-let* ((details (and table
                            (or (clutch--cached-column-details conn table)
                                (and load
                                     (clutch--ensure-column-details conn table))))))
    (let ((by-name (make-hash-table :test 'equal)))
      (dolist (detail details)
        (puthash (downcase (plist-get detail :name)) detail by-name))
      (mapcar (lambda (name)
                (gethash (downcase name) by-name))
              col-names))))

(defun clutch--queue-result-column-details-enrichment (conn table)
  "Start async result column-detail preheat for CONN and TABLE when needed."
  (when (and table
             (not (clutch--cached-column-details conn table)))
    (clutch--ensure-column-details-async conn table)))

(defun clutch--refresh-result-metadata-buffers (conn table)
  "Refresh cached result column metadata for live result buffers on CONN/TABLE."
  (when-let* ((conn-key (and conn (clutch--connection-key conn))))
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and (derived-mode-p 'clutch-result-mode)
                     clutch-connection
                     clutch--result-columns
                     (string= (clutch--connection-key clutch-connection) conn-key)
                     (equal clutch--result-source-table table))
            (setq-local clutch--result-column-details
                        (clutch--result-column-details
                         clutch-connection table clutch--result-columns))))))))

(defun clutch--header-cell (cidx widths &optional active-cidx)
  "Build a single header cell string for column CIDX.
WIDTHS is the effective width vector.
ACTIVE-CIDX is the highlighted column index, if any."
  (let* ((name (nth cidx clutch--result-columns))
         (w (aref widths cidx))
         (label (clutch--header-label name))
         (truncated (if (> (string-width label) w)
                        (truncate-string-to-width label w)
                      label))
         (pads (clutch--center-padding-widths (string-width truncated) w))
         (lead (make-string (car pads) ?\s))
         (trail (make-string (cdr pads) ?\s))
         (label (copy-sequence truncated))
         (active-p (eql cidx active-cidx))
         (base-face 'clutch-field-name-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (body nil))
    ;; Append base/underline style without overwriting icon-specific face.
    (add-face-text-property 0 (length label) base-face 'append label)
    (when active-p
      (add-face-text-property 0 (length label) 'clutch-header-active-face
                              'append label))
    (add-face-text-property 0 (length label) '(:underline t) 'append label)
    ;; Keep sort/pin icons un-underlined for cleaner visual hierarchy.
    (dotimes (i (length label))
      (when (get-text-property i 'clutch-header-icon label)
        (let ((icon-face (or (get-text-property i 'face label) base-face)))
          (put-text-property i (1+ i) 'face
                             (list '(:underline nil) icon-face)
                             label))))
    (setq body (concat pad-str
                       (propertize lead 'face base-face)
                       label
                       (propertize trail 'face base-face)
                       pad-str))
    (add-text-properties 0 (length body)
                         `(clutch-header-col ,cidx)
                         body)
    (concat (propertize "│" 'face 'clutch-border-face)
            body)))

(defun clutch--build-header-line (visible-cols widths nw &optional active-cidx)
  "Build the `header-line-format' string for the column header row.
VISIBLE-COLS, WIDTHS describe columns.
NW is the digit width for the row number column.
ACTIVE-CIDX highlights that column when non-nil."
  (let* ((bface 'clutch-border-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (cells (mapcar (lambda (cidx)
                          (clutch--header-cell cidx widths active-cidx))
                        visible-cols))
         (data-header (concat (apply #'concat cells)
                              (propertize "│" 'face bface))))
    ;; 1 char for mark column + nw for row number + padding
    (concat (propertize "│" 'face bface)
            " " (make-string nw ?\s) pad-str
            data-header)))

(defun clutch--header-line-with-hscroll ()
  "Return the header string shifted to match `window-hscroll'.
The header-line should track body hscroll exactly."
  (when clutch--header-line-string
    (let* ((hs (window-hscroll))
           (str clutch--header-line-string)
           (len (length str)))
      (if (>= hs len)
          ""
        (substring str hs)))))

(defun clutch--header-line-display ()
  "Return the display-ready header-line string for result buffers."
  (concat (propertize " " 'display '(space :align-to 0))
          (or (clutch--header-line-with-hscroll) "")))

(defun clutch--insert-data-rows (rows row-positions render-state)
  "Insert data ROWS into the current buffer.
ROW-POSITIONS stores line starts keyed by rendered row index.
RENDER-STATE contains render lookup tables for staged UI state."
  (cl-loop for _row in rows
           for ridx from 0
           do (aset row-positions ridx (point))
           do (insert (clutch--render-row-line ridx render-state))))

(defun clutch--insert-pending-insert-rows (visible-cols widths nw nrows row-positions
                                                        render-state)
  "Append ghost rows for staged INSERT operations below the real data rows.
VISIBLE-COLS, WIDTHS describe columns.  NW is row-number digit width.
NROWS is the count of real rows (used to compute ghost row indices).
ROW-POSITIONS stores line starts keyed by rendered row index.
RENDER-STATE contains render lookup tables for staged UI state."
  (let ((bface 'clutch-border-face)
        (pad-str (make-string clutch-column-padding ?\s))
        (insert-placeholders (plist-get render-state :insert-placeholders)))
    (cl-loop for fields in clutch--pending-inserts
             for iidx from 0
             for ridx = (+ nrows iidx)
             do (aset row-positions ridx (point))
             for row = (cl-mapcar (lambda (col placeholder)
                                    (if-let* ((entry (assoc col fields)))
                                        (cdr entry)
                                      placeholder))
                                  clutch--result-columns
                                  insert-placeholders)
             for data-row = (propertize
                             (clutch--render-row row ridx visible-cols widths render-state)
                             'face 'clutch-pending-insert-face)
             for num-label = (string-pad (format "I%d" (1+ iidx)) nw nil t)
             do (insert (propertize "│" 'face bface)
                        (propertize "I" 'face 'clutch-pending-insert-face)
                        (propertize num-label 'face 'clutch-pending-insert-face)
                        pad-str
                        data-row "\n"))))

(defun clutch--refresh-header-line ()
  "Rebuild the header-line format without touching the table body."
  (if clutch--column-widths
      (let* ((visible-cols (clutch--visible-columns))
             (widths (clutch--effective-widths))
             (nw (clutch--row-number-digits)))
        (setq clutch--header-line-string
              (clutch--build-header-line visible-cols widths nw
                                         clutch--header-active-col))
        (setq header-line-format '(:eval (clutch--header-line-display))))
    (setq clutch--header-line-string nil
          header-line-format nil)))

(defun clutch--refresh-footer-line ()
  "Rebuild the mode-line footer format without touching the table body."
  (let ((rows (or clutch--filtered-rows clutch--result-rows)))
    (setq clutch--footer-base-string
          (clutch--render-footer
           (length rows) clutch--page-current
           clutch-result-max-rows clutch--page-total-rows
           clutch--page-offset clutch--page-has-more))
    (setq mode-line-format '(:eval (clutch--footer-mode-line-display)))
    (clutch--refresh-footer-display)))

(defun clutch--refresh-result-status-line ()
  "Refresh the result buffer status line without rebuilding the table body."
  (when (derived-mode-p 'clutch-result-mode)
    (clutch--refresh-footer-line)
    (clutch--refresh-header-line)
    (clutch--update-position-indicator)))

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

(defun clutch--row-idx-at-line ()
  "Return the rendered row index for the current line, or nil.
Scans text properties across the line."
  (cl-loop for p from (line-beginning-position) to (line-end-position)
           thereis (get-text-property p 'clutch-row-idx)))

(defun clutch--rows-in-region (beg end)
  "Return sorted rendered row indices in region BEG..END."
  (save-excursion
    (goto-char beg)
    (sort (cl-loop while (< (point) end)
                   for ridx = (clutch--row-idx-at-line)
                   when ridx collect ridx into acc
                   do (forward-line 1)
                   finally return (cl-remove-duplicates acc))
          #'<)))

(defun clutch--selected-row-indices ()
  "Return selected rendered row indices.
Priority: region rows, then current row."
  (or (when (use-region-p)
        (clutch--rows-in-region (region-beginning) (region-end)))
      (when-let* ((ridx (clutch--row-idx-at-line)))
        (list ridx))))

(defun clutch--result-source-table-or-user-error (op)
  "Return current result source table, or signal user-error for OP."
  (or clutch--result-source-table
      (user-error "Cannot %s: source table cannot be detected (multi-table or derived query)"
                  op)))

(defun clutch--cell-at (pos)
  "Return (ROW-IDX COL-IDX FULL-VALUE) at buffer position POS, or nil."
  (when-let* ((ridx (get-text-property pos 'clutch-row-idx)))
    (list ridx
          (get-text-property pos 'clutch-col-idx)
          (get-text-property pos 'clutch-full-value))))

(defun clutch--cell-at-or-near (pos)
  "Return cell triple at POS, or nearest cell on the same line."
  (or (clutch--cell-at pos)
      (save-excursion
        (goto-char pos)
        (let ((bol (line-beginning-position))
              (eol (line-end-position)))
          (or (cl-loop for p downfrom (max bol (1- pos)) to bol
                       thereis (clutch--cell-at p))
              (cl-loop for p from (min eol (1+ pos)) to eol
                       thereis (clutch--cell-at p)))))))

(defun clutch--cell-at-point ()
  "Return (ROW-IDX COL-IDX FULL-VALUE) for the cell at or near point.
If point is on a pipe separator or padding space, scans left then
right on the current line to find the nearest cell."
  (clutch--cell-at-or-near (point)))

(defun clutch--render-result ()
  "Render the result buffer content as one horizontally scrollable table.
Preserves point position (row + column) across the render."
  (let* ((save-ridx (or (get-text-property (point) 'clutch-row-idx)
                        (clutch--row-idx-at-line)))
         (save-cidx (get-text-property (point) 'clutch-col-idx))
         (inhibit-read-only t)
         (visible-cols (clutch--visible-columns))
         (widths (clutch--effective-widths))
         (rows (or clutch--filtered-rows clutch--result-rows))
         (render-state (clutch--build-render-state))
         (nw (clutch--row-number-digits))
         (row-positions (make-vector (+ (length rows)
                                        (length clutch--pending-inserts))
                                     nil)))
    (erase-buffer)
    (setq clutch--row-start-positions row-positions)
    (clutch--refresh-footer-line)
    (clutch--refresh-header-line)
    (clutch--insert-data-rows rows row-positions render-state)
    (clutch--insert-pending-insert-rows visible-cols widths nw (length rows)
                                        row-positions render-state)
    (if save-ridx
        (clutch--goto-cell save-ridx save-cidx)
      (goto-char (point-min)))))

(defun clutch--reindex-row-starts-from (ridx)
  "Recompute `clutch--row-start-positions' from RIDX to the end of the buffer."
  (when (and (vectorp clutch--row-start-positions)
             (integerp ridx)
             (<= 0 ridx)
             (< ridx (length clutch--row-start-positions)))
    (let ((line-pos (aref clutch--row-start-positions ridx))
          (idx ridx)
          (len (length clutch--row-start-positions)))
      (when line-pos
        (save-excursion
          (goto-char line-pos)
          (while (and (< idx len)
                      (not (eobp)))
            (aset clutch--row-start-positions idx (point))
            (setq idx (1+ idx))
            (forward-line 1))
          (while (< idx len)
            (aset clutch--row-start-positions idx nil)
            (setq idx (1+ idx))))))))

(defun clutch--replace-row-at-index (ridx)
  "Re-render row RIDX in place without a full body redraw.
Falls back to `clutch--refresh-display' when row-local replacement is unsafe."
  (let* ((rows (or clutch--filtered-rows clutch--result-rows))
         (nrows (length rows))
         (line-pos (and (vectorp clutch--row-start-positions)
                        (integerp ridx)
                        (<= 0 ridx)
                        (< ridx (length clutch--row-start-positions))
                        (aref clutch--row-start-positions ridx))))
    (if (or (not line-pos)
            (not (integerp ridx))
            (< ridx 0)
            (>= ridx nrows))
        (clutch--refresh-display)
      (let* ((save-ridx (get-text-property (point) 'clutch-row-idx))
             (save-cidx (get-text-property (point) 'clutch-col-idx))
             (next-pos (and (< (1+ ridx) (length clutch--row-start-positions))
                            (aref clutch--row-start-positions (1+ ridx))))
             (end-pos (or next-pos
                          (save-excursion
                            (goto-char line-pos)
                            (forward-line 1)
                            (point))))
             (render-state (clutch--build-render-state))
             (line (clutch--render-row-line ridx render-state))
             (inhibit-read-only t))
        (save-excursion
          (goto-char line-pos)
          (delete-region line-pos end-pos)
          (insert line))
        (clutch--reindex-row-starts-from ridx)
        (when save-ridx
          (clutch--goto-cell save-ridx save-cidx))))))

(defun clutch--col-idx-at-point ()
  "Return the column index at point, from data cells."
  (get-text-property (point) 'clutch-col-idx))

(defun clutch--column-border-position (cidx)
  "Return the buffer column of the left border `│' for column CIDX."
  (let* ((widths (clutch--effective-widths))
         (nw (clutch--row-number-digits))
         (pad clutch-column-padding)
         ;; │ + mark + row-num + padding
         (pos (+ 1 1 nw pad)))
    (dotimes (i cidx)
      (cl-incf pos (+ 1 (* 2 pad) (aref widths i))))
    pos))

(defun clutch--ensure-point-visible-horizontally ()
  "Scroll the result window so the cell at point is fully visible.
When the target column's border is outside the visible area, snap
hscroll to that column's left border so it appears at the window edge."
  (when-let* ((win (get-buffer-window (current-buffer)))
              (cidx (get-text-property (point) 'clutch-col-idx)))
    (let* ((hscroll (window-hscroll win))
           (width (max 1 (window-body-width win)))
           (widths (clutch--effective-widths))
           (pad clutch-column-padding)
           (border (clutch--column-border-position cidx))
           (col-end (+ border 1 (* 2 pad) (aref widths cidx))))
      (cond
       ;; Column border is left of visible area — page backward:
       ;; place target column at the right edge of the window.
       ((< border hscroll)
        (set-window-hscroll win (max 0 (- col-end width))))
       ;; Column right edge extends past visible area — page forward:
       ;; place target column at the left edge of the window.
       ((> col-end (+ hscroll width))
        (set-window-hscroll win border))))))

(defun clutch--goto-cell (ridx cidx)
  "Move point to the cell at ROW-IDX RIDX and COL-IDX CIDX.
Falls back to the same row (any column), then point-min."
  (let* ((line-pos (and (vectorp clutch--row-start-positions)
                        (integerp ridx)
                        (<= 0 ridx)
                        (< ridx (length clutch--row-start-positions))
                        (aref clutch--row-start-positions ridx)))
         found)
    (if line-pos
        (progn
          (goto-char line-pos)
          (let ((eol (line-end-position)))
            (setq found
                  (or (and cidx (text-property-any (point) eol 'clutch-col-idx cidx))
                      (text-property-not-all (point) eol 'clutch-col-idx nil)))
            (if found
                (goto-char found)
              (goto-char (point-min)))))
      (goto-char (point-min))
      (while (and (not found)
                  (setq found (text-property-search-forward
                               'clutch-row-idx ridx #'eq)))
        (let ((beg (prop-match-beginning found)))
          (if (eq (get-text-property beg 'clutch-col-idx) cidx)
              (goto-char beg)
            (setq found nil))))
      (unless found
        ;; Fall back: find the same row, any column
        (goto-char (point-min))
        (if-let* ((m (text-property-search-forward 'clutch-row-idx ridx #'eq)))
            (goto-char (prop-match-beginning m))
          (goto-char (point-min)))))
    (clutch--ensure-point-visible-horizontally)))

(defun clutch--row-number-digits ()
  "Return the digit width needed for row numbers."
  (let* ((row-count (length clutch--result-rows))
         (global-last (+ (or clutch--page-offset
                             (* clutch--page-current clutch-result-max-rows))
                         row-count)))
    (max 3 (length (number-to-string global-last)))))

(defun clutch--refresh-display ()
  "Re-render the current result table after column-width recalculation.
Preserve cursor position (row + column) and the top visible row."
  (when clutch--column-widths
    (let* ((save-ridx (or (get-text-property (point) 'clutch-row-idx)
                          (clutch--row-idx-at-line)))
           (save-cidx (get-text-property (point) 'clutch-col-idx))
           (win (get-buffer-window (current-buffer)))
           (win-width (if win (window-body-width win) 80))
           (save-top-ridx
            (when win
              (with-selected-window win
                (save-excursion
                  (goto-char (window-start win))
                  (clutch--row-idx-at-line))))))
      (setq clutch--last-window-width win-width)
      (setq clutch--header-active-col nil)
      (when clutch--row-overlay
        (delete-overlay clutch--row-overlay)
        (setq clutch--row-overlay nil))
      (clutch--render-result)
      (when save-ridx
        (clutch--goto-cell save-ridx save-cidx)
        (when (and win (integerp save-top-ridx))
          (with-selected-window win
            (when-let* ((top-pos (and (vectorp clutch--row-start-positions)
                                      (<= 0 save-top-ridx)
                                      (< save-top-ridx (length clutch--row-start-positions))
                                      (aref clutch--row-start-positions save-top-ridx))))
              (set-window-start win top-pos))))))))

(defun clutch--window-size-change (frame)
  "Handle window resizing for clutch display buffers in FRAME."
  (dolist (win (window-list frame 'no-mini))
    (let ((buf (window-buffer win)))
      (when (buffer-local-value 'clutch--column-widths buf)
        (let ((new-width (window-body-width win)))
          (unless (eq new-width
                      (buffer-local-value 'clutch--last-window-width buf))
            (with-current-buffer buf
              (clutch--refresh-display))))))))

(defun clutch--enable-window-size-hook ()
  "Ensure `clutch--window-size-change' is installed once."
  (add-hook 'window-size-change-functions #'clutch--window-size-change))

(defun clutch--disable-window-size-hook-if-unused (&optional ignore-buffer)
  "Remove the `window-size' hook when no result buffers remain.
IGNORE-BUFFER is excluded from liveness checks."
  (unless (cl-some (lambda (buf)
                     (and (buffer-live-p buf)
                          (not (eq buf ignore-buffer))
                          (with-current-buffer buf
                            (derived-mode-p 'clutch-result-mode))))
                   (buffer-list))
    (remove-hook 'window-size-change-functions #'clutch--window-size-change)))

(defun clutch--result-buffer-cleanup ()
  "Cleanup hook state when a result buffer is being removed."
  (clutch--disable-window-size-hook-if-unused (current-buffer)))

(provide 'clutch-ui)
;;; clutch-ui.el ends here
