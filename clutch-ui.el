;;; clutch-ui.el --- Result rendering and display helpers -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Internal result rendering and display helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)
(require 'json)
(require 'seq)
(require 'subr-x)
(require 'clutch-backend)

(defvar clutch--executing-p)

(defvar clutch--aggregate-summary)
(defvar clutch--active-edit-cell)
(defvar clutch--base-query)
(defvar clutch--cell-default-placeholder)
(defvar clutch--cell-generated-placeholder)
(defvar-local clutch--column-widths nil
  "Vector of display widths for each result column.")
(defvar-local clutch--column-pixel-widths nil
  "Graphical pixel widths for the current result columns, or nil.")
(defvar-local clutch--column-pixel-metric nil
  "Font metric signature used for `clutch--column-pixel-widths'.")
(defvar-local clutch--cell-render-cache nil
  "Cache of unpadded result cell content and measured pixel widths.")
(defvar-local clutch--cell-render-cache-signature nil
  "Signature for `clutch--cell-render-cache'.")
(defvar-local clutch--char-pixel-width-cache nil
  "Cache of per-character pixel widths for result cell measurement.")
(defvar-local clutch--char-pixel-width-cache-signature nil
  "Signature for `clutch--char-pixel-width-cache'.")
(defvar-local clutch--column-pixel-logical-widths nil
  "Logical column widths used for `clutch--column-pixel-widths'.")
(defvar-local clutch--column-width-refresh-timer nil
  "Idle timer used to coalesce repeated column-width redraws.")
(defvar clutch--conn-sql-product)
(defvar clutch--column-displayer-version 0
  "Version counter for registered column display functions.")
(defvar clutch--dml-result)
(defvar clutch--filter-pattern)
(defvar clutch--fk-info)
(defvar clutch--filtered-rows)
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
(defvar-local clutch--connection-render-state nil
  "Semantic connection state rendered by this buffer's UI.
The connection workflow supplies a plist containing only display inputs; it
must not contain a connection object, params, callbacks, or rendered text.")
(defvar-local clutch--execution-spinner-frame nil
  "Current execution spinner frame supplied by the connection workflow.")
(defvar-local clutch--header-line-string nil
  "Full header-line string before hscroll adjustment.")
(defvar-local clutch--header-sort-function nil
  "Function called by header sort clicks.
The function is called with the column index and the column name captured when
the header cell was rendered.")
(defvar-local clutch--last-window-width nil
  "Last known window body width for the current result buffer.")
(defvar clutch--marked-rows)
(defvar clutch--order-by)
(defvar clutch--page-current)
(defvar clutch--page-has-more)
(defvar clutch--page-offset)
(defvar clutch--page-total-rows)
(defvar clutch--pending-deletes)
(defvar clutch--pending-edits)
(defvar clutch--pending-inserts)
(defvar clutch--query-elapsed)
(defvar clutch--result-source-table)
(defvar clutch--result-server-pageable)
(defvar clutch--result-server-rewritable)
(defvar clutch--result-column-defs)
(defvar clutch--result-columns)
(defvar clutch--result-rows)
(defvar clutch--row-identity)
(defvar clutch--row-identity-status)
(defvar clutch--row-identity-error-message)
(defvar-local clutch--executed-sql-overlay nil
  "Overlay marking the last SQL execution status.")
(defvar-local clutch--row-overlay nil
  "Overlay used to highlight the current row.")
(defvar-local clutch--row-start-positions nil
  "Vector mapping rendered row indices to their line start positions.")
(defvar-local clutch--last-cell-position nil
  "Last resolved data cell as (ROW-IDX . COL-IDX).")
(defvar clutch--local-sort-column-index)
(defvar clutch--sort-column)
(defvar clutch--sort-descending)
(defvar clutch--result-column-details)
(defvar clutch--where-filter)
(defvar clutch--refine-rect)
(defvar clutch--refine-excluded-rows)
(defvar clutch--refine-excluded-cols)
(defvar clutch--refine-overlays)
(defvar clutch--refine-callback)
(defvar clutch--refine-saved-mode-line)
(defvar clutch-column-padding)
(defvar clutch-column-width-max)
(defvar clutch-connection)
(defvar clutch-describe--header-base)
(defvar clutch-result-max-rows)

(defconst clutch--null-cell-display-text "<null>"
  "Display text for database NULL values in result cells.")

(defun clutch--null-display-string ()
  "Return the propertized display string for database NULL values."
  (propertize clutch--null-cell-display-text 'face 'clutch-null-face))

(defconst clutch--column-width-refresh-delay 0.08
  "Seconds before applying a throttled column-width redraw.")

(declare-function clutch--cached-column-details "clutch-schema" (conn table))
(declare-function clutch--ensure-column-details-async "clutch-schema" (conn table))

(defun clutch--result-display-rows ()
  "Return result rows selected by the current client filter state."
  (if clutch--filter-pattern clutch--filtered-rows clutch--result-rows))

(defun clutch--set-column-displayers (symbol value)
  "Set SYMBOL to VALUE and invalidate custom displayer render caches."
  (set-default-toplevel-value symbol value)
  (cl-incf clutch--column-displayer-version))

(defcustom clutch-column-displayers nil
  "Per-table/per-column display functions for result cells.
Each entry is (TABLE-NAME . ((COLUMN-NAME . FUNCTION) ...)).
TABLE-NAME and COLUMN-NAME are matched case-insensitively.
FUNCTION receives the raw cell value and must return a string, which may
include text properties.  Return nil to fall back to default rendering."
  :type '(alist :key-type string
                :value-type (alist :key-type string :value-type function))
  :set #'clutch--set-column-displayers
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
nil from FUNCTION to fall back to default display.  Keep FUNCTION pure and
cheap because it runs on the result rendering path.

Example:

  (defun my-clutch-status-displayer (value)
    (pcase value
      (0 (propertize \"pending\" (quote face) (quote warning)))
      (1 (propertize \"active\" (quote face) (quote success)))
      (2 (propertize \"done\" (quote face) (quote shadow)))))

  (dolist (table \\='(\"orders\" \"tasks\" \"jobs\"))
    (clutch-register-column-displayer
     table \"status\" #\\='my-clutch-status-displayer))"
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
  (cl-incf clutch--column-displayer-version)
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
    (let (removed)
      (setcdr table-entry
              (cl-remove-if
               (lambda (column-entry)
                 (when (clutch--case-insensitive-string=
                        (car-safe column-entry) column)
                   (setq removed t)))
               (cdr table-entry)))
      (when removed
        (when (null (cdr table-entry))
          (setq clutch-column-displayers
                (delq table-entry clutch-column-displayers)))
        (cl-incf clutch--column-displayer-version))))
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

(defun clutch--json-normalize-text (text)
  "Return TEXT parsed and serialized as compact JSON."
  (clutch--json-serialize-text (json-parse-string text)))

(defun clutch--json-value-to-string (val)
  "Convert VAL to valid JSON text for JSON viewing."
  (cond
   ((null val)
    "null")
   ((and (stringp val)
         (fboundp 'json-serialize)
         (fboundp 'json-parse-string))
    (condition-case nil
        (clutch--json-normalize-text val)
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

(defun clutch--json-ts-mode-available-p ()
  "Return non-nil when `json-ts-mode' can be enabled now."
  (and (fboundp 'json-ts-mode)
       (or (not (fboundp 'treesit-language-available-p))
           (treesit-language-available-p 'json))))

(defun clutch--json-display-mode ()
  "Enable the best available JSON display mode in the current buffer."
  (cond
   ((clutch--json-ts-mode-available-p)
    (json-ts-mode))
   ((fboundp 'json-mode)
    (json-mode))
   ((fboundp 'js-mode)
    (js-mode))
   (t
    (special-mode))))

(defun clutch--json-metadata-text (text)
  "Return TEXT pretty-printed as JSON metadata."
  (ignore (json-parse-string text))
  (with-temp-buffer
    (insert text)
    (json-pretty-print-buffer)
    (string-trim-right (buffer-string))))

(defun clutch--show-json-text-buffer (buffer-name text)
  "Show JSON TEXT in BUFFER-NAME and return the displayed buffer."
  (let ((buf (get-buffer-create buffer-name)))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (clutch--json-display-mode)
        (erase-buffer)
        (insert (clutch--json-metadata-text text))
        (insert "\n")
        (font-lock-ensure)
        (setq buffer-read-only t)
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

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

(defun clutch--pixel-padding-string (cells pixels)
  "Return CELLS padding characters displayed as PIXELS pixels.
Keeping the original logical width preserves the existing hscroll and
point-navigation model while tightening graphical alignment."
  (cond
   ((and (> cells 0)
         (= pixels (* cells (default-font-width))))
    (make-string cells ?\s))
   ((> cells 0)
    (let ((padding (make-string cells ?\s)))
      (put-text-property 0 1 'display
                         (if (> pixels 0)
                             `(space :width (,pixels))
                           "")
                         padding)
      (when (> cells 1)
        (put-text-property 1 cells 'display "" padding))
      padding))
   ((> pixels 0)
    ;; A zero-width carrier adds pixels without changing logical columns.
    (propertize (string #x200b) 'display `(space :width (,pixels))))
   (t "")))

(defun clutch--pad-display-string (string width pixel-width &optional right-align
                                          string-pixel-width)
  "Pad STRING to WIDTH text cells for result display.
On graphical displays, pad to PIXEL-WIDTH actual pixels.  RIGHT-ALIGN pads
before STRING.  STRING-PIXEL-WIDTH reuses an existing measurement when non-nil."
  (if pixel-width
      (let* ((cells (max 0 (- width (string-width string))))
             (padding (max 0 (- pixel-width
                                (or string-pixel-width
                                    (string-pixel-width string)))))
             (pad-string (clutch--pixel-padding-string cells padding)))
        (if right-align
            (concat pad-string string)
          (concat string pad-string)))
    (clutch--string-pad string width right-align)))

(defun clutch--center-display-string (string width pixel-width)
  "Center STRING within WIDTH text cells and optional PIXEL-WIDTH."
  (if pixel-width
      (let* ((pads (clutch--center-padding-widths (string-width string) width))
             (extra (max 0 (- pixel-width
                              (string-pixel-width string))))
             (left (/ extra 2))
             (right (- extra left)))
        (concat (clutch--pixel-padding-string (car pads) left)
                string
                (clutch--pixel-padding-string (cdr pads) right)))
    (let* ((pads (clutch--center-padding-widths (string-width string) width))
           (lead (make-string (car pads) ?\s))
           (trail (make-string (cdr pads) ?\s)))
      (concat lead string trail))))

(defun clutch--pixel-metric-signature ()
  "Return the current graphical font metric signature, or nil."
  (when (and (fboundp 'default-font-width)
             (fboundp 'string-pixel-width)
             (display-graphic-p))
    (let* ((cell-width (default-font-width))
           (samples '("m" "i" "W" "中" "あ" "한" "m中あ한"))
           (pixel-widths (mapcar #'string-pixel-width samples)))
      (unless (cl-loop for sample in samples
                       for pixels in pixel-widths
                       always (= pixels (* cell-width
                                           (string-width sample))))
        (list cell-width
              (copy-tree face-remapping-alist)
              pixel-widths)))))

(defun clutch--string-has-properties-p (string)
  "Return non-nil when STRING has any text properties."
  (catch 'found
    (let ((pos 0)
          (len (length string)))
      (while (< pos len)
        (when (text-properties-at pos string)
          (throw 'found t))
        (setq pos (or (next-property-change pos string) len))))
    nil))

(defun clutch--char-pixel-width-cache (pixel-metric)
  "Return the character pixel-width cache for PIXEL-METRIC."
  (unless (and (hash-table-p clutch--char-pixel-width-cache)
               (equal clutch--char-pixel-width-cache-signature pixel-metric))
    (setq clutch--char-pixel-width-cache (make-hash-table :test 'eql)
          clutch--char-pixel-width-cache-signature pixel-metric))
  clutch--char-pixel-width-cache)

(defun clutch--plain-string-pixel-width (string pixel-metric)
  "Return pixel width for plain STRING using per-character PIXEL-METRIC cache."
  (catch 'fallback
    (let ((cache (clutch--char-pixel-width-cache pixel-metric))
          (pixels 0))
      (dotimes (i (length string))
        (let* ((char (aref string i))
               (cached (gethash char cache)))
          (when (zerop (char-width char))
            (throw 'fallback (string-pixel-width string)))
          (setq pixels
                (+ pixels
                   (or cached
                       (let ((width (string-pixel-width (char-to-string char))))
                         (puthash char width cache)
                         width))))))
      pixels)))

(defun clutch--cell-string-pixel-width (string pixel-metric)
  "Return pixel width for result cell STRING under PIXEL-METRIC."
  (if (or (null pixel-metric)
          (clutch--string-has-properties-p string))
      (string-pixel-width string)
    (clutch--plain-string-pixel-width string pixel-metric)))

(defun clutch--format-elapsed (seconds)
  "Format SECONDS as a human-readable duration."
  (if (< seconds 1.0)
      (format "%dms" (round (* seconds 1000)))
    (format "%.3fs" seconds)))

(defun clutch--transient-state-display (state choices)
  "Return a transient state display for STATE from CHOICES.
CHOICES is an alist of (VALUE . LABEL) entries in display order."
  (concat
   (propertize "(" 'face 'transient-delimiter)
   (mapconcat
    (lambda (choice)
      (propertize (cdr choice)
                  'face (if (eq state (car choice))
                            'transient-value
                          'transient-inactive-value)))
    choices
    (propertize "|" 'face 'transient-delimiter))
   (propertize ")" 'face 'transient-delimiter)))

(defun clutch--numeric-type-p (col-def)
  "Return non-nil if COL-DEF is a numeric column type."
  (eq (plist-get col-def :type-category) 'numeric))

(defun clutch--long-field-type-p (col-def)
  "Return non-nil if COL-DEF is a long field type (JSON/BLOB)."
  (memq (plist-get col-def :type-category) '(json blob)))

(defconst clutch--structured-text-leading-chars
  '(?\s ?\t ?\n ?\r)
  "Leading whitespace characters that may precede structured cell text.")

(defun clutch--json-like-string-p (val)
  "Return non-nil when string VAL appears to contain JSON text."
  (and (stringp val)
       (> (length val) 0)
       (let ((first (aref val 0)))
         (and (or (= first ?{)
                  (= first ?\[)
                  (memq first clutch--structured-text-leading-chars))
              (string-match-p "\\`\\s-*[{\\[]" val)))))

(defun clutch--json-cell-value-p (val col-def)
  "Return non-nil when VAL/COL-DEF should use JSON cell rendering."
  (or (eq (plist-get col-def :type-category) 'json)
      (clutch--json-like-string-p val)))

(defun clutch--json-string-end (text start)
  "Return end index of JSON string in TEXT beginning at START."
  (let ((pos (1+ start))
        (len (length text))
        (escaped nil))
    (while (and (< pos len)
                (or escaped (/= (aref text pos) ?\")))
      (setq escaped (and (not escaped) (= (aref text pos) ?\\)))
      (cl-incf pos))
    (min len (1+ pos))))

(defun clutch--json-token-boundary-p (text pos)
  "Return non-nil when POS is a token boundary in TEXT."
  (or (>= pos (length text))
      (memq (aref text pos) '(?\s ?\t ?\n ?\r ?} ?\] ?, ?:))))

(defun clutch--json-key-face ()
  "Return the face used for JSON object keys in result cells."
  (if (facep 'font-lock-property-name-face)
      'font-lock-property-name-face
    'font-lock-variable-name-face))

(defun clutch--json-display-highlight (text)
  "Return TEXT with lightweight JSON token faces for result cells."
  (let ((display (copy-sequence text))
        (pos 0)
        (len (length text))
        (case-fold-search nil))
    (while (< pos len)
      (let ((ch (aref text pos)))
        (cond
         ((= ch ?\")
          (let* ((end (clutch--json-string-end text pos))
                 (after end))
            (while (and (< after len)
                        (memq (aref text after) '(?\s ?\t ?\n ?\r)))
              (cl-incf after))
            (put-text-property
             pos end 'face
             (if (and (< after len) (= (aref text after) ?:))
                 (clutch--json-key-face)
               'font-lock-string-face)
             display)
            (setq pos end)))
         ((memq ch '(?{ ?} ?\[ ?\] ?: ?,))
          (put-text-property pos (1+ pos) 'face 'shadow display)
          (cl-incf pos))
         ((and (string-match
                "-?\\(?:0\\|[1-9][0-9]*\\)\\(?:\\.[0-9]+\\)?\\(?:[eE][+-]?[0-9]+\\)?"
                text pos)
               (= (match-beginning 0) pos))
          (put-text-property
           pos (match-end 0) 'face 'font-lock-constant-face display)
          (setq pos (match-end 0)))
         ((and (string-match "\\(?:true\\|false\\|null\\)" text pos)
               (= (match-beginning 0) pos)
               (clutch--json-token-boundary-p text (match-end 0)))
          (put-text-property
           pos (match-end 0) 'face 'font-lock-keyword-face display)
          (setq pos (match-end 0)))
         (t
          (cl-incf pos)))))
    display))

(defconst clutch--xml-cell-highlight-max-chars 2000
  "Maximum XML cell length eligible for inline result-cell highlighting.")

(defun clutch--xml-display-highlight (text)
  "Return TEXT with lightweight XML token faces for result cells."
  (let ((display (copy-sequence text))
        (pos 0)
        (tag-re "<\\(?:!--[^>]*--\\|!\\[CDATA\\[[^>]*\\]\\]\\|/?\\([[:alpha:]_][[:alnum:]_.:-]*\\)\\([^<>]*\\)\\|\\?xml[^>]*\\?\\)>")
        (attr-re "\\([[:alpha:]_][[:alnum:]_.:-]*\\)\\s-*=\\s-*\\(\"[^\"]*\"\\|'[^']*'\\)")
        (case-fold-search nil))
    (while (string-match tag-re text pos)
      (let* ((beg (match-beginning 0))
             (end (match-end 0))
             (token (match-string 0 text))
             (tag-beg (match-beginning 1))
             (tag-end (match-end 1)))
        (cond
         ((string-prefix-p "<!--" token)
          (put-text-property beg end 'face 'font-lock-comment-face display))
         ((string-prefix-p "<![CDATA[" token)
          (put-text-property beg end 'face 'font-lock-string-face display))
         (tag-beg
          (put-text-property beg (min end (1+ beg)) 'face 'shadow display)
          (put-text-property (max beg (1- end)) end 'face 'shadow display)
          (when (and (< (1+ beg) end) (= (aref text (1+ beg)) ?/))
            (put-text-property (1+ beg) (+ beg 2) 'face 'shadow display))
          (put-text-property tag-beg tag-end 'face 'font-lock-function-name-face display)
          (let ((scan tag-end))
            (while (and (string-match attr-re text scan)
                        (< (match-beginning 0) end))
              (put-text-property (match-beginning 1) (match-end 1)
                                 'face (clutch--json-key-face) display)
              (put-text-property (match-beginning 2) (match-end 2)
                                 'face 'font-lock-string-face display)
              (put-text-property (1- (match-beginning 2)) (match-beginning 2)
                                 'face 'shadow display)
              (setq scan (match-end 0))))))
        (setq pos end)))
    display))

(defun clutch--truncate-display-string (text width)
  "Return TEXT truncated to WIDTH with a compact ellipsis when possible."
  (if (<= (string-width text) width)
      text
    (truncate-string-to-width text width nil nil
                              (and (>= width 1) "…"))))

(defun clutch--cell-visible-prefix (text width)
  "Return (DISPLAY . TRUNCATED) for TEXT rendered in cell WIDTH.
Newlines are shown as `↵'.  Long strings are scanned only until
the visible prefix is known."
  (let ((limit (max 0 width))
        (used 0)
        (pos 0)
        (len (length text))
        parts
        truncated)
    (while (and (< pos len) (not truncated))
      (let* ((char (aref text pos))
             (piece (if (= char ?\n) "↵" (char-to-string char)))
             (piece-width (string-width piece)))
        (if (> (+ used piece-width) limit)
            (setq truncated t)
          (push piece parts)
          (setq used (+ used piece-width)
                pos (1+ pos)))))
    (if (and (not truncated) (= pos len))
        (cons (string-join (nreverse parts) "") nil)
      (cons (if (>= width 1)
                (concat (truncate-string-to-width
                         (string-join (nreverse parts) "")
                         (1- width) nil nil "")
                        "…")
              "")
            t))))

(defun clutch--xml-like-string-p (val)
  "Return non-nil when string VAL appears to contain XML text.
Uses a stricter heuristic to avoid misclassifying plain \"<...\" text."
  (and (stringp val)
       (> (length val) 0)
       (or (= (aref val 0) ?<)
           (memq (aref val 0) clutch--structured-text-leading-chars))
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

(defun clutch--blob-text-display-value-p (val)
  "Return non-nil when blob VAL should be displayed as structured text."
  (and (stringp val)
       (or (clutch--json-like-string-p val)
           (clutch--xml-like-string-p val))))

(defun clutch--value-placeholder (val col-def)
  "Return compact placeholder text for VAL/COL-DEF in result grid."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((and (eq cat 'blob)
           (not (clutch--blob-text-display-value-p val)))
      "<BLOB>")
     (t nil))))

(defun clutch--cell-placeholder-value (val)
  "Return display placeholder text for special cell VAL, or nil."
  (pcase val
    (:clutch-generated-placeholder "<generated>")
    (:clutch-default-placeholder "<default>")
    (_ nil)))

(defun clutch--column-width-sample (val)
  "Return the untruncated result-cell text used to size VAL."
  (or (clutch--cell-placeholder-value val)
      (and (null val) clutch--null-cell-display-text)
      (clutch--format-value val)))

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
      (if (and (eq (plist-get (nth i column-defs) :type-category) 'blob)
               (<= max-w clutch-column-width-max)
               (not (seq-some (lambda (row)
                                (clutch--blob-text-display-value-p (nth i row)))
                              sample)))
          (aset widths i 10)
        (let ((header-w (string-width (nth i col-names)))
              (data-w 0))
          (dolist (row sample)
            (let ((formatted (clutch--column-width-sample (nth i row))))
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

(defun clutch--visible-column-specs (visible-cols widths)
  "Return render specs for VISIBLE-COLS using WIDTHS.
Each spec is [CIDX WIDTH COL-DEF NUMERIC-P].  Precomputing these values keeps
wide-table rendering from repeatedly walking column definition lists."
  (let* ((defs (and clutch--result-column-defs
                    (vconcat clutch--result-column-defs)))
         (def-count (length defs)))
    (mapcar (lambda (cidx)
              (let ((col-def (and (< cidx def-count)
                                  (aref defs cidx))))
                (vector cidx
                        (aref widths cidx)
                        col-def
                        (clutch--numeric-type-p col-def))))
            visible-cols)))

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

(defvar clutch--header-sort-indicator-cache (make-hash-table :test 'equal)
  "Cache header sort indicators by icon identity and display metrics.")

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
installed, the family is unsupported, or the renderer returns nil/empty."
  (pcase-let ((`(,family . ,icon-name) name))
    (let* ((fn (alist-get family clutch--nerd-icons-function-alist))
           (icon (and (clutch--nerd-icons-available-p)
                      (if (and fn (fboundp fn))
                          (apply fn icon-name icon-args)
                        (clutch--nerd-icons-warn-unavailable-family family)
                        nil))))
      (if (and (stringp icon)
               (not (string-empty-p icon)))
          icon
        (or fallback "")))))

(defun clutch--header-sort-indicator-glyph (spec fallback)
  "Return sort indicator SPEC/FALLBACK snapped to integral cell width."
  (let* ((graphical (display-graphic-p))
         (cell-width (and graphical
                          (fboundp 'default-font-width)
                          (default-font-width)))
         (metric (and cell-width
                      (> cell-width 0)
                      (fboundp 'string-pixel-width)
                      (list cell-width
                            (frame-parameter nil 'font)
                            face-remapping-alist)))
         (key (list spec fallback metric))
         (missing (make-symbol "missing"))
         (cached (gethash key clutch--header-sort-indicator-cache missing)))
    (if (not (eq cached missing))
        (copy-sequence cached)
      (let* ((raw (clutch--icon spec fallback))
             (indicator
              (if (and metric (not (string-empty-p raw)))
                  (let* ((pixels (string-pixel-width raw))
                         (raw-width (string-width raw))
                         (cells (max raw-width
                                     (ceiling (/ (float pixels) cell-width))))
                         (target (* cells cell-width))
                         (pad (- target pixels)))
                    (if (or (> cells raw-width) (> pad 0))
                        (concat raw
                                (clutch--pixel-padding-string
                                 (- cells raw-width) pad))
                      raw))
                raw)))
        (puthash key indicator clutch--header-sort-indicator-cache)
        (copy-sequence indicator)))))

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

(defconst clutch--db-icon-specs
  ;; Each entry: (BACKEND . (ICON-SPEC FALLBACK :color COLOR &rest ICON-ARGS))
  ;; :color sets the icon foreground; remaining ICON-ARGS (e.g. :height) are
  ;; forwarded to the nerd-icons function.
  '((mysql      . ((devicon . "nf-dev-mysql")              ""  :color "#469AD7"))
    (pg         . ((devicon . "nf-dev-postgresql")         ""  :color "#336791"))
    (sqlite     . ((devicon . "nf-dev-sqlite")             ""  :color "#3A7EC6"))
    (jdbc       . ((mdicon  . "nf-md-database_cog_outline") "" :color "#59636e"))
    (oracle     . ((mdicon  . "nf-md-alpha_o_circle")      "O" :color "#C74634"))
    (sqlserver  . ((devicon . "nf-dev-microsoftsqlserver") ""  :color "#CC2927"))
    (snowflake  . ((mdicon  . "nf-md-snowflake")           "❄" :color "#29B5E8"))
    (db2        . ((mdicon  . "nf-md-database")            ""  :color "#1F70C1"))
    (redshift   . ((mdicon  . "nf-md-database")            ""  :color "#8C4FFF"))
    (clickhouse . ((faicon  . "nf-fa-barcode")             ""  :color "#FFCC00"))
    (mongodb    . ((devicon . "nf-dev-mongodb")            ""  :color "#47A248"))
    (redis      . ((devicon . "nf-dev-redis")              ""  :color "#DC382D")))
  "Alist mapping backend symbols to icon specs.
Each value is (ICON-SPEC FALLBACK :color COLOR &rest ICON-ARGS).
ICON-ARGS beyond :color are forwarded to the nerd-icons render function.")

(defun clutch--db-backend-icon-for-key (key)
  "Return a colored backend icon for KEY, or nil."
  (when-let* ((spec (alist-get key clutch--db-icon-specs)))
    (let* ((rest      (cddr spec))
           (color     (plist-get rest :color))
           (icon-args (cl-loop for (k v) on rest by #'cddr
                               unless (eq k :color) nconc (list k v)))
           (icon      (apply #'clutch--icon (car spec) (cadr spec) icon-args)))
      (if (and color (not (string-empty-p icon)))
          (propertize icon 'face `(:foreground ,color :inherit ,(get-text-property 0 'face icon)))
        icon))))

(defun clutch--completion-backend-icon-prefix (key)
  "Return a minibuffer completion icon prefix for backend KEY."
  (let ((icon (clutch--db-backend-icon-for-key key)))
    (if (and icon
             (not (string-empty-p icon))
             (clutch--nerd-icons-available-p))
        (concat icon " ")
      "")))

(defun clutch--backend-header-line-segment (backend-key backend-label)
  "Return a header-line segment for BACKEND-KEY and BACKEND-LABEL, or nil.
When nerd-icons is available, show only the icon; otherwise fall back
to the display name (e.g. \"MySQL\")."
  (let ((icon (clutch--db-backend-icon-for-key backend-key)))
    (cond
     ((and icon (not (string-empty-p icon))
           (clutch--nerd-icons-available-p))
      icon)
     (backend-label (propertize backend-label 'face 'bold)))))

(defun clutch--connection-state-icon (connected)
  "Return a connection state icon for CONNECTED."
  (if connected
      (clutch--icon '(mdicon . "nf-md-database_check_outline") "⬢")
    (clutch--icon '(mdicon . "nf-md-database_off") "⨯")))

(defun clutch--transaction-header-line-segment (transaction-state)
  "Return a header-line segment for semantic TRANSACTION-STATE, or nil.
TRANSACTION-STATE is one of `auto', `manual', or `dirty'."
  (when transaction-state
    (let* ((state-face (pcase transaction-state
                         ('auto 'success)
                         ('manual 'warning)
                         ('dirty 'error)))
           (icon (clutch--icon-with-face '(mdicon . "nf-md-database_lock")
                                         "⛁" state-face))
           (label (pcase transaction-state
                    ('auto "Tx: Auto")
                    ('manual "Tx: Manual")
                    ('dirty "Tx: Manual*"))))
      (concat (unless (string-empty-p icon)
                (concat icon " "))
              (propertize label 'face state-face)))))

(defun clutch--namespace-header-line-segment (namespace)
  "Return a header-line segment for current NAMESPACE, or nil."
  (when namespace
    (let ((icon (clutch--icon-with-face '(mdicon . "nf-md-sitemap_outline")
                                        "≣" 'header-line)))
      (if (string-empty-p icon)
          namespace
        (format "%s %s" icon namespace)))))

(defun clutch--schema-state-header-line-segment (schema-state)
  "Return a header-line segment for semantic SCHEMA-STATE, or nil."
  (pcase schema-state
    ('refreshing (propertize "schema…" 'face 'shadow))
    ('stale (propertize "schema~" 'face 'warning))
    ('failed (propertize "schema!" 'face 'error))))

(defun clutch--header-line-indent ()
  "Return leading spaces to align header-line text with the buffer text area.
Accounts for the line-number gutter when `display-line-numbers-mode' is on."
  (make-string (max 1 (line-number-display-width)) ?\s))

(defun clutch--render-connection-header-line (state connected-p)
  "Render a connection header-line from semantic STATE and CONNECTED-P."
  (let ((indent (clutch--header-line-indent)))
    (if (not connected-p)
        (let* ((sep          (propertize "  •  " 'face 'shadow))
               (backend      (clutch--backend-header-line-segment
                              (plist-get state :backend-key)
                              (plist-get state :backend-label)))
               (disconnect   (propertize
                              (concat (clutch--connection-state-icon nil)
                                      " DISCONNECTED")
                              'face 'warning))
               (parts        (delq nil (list (if backend
                                                 backend
                                               nil)
                                             disconnect))))
          (concat indent
                  (if parts
                      (mapconcat #'identity parts sep)
                    disconnect)))
      (let* ((sep         (propertize "  •  " 'face 'shadow))
             (backend-sep (propertize "  ›  " 'face 'shadow))
             (backend     (clutch--backend-header-line-segment
                           (plist-get state :backend-key)
                           (plist-get state :backend-label)))
             (connection-label (plist-get state :connection-label))
             (key         (when connection-label
                            (concat (clutch--connection-state-icon t)
                                    " " connection-label)))
             (current-schema
              (clutch--namespace-header-line-segment
               (plist-get state :namespace)))
             (schema      (clutch--schema-state-header-line-segment
                           (plist-get state :schema-state)))
             (tx          (clutch--transaction-header-line-segment
                           (plist-get state :transaction-state)))
             (tail        (delq nil (list current-schema schema tx))))
        (concat indent
                (cond
                 ((and backend key)
                  (concat backend backend-sep key
                          (when tail
                            (concat sep (mapconcat #'identity tail sep)))))
                 (backend backend)
                 (key (mapconcat #'identity (cons key tail) sep))
                 (t (mapconcat #'identity tail sep))))))))

(defun clutch--footer-icon (spec fallback face)
  "Return footer icon SPEC/FALLBACK with explicit FACE."
  (concat (clutch--icon-with-face spec fallback face)
          (propertize " " 'face face)))

(defun clutch--disconnected-badge ()
  "Return a disconnected indicator string with warning face, or nil if connected."
  (unless (plist-get clutch--connection-render-state :connected-p)
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

(defun clutch--header-sort-indicator (name include-unsorted &optional cidx)
  "Return the sort indicator for column NAME.
When INCLUDE-UNSORTED is non-nil, return the neutral sort indicator for unsorted
columns; otherwise return nil for unsorted columns.  CIDX disambiguates
page-local sorts for duplicate column names."
  (let* ((state (cond
                 ((and clutch--sort-column
                       (string= name clutch--sort-column)
                       (or (null clutch--local-sort-column-index)
                           (and (integerp cidx)
                                (= cidx clutch--local-sort-column-index))))
                  (if clutch--sort-descending 'desc 'asc))
                 (include-unsorted 'none))))
    (when state
      (pcase-let ((`(,spec ,fallback)
                   (pcase state
                     ('desc '((octicon . "nf-oct-sort_desc") "↓"))
                     ('asc '((octicon . "nf-oct-sort_asc") "↑"))
                     ('none '((mdicon . "nf-md-sort") "↕")))))
        (clutch--header-sort-indicator-glyph spec fallback)))))

(defun clutch--header-label (name &optional include-unsorted-sort cidx)
  "Build the display label for column NAME.
Appends the sort indicator when the column is active.
When INCLUDE-UNSORTED-SORT is non-nil, append the neutral sort
indicator for unsorted columns too.  CIDX disambiguates duplicate names."
  (let* ((sort (clutch--header-sort-indicator name include-unsorted-sort cidx)))
    (if sort
        (concat (propertize name 'clutch-header-name t) " " sort)
      (propertize name 'clutch-header-name t))))

(defun clutch--header-cell-label (cidx width &optional active-cidx)
  "Return the styled header label for CIDX within logical WIDTH.
ACTIVE-CIDX identifies the highlighted column, if any."
  (let* ((name (nth cidx clutch--result-columns))
         (label (clutch--header-label name t cidx))
         (label (if (> (string-width label) width)
                    (truncate-string-to-width label width)
                  (copy-sequence label))))
    (add-face-text-property 0 (length label) 'clutch-field-name-face
                            'append label)
    (when (eql cidx active-cidx)
      (add-face-text-property 0 (length label) 'clutch-header-active-face
                              'append label))
    ;; Only underline the column name, not its spacer or sort indicator.
    (dotimes (i (length label))
      (when (get-text-property i 'clutch-header-name label)
        (add-face-text-property i (1+ i) '(:underline t) 'append label)))
    label))

(defun clutch--header-sort-keymap (cidx name)
  "Return a header-line keymap that cycles sorting for column CIDX named NAME."
  (let ((map (make-sparse-keymap))
        (col-idx cidx)
        (col-name name))
    (let ((command (lambda (event)
                     (interactive "e")
                     (let* ((start (and event (event-start event)))
                            (win (and start (posn-window start)))
                            (buf (if (windowp win)
                                     (window-buffer win)
                                   (current-buffer))))
                       (with-current-buffer buf
                         (unless clutch--header-sort-function
                           (user-error "Header sorting is not available"))
                         (funcall clutch--header-sort-function
                                  col-idx col-name))))))
      (define-key map [header-line mouse-1] command)
      (define-key map [mouse-1] command))
    map))

(defun clutch--cell-face (val edited cidx active-edit)
  "Return the display face for a cell with VAL at CIDX.
EDITED is the staged edit entry when modified.  ACTIVE-EDIT is non-nil
when the cell is open in a cell edit buffer."
  (cond (edited 'clutch-modified-face)
        (active-edit 'clutch-modified-face)
        ((clutch--cell-placeholder-value val) 'clutch-null-face)
        ((null val) 'clutch-null-face)
        ((assq cidx clutch--fk-info) 'clutch-fk-face)
        (t nil)))

(defun clutch--cell-display-content (val w col-def edited)
  "Return the unpadded display string for a cell value VAL in width W.
COL-DEF is the column definition plist, EDITED is a staged edit cons or nil."
  (let* ((display-val (if edited (cdr edited) val))
         (custom (clutch--cell-custom-display display-val col-def))
         (json-cell (and (not custom)
                         (not edited)
                         (clutch--json-cell-value-p display-val col-def)))
         (xml-cell (and (not custom)
                        (not edited)
                        (clutch--xml-like-string-p display-val)))
         (special-placeholder (and (not custom)
                                   (not edited)
                                   (clutch--cell-placeholder-value display-val)))
         (display (and (not custom)
                       (cond
                        (special-placeholder
                         (cons special-placeholder nil))
                        ((null display-val)
                         (cons clutch--null-cell-display-text nil))
                        (t
                         (clutch--cell-visible-prefix
                          (clutch--format-value display-val)
                          w)))))
         (s (car-safe display))
         (value-truncated (cdr-safe display))
         (placeholder (and value-truncated
                           (not custom)
                           (not edited)
                           (not special-placeholder)
                           (clutch--value-placeholder display-val col-def)))
         (formatted (or custom placeholder s))
         (truncated (if (or custom placeholder)
                        (> (string-width formatted) w)
                      value-truncated)))
    (cond
     ((and json-cell (not truncated))
      (clutch--json-display-highlight formatted))
     ((and xml-cell
           (not truncated)
           (<= (length formatted) clutch--xml-cell-highlight-max-chars))
      (clutch--xml-display-highlight formatted))
     (truncated
      (if (and (not custom) (not placeholder))
          formatted
        (clutch--truncate-display-string formatted w)))
     (t formatted))))

(defun clutch--pending-insert-placeholders ()
  "Return placeholder sentinels aligned with `clutch--result-columns'."
  (when (and clutch--pending-inserts
             clutch-connection
             clutch--result-source-table)
    (let ((details (clutch--cached-column-details
                    clutch-connection clutch--result-source-table)))
      (unless details
        (clutch--ensure-column-details-async
         clutch-connection clutch--result-source-table))
      (if details
          (mapcar (lambda (col-name)
                    (when-let* ((detail (cl-find col-name details
                                                 :key (lambda (d)
                                                        (plist-get d :name))
                                                 :test #'string=)))
                      (cond
                       ((plist-get detail :generated) clutch--cell-generated-placeholder)
                       ((plist-get detail :default) clutch--cell-default-placeholder)
                       (t nil))))
                  clutch--result-columns)
        (make-list (length clutch--result-columns) nil)))))

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
          :active-edit-cell clutch--active-edit-cell
          :row-identity row-identity)))

(defun clutch--row-render-identity (row render-state)
  "Return ROW identity values for RENDER-STATE, or nil."
  (when-let* ((row-identity (plist-get render-state :row-identity)))
    (clutch-db-row-identity-values row row-identity)))

(defun clutch--render-edit-entry-for-identity (identity-values cidx render-state)
  "Return staged edit entry for IDENTITY-VALUES/CIDX from RENDER-STATE."
  (when identity-values
    (gethash (cons identity-values cidx)
             (plist-get render-state :edits))))

(defun clutch--pending-insert-render-rows (render-state)
  "Return staged insert rows expanded with placeholders from RENDER-STATE."
  (let ((placeholders (plist-get render-state :insert-placeholders)))
    (mapcar
     (lambda (fields)
       (cl-mapcar (lambda (column placeholder)
                    (if-let* ((entry (assoc column fields)))
                        (cdr entry)
                      placeholder))
                  clutch--result-columns placeholders))
     clutch--pending-inserts)))

(defun clutch--pixel-layout-scan-columns (widths pixel-metric)
  "Return body columns that need pixel measurement for WIDTHS.
PIXEL-METRIC identifies the current graphical font metrics.  The return value
is nil when all visible columns need scanning, `:none' when no body cells need
scanning, or a list of changed column indices."
  (if (not (and (display-graphic-p)
                pixel-metric
                (equal pixel-metric clutch--column-pixel-metric)
                (vectorp clutch--column-pixel-widths)
                (vectorp clutch--column-pixel-logical-widths)
                (= (length widths)
                   (length clutch--column-pixel-widths)
                   (length clutch--column-pixel-logical-widths))
                (null clutch--pending-edits)
                (null clutch--pending-inserts)))
      nil
    (let (changed)
      (dolist (cidx (clutch--visible-columns))
        (unless (= (aref widths cidx)
                   (aref clutch--column-pixel-logical-widths cidx))
          (push cidx changed)))
      (if changed
          (nreverse changed)
        :none))))

(defun clutch--cell-render-cache (pixel-metric)
  "Return the cell render cache for PIXEL-METRIC."
  (let ((signature (list pixel-metric
                         clutch--column-displayer-version
                         clutch--result-source-table
                         clutch--result-columns)))
    (unless (and (hash-table-p clutch--cell-render-cache)
                 (equal clutch--cell-render-cache-signature signature))
      (setq clutch--cell-render-cache (make-hash-table :test 'equal)
            clutch--cell-render-cache-signature signature))
    clutch--cell-render-cache))

(defun clutch--cached-cell-render (val width cidx col-def edited face
                                       pixel-metric)
  "Return cached (CONTENT . PIXELS) for rendering VAL.
WIDTH is the logical display width, CIDX is the column index, COL-DEF is the
column metadata, EDITED is a staged edit entry, FACE is the cell face, and
PIXEL-METRIC identifies the current graphical font metrics."
  (let* ((display-val (if edited (cdr edited) val))
         (cache-key (list cidx width col-def display-val (and edited t) face))
         (cache (clutch--cell-render-cache pixel-metric))
         (cached (gethash cache-key cache)))
    (or cached
        (let* ((content (clutch--cell-display-content val width col-def edited))
               (measured (if face (copy-sequence content) content))
               pixels)
          (when face
            (add-face-text-property 0 (length measured) face 'append measured))
          (setq pixels (clutch--cell-string-pixel-width measured pixel-metric))
          (puthash cache-key (cons content pixels) cache)))))

(defun clutch--prepare-pixel-layout (widths rows render-state
                                            &optional base-pixel-widths first-ridx
                                            pixel-metric scan-cols)
  "Add graphical column measurements for WIDTHS and ROWS to RENDER-STATE.
The resulting pixel widths are shared by result headers and body cells.
Rendered cell content is cached so custom display functions run once.
When BASE-PIXEL-WIDTHS is non-nil, only grow those existing targets from ROWS,
whose first row index is FIRST-RIDX.
PIXEL-METRIC identifies the current graphical font metrics.
SCAN-COLS limits body cell measurement to those columns.  When it is `:none',
only headers are measured."
  (if pixel-metric
      (let* ((cell-width (default-font-width))
             (pixel-widths (if base-pixel-widths
                               (copy-sequence base-pixel-widths)
                             (make-vector (length widths) 0)))
             (visible-cols (clutch--visible-columns))
             (body-cols (cond
                         ((eq scan-cols :none) nil)
                         (scan-cols scan-cols)
                         (t visible-cols)))
             (body-specs (and body-cols
                              (clutch--visible-column-specs body-cols widths)))
             (all-rows (if base-pixel-widths
                           rows
                         (append rows
                                 (clutch--pending-insert-render-rows
                                  render-state)))))
        (unless base-pixel-widths
          (dotimes (cidx (length widths))
            (aset pixel-widths cidx (* cell-width (aref widths cidx)))))
        (when (and base-pixel-widths (listp scan-cols))
          (dolist (cidx scan-cols)
            (aset pixel-widths cidx (* cell-width (aref widths cidx)))))
        (dolist (cidx visible-cols)
          (let* ((width (aref widths cidx))
                 (label (clutch--header-cell-label cidx width)))
            (aset pixel-widths cidx
                  (max (aref pixel-widths cidx)
                       (string-pixel-width label)))))
        (cl-loop for row in all-rows
                 for ridx from (or first-ridx 0)
                 do
                 (let ((row-vector (if (vectorp row) row (vconcat row)))
                       (identity-values
                        (clutch--row-render-identity row render-state)))
                   (dolist (spec body-specs)
                     (let* ((cidx (aref spec 0))
                            (width (aref spec 1))
                            (col-def (aref spec 2))
                            (val (and (< cidx (length row-vector))
                                      (aref row-vector cidx)))
                            (edited (clutch--render-edit-entry-for-identity
                                     identity-values cidx render-state))
                            (face (clutch--cell-face
                                   val edited cidx
                                   (clutch--active-edit-cell-p
                                    ridx cidx render-state)))
                            (pixels (cdr (clutch--cached-cell-render
                                          val width cidx col-def
                                          edited face pixel-metric))))
                     (aset pixel-widths cidx
                           (max (aref pixel-widths cidx)
                                pixels))))))
        (setq render-state (plist-put render-state :pixel-metric pixel-metric))
        (plist-put render-state :pixel-widths pixel-widths))
    render-state))

(defun clutch--active-edit-cell-p (ridx cidx render-state)
  "Return non-nil when RIDX/CIDX is the active edit cell in RENDER-STATE."
  (let ((active (plist-get render-state :active-edit-cell)))
    (and active
         (= ridx (car active))
         (= cidx (cdr active)))))

(defun clutch--render-cell-value (val ridx cidx width col-def numericp
                                      edited face render-state)
  "Render VAL as cell CIDX at row RIDX.
WIDTH, COL-DEF, and NUMERICP are precomputed column metadata.  EDITED and FACE
are precomputed row/cell state from RENDER-STATE."
  (let* ((pixel-metric (plist-get render-state :pixel-metric))
         (pixel-widths (plist-get render-state :pixel-widths))
         (rendered (and pixel-widths
                        (clutch--cached-cell-render
                         val width cidx col-def edited face pixel-metric)))
         (content (if rendered
                      (car rendered)
                    (clutch--cell-display-content val width col-def edited)))
         (content-pixels (cdr-safe rendered))
         (padded (clutch--pad-display-string
                  content width (and pixel-widths (aref pixel-widths cidx))
                  numericp content-pixels))
         (pad-str (make-string clutch-column-padding ?\s))
         (body nil))
    (when (and (eq face 'clutch-fk-face)
               (not (string-empty-p content)))
      (let ((start (if numericp
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

(defun clutch--render-row (row ridx visible-cols widths render-state
                               &optional column-specs identity-values)
  "Render a single data ROW at row index RIDX.
VISIBLE-COLS is a list of column indices, WIDTHS is the width vector,
and RENDER-STATE carries cached render data.  COLUMN-SPECS and
IDENTITY-VALUES optionally reuse precomputed row metadata.
Returns a propertized string."
  (let ((row-vector (if (vectorp row) row (vconcat row)))
        (identity (or identity-values
                      (clutch--row-render-identity row render-state)))
        (specs (or column-specs
                   (clutch--visible-column-specs visible-cols widths))))
    (concat (mapconcat
             (lambda (spec)
               (let* ((cidx (aref spec 0))
                      (width (aref spec 1))
                      (col-def (aref spec 2))
                      (numericp (aref spec 3))
                      (val (and (< cidx (length row-vector))
                                (aref row-vector cidx)))
                      (edited (clutch--render-edit-entry-for-identity
                               identity cidx render-state))
                      (face (clutch--cell-face
                             val edited cidx
                             (clutch--active-edit-cell-p
                              ridx cidx render-state))))
                 (clutch--render-cell-value
                  val ridx cidx width col-def numericp edited face
                  render-state)))
             specs "")
          (propertize "│" 'face 'clutch-border-face))))

(defun clutch--render-row-line (row ridx visible-cols widths nw render-state
                                    &optional column-specs)
  "Return the rendered buffer line string for ROW at RIDX.
VISIBLE-COLS, WIDTHS, and NW are precomputed table layout values.
RENDER-STATE carries cached lookup tables for staged row state.
COLUMN-SPECS optionally precomputes visible column metadata."
  (let* ((global-first-row (or clutch--page-offset
                               (* clutch--page-current clutch-result-max-rows)))
         (bface 'clutch-border-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (marked-table (plist-get render-state :marked))
         (delete-table (plist-get render-state :deletes))
         (edit-row-table (plist-get render-state :edit-rows))
         (identity-values (clutch--row-render-identity row render-state))
         (deletingp (and identity-values
                         (gethash identity-values delete-table)))
         (editedp (and identity-values
                       (gethash identity-values edit-row-table)))
         (data-row (let ((rendered (clutch--render-row
                                    row ridx visible-cols widths render-state
                                    column-specs identity-values)))
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

(defun clutch--render-pending-insert-row-line
    (row iidx ridx visible-cols widths nw render-state &optional column-specs)
  "Return the rendered ghost insert ROW line.
ROW is the expanded insert row.  IIDX and RIDX are the staged-insert and
display row indexes.  VISIBLE-COLS, WIDTHS, NW, RENDER-STATE, and optional
COLUMN-SPECS use the same rendering contract as ordinary result rows."
  (let* ((bface 'clutch-border-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (data-row (propertize
                    (clutch--render-row row ridx visible-cols widths
                                        render-state column-specs)
                    'face 'clutch-pending-insert-face))
         (num-label (string-pad (format "I%d" (1+ iidx)) nw nil t)))
    (concat (propertize "│" 'face bface)
            (propertize "I" 'face 'clutch-pending-insert-face)
            (propertize num-label 'face 'clutch-pending-insert-face)
            pad-str
            data-row "\n")))

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
  "Build footer part for the active server or current-page sort."
  (when-let* ((state (cond
                      (clutch--order-by
                       (list (car clutch--order-by)
                             (cdr clutch--order-by) nil))
                      (clutch--sort-column
                       (list clutch--sort-column
                             (if clutch--sort-descending "DESC" "ASC") t)))))
    (pcase-let ((`(,column ,direction ,local-p) state))
      (let ((icon (if (string= (downcase direction) "desc")
                      '(octicon . "nf-oct-sort_desc")
                    '(octicon . "nf-oct-sort_asc")))
            (hi 'font-lock-keyword-face))
        (concat (clutch--footer-icon icon "↕" hi)
                (propertize (format "%s[%s]%s"
                                    (upcase direction) column
                                    (if local-p " page" ""))
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
      (let* ((warn-icon 'font-lock-warning-face)
             (warn-text '(:inherit font-lock-warning-face :weight normal))
             (error-message (and (eq clutch--row-identity-status 'error)
                                 clutch--row-identity-error-message))
             (warning (if error-message
                          (format "row identity error: %s"
                                  (truncate-string-to-width
                                   (string-trim error-message)
                                   80 nil nil "…"))
                        "row identity missing")))
        (concat (clutch--footer-icon '(codicon . "nf-cod-warning") "⚠" warn-icon)
                (propertize warning
                            'face warn-text
                            'help-echo
                            (and error-message
                                 (format "Row identity metadata failed: %s"
                                         error-message)))
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
           (clutch--transaction-header-line-segment
            (plist-get clutch--connection-render-state :transaction-state))
           (clutch--footer-sort-part)
           (clutch--footer-mutation-capability-part)
           (clutch--footer-pending-part)))))

(defun clutch--footer-timing-part ()
  "Return the dynamic footer timing segment for the current result buffer."
  (let ((hi 'font-lock-keyword-face))
    (when-let* ((payload (cond
                          (clutch--executing-p
                           (and clutch--execution-spinner-frame
                                (propertize clutch--execution-spinner-frame
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
             (col-name (if (> (string-width raw-name) 20)
                           (truncate-string-to-width raw-name 20 nil nil "...")
                         raw-name))
             (sep (propertize " • " 'face dim)))
        (concat (clutch--footer-icon '(mdicon . "nf-md-cursor_default_click_outline") "⌖" hi)
                (propertize (format "R-%d" (1+ ridx)) 'face hi)
                sep
                (propertize (format "Col-%d/%d" (1+ cidx) total-cols)
                            'face hi)
                (unless (string-empty-p col-name)
                  (concat " " (propertize (format "[%s]" col-name)
                                          'face hi))))))))

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
  "Return column widths adjusted for header sort indicators.
Columns with sort indicators get wider to fit the label."
  (let ((widths (copy-sequence clutch--column-widths)))
    (dotimes (cidx (length widths))
      (let* ((name (nth cidx clutch--result-columns))
             (label (clutch--header-label name t cidx))
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

(defun clutch--key-hint (key description)
  "Return a colored header-line hint for KEY and DESCRIPTION."
  (concat (propertize key 'face 'clutch-key-hint-key-face)
          " "
          (propertize description 'face 'clutch-key-hint-description-face)))

(defun clutch--key-hints (hints)
  "Return HINTS joined as colored header-line shortcut hints.
HINTS is a list of (KEY DESCRIPTION) pairs."
  (mapconcat
   (lambda (hint)
     (pcase-let ((`(,key ,description) hint))
       (clutch--key-hint key description)))
   hints
   (clutch--status-separator)))

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

(defun clutch--header-cell (cidx widths &optional active-cidx)
  "Build a single header cell string for column CIDX.
WIDTHS is the effective width vector.
ACTIVE-CIDX is the highlighted column index, if any."
  (let* ((name (nth cidx clutch--result-columns))
         (w (aref widths cidx))
         (pixel-width (and (vectorp clutch--column-pixel-widths)
                           (< cidx (length clutch--column-pixel-widths))
                           (aref clutch--column-pixel-widths cidx)))
         (label (clutch--header-cell-label cidx w active-cidx))
         (label (clutch--center-display-string label w pixel-width))
         (pad-str (make-string clutch-column-padding ?\s))
         (sort-map (clutch--header-sort-keymap cidx name))
         (body nil))
    (setq body (concat pad-str label pad-str))
    (add-text-properties 0 (length body)
                         `(clutch-header-col ,cidx
                           local-map ,sort-map
                           keymap ,sort-map
                           mouse-face mode-line-highlight
                           help-echo ,(format "mouse-1: cycle sort by %s" name))
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

(defun clutch--pixel-crop-left (string pixels)
  "Return STRING with PIXELS cropped from the left.
Display spaces can be cropped partially.  When the crop point falls inside an
ordinary glyph, replace the clipped remainder with zero-logical-width padding so
following content keeps its pixel position."
  (catch 'done
    (let ((pos 0)
          (len (length string))
          (remaining pixels))
      (while (and (< pos len) (> remaining 0))
        (let* ((next (1+ pos))
               (part (substring string pos next))
               (part-pixels (string-pixel-width part)))
          (cond
           ((<= part-pixels 0)
            (setq pos next))
           ((<= part-pixels remaining)
            (setq remaining (- remaining part-pixels)
                  pos next))
           (t
            (let* ((display (get-text-property pos 'display string))
                   (after (substring string next))
                   (carry (clutch--pixel-padding-string
                           0 (- part-pixels remaining))))
              (pcase display
                (`(space . ,props)
                 (let* ((rest (substring string pos))
                        (width (plist-get props :width))
                        (space-width (cond
                                      ((consp width) (car width))
                                      ((numberp width) width))))
                   (if space-width
                       (progn
                         (put-text-property
                          0 1 'display
                          `(space :width (,(max 0 (- space-width remaining))))
                         rest)
                         (throw 'done rest))
                     (throw 'done
                            (concat carry after)))))
                (_
                 (throw 'done (concat carry after)))))))))
      (substring string pos))))

(defun clutch--header-line-with-hscroll ()
  "Return the header string shifted to match `window-hscroll'.
The header-line should track body hscroll exactly."
  (when clutch--header-line-string
    (let* ((hs (window-hscroll))
           (str clutch--header-line-string)
           (width (string-width str)))
      (if (>= hs width)
          ""
        (if (and (> hs 0)
                 clutch--column-pixel-widths
                 (fboundp 'default-font-width)
                 (fboundp 'string-pixel-width)
                 (display-graphic-p))
            (clutch--pixel-crop-left str (* hs (default-font-width)))
          (truncate-string-to-width str width hs))))))

(defun clutch--schedule-pixel-metric-refresh ()
  "Schedule a redraw if graphical font metrics changed since render."
  (when (and clutch--column-pixel-widths
             clutch--column-pixel-metric
             (not (timerp clutch--column-width-refresh-timer)))
    (let ((metric (clutch--pixel-metric-signature)))
      (when (not (equal metric clutch--column-pixel-metric))
        (setq clutch--column-width-refresh-timer
              (run-at-time 0 nil #'clutch--run-column-width-refresh
                           (current-buffer)))))))

(defun clutch--header-line-display ()
  "Return the display-ready header-line string for result buffers."
  (clutch--schedule-pixel-metric-refresh)
  (concat (propertize " " 'display '(space :align-to 0))
          (or (clutch--header-line-with-hscroll) "")))

(defun clutch--insert-data-rows (rows row-positions visible-cols widths nw
                                      render-state)
  "Insert data ROWS into the current buffer.
ROW-POSITIONS stores line starts keyed by rendered row index.
VISIBLE-COLS, WIDTHS, and NW are precomputed table layout values.
RENDER-STATE contains render lookup tables for staged UI state."
  (let ((pos (point))
        (column-specs (clutch--visible-column-specs visible-cols widths))
        lines)
    (cl-loop for row in rows
             for ridx from 0
             for line = (clutch--render-row-line
                         row ridx visible-cols widths nw render-state
                         column-specs)
             do (aset row-positions ridx pos)
             do (cl-incf pos (length line))
             do (push line lines))
    (insert (mapconcat #'identity (nreverse lines) ""))))

(defun clutch--insert-pending-insert-rows (visible-cols widths nw nrows row-positions
                                                        render-state)
  "Append ghost rows for staged INSERT operations below the real data rows.
VISIBLE-COLS, WIDTHS describe columns.  NW is row-number digit width.
NROWS is the count of real rows (used to compute ghost row indices).
ROW-POSITIONS stores line starts keyed by rendered row index.
RENDER-STATE contains render lookup tables for staged UI state."
  (let ((column-specs (clutch--visible-column-specs visible-cols widths)))
    (cl-loop for row in (clutch--pending-insert-render-rows render-state)
             for iidx from 0
             for ridx = (+ nrows iidx)
             do (aset row-positions ridx (point))
             do (insert (clutch--render-pending-insert-row-line
                         row iidx ridx visible-cols widths nw render-state
                         column-specs)))))

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
  (let ((rows (clutch--result-display-rows)))
    (setq clutch--footer-base-string
          (clutch--render-footer
           (length rows) clutch--page-current
           clutch-result-max-rows clutch--page-total-rows
           clutch--page-offset clutch--page-has-more))
    (setq mode-line-format '(:eval (clutch--footer-mode-line-display)))
    (clutch--refresh-footer-display)))

(defun clutch--refresh-result-status-line (&optional footer-only)
  "Refresh result chrome without rebuilding the table body.
When FOOTER-ONLY is non-nil, preserve the current table header exactly.
DML result banners are semantic outcomes and are always preserved."
  (when (derived-mode-p 'clutch-result-mode)
    (clutch--refresh-footer-line)
    (unless (or footer-only clutch--dml-result)
      (clutch--refresh-header-line)
      (clutch--update-position-indicator))))

(defun clutch--position-indicator-parts (ridx cidx)
  "Return a formatted mode-line position string for RIDX and CIDX."
  (let* ((page-offset (or clutch--page-offset
                          (* clutch--page-current clutch-result-max-rows)))
         (global-row  (+ page-offset ridx))
         (rows        (clutch--result-display-rows))
         (row-count   (length rows))
         (ncols       (length clutch--result-columns))
         (col-name    (when cidx (nth cidx clutch--result-columns)))
         (parts       nil))
    (push (format "R-%d/%s • Col-%d/%d%s"
                  (1+ global-row)
                  (if clutch--page-total-rows
                      (number-to-string clutch--page-total-rows)
                    (number-to-string row-count))
                  (if cidx (1+ cidx) 0) ncols
                  (if col-name (format " [%s]" col-name) ""))
          parts)
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
    (when (and ridx cidx)
      (setq clutch--last-cell-position (cons ridx cidx)))
    (setq mode-line-position
          (when ridx (clutch--position-indicator-parts ridx cidx)))))

(defun clutch--clamp-point-to-result-grid ()
  "Move point back onto the result grid after scrolling past the last row."
  (when (and (vectorp clutch--row-start-positions)
             (> (length clutch--row-start-positions) 0)
             (not (clutch--cell-at-point)))
    (let* ((last-ridx (1- (length clutch--row-start-positions)))
           (last-row-start (aref clutch--row-start-positions last-ridx)))
      (when (>= (point) last-row-start)
        (let* ((win (get-buffer-window (current-buffer)))
               (hscroll (and win (window-hscroll win)))
               (old-point (point)))
          (clutch--goto-cell last-ridx (cdr-safe clutch--last-cell-position))
          (when (integerp hscroll)
            (set-window-hscroll win hscroll))
          (/= (point) old-point))))))

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

(defun clutch--sync-result-cursor-ui ()
  "Synchronize result cursor UI after a command.
Updates row/header/footer cursor state.  Skips scroll commands unless point must
be clamped to the result grid."
  (when clutch--column-widths
    (let ((point-clamped (clutch--clamp-point-to-result-grid)))
      (when (or point-clamped
                (not (memq this-command
                           '(clutch-result-widen-column
                             clutch-result-narrow-column
                             scroll-down-line scroll-up-line
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
            (clutch--refresh-header-line)))))))

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
  (when-let* ((ridx (get-text-property pos 'clutch-row-idx))
              (cidx (get-text-property pos 'clutch-col-idx)))
    (list ridx
          cidx
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
         (save-cidx (or (get-text-property (point) 'clutch-col-idx)
                        (and (equal save-ridx (car-safe clutch--last-cell-position))
                             (cdr-safe clutch--last-cell-position))))
         (inhibit-read-only t)
         (visible-cols (clutch--visible-columns))
         (widths (clutch--effective-widths))
         (rows (clutch--result-display-rows))
         (pixel-metric (clutch--pixel-metric-signature))
         (scan-cols (clutch--pixel-layout-scan-columns widths pixel-metric))
         (base-pixel-widths
          (and scan-cols
               (not (null clutch--column-pixel-widths))
               clutch--column-pixel-widths))
         (render-state (clutch--prepare-pixel-layout
                        widths rows (clutch--build-render-state)
                        base-pixel-widths nil pixel-metric scan-cols))
         (nw (clutch--row-number-digits))
         (row-positions (make-vector (+ (length rows)
                                        (length clutch--pending-inserts))
                                     nil)))
    (let ((pixel-widths (plist-get render-state :pixel-widths)))
      (setq clutch--column-pixel-widths pixel-widths
            clutch--column-pixel-metric pixel-metric
            clutch--column-pixel-logical-widths
            (and pixel-widths (copy-sequence widths))))
    (erase-buffer)
    (setq clutch--row-start-positions row-positions)
    (clutch--refresh-footer-line)
    (clutch--refresh-header-line)
    (clutch--insert-data-rows rows row-positions visible-cols widths nw
                              render-state)
    (clutch--insert-pending-insert-rows visible-cols widths nw (length rows)
                                        row-positions render-state)
    (if save-ridx
        (clutch--goto-cell save-ridx save-cidx)
      (goto-char (point-min)))))

(defun clutch--replace-row-at-index (ridx)
  "Re-render row RIDX in place without a full body redraw.
Falls back to `clutch--refresh-display' when row-local replacement is unsafe."
  (let* ((rows (clutch--result-display-rows))
         (nrows (length rows))
         (total-rows (and (vectorp clutch--row-start-positions)
                          (length clutch--row-start-positions)))
         (line-pos (and (vectorp clutch--row-start-positions)
                        (integerp ridx)
                        (<= 0 ridx)
                        (< ridx total-rows)
                        (aref clutch--row-start-positions ridx))))
    (if (or (not line-pos)
            (not total-rows)
            (not (integerp ridx))
            (< ridx 0)
            (>= ridx total-rows))
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
             (pendingp (>= ridx nrows))
             (iidx (- ridx nrows))
             (widths (clutch--effective-widths))
             (pixel-metric (clutch--pixel-metric-signature))
             (base-render-state (clutch--build-render-state))
             (row (if pendingp
                      (nth iidx
                           (clutch--pending-insert-render-rows
                            base-render-state))
                    (nth ridx rows)))
             (render-state (clutch--prepare-pixel-layout
                            widths (list row)
                            base-render-state
                            clutch--column-pixel-widths ridx
                            pixel-metric))
             (pixel-widths (plist-get render-state :pixel-widths))
             (inhibit-read-only t))
        (if (or (not row)
                (not (equal pixel-widths clutch--column-pixel-widths)))
            (clutch--refresh-display)
          (let* ((visible-cols (clutch--visible-columns))
                 (nw (clutch--row-number-digits))
                 (column-specs (clutch--visible-column-specs visible-cols widths))
                 (line (if pendingp
                           (clutch--render-pending-insert-row-line
                            row iidx ridx visible-cols widths nw
                            render-state column-specs)
                         (clutch--render-row-line
                          row ridx visible-cols widths nw render-state
                          column-specs))))
            (let ((delta (- (length line) (- end-pos line-pos))))
              (save-excursion
                (goto-char line-pos)
                (delete-region line-pos end-pos)
                (insert line))
              (aset clutch--row-start-positions ridx line-pos)
              (unless (zerop delta)
                (cl-loop for idx from (1+ ridx)
                         below (length clutch--row-start-positions)
                         for pos = (aref clutch--row-start-positions idx)
                         when pos
                         do (aset clutch--row-start-positions idx
                                  (+ pos delta)))))
            (when save-ridx
              (clutch--goto-cell save-ridx save-cidx))))))))

(defun clutch--append-pending-insert-row (iidx)
  "Append staged insert ghost row IIDX without a full body redraw when safe."
  (let* ((rows (clutch--result-display-rows))
         (nrows (length rows))
         (ridx (+ nrows iidx))
         (old-count (and (vectorp clutch--row-start-positions)
                         (length clutch--row-start-positions))))
    (if (or (not old-count)
            (/= old-count ridx))
        (clutch--refresh-display)
      (let* ((save-ridx (get-text-property (point) 'clutch-row-idx))
             (save-cidx (get-text-property (point) 'clutch-col-idx))
             (widths (clutch--effective-widths))
             (pixel-metric (clutch--pixel-metric-signature))
             (base-render-state (clutch--build-render-state))
             (row (nth iidx
                       (clutch--pending-insert-render-rows base-render-state)))
             (render-state (clutch--prepare-pixel-layout
                            widths (list row)
                            base-render-state
                            clutch--column-pixel-widths ridx
                            pixel-metric))
             (pixel-widths (plist-get render-state :pixel-widths))
             (inhibit-read-only t))
        (if (or (not row)
                (not (equal pixel-widths clutch--column-pixel-widths)))
            (clutch--refresh-display)
          (let* ((visible-cols (clutch--visible-columns))
                 (nw (clutch--row-number-digits))
                 (column-specs (clutch--visible-column-specs visible-cols widths))
                 (line (clutch--render-pending-insert-row-line
                        row iidx ridx visible-cols widths nw
                        render-state column-specs))
                 (new-positions (make-vector (1+ old-count) nil)))
            (dotimes (idx old-count)
              (aset new-positions idx (aref clutch--row-start-positions idx)))
            (save-excursion
              (goto-char (point-max))
              (aset new-positions ridx (point))
              (insert line))
            (setq clutch--row-start-positions new-positions)
            (when save-ridx
              (clutch--goto-cell save-ridx save-cidx))))))))

(defun clutch--delete-row-at-index (ridx)
  "Delete rendered row RIDX without a full redraw when it is the final row.
Falls back to `clutch--refresh-display' when deleting RIDX would require
renumbering later rendered rows."
  (let* ((old-count (and (vectorp clutch--row-start-positions)
                         (length clutch--row-start-positions)))
         (line-pos (and old-count
                        (integerp ridx)
                        (<= 0 ridx)
                        (< ridx old-count)
                        (aref clutch--row-start-positions ridx))))
    (if (or (not line-pos)
            (not (= ridx (1- old-count))))
        (clutch--refresh-display)
      (let* ((save-ridx (get-text-property (point) 'clutch-row-idx))
             (save-cidx (get-text-property (point) 'clutch-col-idx))
             (end-pos (save-excursion
                        (goto-char line-pos)
                        (forward-line 1)
                        (point)))
             (new-count (1- old-count))
             (new-positions (make-vector new-count nil))
             (inhibit-read-only t))
        (dotimes (idx new-count)
          (aset new-positions idx (aref clutch--row-start-positions idx)))
        (delete-region line-pos end-pos)
        (setq clutch--row-start-positions new-positions)
        (cond
         ((and save-ridx (< save-ridx new-count))
          (clutch--goto-cell save-ridx save-cidx))
         ((> new-count 0)
          (clutch--goto-cell (1- new-count) save-cidx))
         (t
          (goto-char (point-min))))))))

(defun clutch--col-idx-at-point ()
  "Return the column index at point, from data cells."
  (get-text-property (point) 'clutch-col-idx))

(defun clutch--column-border-position (cidx &optional widths nw)
  "Return the buffer column of the left border `│' for column CIDX.
Optional WIDTHS and NW reuse precomputed effective widths and row-number width."
  (let* ((widths (or widths (clutch--effective-widths)))
         (nw (or nw (clutch--row-number-digits)))
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
           (border (clutch--column-border-position cidx widths))
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

(defun clutch--center-column-in-window (cidx)
  "Horizontally scroll the current result window to center CIDX."
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let ((widths (clutch--effective-widths)))
      (when (and (vectorp widths)
                 (<= 0 cidx)
                 (< cidx (length widths)))
        (let* ((window-width (max 1 (window-body-width win)))
               (border (clutch--column-border-position cidx widths))
               (column-width (+ 1 (* 2 clutch-column-padding)
                                (aref widths cidx)))
               (target (if (>= column-width window-width)
                           border
                         (max 0 (- border
                                   (/ (- window-width column-width) 2))))))
          (set-window-hscroll win target))))))

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
    (when-let* ((cell (clutch--cell-at (point))))
      (setq clutch--last-cell-position (cons (nth 0 cell) (nth 1 cell))))
    (clutch--ensure-point-visible-horizontally)))

(defun clutch--row-number-digits ()
  "Return the digit width needed for row numbers."
  (let* ((row-count (length (clutch--result-display-rows)))
         (global-last (+ (or clutch--page-offset
                             (* clutch--page-current clutch-result-max-rows))
                         row-count)))
    (max 3 (length (number-to-string global-last)))))

(defun clutch--refresh-display ()
  "Re-render the current result table after column-width recalculation.
Preserve cursor position, top visible row, and horizontal scroll."
  (when clutch--column-widths
    (when (timerp clutch--column-width-refresh-timer)
      (cancel-timer clutch--column-width-refresh-timer)
      (setq clutch--column-width-refresh-timer nil))
    (let* ((save-ridx (or (get-text-property (point) 'clutch-row-idx)
                          (clutch--row-idx-at-line)))
           (save-cidx (or (get-text-property (point) 'clutch-col-idx)
                          (and (equal save-ridx (car-safe clutch--last-cell-position))
                               (cdr-safe clutch--last-cell-position))))
           (win (get-buffer-window (current-buffer)))
           (win-width (if win (window-body-width win) 80))
           (save-hscroll (and win (window-hscroll win)))
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
      (if (and (window-live-p win)
               (eq (window-buffer win) (current-buffer)))
          (with-selected-window win
            (clutch--render-result))
        (clutch--render-result))
      (when save-ridx
        (clutch--goto-cell save-ridx save-cidx)
        (when (and win (integerp save-top-ridx))
          (with-selected-window win
            (when-let* ((top-pos (and (vectorp clutch--row-start-positions)
                                      (<= 0 save-top-ridx)
                                      (< save-top-ridx
                                         (length clutch--row-start-positions))
                                      (aref clutch--row-start-positions
                                            save-top-ridx))))
              (set-window-start win top-pos)))))
      (when (and (integerp save-hscroll)
                 (window-live-p win)
                 (eq (window-buffer win) (current-buffer)))
        (set-window-hscroll win save-hscroll)))))

(defun clutch--run-column-width-refresh (buffer)
  "Apply a pending coalesced column-width redraw in BUFFER."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (setq clutch--column-width-refresh-timer nil)
      (when (derived-mode-p 'clutch-result-mode)
        (clutch--refresh-display)))))

(defun clutch--schedule-column-width-refresh ()
  "Schedule a throttled redraw for modified column widths."
  (unless (timerp clutch--column-width-refresh-timer)
    (let ((buffer (current-buffer)))
      (setq clutch--column-width-refresh-timer
            (run-at-time clutch--column-width-refresh-delay
                         nil
                         #'clutch--run-column-width-refresh
                         buffer)))))

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
  (when (timerp clutch--column-width-refresh-timer)
    (cancel-timer clutch--column-width-refresh-timer)
    (setq clutch--column-width-refresh-timer nil))
  (clutch--disable-window-size-hook-if-unused (current-buffer)))

(provide 'clutch-ui)
;;; clutch-ui.el ends here
