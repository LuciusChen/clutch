;;; clutch-edit.el --- Staged result edit and insert workflow -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Internal result editing, insert, and staged mutation helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'clutch-schema)
(require 'json)

(defvar clutch--cached-pk-indices)
(defvar clutch--row-identity)
(defvar clutch--filtered-rows)
(defvar clutch--last-query)
(defvar clutch--connection-params)
(defvar clutch--active-edit-cell)
(defvar clutch--result-column-defs)
(defvar clutch--result-columns)
(defvar clutch--result-rows)
(defvar clutch--result-source-table)
(defvar clutch--row-identity-error-message)
(defvar clutch--row-identity-status)
(defvar clutch--row-start-positions)
(defvar clutch-insert-validation-idle-delay)
(defvar clutch-connection)
(defvar clutch-record--result-buffer)
(defvar clutch-record--row-idx)

(defvar-local clutch--fk-info nil
  "Foreign key info for the current result.")
(defvar clutch--marked-rows)
(defvar-local clutch--pending-deletes nil
  "List of row identity vectors staged for deletion.")
(defvar-local clutch--pending-edits nil
  "Alist of staged edits: ((IDENTITY-VEC . COL-IDX) . NEW-VALUE).")
(defvar-local clutch--pending-inserts nil
  "List of field alists staged for insertion.")

(declare-function clutch--execute "clutch-query" (sql &optional conn result-context))
(declare-function clutch--format-value "clutch-ui" (value))
(declare-function clutch--json-ts-mode-available-p "clutch-ui" ())
(declare-function clutch--json-value-to-string "clutch-ui" (value))
(declare-function clutch--json-like-string-p "clutch-ui" (value))
(declare-function clutch--key-hints "clutch-ui" (hints))
(declare-function clutch--null-display-string "clutch-ui" ())
(declare-function clutch--result-source-table-or-user-error "clutch-ui" (op))
(declare-function clutch--result-display-rows "clutch-ui" ())
(declare-function clutch--run-db-query "clutch-connection" (conn sql &optional params))
(declare-function clutch--string-pad "clutch-ui" (s width &optional pad-left numeric))
(declare-function clutch--visible-column-names "clutch-ui" ())
(declare-function clutch--visible-columns "clutch-ui" ())
(declare-function clutch--refresh-footer-line "clutch-ui" ())
(declare-function clutch--refresh-display "clutch-ui" ())
(declare-function clutch--replace-row-at-index "clutch-ui" (ridx))
(declare-function clutch-record--render "clutch-result" ())
(declare-function clutch--message-count "clutch-ui" (value))
(declare-function clutch--message-keyword "clutch-ui" (value))
(declare-function clutch--message-path "clutch-ui" (value))
(declare-function clutch--status-separator "clutch-ui" ())
(declare-function clutch--cell-at-point "clutch-ui" ())
(declare-function clutch--goto-cell "clutch-ui" (ridx cidx))
(declare-function clutch--row-idx-at-line "clutch-ui" ())
(declare-function clutch--selected-row-indices "clutch-ui" ())
(declare-function clutch-db-escape-identifier "clutch-backend" (conn name))
(declare-function clutch-db-result-affected-rows "clutch-backend" (result))
(declare-function clutch-db-result-p "clutch-backend" (result))
(declare-function clutch-db-sql-find-top-level-clause "clutch-backend" (sql pattern &optional start))
(declare-function clutch-db-substitute-params "clutch-backend" (sql params render-fn))
(declare-function clutch-db-foreign-keys "clutch-backend" (conn table))

(defun clutch-edit--sql-surface-p ()
  "Return non-nil when SQL staged mutation is available in this result."
  (clutch-db-sql-surface-p clutch-connection clutch--connection-params))

(defun clutch-edit--require-sql-staged-mutation (op)
  "Signal unless SQL staged mutation OP is available for this result."
  (unless (clutch-edit--sql-surface-p)
    (user-error
     "%s is SQL-only and is not available for non-SQL results" op)))

;;;; Cell editing (C-c ')

(defvar-local clutch-result--edit-callback nil
  "Callback for the cell edit buffer: (lambda (new-value) ...).")

(defvar-local clutch-result--edit-result-buffer nil
  "The result buffer to commit edits to after `clutch-result-edit-finish'.")

(defvar-local clutch-result-edit--return-buffer nil
  "Buffer that opened the current single-cell edit buffer.")

(defvar-local clutch-result-edit--result-viewport nil
  "Result window viewport captured when the edit buffer opened.")

(defvar-local clutch-result-edit--initial-value-state nil
  "Cons of null-state and editable text captured when the edit buffer opened.")

(defvar-local clutch-result-edit--column-name nil
  "Column name for the current single-cell edit buffer.")

(defvar-local clutch-result-edit--column-def nil
  "Column definition plist for the current single-cell edit buffer.")

(defvar-local clutch-result-edit--column-detail nil
  "Schema detail plist for the current single-cell edit buffer.")

(defvar-local clutch-result-edit--row-idx nil
  "Source row index for the current single-cell edit buffer.")

(defvar-local clutch-result-edit--validation-timer nil
  "Idle validation timer for the current single-cell edit buffer.")

(defvar-local clutch-result-edit--error-message nil
  "Current validation message for the single-cell edit buffer, or nil.")

(defvar-local clutch-result-edit--target-cell nil
  "Cons cell (ROW-IDX . COL-IDX) represented by this edit buffer.")

(defvar-local clutch-result-edit--null-p nil
  "Non-nil when this edit buffer is explicitly staging database NULL.")

(defvar-local clutch-result-edit--null-placeholder-overlay nil
  "Overlay displaying the database NULL placeholder in an edit buffer.")

(defvar-local clutch-result-edit-json--parent-buffer nil
  "Parent edit buffer for the current JSON sub-editor.")

(defvar-local clutch-result-edit-json--field-name nil
  "Field name for the current JSON sub-editor.")

(defvar-local clutch-result-edit-json--whole-edit-p nil
  "Non-nil when this JSON editor owns the whole result cell edit.")

(defvar clutch--result-edit-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") #'clutch-result-edit-finish)
    (define-key map (kbd "C-c C-k") #'clutch-result-edit-cancel)
    (define-key map (kbd "C-c '") #'clutch-result-edit-json-field)
    (define-key map (kbd "M-TAB") #'clutch-result-edit-complete-field)
    (define-key map (kbd "C-M-i") #'clutch-result-edit-complete-field)
    (define-key map (kbd "C-c .") #'clutch-result-edit-set-current-time)
    (define-key map (kbd "C-c C-n") #'clutch-result-edit-set-null)
    map)
  "Keymap for the cell edit buffer.")

(define-minor-mode clutch--result-edit-mode
  "Minor mode for editing a database cell value.
\\<clutch--result-edit-mode-map>
\\[clutch-result-edit-finish]  Accept edit
\\[clutch-result-edit-cancel]  Cancel
\\[clutch-result-edit-complete-field]  Complete enum/bool-like values
\\[clutch-result-edit-set-current-time]  Set temporal field to now
\\[clutch-result-edit-set-null]  Set database NULL
\\[clutch-result-edit-json-field]  Open JSON editor"
  :lighter " DB-Edit"
  :keymap clutch--result-edit-mode-map
  (if clutch--result-edit-mode
      (progn
        (add-hook 'after-change-functions #'clutch-result-edit--after-change nil t)
        (add-hook 'kill-buffer-hook #'clutch-result-edit--clear-active-target nil t))
    (remove-hook 'after-change-functions #'clutch-result-edit--after-change t)
    (remove-hook 'kill-buffer-hook #'clutch-result-edit--clear-active-target t)
    (clutch-result-edit--cancel-validation-timer)
    (clutch-result-edit--set-null-state nil)
    (setq-local clutch-result-edit--error-message nil)))

(defun clutch-result--column-detail (result-buf col-name)
  "Return schema detail plist for COL-NAME in RESULT-BUF, or nil."
  (with-current-buffer result-buf
    (when-let* ((table clutch--result-source-table)
                (details (clutch--ensure-column-details clutch-connection table t)))
      (cl-find-if (lambda (detail)
                    (equal (plist-get detail :name) col-name))
                  details))))

(defun clutch-result--field-candidates-from-detail (detail)
  "Return completion candidates derived from column DETAIL, or nil."
  (let ((type (downcase (or (plist-get detail :type) ""))))
    (or (clutch-result-insert--enum-candidates type)
        (cond
         ((member type '("boolean" "bool"))
          '("true" "false"))
         ((member type '("tinyint(1)" "bit(1)"))
          '("0" "1"))
         (t nil)))))

(defun clutch-result--field-json-p (col-def detail)
  "Return non-nil when COL-DEF/DETAIL describe a JSON field."
  (let ((type (downcase (or (plist-get detail :type) ""))))
    (or (eq (plist-get col-def :type-category) 'json)
        (string= type "json"))))

(defun clutch-result--editable-field-string (value col-def detail)
  "Return editable text for VALUE in a field described by COL-DEF/DETAIL."
  (if (clutch-result--field-json-p col-def detail)
      (clutch--json-value-to-string value)
    (clutch--format-value value)))

(defun clutch-result--field-metadata-tags (col-def detail)
  "Return short metadata tags for COL-DEF and DETAIL."
  (let* ((type-category (plist-get col-def :type-category))
         (type (downcase (or (plist-get detail :type) "")))
         tags)
    (when (clutch-result-insert--enum-candidates type)
      (push "enum" tags))
    (when (or (member type '("boolean" "bool"))
              (member type '("tinyint(1)" "bit(1)")))
      (push "bool" tags))
    (when (clutch-result--field-json-p col-def detail)
      (push "json" tags))
    (when (eq type-category 'date)
      (push "date" tags))
    (when (eq type-category 'time)
      (push "time" tags))
    (when (eq type-category 'datetime)
      (push "datetime" tags))
    (nreverse tags)))

(defun clutch-result--field-validation-message (field-name value col-def detail)
  "Validate FIELD-NAME/VALUE for COL-DEF/DETAIL.
Return nil when validation succeeds, or signal `user-error' when invalid."
  (clutch-result--validate-field-value field-name value col-def detail)
  nil)

(defun clutch-result-edit--field-candidates ()
  "Return completion candidates for the current edit buffer, or nil."
  (clutch-result--field-candidates-from-detail clutch-result-edit--column-detail))

(defun clutch-result-edit-completion-at-point ()
  "Return CAPF data for the current edit buffer, or nil."
  (when-let* ((candidates (clutch-result-edit--field-candidates)))
    (list (point-min) (point-max) candidates :exclusive 'no)))

(defun clutch-result-edit--temporal-p ()
  "Return non-nil when the current edit buffer is for a temporal column."
  (memq (plist-get clutch-result-edit--column-def :type-category)
        '(date time datetime)))

(defun clutch-result-edit--json-p ()
  "Return non-nil when the current edit buffer is for a JSON column."
  (clutch-result--field-json-p clutch-result-edit--column-def
                               clutch-result-edit--column-detail))

(defun clutch-result-edit--json-text-p (text)
  "Return non-nil when TEXT is a JSON object or array string."
  (and (clutch--json-like-string-p text)
       (condition-case nil
           (progn
             (ignore (json-parse-string text))
             t)
         (error nil))))

(defun clutch-result-edit--json-editor-available-p ()
  "Return non-nil when the current edit buffer can use the JSON editor."
  (or (clutch-result-edit--json-p)
      (and (not clutch-result-edit--null-p)
           (clutch-result-edit--json-text-p
            (string-trim
             (buffer-substring-no-properties (point-min) (point-max)))))))

(defun clutch-result-edit--metadata-tags ()
  "Return short metadata tags for the current edit buffer."
  (clutch-result--field-metadata-tags clutch-result-edit--column-def
                                      clutch-result-edit--column-detail))

(defun clutch-result-edit--set-null-state (enabled)
  "Set whether this edit buffer represents database NULL.
When ENABLED is non-nil, the buffer text is cleared and a visual
placeholder is displayed.  The placeholder is not part of the buffer text."
  (when (overlayp clutch-result-edit--null-placeholder-overlay)
    (delete-overlay clutch-result-edit--null-placeholder-overlay))
  (setq-local clutch-result-edit--null-placeholder-overlay nil
              clutch-result-edit--null-p enabled)
  (when enabled
    (erase-buffer)
    (let ((ov (make-overlay (point-min) (point-min) nil t nil)))
      (overlay-put ov 'after-string (clutch--null-display-string))
      (setq-local clutch-result-edit--null-placeholder-overlay ov))
    (goto-char (point-min))))

(defun clutch-result-edit--refresh-target-row (ridx)
  "Refresh rendered result row RIDX when the row is currently rendered."
  (when (and (integerp ridx)
             (vectorp clutch--row-start-positions)
             (<= 0 ridx)
             (< ridx (length clutch--row-start-positions)))
    (clutch--replace-row-at-index ridx)))

(defun clutch-result-edit--clear-active-target ()
  "Clear this edit buffer's active-cell marker from its result buffer."
  (let ((result-buf clutch-result--edit-result-buffer)
        (cell clutch-result-edit--target-cell))
    (when (and cell (buffer-live-p result-buf))
      (with-current-buffer result-buf
        (when (equal clutch--active-edit-cell cell)
          (setq-local clutch--active-edit-cell nil)
          (clutch-result-edit--refresh-target-row (car cell))))))
  (setq-local clutch-result-edit--target-cell nil))

(defun clutch-result-edit--restore-result-position
    (result-buf cell &optional return-buf viewport)
  "Restore point in RESULT-BUF to CELL and return to RETURN-BUF.
VIEWPORT, when non-nil, restores the result window start and hscroll."
  (when (and (buffer-live-p result-buf) cell)
    (pcase-let ((`(,ridx . ,cidx) cell))
      (if-let* ((win (and (or (not (buffer-live-p return-buf))
                              (eq return-buf result-buf))
                          (get-buffer-window result-buf t))))
          (with-selected-window win
            (clutch--goto-cell ridx cidx)
            (when viewport
              (when-let* ((raw-start (plist-get viewport :window-start))
                          (start (if (markerp raw-start)
                                     (marker-position raw-start)
                                   raw-start))
                          ((<= (point-min) start))
                          ((<= start (point-max))))
                (set-window-start win start))
              (when-let* ((hscroll (plist-get viewport :hscroll)))
                (set-window-hscroll win hscroll))))
        (with-current-buffer result-buf
          (clutch--goto-cell ridx cidx)))))
  (when (and (buffer-live-p return-buf)
             (not (eq return-buf result-buf)))
    (if-let* ((win (get-buffer-window return-buf t)))
        (select-window win)
      (pop-to-buffer return-buf))))

(defun clutch-result-edit--refresh-record-return-buffer (return-buf cidx)
  "Refresh RETURN-BUF when it is a Record buffer and keep point on CIDX."
  (when (and (integerp cidx)
             (buffer-live-p return-buf)
             (with-current-buffer return-buf
               (derived-mode-p 'clutch-record-mode)))
    (with-current-buffer return-buf
      (clutch-record--render)
      (goto-char (point-min))
      (when-let* ((match (text-property-search-forward
                          'clutch-col-idx cidx #'eq)))
        (goto-char (prop-match-beginning match))))))

(defun clutch-result-edit--header-line (_ridx _col-name)
  "Build the edit-buffer header line."
  (let* ((tags (clutch-result-edit--metadata-tags))
         (title (concat " Editing cell"
                        (when tags
                          (format " [%s]" (string-join tags " ")))))
         (affordances nil))
    (when (clutch-result-edit--field-candidates)
      (push '("M-TAB" "Complete") affordances))
    (when (clutch-result-edit--temporal-p)
      (push '("C-c ." "Now") affordances))
    (when (clutch-result-edit--json-editor-available-p)
      (push '("C-c '" "Edit JSON") affordances))
    (concat title
            (clutch--status-separator)
            (clutch--key-hints
             (append (nreverse affordances)
                     '(("C-c C-c" "Stage edit")
                       ("C-c C-k" "Cancel")
                       ("C-c C-n" "Set NULL")))))))

(defun clutch-result-edit--refresh-header-line ()
  "Refresh the edit-buffer header line, including any validation token."
  (setq-local
   header-line-format
   (concat
    (clutch-result-edit--header-line
     (or clutch-result-edit--row-idx 0)
     clutch-result-edit--column-name)
    (if clutch-result-edit--error-message
        (propertize
         (format "  [%s]"
                 (clutch-result-insert--short-error-message
                  clutch-result-edit--error-message))
         'face 'clutch-insert-inline-error-face)
      ""))))

(defun clutch-result-edit--cancel-validation-timer ()
  "Cancel any pending edit-buffer validation timer."
  (when (timerp clutch-result-edit--validation-timer)
    (cancel-timer clutch-result-edit--validation-timer))
  (setq-local clutch-result-edit--validation-timer nil))

(defun clutch-result-edit--current-validation-message ()
  "Return a validation message for the current edit buffer, or nil."
  (let* ((raw-value (string-trim-right (buffer-string)))
         (value (if clutch-result-edit--null-p nil raw-value)))
    (condition-case err
        (clutch-result--field-validation-message
         clutch-result-edit--column-name
         value
         clutch-result-edit--column-def
         clutch-result-edit--column-detail)
      (user-error (error-message-string err)))))

(defun clutch-result-edit--validate-live ()
  "Run local validation for the current edit buffer and refresh UI."
  (setq-local clutch-result-edit--error-message
              (clutch-result-edit--current-validation-message))
  (clutch-result-edit--refresh-header-line))

(defun clutch-result-edit--run-idle-validation (buffer)
  "Validate edit BUFFER after an idle delay."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (setq-local clutch-result-edit--validation-timer nil)
      (clutch-result-edit--validate-live))))

(defun clutch-result-edit--schedule-validation ()
  "Validate the current edit buffer after a short idle delay.
All field types use the same delay so feedback timing is consistent."
  (clutch-result-edit--cancel-validation-timer)
  (setq-local clutch-result-edit--validation-timer
              (run-with-idle-timer
               clutch-insert-validation-idle-delay nil
               #'clutch-result-edit--run-idle-validation
               (current-buffer))))

(defun clutch-result-edit--after-change (_beg _end _len)
  "Schedule local validation after any edit-buffer change."
  (when (and clutch-result-edit--null-p
             (> (buffer-size) 0))
    (clutch-result-edit--set-null-state nil))
  (clutch-result-edit--schedule-validation))

;;;###autoload
(defun clutch-result-edit-complete-field ()
  "Complete the current edit buffer when the column has candidates."
  (interactive)
  (let ((handled (completion-at-point)))
    (unless handled
      (let* ((candidates (or (clutch-result-edit--field-candidates)
                             (user-error "No completion candidates for %s"
                                         clutch-result-edit--column-name)))
             (current (buffer-substring-no-properties (point-min) (point-max)))
             (choice (completing-read (format "%s: " clutch-result-edit--column-name)
                                      candidates nil t current nil current)))
        (erase-buffer)
        (insert choice)))))

;;;###autoload
(defun clutch-result-edit-set-current-time ()
  "Replace the current edit buffer contents with a current time value."
  (interactive)
  (let ((value (or (clutch-result-insert--current-time-value
                    (plist-get clutch-result-edit--column-def :type-category))
                   (user-error "Column %s is not a date/time field"
                               clutch-result-edit--column-name))))
    (erase-buffer)
    (insert value)
    (goto-char (point-max))))

(defun clutch-result-edit-set-null ()
  "Set the current edit buffer to database NULL."
  (interactive)
  (clutch-result-edit--set-null-state t)
  (clutch-result-edit--validate-live)
  (message "Cell set to NULL"))

;;;###autoload
(defun clutch-result-edit-json-field (&optional whole-edit-p)
  "Open a dedicated JSON editor for the current edit buffer.
When WHOLE-EDIT-P is non-nil, saving or canceling the JSON editor also saves or
cancels the parent edit buffer."
  (interactive)
  (unless (clutch-result-edit--json-editor-available-p)
    (user-error "Column %s is not JSON and current text is not a JSON object or array"
                clutch-result-edit--column-name))
  (let* ((parent-buf (current-buffer))
         (field-name clutch-result-edit--column-name)
         (parent-text (buffer-substring-no-properties (point-min) (point-max)))
         (initial-text (clutch-result--json-normalize-string
                        (string-trim parent-text)
                        field-name))
         (buf-name (format "*clutch-edit-json: %s*" clutch-result-edit--column-name)))
    (let ((buf (clutch--open-json-sub-editor
                buf-name initial-text field-name
                #'clutch-result-edit-json-finish
                #'clutch-result-edit-json-cancel)))
      (with-current-buffer buf
        (setq-local clutch-result-edit-json--parent-buffer parent-buf
                    clutch-result-edit-json--field-name field-name
                    clutch-result-edit-json--whole-edit-p whole-edit-p))
      buf)))

(defun clutch--finish-json-sub-editor (parent missing-message updater
                                              &optional success-message)
  "Close the current JSON sub-editor and apply UPDATER in PARENT.
Signal MISSING-MESSAGE when PARENT is dead.  UPDATER runs in PARENT.
When SUCCESS-MESSAGE is non-nil, echo it after returning to PARENT."
  (unless (buffer-live-p parent)
    (user-error "%s" missing-message))
  (quit-window 'kill)
  (with-current-buffer parent
    (funcall updater))
  (pop-to-buffer parent)
  (when success-message
    (message "%s" success-message)))

(defun clutch--cancel-json-sub-editor (parent &optional restorer)
  "Close the current JSON sub-editor and return to live PARENT.
When RESTORER is non-nil, run it in PARENT before switching back."
  (quit-window 'kill)
  (when (buffer-live-p parent)
    (with-current-buffer parent
      (when restorer
        (funcall restorer)))
    (pop-to-buffer parent)))

;;;###autoload
(defun clutch-result-edit-json-finish ()
  "Save the JSON sub-editor contents back to the parent edit buffer."
  (interactive)
  (let* ((parent clutch-result-edit-json--parent-buffer)
         (field-name clutch-result-edit-json--field-name)
         (raw (string-trim (buffer-substring-no-properties (point-min) (point-max))))
         (value (clutch-result--json-normalize-string raw field-name)))
    (if clutch-result-edit-json--whole-edit-p
        (progn
          (unless (buffer-live-p parent)
            (user-error "Edit buffer no longer exists"))
          (quit-window 'kill)
          (with-current-buffer parent
            (erase-buffer)
            (insert value)
            (goto-char (point-min))
            (clutch-result-edit--finish-buffer t)))
      (clutch--finish-json-sub-editor
       parent "Edit buffer no longer exists"
       (lambda ()
         (erase-buffer)
         (insert value)
         (goto-char (point-min)))
       (format "Updated JSON for %s" field-name)))))

;;;###autoload
(defun clutch-result-edit-json-cancel ()
  "Cancel JSON sub-editing and return to the parent edit buffer."
  (interactive)
  (let ((parent clutch-result-edit-json--parent-buffer)
        (whole-edit-p clutch-result-edit-json--whole-edit-p))
    (if whole-edit-p
        (progn
          (quit-window 'kill)
          (when (buffer-live-p parent)
            (with-current-buffer parent
              (clutch-result-edit--cancel-buffer t))))
      (clutch--cancel-json-sub-editor parent))))

(defun clutch-result--edit-pending-insert (ridx)
  "Re-open staged insert at result row RIDX in the insert buffer."
  (let* ((display-rows (clutch--result-display-rows))
         (nrows (length display-rows))
         (iidx (- ridx nrows)))
    (unless (and (>= iidx 0) (< iidx (length clutch--pending-inserts)))
      (user-error "No staged insert at this row"))
    (let ((table (or clutch--result-source-table
                     (user-error "Cannot detect source table")))
          (fields (nth iidx clutch--pending-inserts)))
      (clutch-result-insert--open-buffer table (current-buffer) fields iidx))))

(defun clutch-result-edit--open-cell (cell return-buffer)
  "Open an edit buffer for CELL using the current result buffer state.
RETURN-BUFFER is the buffer that invoked the edit command."
  (pcase-let* ((`(,ridx ,cidx ,val) cell))
    (if (>= ridx (length (clutch--result-display-rows)))
        (clutch-result--edit-pending-insert ridx)
      (let* ((op "edit cell")
             (table (clutch--result-source-table-or-user-error op))
             (row-identity (clutch-result--row-identity-or-user-error table op))
             (display-row (or (nth ridx (clutch--result-display-rows))
                              (user-error "No row at point")))
             (original (nth cidx display-row))
             (col-name (nth cidx clutch--result-columns))
             (col-def (nth cidx clutch--result-column-defs))
             (detail (clutch-result--column-detail (current-buffer) col-name))
             (original-state
              (cons (null original)
                    (if (null original)
                        ""
                      (string-trim-right
                       (clutch-result--editable-field-string
                        original col-def detail)))))
             (target-row (list
                          :identity (clutch-db-row-identity-values
                                     display-row row-identity)
                          :original original
                          :original-state original-state))
             (result-buf (current-buffer))
             (target-cell (cons ridx cidx))
             (viewport (when-let* ((win (get-buffer-window result-buf t)))
                         (list :window-start (window-start win)
                               :hscroll (window-hscroll win))))
             (edit-buf (get-buffer-create
                        (format "*clutch-edit: [%d].%s*" ridx col-name))))
        (with-current-buffer edit-buf
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name col-name
                      clutch-result-edit--column-def col-def
                      clutch-result-edit--column-detail detail
                      clutch-result-edit--row-idx ridx
                      clutch-result-edit--target-cell target-cell
                      clutch-result--edit-result-buffer result-buf
                      clutch-result-edit--return-buffer return-buffer
                      clutch-result-edit--result-viewport viewport
                      clutch-result-edit--initial-value-state nil
                      completion-at-point-functions
                      '(clutch-result-edit-completion-at-point))
          (erase-buffer)
          (if (null val)
              (clutch-result-edit--set-null-state t)
            (clutch-result-edit--set-null-state nil)
            (insert (clutch-result--editable-field-string val col-def detail)))
          (setq-local clutch-result-edit--initial-value-state
                      (cons clutch-result-edit--null-p
                            (string-trim-right
                             (buffer-substring-no-properties
                              (point-min) (point-max)))))
          (goto-char (point-min))
          (clutch-result-edit--refresh-header-line)
          (setq-local clutch-result--edit-callback
                      (lambda (new-value)
                        (unless (buffer-live-p result-buf)
                          (user-error "Result buffer no longer exists"))
                        (with-current-buffer result-buf
                          (clutch-result--apply-edit
                           ridx cidx new-value target-row))))
          (with-current-buffer result-buf
            (setq-local clutch--active-edit-cell target-cell)
            (clutch-result-edit--refresh-target-row ridx)))
        (if (with-current-buffer edit-buf
              (and (not clutch-result-edit--null-p)
                   (clutch-result-edit--json-editor-available-p)))
            (with-current-buffer edit-buf
              (clutch-result-edit-json-field t))
          (pop-to-buffer edit-buf))))))

;;;###autoload
(defun clutch-result-edit-cell ()
  "Edit or re-edit the value at point in a dedicated buffer."
  (interactive)
  (let* ((return-buffer (current-buffer))
         (result-buffer
          (if (derived-mode-p 'clutch-record-mode)
              (or (and (buffer-live-p clutch-record--result-buffer)
                       clutch-record--result-buffer)
                  (user-error "Result buffer no longer exists"))
            (current-buffer))))
    (with-current-buffer result-buffer
      (clutch-edit--require-sql-staged-mutation "Edit / re-edit"))
    (let ((cell (or (clutch--cell-at-point)
                    (user-error "No cell at point"))))
      (if (eq result-buffer return-buffer)
          (clutch-result-edit--open-cell cell return-buffer)
        (let ((opened (with-current-buffer result-buffer
                        (clutch-result-edit--open-cell cell return-buffer))))
          (when (and (buffer-live-p opened)
                     (not (eq (current-buffer) opened)))
            (pop-to-buffer opened))
          opened)))))

;;;###autoload
(defun clutch-result-edit-finish ()
  "Stage the edit and return to the result buffer.
Use \\<clutch-result-mode-map>\\[clutch-result-commit] in the result buffer to commit all staged edits."
  (interactive)
  (clutch-result-edit--finish-buffer))

(defun clutch-result-edit--finish-buffer (&optional kill-buffer-directly)
  "Stage the current edit buffer and return to the result buffer.
When KILL-BUFFER-DIRECTLY is non-nil, kill the current buffer without relying on
the selected window."
  (let* ((raw-value (string-trim-right (buffer-string)))
         (new-value (if clutch-result-edit--null-p nil raw-value))
         (new-state (cons clutch-result-edit--null-p raw-value))
         (cb clutch-result--edit-callback)
         (result-buf clutch-result--edit-result-buffer)
         (return-buf clutch-result-edit--return-buffer)
         (target-cell clutch-result-edit--target-cell)
         (viewport clutch-result-edit--result-viewport))
    (clutch-result-edit--cancel-validation-timer)
    (clutch-result--validate-field-value
     clutch-result-edit--column-name
     new-value
     clutch-result-edit--column-def
     clutch-result-edit--column-detail)
    (setq-local clutch-result-edit--error-message nil)
    (when (and cb
               (not (equal new-state
                           clutch-result-edit--initial-value-state)))
      (funcall cb new-value))
    (clutch-result-edit--refresh-record-return-buffer return-buf
                                                      (cdr target-cell))
    (clutch-result-edit--clear-active-target)
    (if kill-buffer-directly
        (kill-buffer (current-buffer))
      (quit-window 'kill))
    (clutch-result-edit--restore-result-position result-buf target-cell
                                                 return-buf
                                                 viewport)))

;;;###autoload
(defun clutch-result-edit-cancel ()
  "Cancel the edit and return to the result buffer."
  (interactive)
  (clutch-result-edit--cancel-buffer))

(defun clutch-result-edit--cancel-buffer (&optional kill-buffer-directly)
  "Cancel the current edit buffer and return to the result buffer.
When KILL-BUFFER-DIRECTLY is non-nil, kill the current buffer without relying on
the selected window."
  (let ((result-buf clutch-result--edit-result-buffer)
        (return-buf clutch-result-edit--return-buffer)
        (target-cell clutch-result-edit--target-cell)
        (viewport clutch-result-edit--result-viewport))
    (clutch-result-edit--cancel-validation-timer)
    (clutch-result-edit--clear-active-target)
    (if kill-buffer-directly
        (kill-buffer (current-buffer))
      (quit-window 'kill))
    (clutch-result-edit--restore-result-position result-buf target-cell
                                                 return-buf
                                                 viewport)))

(defun clutch-result--apply-edit (ridx cidx new-value target-row)
  "Record edit for row RIDX, column CIDX with NEW-VALUE.
TARGET-ROW is a plist captured when the edit buffer opened.
Refresh the affected row and footer in place when possible."
  (let* ((table (clutch--result-source-table-or-user-error "Stage UPDATE"))
         (row-identity (clutch-result--row-identity-or-user-error table "Stage UPDATE"))
         (display-rows (clutch--result-display-rows))
         (row (or (nth ridx display-rows)
                  (user-error "Edited row no longer exists; reopen the edit buffer")))
         (identity-vec (clutch-db-row-identity-values row row-identity))
         (expected-identity (plist-get target-row :identity))
         (key (cons identity-vec cidx))
         (original (plist-get target-row :original))
         (original-state (plist-get target-row :original-state))
         (new-state (cons (null new-value)
                          (if (null new-value) "" new-value))))
    (unless (and (plist-member target-row :identity)
                 (plist-member target-row :original)
                 (plist-member target-row :original-state))
      (user-error "Missing edit target row; reopen the edit buffer"))
    (when (not (equal identity-vec expected-identity))
      (user-error "Edited row changed; reopen the edit buffer"))
    (when (not (equal (nth cidx row) original))
      (user-error "Edited row changed; reopen the edit buffer"))
    (if (equal new-state original-state)
        (setq clutch--pending-edits
              (cl-remove key clutch--pending-edits :test #'equal :key #'car))
      (let ((existing (cl-assoc key clutch--pending-edits :test #'equal)))
        (if existing
            (setcdr existing new-value)
          (push (cons key new-value) clutch--pending-edits)))))
  (clutch--replace-row-at-index ridx)
  (clutch--refresh-footer-line)
  (force-mode-line-update)
  (if clutch--pending-edits
      (message "%s staged edit%s — C-c C-c to commit"
               (clutch--message-count (length clutch--pending-edits))
               (if (= (length clutch--pending-edits) 1) "" "s"))
    (message "Edit reverted to original")))

;;;; Commit staged changes

(defun clutch-result--row-identity-or-user-error (table op)
  "Return row identity metadata for TABLE, or signal `user-error' for OP."
  (or clutch--row-identity
      (when (eq clutch--row-identity-status 'error)
        (user-error "Cannot %s: row identity metadata failed for table %s: %s"
                    op table
                    (or clutch--row-identity-error-message
                        "unknown error")))
      (user-error "Cannot %s: no primary, unique, or row locator identity available for table %s"
                  op table)))

(defun clutch--load-fk-info ()
  "Load foreign key info for the current result's source table.
Populates `clutch--fk-info' with an alist mapping
column indices to their referenced table and column."
  (setq clutch--fk-info nil)
  (when-let* ((conn clutch-connection)
              (table clutch--result-source-table)
              (col-names clutch--result-columns))
    (condition-case err
        (let ((fks (clutch-db-foreign-keys conn table)))
          (pcase-dolist (`(,col-name . ,ref-info) fks)
            (let ((idx (cl-position col-name col-names :test #'string=)))
              (when idx
                (push (cons idx ref-info) clutch--fk-info)))))
      (clutch-db-error
       (clutch--remember-recoverable-metadata-warning
        conn "foreign-key metadata" err `(:table ,table))
       (message "Foreign-key metadata unavailable: %s"
                (error-message-string err))))))

(defun clutch-result--ensure-where-guard (statements op-name)
  "Ensure every statement in STATEMENTS has a top-level WHERE for OP-NAME."
  (dolist (stmt statements)
    (unless (clutch-db-sql-find-top-level-clause
             (clutch-db-sql-normalize stmt) "WHERE")
      (user-error "%s blocked: statement without WHERE: %s"
                  op-name
                  (truncate-string-to-width (string-trim stmt) 120 nil nil "…")))))

(defun clutch--pk-where-parts (conn pk-names pk-values)
  "Return `(PARTS . PARAMS)' for PK-NAMES with PK-VALUES using CONN.
NULL primary-key values stay literal as `IS NULL' and are not added to
the parameter list."
  (let (parts params)
    (cl-mapc
     (lambda (col val)
       (let ((column-sql (clutch-db-escape-identifier conn col)))
         (if (null val)
             (push (format "%s IS NULL" column-sql) parts)
           (push (format "%s = ?" column-sql) parts)
           (push val params))))
     pk-names pk-values)
    (cons (nreverse parts) (nreverse params))))

(defun clutch-result--render-statements (statements)
  "Return preview SQL strings for mutation STATEMENTS."
  (mapcar (lambda (statement)
            (pcase-let ((`(,sql . ,params) statement))
              (clutch-db-substitute-params
               sql params
               (lambda (value)
                 (clutch-db-value-to-literal
                  clutch-connection value #'clutch--format-value)))))
          statements))

(defun clutch--row-identity-where-parts (conn row-identity values)
  "Return `(PARTS . PARAMS)' for ROW-IDENTITY using VALUES on CONN."
  (if-let* ((where-sql (plist-get row-identity :where-sql)))
      (cons (list where-sql) (append values nil))
    (clutch--pk-where-parts
     conn
     (plist-get row-identity :columns)
     (append values nil))))

(defun clutch-result--build-update-stmt (table identity-vec edits col-names row-identity)
  "Build an UPDATE statement spec for TABLE.
IDENTITY-VEC is the row identity vector, EDITS is a list of
\(cidx . value), COL-NAMES are column names, and ROW-IDENTITY describes the
WHERE predicate."
  (let ((conn clutch-connection))
    (let ((set-parts nil)
          (set-params nil)
          (where-spec (clutch--row-identity-where-parts
                       conn row-identity identity-vec)))
      (dolist (edit edits)
        (push (format "%s = ?"
                      (clutch-db-escape-identifier
                       conn (nth (car edit) col-names)))
              set-parts)
        (push (cdr edit) set-params))
      (cons (format "UPDATE %s SET %s WHERE %s"
                    (clutch-db-escape-identifier conn table)
                    (mapconcat #'identity (nreverse set-parts) ", ")
                    (mapconcat #'identity (car where-spec) " AND "))
            (append (nreverse set-params) (cdr where-spec))))))

(defun clutch-result--build-update-statements ()
  "Build UPDATE statement specs from staged edits."
  (unless clutch--pending-edits
    (user-error "No staged edits"))
  (let* ((table (clutch--result-source-table-or-user-error "Build UPDATE"))
         (row-identity (clutch-result--row-identity-or-user-error table "Build UPDATE"))
         (col-names clutch--result-columns)
         (by-identity (make-hash-table :test 'equal))
         statements)
    (pcase-dolist (`((,identity-vec . ,cidx) . ,val) clutch--pending-edits)
      (push (cons cidx val) (gethash identity-vec by-identity)))
    (maphash
     (lambda (identity-vec edits)
       (push (clutch-result--build-update-stmt
              table identity-vec edits col-names row-identity)
             statements))
     by-identity)
    statements))

(defun clutch-result--build-pending-insert-statements ()
  "Build INSERT statement specs for staged new rows."
  (let ((table (or clutch--result-source-table
                   (user-error "Cannot detect source table"))))
    (mapcar (lambda (fields)
              (clutch-result-insert--build-sql clutch-connection table fields))
            clutch--pending-inserts)))

(defun clutch-result--build-pending-delete-statements ()
  "Build DELETE statement specs from staged deletes."
  (let* ((table (clutch--result-source-table-or-user-error "Build DELETE"))
         (row-identity (clutch-result--row-identity-or-user-error
                        table "Build DELETE")))
    (mapcar (lambda (identity-vec)
              (clutch-result--build-delete-stmt-for-identity
               table identity-vec row-identity))
            clutch--pending-deletes)))

(defun clutch-result--pending-sql-statements ()
  "Return the staged SQL statements that would run on commit."
  (unless (or clutch--pending-inserts clutch--pending-edits clutch--pending-deletes)
    (user-error "No staged SQL"))
  (clutch-result--render-statements
   (append
    (when clutch--pending-inserts
      (clutch-result--build-pending-insert-statements))
    (when clutch--pending-edits
      (clutch-result--build-update-statements))
    (when clutch--pending-deletes
      (clutch-result--build-pending-delete-statements)))))

(defun clutch-result--pending-sql-content (&optional stmts)
  "Return STMTS as a trailing-newline-terminated staged SQL batch string.
When STMTS is nil, build statements from the current staged state."
  (let ((stmts (or stmts (clutch-result--pending-sql-statements))))
    (if stmts
        (concat (mapconcat (lambda (s) (concat s ";")) stmts "\n") "\n")
      "")))

;;;###autoload
(defun clutch-result-copy-pending-sql ()
  "Copy the staged SQL batch to the kill ring."
  (interactive)
  (let ((stmts (clutch-result--pending-sql-statements)))
    (kill-new (clutch-result--pending-sql-content stmts))
    (message "Copied %s staged %s statement%s"
             (clutch--message-count (length stmts))
             (clutch--message-keyword "SQL")
             (if (= (length stmts) 1) "" "s"))))

;;;###autoload
(defun clutch-result-save-pending-sql ()
  "Save the staged SQL batch to a file."
  (interactive)
  (let* ((stmts (clutch-result--pending-sql-statements))
         (sql (clutch-result--pending-sql-content stmts))
         (path (read-file-name "Save staged SQL to file: " nil nil nil "staged.sql")))
    (with-temp-buffer
      (insert sql)
      (write-region (point-min) (point-max) path nil 'silent))
    (message "Saved %s staged %s statement%s to %s"
             (clutch--message-count (length stmts))
             (clutch--message-keyword "SQL")
             (if (= (length stmts) 1) "" "s")
             (clutch--message-path path))))

(defun clutch-result--execute-mutation-stmt (stmt &optional require-single-row)
  "Execute mutation STMT.
When REQUIRE-SINGLE-ROW is non-nil, signal `user-error' if the backend reports
an affected row count other than one."
  (pcase-let ((`(,sql . ,params) stmt))
    (condition-case err
        (let ((result (clutch--run-db-query clutch-connection sql params)))
          (when-let* ((affected (and require-single-row
                                     (clutch-db-result-p result)
                                     (clutch-db-result-affected-rows result))))
            (unless (= affected 1)
              (user-error "Mutation matched %d rows; expected exactly 1"
                          affected)))
          result)
      (clutch-db-error
       (user-error "%s" (clutch--humanize-db-error
                         (error-message-string err)))))))

;;;###autoload
(defun clutch-result-commit ()
  "Commit all staged row mutations: INSERT, UPDATE, DELETE.
Execute INSERTs first, then UPDATEs, then DELETEs."
  (interactive)
  (unless (or clutch--pending-edits clutch--pending-deletes clutch--pending-inserts)
    (user-error "No staged changes"))
  (let* ((insert-stmts (when clutch--pending-inserts
                         (clutch-result--build-pending-insert-statements)))
         (update-stmts (when clutch--pending-edits
                         (clutch-result--build-update-statements)))
         (delete-stmts (when clutch--pending-deletes
                         (clutch-result--build-pending-delete-statements)))
         (insert-preview (when insert-stmts
                           (clutch-result--render-statements insert-stmts)))
         (update-preview (when update-stmts
                           (clutch-result--render-statements update-stmts)))
         (delete-preview (when delete-stmts
                           (clutch-result--render-statements delete-stmts)))
         (all-stmts (append insert-stmts update-stmts delete-stmts))
         (preview-stmts (append insert-preview update-preview delete-preview))
         (sql-text (mapconcat (lambda (s) (concat s ";")) preview-stmts "\n")))
    (clutch-result--ensure-where-guard delete-preview "DELETE")
    (clutch-result--ensure-where-guard update-preview "UPDATE")
    (when (yes-or-no-p (format "Execute %d statement%s?\n\n%s\n\nProceed? "
                               (length all-stmts)
                               (if (= (length all-stmts) 1) "" "s")
                               sql-text))
      (dolist (stmt insert-stmts)
        (clutch-result--execute-mutation-stmt stmt))
      (dolist (stmt update-stmts)
        (clutch-result--execute-mutation-stmt stmt t))
      (dolist (stmt delete-stmts)
        (clutch-result--execute-mutation-stmt stmt t))
      (setq clutch--pending-edits nil
            clutch--pending-deletes nil
            clutch--pending-inserts nil
            clutch--marked-rows nil)
      (message "%s change%s committed"
               (clutch--message-count (length all-stmts))
               (if (= (length all-stmts) 1) "" "s"))
      (clutch--execute clutch--last-query clutch-connection))))

;;;; Delete rows

(defun clutch-result--build-delete-stmt-for-identity
    (table identity-vec row-identity)
  "Build a DELETE statement spec for TABLE using IDENTITY-VEC and ROW-IDENTITY."
  (let* ((conn clutch-connection)
         (where-spec (clutch--row-identity-where-parts
                      conn row-identity identity-vec)))
    (cons (format "DELETE FROM %s WHERE %s"
                  (clutch-db-escape-identifier conn table)
                  (mapconcat #'identity (car where-spec) " AND "))
          (cdr where-spec))))

;;;###autoload
(defun clutch-result-delete-rows ()
  "Stage selected rows for deletion.
Use \\[clutch-result-commit] in the result buffer to commit."
  (interactive)
  (clutch-edit--require-sql-staged-mutation "Stage delete")
  (let* ((indices (or (clutch--selected-row-indices)
                      (user-error "No row at point")))
         (table (clutch--result-source-table-or-user-error "Stage DELETE"))
         (row-identity (clutch-result--row-identity-or-user-error table "Stage DELETE"))
         (display-rows (clutch--result-display-rows)))
    (dolist (ridx indices)
      (let ((identity-vec (clutch-db-row-identity-values
                           (nth ridx display-rows)
                           row-identity)))
        (cl-pushnew identity-vec clutch--pending-deletes :test #'equal)))
    (dolist (ridx indices)
      (clutch--replace-row-at-index ridx))
    (clutch--refresh-footer-line)
    (force-mode-line-update)
    (message "%s row%s staged for deletion — C-c C-c to commit"
             (clutch--message-count (length indices))
             (if (= (length indices) 1) "" "s"))))

;;;; Insert row

(defconst clutch-result-insert--field-line-re
  "^\\([^:\n[]+\\)\\(?: \\[[^]]+\\]\\)?: "
  "Regexp matching an insert buffer field line prefix.")

(defvar clutch-result-insert-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c '") #'clutch-result-insert-edit-json-field)
    (define-key map (kbd "RET") #'clutch-result-insert-submit-field)
    (define-key map (kbd "<return>") #'clutch-result-insert-submit-field)
    (define-key map (kbd "TAB") #'clutch-result-insert-next-field)
    (define-key map (kbd "<backtab>") #'clutch-result-insert-prev-field)
    (define-key map (kbd "M-TAB") #'clutch-result-insert-complete-field)
    (define-key map (kbd "C-M-i") #'clutch-result-insert-complete-field)
    (define-key map (kbd "C-c .") #'clutch-result-insert-fill-current-time)
    (define-key map (kbd "C-c C-a") #'clutch-result-insert-toggle-field-layout)
    (define-key map (kbd "C-c C-y") #'clutch-result-insert-import-delimited)
    (define-key map (kbd "C-c C-c") #'clutch-result-insert-commit)
    (define-key map (kbd "C-c C-k") #'clutch-result-insert-cancel)
    map)
  "Keymap for the INSERT form buffer.")

(defvar clutch-result-insert-mode-hook nil
  "Hook run after `clutch-result-insert-mode' is enabled.")

(defvar clutch--result-insert-major-mode-map clutch-result-insert-mode-map
  "Keymap used internally by `clutch--result-insert-major-mode'.")

(defvar clutch--result-insert-major-mode-hook nil
  "Internal hook run after `clutch--result-insert-major-mode' is enabled.")

(defvar clutch--result-insert-json-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") #'clutch-result-insert-json-finish)
    (define-key map (kbd "C-c C-k") #'clutch-result-insert-json-cancel)
    map)
  "Keymap for the `insert-buffer' JSON editor.")

(define-minor-mode clutch--result-insert-json-mode
  "Minor mode for editing JSON values from the insert buffer."
  :lighter " DB-Insert-JSON"
  :keymap clutch--result-insert-json-mode-map)

(define-derived-mode clutch--result-insert-major-mode text-mode "clutch-insert"
  "Major mode for editing a new row to INSERT.
\\<clutch-result-insert-mode-map>
\\[clutch-result-insert-commit]	stage row insertion
\\[clutch-result-insert-cancel]	cancel editing"
  (setq-local truncate-lines t)
  (add-hook 'post-command-hook #'clutch-result-insert--normalize-point nil t)
  (add-hook 'after-change-functions #'clutch-result-insert--after-change nil t)
  (add-hook 'kill-buffer-hook #'clutch-result-insert--cleanup nil t)
  (clutch-result-insert--ensure-field-state))

;;;###autoload
(defun clutch-result-insert-mode (&optional _arg)
  "Activate `clutch--result-insert-major-mode', ignoring optional ARG."
  (interactive)
  (clutch--result-insert-major-mode)
  (run-hooks 'clutch-result-insert-mode-hook))

(defvar-local clutch-result-insert--result-buffer nil
  "Reference to the parent result buffer (Insert buffer local).")

(defvar-local clutch-result-insert--table nil
  "Table name for the INSERT (Insert buffer local).")

(defvar-local clutch-result-insert--all-columns nil
  "All insertable columns in canonical render order.")

(defvar-local clutch-result-insert--seed-fields nil
  "Canonical insert field values for all columns, visible or hidden.")

(defvar-local clutch-result-insert--show-all-fields nil
  "Non-nil when the insert buffer shows every column.")

(defvar-local clutch-result-insert--omit-primary-key-fields nil
  "Non-nil when sparse insert layout hides primary-key fields.")

(defvar-local clutch-result-insert--column-details-cache nil
  "Cached schema detail plists keyed by column name for the insert buffer.")

(defvar-local clutch-result-insert--pending-index nil
  "Staged insert index being edited, or nil for a new insert.")

(defvar-local clutch-result-insert--source-table nil
  "Source result table recorded when this insert buffer was opened.")

(defvar-local clutch-result-insert--fields nil
  "Structured field state for the current insert buffer.")

(defvar-local clutch-result-insert--validation-timer nil
  "Idle validation timer for the current insert buffer.")

(defvar-local clutch-result-insert--active-field-overlay nil
  "Overlay highlighting the active insert field line.")

(defvar-local clutch-result-insert--active-prefix-overlay nil
  "Overlay highlighting the active insert field prefix.")

(defvar-local clutch-result-insert--label-width 0
  "Display width reserved for insert field labels.")

(defvar-local clutch-result-insert-json--parent-buffer nil
  "Insert buffer that opened the current JSON editor.")

(defvar-local clutch-result-insert-json--field-name nil
  "Insert-field name edited by the current JSON editor.")

(defun clutch-result-insert--cancel-validation-timer ()
  "Cancel any pending insert-field validation timer."
  (when (timerp clutch-result-insert--validation-timer)
    (cancel-timer clutch-result-insert--validation-timer))
  (setq-local clutch-result-insert--validation-timer nil))

(defun clutch-result-insert--cleanup ()
  "Tear down timers and overlays owned by the current insert buffer."
  (clutch-result-insert--cancel-validation-timer)
  (when (overlayp clutch-result-insert--active-field-overlay)
    (delete-overlay clutch-result-insert--active-field-overlay))
  (when (overlayp clutch-result-insert--active-prefix-overlay)
    (delete-overlay clutch-result-insert--active-prefix-overlay))
  (setq-local clutch-result-insert--active-field-overlay nil
              clutch-result-insert--active-prefix-overlay nil)
  (dolist (field clutch-result-insert--fields)
    (when-let* ((ov (plist-get field :error-overlay)))
      (delete-overlay ov)
      (setf (plist-get field :error-overlay) nil
            (plist-get field :error-message) nil))))

(defun clutch-result-insert--annotate-field-line (field-name line-start line-end)
  "Attach FIELD-NAME metadata to the buffer text from LINE-START to LINE-END."
  (add-text-properties line-start line-end
                       `(clutch-insert-field-name ,field-name)))

(defun clutch-result-insert--field-name-at-position (&optional pos)
  "Return insert field name at POS, or at point when POS is nil."
  (let ((pos (or pos (point))))
    (or (get-text-property pos 'clutch-insert-field-name)
        (and (> pos (point-min))
             (get-text-property (1- pos) 'clutch-insert-field-name)))))

(defun clutch-result-insert--all-column-names ()
  "Return canonical column order for the current insert buffer."
  (or clutch-result-insert--all-columns
      (progn
        (clutch-result-insert--ensure-field-state)
        (setq-local clutch-result-insert--all-columns
                    (mapcar (lambda (field)
                              (plist-get field :name))
                            clutch-result-insert--fields)))))

(defun clutch-result-insert--canonicalize-fields (col-names fields)
  "Return COL-NAMES paired with string values from FIELDS.
FIELDS is an alist keyed by column name.  Missing columns become empty strings."
  (cl-loop for col in col-names
           collect (cons col (or (cdr (assoc col fields)) ""))))

(defun clutch-result-insert--field-empty-p (value)
  "Return non-nil when VALUE should be treated as empty."
  (or (null value) (string-empty-p value)))

(defun clutch-result-insert--ensure-field-state ()
  "Populate structured field state from the current buffer when missing."
  (unless clutch-result-insert--fields
    (let (field-states)
      (save-excursion
        (goto-char (point-min))
        (while (not (eobp))
          (beginning-of-line)
          (when (looking-at clutch-result-insert--field-line-re)
            (let* ((name (string-trim-right (match-string-no-properties 1)))
                   (value-start (match-end 0))
                   (line-start (line-beginning-position))
                   (line-end (min (point-max) (1+ (line-end-position)))))
              (clutch-result-insert--annotate-field-line name line-start line-end)
              (push (list :name name
                          :value-marker (copy-marker value-start))
                    field-states)))
          (forward-line 1)))
      (setq-local clutch-result-insert--fields (nreverse field-states)))))

(defun clutch-result-insert--field-state (field-name)
  "Return structured insert field state for FIELD-NAME, or nil."
  (clutch-result-insert--ensure-field-state)
  (cl-find field-name clutch-result-insert--fields
           :key (lambda (field) (plist-get field :name))
           :test #'string=))

(defun clutch-result-insert--field-cell (field-name)
  "Return the list cell holding FIELD-NAME in `clutch-result-insert--fields'."
  (clutch-result-insert--ensure-field-state)
  (cl-loop for cell on clutch-result-insert--fields
           when (string= (plist-get (car cell) :name) field-name)
           return cell))

(defun clutch-result-insert--set-field-prop (field prop value)
  "Store VALUE for PROP on structured FIELD in canonical field state."
  (when-let* ((name (plist-get field :name))
              (cell (clutch-result-insert--field-cell name)))
    (setcar cell (plist-put (car cell) prop value))
    value))

(defun clutch-result-insert--field-state-at-position (&optional pos)
  "Return structured insert field state at POS, or at point when POS is nil."
  (clutch-result-insert--ensure-field-state)
  (or (when-let* ((field-name (clutch-result-insert--field-name-at-position pos)))
        (clutch-result-insert--field-state field-name))
      (let ((pos (or pos (point))))
        (cl-find-if
         (lambda (field)
           (when-let* ((marker (plist-get field :value-marker))
                       (start (marker-position marker)))
             (save-excursion
               (goto-char start)
               (and (<= (line-beginning-position) pos)
                    (<= pos (line-end-position))))))
         clutch-result-insert--fields))))

(defun clutch-result-insert--field-value-bounds (field)
  "Return editable value bounds for structured FIELD, or nil."
  (when-let* ((marker (plist-get field :value-marker))
              (beg (marker-position marker)))
    (save-excursion
      (goto-char beg)
      (cons beg (line-end-position)))))

(defun clutch-result-insert--field-value (field)
  "Return the current buffer value for structured FIELD."
  (when-let* ((bounds (clutch-result-insert--field-value-bounds field)))
    (buffer-substring-no-properties (car bounds) (cdr bounds))))

(defun clutch-result-insert--ensure-seed-fields ()
  "Populate canonical field values from the current visible buffer when missing."
  (unless clutch-result-insert--seed-fields
    (clutch-result-insert--ensure-field-state)
    (setq-local clutch-result-insert--seed-fields
                (clutch-result-insert--canonicalize-fields
                 (clutch-result-insert--all-column-names)
                 (cl-loop for field in clutch-result-insert--fields
                          collect (cons (plist-get field :name)
                                        (or (clutch-result-insert--field-value field)
                                            "")))))))

(defun clutch-result-insert--seed-field-value (field-name)
  "Return canonical insert value for FIELD-NAME."
  (clutch-result-insert--ensure-seed-fields)
  (cdr (assoc field-name clutch-result-insert--seed-fields)))

(defun clutch-result-insert--set-seed-field-value (field-name value)
  "Store VALUE as the canonical insert value for FIELD-NAME."
  (clutch-result-insert--ensure-seed-fields)
  (when-let* ((cell (assoc field-name clutch-result-insert--seed-fields)))
    (setcdr cell (or value "")))
  value)

(defun clutch-result-insert--record-visible-values ()
  "Copy visible insert buffer values into canonical insert state."
  (clutch-result-insert--ensure-seed-fields)
  (clutch-result-insert--ensure-field-state)
  (dolist (field clutch-result-insert--fields)
    (clutch-result-insert--set-seed-field-value
     (plist-get field :name)
     (clutch-result-insert--field-value field))))

(defun clutch-result-insert--json-field-p (field)
  "Return non-nil for structured JSON FIELD values."
  (let* ((name (plist-get field :name))
         (detail (or (plist-get field :detail)
                     (clutch-result-insert--column-detail name)))
         (col-def (or (plist-get field :column-def)
                      (clutch-result-insert--column-def name))))
    (clutch-result--field-json-p col-def detail)))

(defun clutch-result-insert--clear-field-error (field)
  "Clear inline validation state for structured FIELD."
  (when-let* ((ov (or (plist-get field :error-overlay)
                      (when-let* ((stored (clutch-result-insert--field-state
                                           (plist-get field :name))))
                        (plist-get stored :error-overlay)))))
    (delete-overlay ov))
  (clutch-result-insert--set-field-prop field :error-overlay nil)
  (clutch-result-insert--set-field-prop field :error-message nil))

(defun clutch-result-insert--short-error-message (message)
  "Return a compact inline summary for validation MESSAGE."
  (cond
   ((string-match-p " must be one of: " message) "invalid enum")
   ((string-match-p " expects true/false\\'" message) "invalid bool")
   ((string-match-p " expects 0/1\\'" message) "invalid bool")
   ((string-match-p " expects a numeric value\\'" message) "invalid numeric")
   ((string-match-p " expects YYYY-MM-DD\\'" message) "invalid date")
   ((string-match-p " expects HH:MM\\[:SS\\]\\'" message) "invalid time")
   ((string-match-p " expects YYYY-MM-DD HH:MM\\[:SS\\]\\'" message) "invalid datetime")
   ((string-match-p " expects valid JSON\\'" message) "invalid JSON")
   (t "invalid value")))

(defun clutch-result-insert--show-field-error (field message)
  "Show inline validation MESSAGE for structured FIELD."
  (clutch-result-insert--clear-field-error field)
  (when-let* ((bounds (clutch-result-insert--field-value-bounds field)))
    (let* ((beg (car bounds))
           (end (cdr bounds))
           (ov (make-overlay beg end nil t t))
           (hint (propertize
                  (format "  [%s]"
                          (clutch-result-insert--short-error-message message))
                  'face 'clutch-insert-inline-error-face)))
      (overlay-put ov 'face 'clutch-insert-field-error-face)
      (overlay-put ov 'after-string hint)
      (overlay-put ov 'help-echo message)
      (overlay-put ov 'priority 100)
      (overlay-put ov 'evaporate t)
      (clutch-result-insert--set-field-prop field :error-overlay ov)
      (clutch-result-insert--set-field-prop field :error-message message))))

(defun clutch-result-insert--field-validation-message (field)
  "Return a validation message for structured FIELD, or nil."
  (let* ((name (plist-get field :name))
         (value (or (clutch-result-insert--field-value field)
                    (clutch-result-insert--seed-field-value name)))
         (detail (or (plist-get field :detail)
                     (clutch-result-insert--column-detail name)))
         (col-def (or (plist-get field :column-def)
                      (clutch-result-insert--column-def name))))
    (unless (string-empty-p value)
      (condition-case err
          (clutch-result--field-validation-message name value col-def detail)
        (user-error (error-message-string err))))))

(defun clutch-result-insert--validate-field-live (field)
  "Run local validation for structured FIELD and update inline UI."
  (when-let* ((name (plist-get field :name)))
    (clutch-result-insert--set-seed-field-value
     name (clutch-result-insert--field-value field)))
  (if-let* ((message (clutch-result-insert--field-validation-message field)))
      (clutch-result-insert--show-field-error field message)
    (clutch-result-insert--clear-field-error field)))

(defun clutch-result-insert--run-idle-validation (buffer field-name)
  "Validate FIELD-NAME in BUFFER after an idle delay."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (setq-local clutch-result-insert--validation-timer nil)
      (when-let* ((field (clutch-result-insert--field-state field-name)))
        (clutch-result-insert--validate-field-live field)))))

(defun clutch-result-insert--schedule-field-validation (field)
  "Validate structured FIELD after a short idle delay.
All field types use the same delay so feedback timing is consistent."
  (clutch-result-insert--cancel-validation-timer)
  (setq-local clutch-result-insert--validation-timer
              (run-with-idle-timer
               clutch-insert-validation-idle-delay nil
               #'clutch-result-insert--run-idle-validation
               (current-buffer)
               (plist-get field :name))))

(defun clutch-result-insert--after-change (beg end _len)
  "Locally validate the changed insert field spanning BEG..END."
  (when-let* ((field (or (clutch-result-insert--field-state-at-position beg)
                         (and (> beg (point-min))
                              (clutch-result-insert--field-state-at-position (1- beg)))
                         (clutch-result-insert--field-state-at-position end))))
    (clutch-result-insert--validate-field-live field)
    (clutch-result-insert--schedule-field-validation field)))

(defun clutch-result-insert--field-name-at-line ()
  "Return the insert field name for the current line, or nil."
  (save-excursion
    (beginning-of-line)
    (when-let* ((field (clutch-result-insert--field-state-at-position)))
      (plist-get field :name))))

(defun clutch-result-insert--field-label-width (col-names)
  "Return the display width needed for labeled insert fields in COL-NAMES."
  (cl-loop for col in col-names
           maximize (let* ((tags (clutch-result-insert--field-tag-list col))
                           (tag-text (if tags
                                         (format " [%s]" (string-join tags " "))
                                       "")))
                      (string-width (concat col tag-text)))))

(defun clutch-result-insert--field-label-padded (col-name)
  "Return padded insert label text for COL-NAME."
  (let* ((tags (clutch-result-insert--field-tag-list col-name))
         (tag-text (if tags
                       (format " [%s]" (string-join tags " "))
                     ""))
         (plain-width (string-width (concat col-name tag-text)))
         (gap-width (max 0 (- clutch-result-insert--label-width plain-width)))
         (name-part (clutch--string-pad
                     (propertize col-name
                                 'face 'clutch-field-name-face)
                     (+ (string-width col-name) gap-width)))
         (tag-part (when tags
                     (propertize tag-text
                                 'face 'clutch-insert-field-tag-face))))
    (concat name-part tag-part)))

(defun clutch-result--json-normalize-string (value field-name)
  "Return VALUE normalized as compact JSON for FIELD-NAME."
  (if (string-empty-p value)
      ""
    (condition-case nil
        (if (fboundp 'json-serialize)
            (clutch--json-serialize-text (json-parse-string value))
          value)
      (error (user-error "Field %s expects valid JSON" field-name)))))

(defun clutch-result-insert--json-editor-mode ()
  "Select the best available major mode for JSON field editing."
  (cond
   ((clutch--json-ts-mode-available-p)
    (json-ts-mode))
   ((fboundp 'js-mode)
    (js-mode))
   (t
    (text-mode))))

(defun clutch--open-json-sub-editor (buffer-name initial-text field-name finish-fn cancel-fn)
  "Open a shared JSON sub-editor buffer and return it.
BUFFER-NAME is the buffer to reuse or create.
INITIAL-TEXT seeds the editor contents.
FIELD-NAME is shown in the header line.
FINISH-FN and CANCEL-FN become the local save and cancel bindings."
  (let ((buf (get-buffer-create buffer-name)))
    (with-current-buffer buf
      (erase-buffer)
      (insert initial-text)
      (goto-char (point-min))
      (condition-case nil
          (unless (string-empty-p initial-text)
            (json-pretty-print-buffer))
        (error nil))
      (clutch-result-insert--json-editor-mode)
      (when (fboundp 'font-lock-ensure)
        (font-lock-ensure (point-min) (point-max)))
      (let ((map (copy-keymap (or (current-local-map) (make-sparse-keymap)))))
        (define-key map (kbd "C-c C-c") finish-fn)
        (define-key map (kbd "C-c C-k") cancel-fn)
        (use-local-map map))
      (setq-local header-line-format
                  (concat
                   (format " JSON field %s" field-name)
                   (clutch--status-separator)
                   (clutch--key-hints '(("C-c C-c" "Save JSON")
                                        ("C-c C-k" "Cancel"))))))
    (pop-to-buffer buf)))

(defun clutch-result-insert--current-field-or-error ()
  "Return the structured field at point or signal a user error."
  (or (clutch-result-insert--field-state-at-position)
      (user-error "Point is not on an insert field")))

(defun clutch-result-insert--current-field-value-start ()
  "Return the start position of the current insert field value, or nil."
  (when-let* ((field (clutch-result-insert--field-state-at-position))
              (bounds (clutch-result-insert--field-value-bounds field)))
    (car bounds)))

(defun clutch-result-insert--current-field-value-position ()
  "Return point for the current insert field value, or nil."
  (when-let* ((field (clutch-result-insert--field-state-at-position))
              (bounds (clutch-result-insert--field-value-bounds field)))
    (cdr bounds)))

(defun clutch-result-insert--adjacent-field-value-position (&optional backward)
  "Return point for the adjacent insert field value.
When BACKWARD is non-nil, move to the previous field; otherwise move to the
next field.  Returns nil when no matching field exists."
  (when-let* ((current (clutch-result-insert--field-state-at-position))
              (fields clutch-result-insert--fields)
              (idx (cl-position current fields :test #'eq))
              (next-idx (+ idx (if backward -1 1)))
              ((<= 0 next-idx))
              ((< next-idx (length fields)))
              (field (nth next-idx fields))
              (bounds (clutch-result-insert--field-value-bounds field)))
    (cdr bounds)))

(defun clutch-result-insert--current-field-name ()
  "Return the current insert field name, or nil."
  (save-excursion
    (beginning-of-line)
    (clutch-result-insert--field-name-at-line)))

(defun clutch-result-insert--current-field-value-bounds ()
  "Return value bounds of the current insert field, or nil."
  (when-let* ((field (clutch-result-insert--field-state-at-position)))
    (clutch-result-insert--field-value-bounds field)))

(defun clutch-result-insert--column-def (field-name)
  "Return result column plist for FIELD-NAME from the parent result buffer."
  (or (plist-get (clutch-result-insert--field-state field-name) :column-def)
      (when-let* ((result-buf clutch-result-insert--result-buffer)
                  ((buffer-live-p result-buf)))
        (with-current-buffer result-buf
          (when-let* ((idx (cl-position field-name clutch--result-columns
                                        :test #'string=)))
            (nth idx clutch--result-column-defs))))))

(defun clutch-result-insert--column-detail (field-name)
  "Return detailed column plist for FIELD-NAME from the parent result buffer."
  (or (plist-get (clutch-result-insert--field-state field-name) :detail)
      (cl-find field-name clutch-result-insert--column-details-cache
               :key (lambda (d) (plist-get d :name))
               :test #'string=)
      (let ((table clutch-result-insert--table))
        (when-let* ((result-buf clutch-result-insert--result-buffer)
                    ((buffer-live-p result-buf)))
          (with-current-buffer result-buf
            (when-let* ((conn clutch-connection)
                        (details (clutch--ensure-column-details conn table)))
              (setq-local clutch-result-insert--column-details-cache details)
              (cl-find field-name details
                       :key (lambda (d) (plist-get d :name))
                       :test #'string=)))))))

(defun clutch-result-insert--enum-candidates (type)
  "Return enum candidates parsed from SQL TYPE, or nil."
  (when (and (stringp type)
             (string-match-p "\\`enum(" (downcase type)))
    (let ((start 0)
          vals)
      (while (string-match "'\\([^']*\\(?:''[^']*\\)*\\)'" type start)
        (push (replace-regexp-in-string "''" "'" (match-string 1 type)) vals)
        (setq start (match-end 0)))
      (nreverse vals))))

(defun clutch-result-insert--field-candidates (field-name)
  "Return completion candidates for FIELD-NAME, or nil."
  (clutch-result--field-candidates-from-detail
   (clutch-result-insert--column-detail field-name)))

(defun clutch-result-insert-completion-at-point ()
  "Return CAPF data for the current insert field, or nil."
  (when-let* ((field-name (clutch-result-insert--current-field-name))
              (candidates (clutch-result-insert--field-candidates field-name))
              (bounds (clutch-result-insert--current-field-value-bounds)))
    (let ((beg (car bounds))
          (end (cdr bounds)))
      (list beg end candidates :exclusive 'no))))

(defun clutch-result-insert--current-time-value (type-category &optional time)
  "Return a formatted current time string for TYPE-CATEGORY.
TIME defaults to `current-time'."
  (let ((ts (or time (current-time))))
    (pcase type-category
      ('date (format-time-string "%F" ts))
      ('time (format-time-string "%T" ts))
      ('datetime (format-time-string "%F %T" ts))
      (_ nil))))

(defun clutch-result-insert--field-tag-list (col-name)
  "Return display tags for insert field COL-NAME."
  (let* ((detail (clutch-result-insert--column-detail col-name))
         (col-def (clutch-result-insert--column-def col-name))
         tags)
    (when (plist-get detail :generated)
      (push "generated" tags))
    (when-let* ((default (plist-get detail :default)))
      (push (format "default=%s" default) tags))
    (setq tags (append (nreverse (clutch-result--field-metadata-tags col-def detail))
                       tags))
    (when (and detail
               (not (plist-get detail :nullable))
               (not (plist-get detail :generated))
               (not (plist-get detail :default)))
      (push "required" tags))
    (nreverse tags)))

(defun clutch-result-insert--column-generated-p (col-name)
  "Return non-nil when COL-NAME is generated by the database."
  (plist-get (clutch-result-insert--column-detail col-name) :generated))

(defun clutch-result-insert--column-primary-key-p (col-name)
  "Return non-nil when COL-NAME is a primary-key column."
  (or (plist-get (clutch-result-insert--column-detail col-name) :primary-key)
      (when-let* ((result-buf clutch-result-insert--result-buffer)
                  ((buffer-live-p result-buf)))
        (with-current-buffer result-buf
          (when-let* ((idx (cl-position col-name clutch--result-columns
                                        :test #'string=)))
            (memq idx clutch--cached-pk-indices))))))

(defun clutch-result-insert--column-sparse-visible-p (col-name)
  "Return non-nil when sparse insert layout should show COL-NAME."
  (let ((detail (clutch-result-insert--column-detail col-name))
        (value (clutch-result-insert--seed-field-value col-name)))
    (and (not (plist-get detail :generated))
         (not (and clutch-result-insert--omit-primary-key-fields
                   (clutch-result-insert--column-primary-key-p col-name)))
         (or (not detail)
             (not (clutch-result-insert--field-empty-p value))
             (not (plist-member detail :default))
             (not (plist-get detail :nullable))))))

(defun clutch-result-insert--visible-columns ()
  "Return the currently rendered insert columns."
  (let* ((all-cols (clutch-result-insert--all-column-names))
         (fallback (or (cl-remove-if #'clutch-result-insert--column-generated-p all-cols)
                       all-cols)))
    (if clutch-result-insert--show-all-fields
        all-cols
      (or (cl-remove-if-not #'clutch-result-insert--column-sparse-visible-p all-cols)
          fallback))))

(defun clutch-result-insert--header-line ()
  "Return the insert form header line."
  (concat
   (format " INSERT into %s [%s]"
           clutch-result-insert--table
           (if clutch-result-insert--show-all-fields "all columns" "sparse"))
   (clutch--status-separator)
   (clutch--key-hints
    `(("C-c C-c" "Stage insert")
      ("C-c C-k" "Cancel")
      ("C-c C-a" ,(if clutch-result-insert--show-all-fields
                      "Sparse fields"
                    "All fields"))))))

(defun clutch-result-insert--refresh-header-line ()
  "Refresh the insert form header line."
  (setq-local header-line-format (clutch-result-insert--header-line)))

(defun clutch-result-insert--line-prefix-end ()
  "Return the end position of the current insert field prefix."
  (clutch-result-insert--current-field-value-start))

(defun clutch-result-insert--field-prefix-read-only-p ()
  "Return non-nil when point is inside the read-only insert field prefix."
  (when-let* ((prefix-end (clutch-result-insert--line-prefix-end)))
    (< (point) prefix-end)))

(defun clutch-result-insert--normalize-point ()
  "Keep point out of the read-only insert field prefix."
  (when (clutch-result-insert--field-prefix-read-only-p)
    (goto-char (or (clutch-result-insert--current-field-value-start)
                   (point))))
  (when-let* ((field (clutch-result-insert--field-state-at-position))
              (bounds (clutch-result-insert--field-value-bounds field)))
    (let ((line-start (save-excursion
                        (goto-char (car bounds))
                        (line-beginning-position)))
          (prefix-end (save-excursion
                        (goto-char (car bounds))
                        (- (point) 2)))
          (line-end (save-excursion
                      (goto-char (cdr bounds))
                      (line-end-position))))
      (unless (overlayp clutch-result-insert--active-field-overlay)
        (setq-local clutch-result-insert--active-field-overlay
                    (make-overlay line-start line-end nil t t))
        (overlay-put clutch-result-insert--active-field-overlay
                     'face 'clutch-insert-active-field-face)
        (overlay-put clutch-result-insert--active-field-overlay 'priority -1)
        (overlay-put clutch-result-insert--active-field-overlay 'evaporate t))
      (move-overlay clutch-result-insert--active-field-overlay line-start line-end)
      (unless (overlayp clutch-result-insert--active-prefix-overlay)
        (setq-local clutch-result-insert--active-prefix-overlay
                    (make-overlay line-start prefix-end nil t t))
        (overlay-put clutch-result-insert--active-prefix-overlay
                     'face 'clutch-insert-active-field-name-face)
        (overlay-put clutch-result-insert--active-prefix-overlay 'priority 101)
        (overlay-put clutch-result-insert--active-prefix-overlay 'evaporate t))
      (move-overlay clutch-result-insert--active-prefix-overlay line-start prefix-end))))

;;;###autoload
(defun clutch-result-insert-next-field ()
  "Move point to the next insert field value."
  (interactive)
  (if-let* ((pos (clutch-result-insert--adjacent-field-value-position)))
      (progn
        (goto-char pos)
        (clutch-result-insert--normalize-point))
    (user-error "No next insert field")))

;;;###autoload
(defun clutch-result-insert-prev-field ()
  "Move point to the previous insert field value."
  (interactive)
  (if-let* ((pos (clutch-result-insert--adjacent-field-value-position 'backward)))
      (progn
        (goto-char pos)
        (clutch-result-insert--normalize-point))
    (user-error "No previous insert field")))

;;;###autoload
(defun clutch-result-insert-submit-field ()
  "Accept the current field value and move to the next field."
  (interactive)
  (clutch-result-insert-next-field))

;;;###autoload
(defun clutch-result-insert-complete-field ()
  "Complete the current insert field.
First try standard `completion-at-point' so Corfu/Company can integrate.
If nothing handles the completion, fall back to `completing-read'."
  (interactive)
  (let ((handled (completion-at-point)))
    (unless handled
      (let* ((field-name (or (clutch-result-insert--current-field-name)
                             (user-error "Point is not on an insert field")))
             (candidates (or (clutch-result-insert--field-candidates field-name)
                             (user-error "No completion candidates for %s" field-name)))
             (bounds (or (clutch-result-insert--current-field-value-bounds)
                         (user-error "Cannot edit field %s" field-name)))
             (current (buffer-substring-no-properties (car bounds) (cdr bounds)))
             (choice (completing-read (format "%s: " field-name)
                                      candidates nil t current nil current)))
        (delete-region (car bounds) (cdr bounds))
        (goto-char (car bounds))
        (insert choice)))))

;;;###autoload
(defun clutch-result-insert-fill-current-time ()
  "Replace the current insert field with a current date/time value."
  (interactive)
  (let* ((field-name (or (clutch-result-insert--current-field-name)
                         (user-error "Point is not on an insert field")))
         (col-def (or (clutch-result-insert--column-def field-name)
                      (user-error "No column metadata for %s" field-name)))
         (type-category (plist-get col-def :type-category))
         (value (or (clutch-result-insert--current-time-value type-category)
                    (user-error "Field %s is not a date/time column" field-name))))
    (pcase-let ((`(,beg . ,end)
                 (or (clutch-result-insert--current-field-value-bounds)
                     (user-error "Cannot edit field %s" field-name))))
      (delete-region beg end)
      (goto-char beg)
      (insert value))))

;;;###autoload
(defun clutch-result-insert-edit-json-field ()
  "Open a dedicated editor for the JSON field at point."
  (interactive)
  (let* ((field (clutch-result-insert--current-field-or-error))
         (field-name (plist-get field :name))
         (raw (string-trim (or (clutch-result-insert--field-value field) "")))
         (parent-buf (current-buffer)))
    (unless (clutch-result-insert--json-field-p field)
      (user-error "Field %s is not a JSON column" field-name))
    (let ((value (clutch-result--json-normalize-string raw field-name)))
      (clutch-result-insert--set-seed-field-value field-name value)
      (let ((buf (clutch--open-json-sub-editor
                  (format "*clutch-insert-json: %s.%s*"
                          clutch-result-insert--table field-name)
                  value field-name
                  #'clutch-result-insert-json-finish
                  #'clutch-result-insert-json-cancel)))
        (with-current-buffer buf
          (clutch--result-insert-json-mode 1)
          (setq-local clutch-result-insert-json--parent-buffer parent-buf
                      clutch-result-insert-json--field-name field-name))
        buf))))

;;;###autoload
(defun clutch-result-insert-json-finish ()
  "Save the JSON editor contents back to the parent insert buffer."
  (interactive)
  (let* ((parent clutch-result-insert-json--parent-buffer)
         (field-name clutch-result-insert-json--field-name)
         (raw (string-trim (buffer-substring-no-properties (point-min) (point-max))))
         (value (clutch-result--json-normalize-string raw field-name)))
    (clutch--finish-json-sub-editor
     parent "Insert buffer no longer exists"
     (lambda ()
       (when-let* ((field (clutch-result-insert--field-state field-name))
                   (bounds (clutch-result-insert--field-value-bounds field)))
         (let ((inhibit-modification-hooks nil))
           (delete-region (car bounds) (cdr bounds))
           (goto-char (car bounds))
           (insert value)
           (clutch-result-insert--validate-field-live field)
           (clutch-result-insert--normalize-point)))))))

;;;###autoload
(defun clutch-result-insert-json-cancel ()
  "Cancel JSON field editing and return to the insert buffer."
  (interactive)
  (clutch--cancel-json-sub-editor
   clutch-result-insert-json--parent-buffer
   #'clutch-result-insert--normalize-point))

(defun clutch-result-insert--populate-buffer (table col-names &optional fields)
  "Populate the current insert buffer for TABLE and COL-NAMES.
COL-NAMES is the canonical full column order.  When FIELDS is non-nil, replace
the canonical seed values before rendering the current visible subset."
  (setq-local clutch-result-insert--table table
              clutch-result-insert--all-columns (copy-sequence col-names))
  (when fields
    (setq-local clutch-result-insert--seed-fields
                (clutch-result-insert--canonicalize-fields col-names fields)))
  (clutch-result-insert--ensure-seed-fields)
  (let* ((visible-cols (clutch-result-insert--visible-columns))
         (label-width (clutch-result-insert--field-label-width visible-cols)))
    (let ((inhibit-read-only t)
          (inhibit-modification-hooks t)
          field-states)
      (clutch-result-insert--cleanup)
      (setq-local clutch-result-insert--label-width label-width)
      (erase-buffer)
      (dolist (col visible-cols)
        (let ((prefix-start (point)))
          (insert (clutch-result-insert--field-label-padded col)
                  ": ")
          (add-text-properties prefix-start (point)
                               '(read-only t front-sticky t rear-nonsticky t))
          (let ((value (clutch-result-insert--seed-field-value col))
                (value-marker (copy-marker (point))))
            (insert value "\n")
            (clutch-result-insert--annotate-field-line col prefix-start (point))
            (push (list :name col
                        :detail (clutch-result-insert--column-detail col)
                        :column-def (clutch-result-insert--column-def col)
                        :value-marker value-marker)
                  field-states))))
      (setq-local clutch-result-insert--fields (nreverse field-states))))
  (clutch-result-insert--refresh-header-line)
  (goto-char (point-min))
  (goto-char (or (clutch-result-insert--current-field-value-position)
                 (point-min)))
  (clutch-result-insert--normalize-point))

(defun clutch-result-insert--clone-copy-column-p (detail cidx)
  "Return non-nil when clone-to-insert should copy column at CIDX.
DETAIL is the column detail plist when available."
  (and (not (plist-get detail :generated))
       (not (plist-get detail :primary-key))
       (not (memq cidx clutch--cached-pk-indices))))

(defun clutch-result-insert--filter-clone-fields (table fields)
  "Return cloned insert FIELDS with generated and primary-key columns removed.
TABLE is used to resolve column details for the current result buffer."
  (let ((details (when clutch-connection
                   (clutch--ensure-column-details clutch-connection table))))
    (cl-loop for col in clutch--result-columns
             for cidx from 0
             for col-def = (nth cidx clutch--result-column-defs)
             for detail = (cl-find col details
                                   :key (lambda (item) (plist-get item :name))
                                   :test #'string=)
             for cell = (assoc col fields)
             when (and cell
                       (not (plist-get col-def :hidden))
                       (clutch-result-insert--clone-copy-column-p detail cidx))
             collect (cons col (cdr cell)))))

(defun clutch-result-insert--open-buffer
    (table result-buf &optional fields pending-index omit-primary-key-fields)
  "Open an insert buffer for TABLE backed by RESULT-BUF.
FIELDS prefill the buffer.  PENDING-INDEX re-edits an existing staged insert.
When OMIT-PRIMARY-KEY-FIELDS is non-nil, sparse layout hides primary-key
fields until the user expands to all columns."
  (let* ((col-names
          (with-current-buffer result-buf
            (clutch--visible-column-names)))
         (buf (get-buffer-create (format "*clutch-insert: %s*" table))))
    (with-current-buffer buf
      (let ((inhibit-read-only t)
            (inhibit-modification-hooks t))
        (erase-buffer)
        (setq-local clutch-result-insert--fields nil
                    clutch-result-insert--seed-fields nil
                    clutch-result-insert--all-columns nil
                    clutch-result-insert--column-details-cache nil
                    clutch-result-insert--omit-primary-key-fields nil))
      (clutch-result-insert-mode)
      (setq-local clutch-result-insert--result-buffer result-buf
                  clutch-result-insert--table table
                  clutch-result-insert--source-table table
                  clutch-result-insert--all-columns (copy-sequence col-names)
                  clutch-result-insert--seed-fields
                  (clutch-result-insert--canonicalize-fields col-names fields)
                  clutch-result-insert--column-details-cache
                  (with-current-buffer result-buf
                    (when-let* ((conn clutch-connection))
                      (clutch--ensure-column-details conn table)))
                  clutch-result-insert--omit-primary-key-fields
                  omit-primary-key-fields
                  clutch-result-insert--show-all-fields nil
                  clutch-result-insert--pending-index pending-index
                  completion-at-point-functions
                  '(clutch-result-insert-completion-at-point))
      (clutch-result-insert--populate-buffer table col-names))
    (pop-to-buffer buf)))

(defun clutch-result-insert--ensure-live-result-context ()
  "Signal when the parent result buffer no longer matches this insert form."
  (let ((result-buf clutch-result-insert--result-buffer)
        (source-table clutch-result-insert--source-table))
    (unless (buffer-live-p result-buf)
      (user-error "Result buffer no longer exists"))
    (with-current-buffer result-buf
      (unless (equal clutch--result-source-table source-table)
        (user-error "Result table changed; reopen the insert buffer")))))

;;;###autoload
(defun clutch-result-insert-row ()
  "Open an edit buffer to INSERT a new row into the current table."
  (interactive)
  (clutch-edit--require-sql-staged-mutation "Stage insert")
  (let* ((table (or clutch--result-source-table
                    (user-error "Cannot detect source table")))
         (result-buf (current-buffer)))
    (clutch-result-insert--open-buffer table result-buf)))

(defun clutch-result-insert--row-values-with-pending-edits (row)
  "Return ROW as a list, applying any staged edits in the current result buffer."
  (let* ((values (append row nil))
         (row-identity clutch--row-identity)
         (identity-vec (and row-identity
                            (clutch-db-row-identity-values
                             row row-identity))))
    (when identity-vec
      (dolist (edit clutch--pending-edits)
        (pcase-let ((`((,edit-identity . ,cidx) . ,new-value) edit))
          (when (equal edit-identity identity-vec)
            (setcar (nthcdr cidx values) new-value)))))
    values))

(defun clutch-result-insert--clone-fields-from-row-values (table row-values)
  "Return prefilled insert fields for TABLE from ROW-VALUES."
  (let ((details (when clutch-connection
                   (clutch--ensure-column-details clutch-connection table)))
        fields)
    (cl-loop for col in clutch--result-columns
             for cidx from 0
             for col-def = (nth cidx clutch--result-column-defs)
             for value = (nth cidx row-values)
             for detail = (cl-find col details
                                   :key (lambda (item) (plist-get item :name))
                                   :test #'string=)
             unless (plist-get col-def :hidden)
             do (push (cons col (if (null value)
                                    ""
                                  (clutch-result--editable-field-string
                                   value col-def detail)))
                      fields))
    (clutch-result-insert--filter-clone-fields table (nreverse fields))))

(defun clutch-result-insert--clone-fields-from-result-row (result-buf ridx)
  "Return prefilled insert fields for result row RIDX in RESULT-BUF."
  (with-current-buffer result-buf
    (let* ((table (or clutch--result-source-table
                      (user-error "Cannot detect source table")))
           (display-rows (clutch--result-display-rows))
           (nrows (length display-rows)))
      (if (>= ridx nrows)
          (clutch-result-insert--filter-clone-fields
           table
           (or (nth (- ridx nrows) clutch--pending-inserts)
               (user-error "No staged insert at this row")))
        (let* ((row (or (nth ridx display-rows)
                        (user-error "No row at point")))
               (values (clutch-result-insert--row-values-with-pending-edits row)))
          (clutch-result-insert--clone-fields-from-row-values table values))))))

;;;###autoload
(defun clutch-clone-row-to-insert ()
  "Open a prefilled insert buffer cloned from the current result or record row."
  (interactive)
  (let* ((record-source-p (derived-mode-p 'clutch-record-mode))
         (source (cond
                  (record-source-p
                   (list clutch-record--result-buffer clutch-record--row-idx))
                  ((derived-mode-p 'clutch-result-mode)
                   (list (current-buffer)
                         (or (clutch--row-idx-at-line)
                             (user-error "No row at point"))))
                  (t
                   (user-error "Clone row is only available from result or record buffers"))))
         (result-buf (nth 0 source))
         (ridx (nth 1 source)))
    (unless (buffer-live-p result-buf)
      (user-error "Result buffer no longer exists"))
    (with-current-buffer result-buf
      (clutch-edit--require-sql-staged-mutation "Clone row to insert"))
    (let* ((table (with-current-buffer result-buf
                    (or clutch--result-source-table
                        (user-error "Cannot detect source table"))))
           (fields
            (if record-source-p
                (with-current-buffer result-buf
                  (let* ((display-rows (clutch--result-display-rows))
                         (row (or (nth ridx display-rows)
                                  (user-error "Row %d no longer exists" ridx)))
                         (values (clutch-result-insert--row-values-with-pending-edits
                                  row)))
                    (clutch-result-insert--clone-fields-from-row-values table values)))
              (clutch-result-insert--clone-fields-from-result-row
               result-buf ridx))))
      (clutch-result-insert--open-buffer table result-buf fields nil t))))

;;;###autoload
(defun clutch-result-insert-toggle-field-layout ()
  "Toggle the insert buffer between sparse and all-column layouts."
  (interactive)
  (clutch-result-insert--record-visible-values)
  (setq-local clutch-result-insert--show-all-fields
              (not clutch-result-insert--show-all-fields))
  (clutch-result-insert--populate-buffer
   clutch-result-insert--table
   (clutch-result-insert--all-column-names))
  (message "Insert form now shows %s"
           (clutch--message-keyword
            (if clutch-result-insert--show-all-fields
                "all columns"
              "sparse columns"))))

(defun clutch-result-insert--read-import-text ()
  "Return import text from the active region or the current kill."
  (if (use-region-p)
      (buffer-substring-no-properties (region-beginning) (region-end))
    (condition-case nil
        (current-kill 0 t)
      (error
       (user-error "Kill ring is empty")))))

(defun clutch-result-insert--parse-delimited-text (text)
  "Parse delimited TEXT as TSV or CSV.
Returns a cons cell (DELIMITER . ROWS), where DELIMITER is a character and
ROWS is a list of string lists."
  (let ((delim (if (string-match-p "\t" text) ?\t ?,))
        (rows nil)
        (row nil)
        (field nil)
        (quoted nil)
        (i 0)
        (len (length text)))
    (while (< i len)
      (let ((ch (aref text i)))
        (cond
         (quoted
          (cond
           ((eq ch ?\")
            (if (and (< (1+ i) len)
                     (eq (aref text (1+ i)) ?\"))
                (progn
                  (push "\"" field)
                  (setq i (1+ i)))
              (setq quoted nil)))
           (t
            (push (char-to-string ch) field))))
         ((eq ch ?\")
          (setq quoted t))
         ((eq ch delim)
          (push (apply #'concat (nreverse field)) row)
          (setq field nil))
         ((eq ch ?\r))
         ((eq ch ?\n)
          (push (apply #'concat (nreverse field)) row)
          (push (nreverse row) rows)
          (setq field nil
                row nil))
         (t
          (push (char-to-string ch) field))))
      (setq i (1+ i)))
    (when quoted
      (user-error "Import text has an unterminated quoted field"))
    (push (apply #'concat (nreverse field)) row)
    (push (nreverse row) rows)
    (cons delim
          (cl-remove-if
           (lambda (cells)
             (cl-every #'string-empty-p cells))
           (nreverse rows)))))

(defun clutch-result-insert--header-row-p (row row-count)
  "Return non-nil for ROW as a column-name header among ROW-COUNT rows."
  (let ((all-cols (clutch-result-insert--all-column-names)))
    (and (> row-count 1)
         row
         (cl-every (lambda (cell) (member cell all-cols)) row)
         (= (length row)
            (length (cl-remove-duplicates row :test #'string=))))))

(defun clutch-result-insert--visible-field-names ()
  "Return the currently displayed insert field names."
  (clutch-result-insert--ensure-field-state)
  (mapcar (lambda (field) (plist-get field :name))
          clutch-result-insert--fields))

(defun clutch-result-insert--import-target-columns (rows)
  "Return (COLUMNS . DATA-ROWS) for imported ROWS."
  (let* ((row-count (length rows))
         (header (car rows))
         (columns (if (clutch-result-insert--header-row-p header row-count)
                      header
                    (clutch-result-insert--visible-field-names)))
         (data-rows (if (clutch-result-insert--header-row-p header row-count)
                        (cdr rows)
                      rows)))
    (unless data-rows
      (user-error "Import text contains no data rows"))
    (dolist (row data-rows)
      (when (> (length row) (length columns))
        (user-error "Import row has %d values but only %d target columns"
                    (length row) (length columns))))
    (cons columns data-rows)))

(defun clutch-result-insert--fields-from-import-row (columns values)
  "Return staged insert fields mapping COLUMNS to VALUES."
  (cl-loop for col in columns
           for value in values
           unless (clutch-result-insert--field-empty-p value)
           collect (cons col value)))

(defun clutch-result-insert--clear-seed-fields ()
  "Reset all canonical insert values to empty strings."
  (setq-local clutch-result-insert--seed-fields
              (clutch-result-insert--canonicalize-fields
               (clutch-result-insert--all-column-names)
               nil)))

(defun clutch-result-insert--stage-imported-rows (rows)
  "Stage imported ROWS as INSERT operations in the parent result buffer."
  (clutch-result-insert--ensure-live-result-context)
  (with-current-buffer clutch-result-insert--result-buffer
    (setq clutch--pending-inserts
          (append clutch--pending-inserts rows))
    (clutch--refresh-display)))

(defun clutch-result-insert--delimiter-name (delim)
  "Return a user-facing delimiter name for DELIM."
  (if (eq delim ?\t) "TSV" "CSV"))

;;;###autoload
(defun clutch-result-insert-import-delimited (&optional text)
  "Import TSV or CSV into the current insert buffer.
Uses TEXT when non-nil.
Otherwise reads from the active region or the current kill.
Single-row imports prefill the current form.  Multi-row imports stage inserts
immediately."
  (interactive)
  (clutch-result-insert--record-visible-values)
  (pcase-let* ((raw (or text (clutch-result-insert--read-import-text)))
               (`(,delim . ,rows) (clutch-result-insert--parse-delimited-text raw))
               (`(,columns . ,data-rows) (clutch-result-insert--import-target-columns rows))
               (row-count (length data-rows)))
    (cond
     ((= row-count 1)
      (cl-loop for col in columns
               for value in (car data-rows)
               do (clutch-result-insert--set-seed-field-value col value))
      (clutch-result-insert--populate-buffer
       clutch-result-insert--table
       (clutch-result-insert--all-column-names))
      (message "Imported %s row from %s into the insert form"
               (clutch--message-count 1)
               (clutch--message-keyword
                (clutch-result-insert--delimiter-name delim))))
     (clutch-result-insert--pending-index
      (user-error "Cannot bulk import multiple rows while editing a staged insert"))
     (t
      (let ((field-rows (mapcar (lambda (row)
                                  (clutch-result-insert--fields-from-import-row
                                   columns row))
                                data-rows)))
        (dolist (fields field-rows)
          (unless fields
            (user-error "Cannot stage an empty imported row"))
          (clutch-result-insert--validate-fields fields))
        (clutch-result-insert--stage-imported-rows field-rows)
        (clutch-result-insert--clear-seed-fields)
        (clutch-result-insert--populate-buffer
         clutch-result-insert--table
         (clutch-result-insert--all-column-names))
        (message "%s rows staged from %s import — C-c C-c to commit"
                 (clutch--message-count row-count)
                 (clutch--message-keyword
                  (clutch-result-insert--delimiter-name delim))))))))

(defun clutch-result-insert--parse-fields ()
  "Parse the insert buffer into an alist of (COLUMN . VALUE).
Skips columns with empty values."
  (clutch-result-insert--record-visible-values)
  (cl-loop for col in (clutch-result-insert--all-column-names)
           for value = (clutch-result-insert--seed-field-value col)
           unless (clutch-result-insert--field-empty-p value)
           collect (cons col value)))

(defun clutch-result-insert--validate-json (field-name value)
  "Signal `user-error' when VALUE is not valid JSON for FIELD-NAME."
  (condition-case nil
      (progn
        (ignore (json-parse-string value :object-type 'alist
                                   :array-type 'list
                                   :null-object nil))
        t)
    (error
     (user-error "Field %s expects valid JSON" field-name))))

(defun clutch-result-insert--valid-date-value-p (value)
  "Return non-nil when VALUE is a valid YYYY-MM-DD date."
  (when (string-match
         "\\`\\([0-9]\\{4\\}\\)-\\([0-9]\\{2\\}\\)-\\([0-9]\\{2\\}\\)\\'"
         value)
    (let ((year (string-to-number (match-string 1 value)))
          (month (string-to-number (match-string 2 value)))
          (day (string-to-number (match-string 3 value))))
      (condition-case nil
          (pcase-let ((`(,_sec ,_min ,_hour ,parsed-day ,parsed-month ,parsed-year . ,_)
                       (decode-time (encode-time 0 0 0 day month year))))
            (and (= parsed-year year)
                 (= parsed-month month)
                 (= parsed-day day)))
        (error nil)))))

(defun clutch-result-insert--valid-time-value-p (value)
  "Return non-nil when VALUE is a valid HH:MM[:SS] time."
  (when (string-match
         "\\`\\([0-9]\\{2\\}\\):\\([0-9]\\{2\\}\\)\\(?::\\([0-9]\\{2\\}\\)\\)?\\'"
         value)
    (let ((hour (string-to-number (match-string 1 value)))
          (minute (string-to-number (match-string 2 value)))
          (second (if (match-string 3 value)
                      (string-to-number (match-string 3 value))
                    0)))
      (and (<= 0 hour 23)
           (<= 0 minute 59)
           (<= 0 second 59)))))

(defun clutch-result-insert--valid-datetime-value-p (value)
  "Return non-nil when VALUE is a valid YYYY-MM-DD HH:MM[:SS] datetime."
  (when (string-match "\\`\\(.+\\) \\(.+\\)\\'" value)
    (let ((date-part (match-string 1 value))
          (time-part (match-string 2 value)))
      (and (clutch-result-insert--valid-date-value-p date-part)
           (clutch-result-insert--valid-time-value-p time-part)))))

(defun clutch-result-insert--valid-numeric-value-p (value)
  "Return non-nil for VALUE matching a numeric literal."
  (string-match-p
   "\\`[+-]?\\(?:[0-9]+\\(?:\\.[0-9]*\\)?\\|\\.[0-9]+\\)\\(?:[eE][+-]?[0-9]+\\)?\\'"
   value))

(defun clutch-result--validate-field-value (field-name value col-def detail)
  "Validate VALUE for FIELD-NAME using COL-DEF and schema DETAIL."
  (when (and detail value)
    (let* ((type-category (plist-get col-def :type-category))
           (type (downcase (or (plist-get detail :type) "")))
           (enum-candidates (clutch-result-insert--enum-candidates type)))
      (cond
       (enum-candidates
        (unless (member value enum-candidates)
          (user-error "Field %s must be one of: %s"
                      field-name
                      (string-join enum-candidates ", "))))
       ((member type '("boolean" "bool"))
        (unless (member (downcase value) '("true" "false"))
          (user-error "Field %s expects true/false" field-name)))
       ((member type '("tinyint(1)" "bit(1)"))
        (unless (member value '("0" "1"))
          (user-error "Field %s expects 0/1" field-name)))
       ((and (eq type-category 'numeric)
             (not (clutch-result-insert--valid-numeric-value-p value)))
        (user-error "Field %s expects a numeric value" field-name))
       ((and (eq type-category 'date)
             (not (clutch-result-insert--valid-date-value-p value)))
        (user-error "Field %s expects YYYY-MM-DD" field-name))
       ((and (eq type-category 'time)
             (not (clutch-result-insert--valid-time-value-p value)))
        (user-error "Field %s expects HH:MM[:SS]" field-name))
       ((and (eq type-category 'datetime)
             (not (clutch-result-insert--valid-datetime-value-p value)))
        (user-error "Field %s expects YYYY-MM-DD HH:MM[:SS]" field-name))
       ((or (eq type-category 'json)
            (string= type "json"))
        (clutch-result-insert--validate-json field-name value))))))

(defun clutch-result-insert--validate-field (field-name value)
  "Validate VALUE for FIELD-NAME in the current insert buffer."
  (when-let* ((detail (clutch-result-insert--column-detail field-name)))
    (clutch-result--validate-field-value
     field-name value (clutch-result-insert--column-def field-name) detail)))

(defun clutch-result-insert--validate-fields (fields)
  "Validate staged insert FIELDS in the current insert buffer."
  (dolist (field fields)
    (clutch-result-insert--validate-field (car field) (cdr field))))

(defun clutch-result-insert--build-sql (conn table fields)
  "Build an INSERT statement spec for TABLE with FIELDS using CONN.
FIELDS is an alist of (column-name . value-string)."
  (let ((cols (mapconcat (lambda (field)
                           (clutch-db-escape-identifier conn (car field)))
                         fields ", "))
        (placeholders (mapconcat (lambda (_field) "?") fields ", "))
        (params (mapcar (lambda (field)
                          (let ((value (cdr field)))
                            (if (string= (upcase value) "NULL")
                                nil
                              value)))
                        fields)))
    (cons (format "INSERT INTO %s (%s) VALUES (%s)"
                  (clutch-db-escape-identifier conn table)
                  cols
                  placeholders)
          params)))

;;;###autoload
(defun clutch-result-insert-commit ()
  "Stage the new row for insertion and return to the result buffer.
Use \\[clutch-result-commit] in the result buffer to commit."
  (interactive)
  (let* ((fields (clutch-result-insert--parse-fields))
         (result-buf clutch-result-insert--result-buffer)
         (pending-index clutch-result-insert--pending-index))
    (unless fields (user-error "No values entered"))
    (clutch-result-insert--ensure-live-result-context)
    (when pending-index
      (with-current-buffer result-buf
        (unless (nthcdr pending-index clutch--pending-inserts)
          (user-error "Staged insert no longer exists"))))
    (clutch-result-insert--validate-fields fields)
    (quit-window 'kill)
    (with-current-buffer result-buf
      (if pending-index
          (setcar (nthcdr pending-index clutch--pending-inserts) fields)
        (setq clutch--pending-inserts
              (append clutch--pending-inserts (list fields))))
      (clutch--refresh-display)
      (if pending-index
          (message "Staged insert updated — C-c C-c to commit")
        (message "%s insertion%s staged — C-c C-c to commit"
                 (clutch--message-count (length clutch--pending-inserts))
                 (if (= (length clutch--pending-inserts) 1) "" "s"))))))

;;;###autoload
(defun clutch-result-insert-cancel ()
  "Cancel the INSERT and close the edit buffer."
  (interactive)
  (quit-window 'kill))


(provide 'clutch-edit)
;;; clutch-edit.el ends here
