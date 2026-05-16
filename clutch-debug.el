;;; clutch-debug.el --- Debug capture workflow -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; Internal debug capture, problem records, and debug-buffer rendering.

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'clutch-ui)
(require 'subr-x)

(defcustom clutch-debug-event-limit 25
  "Maximum number of recent debug events kept per buffer or connection.
Only recorded while `clutch-debug-mode' is enabled."
  :type 'natnum
  :group 'clutch)

(defconst clutch-debug-buffer-name "*clutch-debug*"
  "Name of the dedicated clutch debug buffer.")

(defvar clutch-connection)
(defvar clutch-debug-mode nil
  "Non-nil when Clutch debug capture is enabled.")
(defvar-local clutch--buffer-error-details nil
  "Current problem record scoped to this buffer.")
(defvar clutch--problem-records-by-conn (make-hash-table :test 'eq :weakness 'key)
  "Current problem records keyed by live connection object.")
(defvar-local clutch--debug-events nil
  "Recent redacted debug events captured for this buffer.")
(defvar clutch--debug-events-by-conn (make-hash-table :test 'eq :weakness 'key)
  "Recent redacted debug events keyed by live connection object.")

(declare-function clutch--connection-key "clutch-connection" (connection))
(declare-function clutch--format-elapsed "clutch-ui" (seconds))

(defvar clutch--debug-buffer-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "q") #'quit-window)
    map)
  "Keymap for `clutch--debug-buffer-mode'.")

(define-derived-mode clutch--debug-buffer-mode special-mode "clutch-debug"
  "Mode for inspecting the dedicated clutch debug buffer.")

(defun clutch--debug-buffer ()
  "Return the dedicated clutch debug buffer, creating it if needed."
  (let ((buf (get-buffer-create clutch-debug-buffer-name)))
    (with-current-buffer buf
      (unless (derived-mode-p 'clutch--debug-buffer-mode)
        (clutch--debug-buffer-mode))
      (setq-local header-line-format " Clutch debug capture"))
    buf))

(defun clutch--reset-debug-buffer ()
  "Reset the dedicated clutch debug buffer for a new capture window."
  (with-current-buffer (clutch--debug-buffer)
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (format "Clutch Debug\n============\nStarted: %s\n"
                      (format-time-string "%F %T"))))))

(defun clutch--debug-buffer-source-label (buffer)
  "Return a human-readable source label for BUFFER."
  (when (buffer-live-p buffer)
    (buffer-name buffer)))

(defun clutch--debug-buffer-connection-label (connection)
  "Return a human-readable connection label for CONNECTION."
  (when connection
    (condition-case nil
        (clutch--connection-key connection)
      (error nil))))

(defun clutch--debug-format-label (key)
  "Return a human-readable label for KEY."
  (capitalize
   (replace-regexp-in-string
    "-" " "
    (string-remove-prefix ":" (format "%s" key)))))

(defun clutch--debug-indent-block (text spaces)
  "Indent TEXT by SPACES."
  (let ((prefix (make-string spaces ?\s)))
    (string-join
     (mapcar (lambda (line)
               (if (string-empty-p line)
                   line
                 (concat prefix line)))
             (split-string (string-trim-right text) "\n"))
     "\n")))

(defun clutch--debug-format-plist-data (plist)
  "Return a human-readable string for PLIST."
  (with-temp-buffer
    (cl-loop for (key val) on plist by #'cddr
             when val
             do (let ((rendered (clutch--debug-format-data val)))
                  (when rendered
                    (if (string-match-p "\n" rendered)
                        (insert (format "%s:\n%s\n"
                                        (clutch--debug-format-label key)
                                        (clutch--debug-indent-block rendered 2)))
                      (insert (format "%s: %s\n"
                                      (clutch--debug-format-label key)
                                      rendered))))))
    (string-trim-right (buffer-string))))

(defun clutch--debug-format-list-data (items)
  "Return a human-readable string for ITEMS."
  (string-join
   (mapcar
    (lambda (item)
      (let ((rendered (clutch--debug-format-data item)))
        (if (string-match-p "\n" rendered)
            (concat "-\n" (clutch--debug-indent-block rendered 2))
          (concat "- " rendered))))
    items)
   "\n"))

(defun clutch--debug-format-data (data)
  "Return a human-readable string for DATA."
  (cond
   ((null data) nil)
   ((stringp data) data)
   ((vectorp data) (clutch--debug-format-data (append data nil)))
   ((and (listp data) (keywordp (car-safe data)))
    (clutch--debug-format-plist-data data))
   ((listp data)
    (clutch--debug-format-list-data data))
   (t (format "%s" data))))

(defun clutch--debug-insert-field (label value)
  "Insert LABEL and VALUE into the current debug output buffer."
  (when-let* ((rendered (clutch--debug-format-data value)))
    (if (string-match-p "\n" rendered)
        (insert (format "%s:\n%s\n"
                        label
                        (clutch--debug-indent-block rendered 2)))
      (insert (format "%s: %s\n" label rendered)))))

(defun clutch--debug-insert-section (title value)
  "Insert a TITLE section containing VALUE into the current debug buffer."
  (when-let* ((rendered (clutch--debug-format-data value)))
    (insert "\n" title "\n")
    (insert (clutch--debug-indent-block rendered 2) "\n")))

(defun clutch--debug-context-without-inline-sql (context)
  "Return CONTEXT plist without inline SQL payload entries."
  (when context
    (cl-loop for (key val) on context by #'cddr
             unless (memq key '(:generated-sql :sql))
             append (list key val))))

(defun clutch--append-debug-buffer-entry (heading body)
  "Append HEADING and BODY to the dedicated debug buffer."
  (with-current-buffer (clutch--debug-buffer)
    (let ((inhibit-read-only t))
      (goto-char (point-max))
      (unless (bobp)
        (insert "\n\n"))
      (insert heading "\n")
      (insert (make-string (length heading) ?-) "\n")
      (when body
        (insert body))
      (unless (or (bobp) (eq (char-before) ?\n))
        (insert "\n")))))

(defun clutch--append-problem-record-to-debug-buffer (buffer connection problem)
  "Append PROBLEM for BUFFER and CONNECTION to the dedicated debug buffer."
  (when (and clutch-debug-mode problem)
    (let* ((backend (plist-get problem :backend))
           (diag (plist-get problem :diag))
           (debug-payload (plist-get problem :debug))
           (stderr-tail (plist-get problem :stderr-tail))
           (context (copy-tree (plist-get diag :context)))
           (sql (plist-get context :sql))
           (generated-sql (plist-get context :generated-sql))
           (display-context
            (clutch--debug-context-without-inline-sql context))
           (body
            (with-temp-buffer
              (clutch--debug-insert-field "Recorded"
                                          (format-time-string "%F %T"))
              (clutch--debug-insert-field "Backend"
                                          (and backend
                                               (upcase (symbol-name backend))))
              (clutch--debug-insert-field "Source"
                                          (clutch--debug-buffer-source-label buffer))
              (clutch--debug-insert-field "Connection"
                                          (clutch--debug-buffer-connection-label connection))
              (clutch--debug-insert-field "Summary"
                                          (plist-get problem :summary))
              (clutch--debug-insert-field "Category" (plist-get diag :category))
              (clutch--debug-insert-field "Operation" (plist-get diag :op))
              (clutch--debug-insert-field "Request ID" (plist-get diag :request-id))
              (clutch--debug-insert-field "Conn ID" (plist-get diag :conn-id))
              (clutch--debug-insert-field "Exception"
                                          (plist-get diag :exception-class))
              (clutch--debug-insert-field "SQLState" (plist-get diag :sql-state))
              (clutch--debug-insert-field "Vendor code" (plist-get diag :vendor-code))
              (clutch--debug-insert-field "Raw message"
                                          (plist-get diag :raw-message))
              (clutch--debug-insert-section "SQL" sql)
              (clutch--debug-insert-section "Generated SQL" generated-sql)
              (clutch--debug-insert-section "Context" display-context)
              (clutch--debug-insert-section "Cause chain"
                                            (plist-get diag :cause-chain))
              (clutch--debug-insert-section "Backend debug" debug-payload)
              (clutch--debug-insert-section "Agent stderr tail" stderr-tail)
              (string-trim-right (buffer-string)))))
      (clutch--append-debug-buffer-entry "Problem" body))))

(defun clutch--append-debug-event-to-buffer (buffer connection event)
  "Append EVENT for BUFFER and CONNECTION to the dedicated debug buffer."
  (when clutch-debug-mode
    (let* ((backend (plist-get event :backend))
           (body
            (with-temp-buffer
              (clutch--debug-insert-field "Recorded"
                                          (or (plist-get event :time)
                                              (format-time-string "%F %T")))
              (clutch--debug-insert-field "Operation" (plist-get event :op))
              (clutch--debug-insert-field "Phase" (plist-get event :phase))
              (clutch--debug-insert-field "Backend"
                                          (and backend
                                               (upcase (symbol-name backend))))
              (clutch--debug-insert-field "Source"
                                          (clutch--debug-buffer-source-label buffer))
              (clutch--debug-insert-field "Connection"
                                          (clutch--debug-buffer-connection-label connection))
              (when-let* ((elapsed (plist-get event :elapsed)))
                (clutch--debug-insert-field "Elapsed"
                                            (clutch--format-elapsed elapsed)))
              (clutch--debug-insert-field "Summary" (plist-get event :summary))
              (clutch--debug-insert-field "SQL preview"
                                          (plist-get event :sql-preview))
              (clutch--debug-insert-section "Context"
                                            (plist-get event :context))
              (string-trim-right (buffer-string)))))
      (clutch--append-debug-buffer-entry "Trace Event" body))))

(defun clutch--clear-debug-capture ()
  "Forget captured debug events and reset the dedicated debug buffer."
  (setq clutch--debug-events-by-conn (make-hash-table :test 'eq :weakness 'key))
  (dolist (buf (buffer-list))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (setq-local clutch--debug-events nil))))
  (clutch--reset-debug-buffer))

(defun clutch--attached-buffer-for-connection (connection)
  "Return one live buffer attached to CONNECTION, or nil."
  (when connection
    (or (and (eq clutch-connection connection)
             (current-buffer))
        (cl-loop for buf in (buffer-list)
                 when (and (buffer-live-p buf)
                           (eq (buffer-local-value 'clutch-connection buf)
                               connection))
                 return buf))))

(defun clutch--replay-problem-records-to-debug-buffer ()
  "Replay stored problem records into the dedicated debug buffer.
This preserves historical failure context when debug capture starts after a
problem was already recorded."
  (let (records seen)
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when clutch--buffer-error-details
            (let ((entry (list :buffer buf
                               :connection clutch-connection
                               :problem (copy-tree clutch--buffer-error-details))))
              (unless (member entry seen)
                (push entry seen)
                (push entry records)))))))
    (maphash
     (lambda (connection problem)
       (let ((entry (list :buffer (clutch--attached-buffer-for-connection connection)
                          :connection connection
                          :problem (copy-tree problem))))
         (unless (member entry seen)
           (push entry seen)
           (push entry records))))
     clutch--problem-records-by-conn)
    (when records
      (clutch--append-debug-buffer-entry
       "Historical Problems"
       "Recorded before debug mode was enabled.")
      (dolist (entry (nreverse records))
        (clutch--append-problem-record-to-debug-buffer
         (plist-get entry :buffer)
         (plist-get entry :connection)
         (plist-get entry :problem))))))

;;;###autoload
(define-minor-mode clutch-debug-mode
  "Capture additional redacted troubleshooting data for clutch workflows.
When enabled, clutch records a bounded recent-event trace per buffer and per
connection.  JDBC requests also ask the agent for an optional debug payload,
and captured output is appended to the dedicated `*clutch-debug*' buffer."
  :global t
  :group 'clutch
  :lighter " ClutchDbg"
  (when clutch-debug-mode
    (clutch--clear-debug-capture)
    (clutch--replay-problem-records-to-debug-buffer)))

(defun clutch--remember-problem-record (&rest args)
  "Store the current problem record described by ARGS.
Recognized keys are :buffer, :connection, and :problem.  Problem records are
stored buffer-locally and, when CONNECTION is non-nil, in the shared
connection-scoped registry."
  (let* ((buffer (or (plist-get args :buffer) (current-buffer)))
         (connection (plist-get args :connection))
         (problem (copy-tree (plist-get args :problem))))
    (when (and buffer (buffer-live-p buffer))
      (with-current-buffer buffer
        (setq-local clutch--buffer-error-details problem)))
    (when connection
      (if problem
          (puthash connection problem clutch--problem-records-by-conn)
        (remhash connection clutch--problem-records-by-conn)))
    (when problem
      (clutch--append-problem-record-to-debug-buffer buffer connection problem))
    problem))

(defun clutch--forget-problem-record (&optional buffer connection)
  "Forget the current problem record for BUFFER and CONNECTION."
  (when (and buffer (buffer-live-p buffer))
    (with-current-buffer buffer
      (setq-local clutch--buffer-error-details nil)))
  (when connection
    (remhash connection clutch--problem-records-by-conn)))

(defun clutch--forget-problem-records-for-connection (connection)
  "Forget problem records for CONNECTION across all attached buffers."
  (when connection
    (remhash connection clutch--problem-records-by-conn)
    (dolist (buf (buffer-list))
      (when (and (buffer-live-p buf)
                 (eq (buffer-local-value 'clutch-connection buf) connection))
        (with-current-buffer buf
          (setq-local clutch--buffer-error-details nil))))))

(defun clutch--clear-connection-problem-capture (connection)
  "Forget problem records and backend-local diagnostics for CONNECTION."
  (when connection
    (clutch--forget-problem-records-for-connection connection)
    (clutch-db-clear-error-details connection)))

(defun clutch--debug-sql-preview (sql)
  "Return a compact single-line preview of SQL."
  (when sql
    (truncate-string-to-width
     (replace-regexp-in-string "[\n\r\t ]+" " " (string-trim sql))
     160 0 nil "...")))

(defun clutch--debug-trim-events (events)
  "Return EVENTS truncated to `clutch-debug-event-limit'."
  (let ((limit (max 1 clutch-debug-event-limit)))
    (cl-subseq events 0 (min limit (length events)))))

(defun clutch--normalize-debug-event (event)
  "Return EVENT normalized for storage."
  (let ((normalized (copy-tree event)))
    (unless (plist-get normalized :time)
      (setq normalized
            (plist-put normalized :time
                       (format-time-string "%F %T"))))
    (when-let* ((sql (plist-get normalized :sql)))
      (setq normalized (plist-put normalized :sql-preview
                                  (clutch--debug-sql-preview sql)))
      (setq normalized (plist-put normalized :sql-length (length sql)))
      (cl-remf normalized :sql))
    normalized))

(defun clutch--remember-debug-event (&rest event)
  "Record EVENT for the current buffer and optional connection.
Recognized keys include :buffer, :connection, :op, :phase, :summary, :sql,
:backend, :context, and :elapsed.  Recording is disabled unless
`clutch-debug-mode' is non-nil."
  (when clutch-debug-mode
    (let* ((buffer (or (plist-get event :buffer) (current-buffer)))
           (conn (plist-get event :connection))
           (normalized (clutch--normalize-debug-event event)))
      (cl-remf normalized :buffer)
      (cl-remf normalized :connection)
      (when (buffer-live-p buffer)
        (with-current-buffer buffer
          (setq-local clutch--debug-events
                      (clutch--debug-trim-events
                       (cons normalized clutch--debug-events)))))
      (when conn
        (puthash conn
                 (clutch--debug-trim-events
                  (cons normalized (gethash conn clutch--debug-events-by-conn)))
                 clutch--debug-events-by-conn))
      (clutch--append-debug-event-to-buffer buffer conn normalized)
      normalized)))

(provide 'clutch-debug)

;;; clutch-debug.el ends here
