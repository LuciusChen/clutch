;;; clutch-test-edit.el --- Staged edit ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Cell editing, the insert buffer, staged mutations, validation, the JSON
;; sub-editor, clones and qualified sources tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; Edit — cell editing

(defun clutch-test--open-edit-cell (result-buf cell table &optional details)
  "Open an edit buffer from RESULT-BUF for CELL on TABLE.
DETAILS, when non-nil, is returned by `clutch--ensure-column-details'."
  (with-current-buffer result-buf
    (setq-local clutch--result-source-table table))
  (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
            ((symbol-function 'clutch--cell-at-point)
             (lambda () cell))
            ((symbol-function 'clutch--ensure-column-details)
             (lambda (_conn _table &optional _strict)
               details))
            ((symbol-function 'pop-to-buffer)
             (lambda (buf &rest _args) buf)))
    (with-current-buffer result-buf
      (clutch-result-edit-cell))))

(defmacro clutch-test--with-open-edit-cell
    (edit-var result-var result-spec cell table details &rest body)
  "Open EDIT-VAR from RESULT-VAR initialized by RESULT-SPEC, then run BODY."
  (declare (indent 6))
  (let ((spec result-spec))
    (unless (memq :connection-params spec)
      (setq spec (append spec '(:connection-params '(:backend mysql)))))
    `(clutch-test--with-result-state-buffer ,result-var ,spec
       (let ((,edit-var (clutch-test--open-edit-cell
                         ,result-var ,cell ,table ,details)))
         (unwind-protect
             (progn ,@body)
           (when (buffer-live-p ,edit-var)
             (kill-buffer ,edit-var)))))))

(defmacro clutch-test--with-auto-json-edit-cell
    (json-var parent-var result-var &rest body)
  "Open an auto JSON edit cell and run BODY with buffers bound."
  (declare (indent 3))
  `(let (,parent-var)
     (unwind-protect
         (clutch-test--with-open-edit-cell ,json-var ,result-var
             (:columns '("payload")
              :column-defs '((:name "payload" :type-category text))
              :rows '(("{\"ok\":true}"))
              :row-identity
              (clutch-test--primary-row-identity "events" '("payload") '(0)))
             '(0 0 "{\"ok\":true}")
             "events"
             (list (list :name "payload" :type "text"))
           (with-current-buffer ,json-var
             (setq ,parent-var clutch-result-edit-json--parent-buffer)
             (should clutch-result-edit-json--whole-edit-p))
           ,@body)
       (when (buffer-live-p ,parent-var)
         (kill-buffer ,parent-var)))))

(defmacro clutch-test--with-result-edit-buffer (var initial-text &rest body)
  "Bind VAR to an edit buffer seeded with INITIAL-TEXT while running BODY."
  (declare (indent 2))
  `(let ((,var (generate-new-buffer "*clutch-edit-test*")))
     (unwind-protect
         (with-current-buffer ,var
           (insert ,initial-text)
           (clutch--result-edit-mode 1)
           ,@body)
       (kill-buffer ,var))))

(ert-deftest clutch-test-edit-pending-insert-reopens-prefilled-insert-buffer ()
  "Editing a ghost insert row should reopen the staged insert with its values."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection nil
         :connection-params '(:backend mysql)
         :source-table "shipping_incidents"
         :columns '("id" "severity" "owner")
         :rows '((1 "low" "alice"))
         :pending-inserts '((("severity" . "high") ("owner" . "bob"))))
      (cl-letf (((symbol-function 'clutch--cell-at-point)
                 (lambda () (list 1 1 "high"))))
        (with-current-buffer result-buf
          (clutch-result-edit-cell))
        (with-current-buffer insert-buf
          (should (equal clutch-result-insert--pending-index 0))
          (should (equal clutch-result-insert--table "shipping_incidents"))
          (should (string-match-p "^severity[ ]*: high$" (buffer-string)))
          (should (string-match-p "^owner[ ]*: bob$" (buffer-string))))))))

(ert-deftest clutch-test-edit-cell-shows-metadata-and-completion-hints ()
  "Edit buffer should expose enum metadata and completion affordances."
  (clutch-test--with-open-edit-cell buf result-buf
      (:connection (make-clutch-jdbc-conn :params '(:driver oracle))
       :columns '("severity")
       :column-defs '((:name "severity" :type-category text))
       :rows '(("low"))
       :row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("severity") '(0)))
      '(0 0 "low")
      "shipping_incidents"
      (list (list :name "severity" :type "enum('low','medium','high')"
                  :nullable t :default "'low'"))
    (with-current-buffer buf
      (should (string-match-p "\\[enum\\]" (format "%s" header-line-format)))

      (should (string-match-p "Set NULL.*Set DEFAULT"
                              (format "%s" header-line-format)))
      (should-not (string-match-p "Editing row" (format "%s" header-line-format)))
      (pcase-let ((`(,beg ,end ,candidates . ,_)
                   (clutch-result-edit-completion-at-point)))
        (should (= beg (point-min)))
        (should (= end (point-max)))
        (should (equal candidates '("low" "medium" "high")))))))

(ert-deftest clutch-test-jdbc-blob-encoding-survives-complete-edit-path ()
  "JDBC BLOB encoding should survive normalization, editing, staging, and wire."
  (let* ((modified "{\"message\":\"已修改\"}")
         (source
          (car (clutch-jdbc--normalize-row
                '((:__type "blob" :length 18
                   :text "{\"message\":\"原值\"}" :encoding "GB18030")))))
         parent-buf)
    (unwind-protect
        (clutch-test--with-open-edit-cell json-buf result-buf
            (:connection (make-clutch-jdbc-conn :params '(:driver oracle))
             :columns '("ID" "CONTENT")
             :column-defs '((:name "ID" :type-category numeric)
                            (:name "CONTENT" :type-category blob
                             :backend-type "BLOB"))
             :rows (list (list 1 source))
             :row-identity
             (clutch-test--primary-row-identity "DOCUMENTS" '("ID") '(0)))
            (list 0 1 source)
            "DOCUMENTS"
            (list (list :name "CONTENT" :type "BLOB"
                        :backend-type "BLOB"))
          (with-current-buffer json-buf
            (setq parent-buf clutch-result-edit-json--parent-buffer)
            (should clutch-result-edit-json--whole-edit-p)
            (erase-buffer)
            (insert modified))
          (with-current-buffer parent-buf
            (should (equal clutch-result-edit--blob-encoding "GB18030")))
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&optional kill _window)
                       (when kill
                         (kill-buffer (current-buffer))))))
            (with-current-buffer json-buf
              (clutch-result-edit-json-finish)))
          (let* ((staged (with-current-buffer result-buf
                           (cdar clutch--pending-edits)))
                 (wire
                  (clutch-jdbc--wire-param
                   (clutch-db-typed-param staged "BLOB"))))
            (should (equal staged modified))
            (should
             (equal
              (base64-decode-string (alist-get 'base64 wire))
              (encode-coding-string
               modified (coding-system-from-name "GB18030"))))))
      (when (buffer-live-p parent-buf)
        (kill-buffer parent-buf)))))

(ert-deftest clutch-test-edit-cell-opens-null-state-placeholder ()
  "Editing a NULL cell should show a placeholder while keeping buffer text empty."
  (clutch-test--with-open-edit-cell buf result-buf
      (:columns '("id" "note")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "note" :type-category text))
       :rows '((1 nil))
       :row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("id") '(0)))
      '(0 1 nil)
      "shipping_incidents"
      (list (list :name "note" :type "text"))
    (with-current-buffer buf
      (should (eq clutch-result-edit--special-value 'null))
      (should (equal (buffer-string) ""))
      (should (overlayp clutch-result-edit--special-placeholder-overlay))
      (should (equal
               (substring-no-properties
                (overlay-get clutch-result-edit--special-placeholder-overlay
                             'after-string))
               "<null>"))
      (should (eq (get-text-property
                   0 'face
                   (overlay-get clutch-result-edit--special-placeholder-overlay
                                'after-string))
                  'clutch-null-face))
      (insert "hello")
      (should-not clutch-result-edit--special-value)
      (should-not (overlayp clutch-result-edit--special-placeholder-overlay))
      (should (equal (buffer-string) "hello")))))

(ert-deftest clutch-test-edit-special-value-hints-follow-column-detail ()
  "Edit hints and commands should reject unsupported column special values."
  (clutch-test--with-result-edit-buffer _edit-buf "keep"
    (setq-local clutch-result-edit--column-name "status"
                clutch-result-edit--default-supported-p t)
    (dolist (case '((nil nil nil nil)
                    (t nil t nil)
                    (nil "'new'" nil t)
                    (t "'new'" t t)))
      (pcase-let ((`(,nullable ,default ,show-null ,show-default) case))
        (setq-local clutch-result-edit--column-detail
                    (list :name "status" :nullable nullable :default default))
        (let ((header (clutch-result-edit--header-line)))
          (should (equal (list (and (string-match-p "Set NULL" header) t)
                               (and (string-match-p "Set DEFAULT" header) t))
                         (list show-null show-default))))))
    (setq-local clutch-result-edit--column-detail
                '(:name "status" :nullable nil))
    (should-error (clutch-result-edit-set-null) :type 'user-error)
    (should-error (clutch-result-edit-set-default) :type 'user-error)
    (should (equal (buffer-string) "keep"))))

(ert-deftest clutch-test-edit-cell-original-value-does-not-stage ()
  "Submitting or restoring the effective value should preserve staged state."
  (dolist (case `((unchanged 42 nil nil "42" nil nil)
                  (reverted "43" ((([1] . 1) . "43")) "42" "43" nil nil)
                  (default-unchanged
                   ,clutch--cell-default-placeholder
                   ((([1] . 1) . ,clutch--cell-default-placeholder))
                   nil "" default
                   ((([1] . 1) . ,clutch--cell-default-placeholder)))
                  (default-reverted
                   ,clutch--cell-default-placeholder
                   ((([1] . 1) . ,clutch--cell-default-placeholder))
                   "42" "" default nil)))
    (pcase-let ((`(,label ,opened-value ,pending-edits ,replacement
                          ,initial-text ,special-value ,expected-pending)
                 case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-open-edit-cell buf result-buf
            (:connection (make-clutch-jdbc-conn :params '(:driver oracle))
             :columns '("id" "qty")
             :column-defs '((:name "id" :type-category numeric)
                            (:name "qty" :type-category numeric))
             :rows '((1 42))
             :row-identity
             (clutch-test--primary-row-identity "orders" '("id") '(0))
             :pending-edits pending-edits)
            (list 0 1 opened-value)
            "orders"
            (list (list :name "qty" :type "int" :default "0"))
          (with-current-buffer buf
            (should (equal (buffer-string) initial-text))
            (should (eq clutch-result-edit--special-value special-value))
            (when special-value
              (should (equal
                       (substring-no-properties
                        (overlay-get clutch-result-edit--special-placeholder-overlay
                                     'after-string))
                       "<default>")))
            (when replacement
              (erase-buffer)
              (insert replacement))
            (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                      ((symbol-function 'quit-window) #'ignore)
                      ((symbol-function 'message) #'ignore))
              (clutch-result-edit-finish))
            (should (equal (with-current-buffer result-buf
                             clutch--pending-edits)
                           expected-pending))))))))

(ert-deftest clutch-test-edit-cell-rejects-stale-source ()
  "Finishing an edit should not stage over a changed visible row or cell."
  (dolist (case '((row-identity
                   ((1 "alice") (2 "bob"))
                   ((2 "bob") (1 "alice")))
                  (cell-value
                   ((1 "alice"))
                   ((1 "remote")))))
    (pcase-let ((`(,label ,initial-rows ,updated-rows) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-open-edit-cell buf result-buf
            (:columns '("id" "name")
             :column-defs '((:name "id" :type-category numeric)
                            (:name "name" :type-category text))
             :rows initial-rows
             :row-identity
             (clutch-test--primary-row-identity "users" '("id") '(0))
             :pending-edits nil)
            '(0 1 "alice")
            "users"
            (list (list :name "name" :type "text"))
          (with-current-buffer result-buf
            (setq-local clutch--result-rows updated-rows))
          (with-current-buffer buf
            (erase-buffer)
            (insert "ann")
            (cl-letf (((symbol-function 'clutch--replace-row-at-index)
                       #'ignore)
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                      ((symbol-function 'quit-window) #'ignore))
              (let ((err (should-error (clutch-result-edit-finish)
                                       :type 'user-error)))
                (should (string-match-p "Edited row changed"
                                        (error-message-string err))))))
          (should-not (with-current-buffer result-buf
                        clutch--pending-edits)))))))

(ert-deftest clutch-test-edit-cell-entry-errors ()
  "Edit entry should fail early when row identity metadata is unavailable."
  (dolist (case
           '((no-identity
              nil nil
              "Cannot edit cell: no primary, unique, or row locator identity available for table users")
             (metadata-error
              error "metadata failed"
              "Cannot edit cell: row identity metadata failed for table users: metadata failed")
             (table-history
              nil nil
              "Cannot edit cell: the query reads table users as of another time"
              "SELECT * FROM users FOR SYSTEM_TIME ALL")))
    (pcase-let ((`(,label ,status ,message ,expected ,query) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-result-state-buffer result-buf
            (:connection-params '(:backend mysql)
             :source-table "users"
             :last-query (or query "SELECT * FROM users")
             :columns '("id" "name")
             :column-defs '((:name "id" :type-category numeric)
                            (:name "name" :type-category text))
             :rows '((1 "alice"))
             :row-identity-status status
             :row-identity-error-message message)
          (cl-letf (((symbol-function 'clutch--cell-at-point)
                     (lambda () '(0 1 "alice"))))
            (with-current-buffer result-buf
              (let ((err (should-error (clutch-result-edit-cell)
                                       :type 'user-error)))
                (should (string-match-p (regexp-quote expected)
                                        (error-message-string err)))
                (when (eq status 'error)
                  (should (string-match-p
                           (regexp-quote clutch-debug-buffer-name)
                           (error-message-string err)))))
              (should-not (get-buffer "*clutch-edit: [0].name*")))))))))

(ert-deftest clutch-test-edit-cell-json-sub-editor-contract ()
  "JSON cells should open the JSON sub-editor with serialized JSON text."
  (let ((object (make-hash-table :test 'equal)))
    (puthash "test" t object)
    (puthash "data" (vector 1 2) object)
    (dolist (case (list (list :label "raw json"
                              :payload "{\"a\":1}"
                              :expected "{\n  \"a\": 1\n}"
                              :buffer-match "\\*clutch-edit-json: payload\\*"
                              :header-match "JSON field payload")
                        (list :label "parsed object"
                              :payload object
                              :matches '("\"test\": true" "\"data\": \\[")
                              :not-matches '("#s(hash-table"))
                        (list :label "json string"
                              :payload "hello"
                              :expected "\"hello\"")))
      (ert-info ((format "case: %s" (plist-get case :label)))
        (let ((payload (plist-get case :payload)))
          (clutch-test--with-open-edit-cell buf result-buf
              (:columns '("payload")
               :column-defs '((:name "payload" :type-category json))
               :rows (list (list payload))
               :row-identity
               (clutch-test--primary-row-identity
                "shipping_incidents" '("payload") '(0)))
              (list 0 0 payload)
              "shipping_incidents"
              (list (list :name "payload" :type "json"))
            (when-let* ((buffer-match (plist-get case :buffer-match)))
              (should (string-match-p buffer-match (buffer-name buf))))
            (with-current-buffer buf
              (should (equal clutch-result-edit-json--field-name "payload"))
              (when-let* ((header-match (plist-get case :header-match)))
                (should (string-match-p header-match
                                        (format "%s" header-line-format))))
              (let ((text (buffer-substring-no-properties
                           (point-min) (point-max))))
                (when-let* ((expected (plist-get case :expected)))
                  (should (equal text expected)))
                (dolist (pattern (plist-get case :matches))
                  (should (string-match-p pattern text)))
                (dolist (pattern (plist-get case :not-matches))
                  (should-not (string-match-p pattern text)))))))))))

(ert-deftest clutch-test-edit-cell-json-looking-text-opens-json-sub-editor ()
  "Text cells containing JSON objects should still use the JSON editor."
  (clutch-test--with-open-edit-cell buf result-buf
      (:columns '("payload")
       :column-defs '((:name "payload" :type-category text))
       :rows '(("{\"order\":{\"id\":42},\"lines\":[1,2]}"))
       :row-identity
       (clutch-test--primary-row-identity "events" '("payload") '(0)))
      '(0 0 "{\"order\":{\"id\":42},\"lines\":[1,2]}")
      "events"
      (list (list :name "payload" :type "text"))
    (should (string-match-p "\\*clutch-edit-json: payload\\*"
                            (buffer-name buf)))
    (with-current-buffer buf
      (should (string-match-p "JSON field payload"
                              (format "%s" header-line-format)))
      (should (equal (buffer-substring-no-properties (point-min) (point-max))
                     "{\n  \"order\": {\n    \"id\": 42\n  },\n  \"lines\": [\n    1,\n    2\n  ]\n}")))))

(ert-deftest clutch-test-edit-cell-auto-json-closes-edit-flow ()
  "Auto-opened JSON editors should close the parent edit flow."
  (dolist (case '(cancel finish))
    (ert-info ((format "case: %s" case))
      (clutch-test--with-auto-json-edit-cell json-buf parent-buf result-buf
        (when (eq case 'finish)
          (with-current-buffer json-buf
            (erase-buffer)
            (insert "{\"ok\":false}")))
        (cl-letf (((symbol-function 'quit-window)
                   (lambda (&optional kill _window)
                     (when kill
                       (kill-buffer (current-buffer)))))
                  ((symbol-function 'clutch--ensure-column-details)
                   (lambda (_conn _table &optional _strict)
                     (list (list :name "payload" :type "text")))))
          (with-current-buffer json-buf
            (pcase case
              ('cancel (clutch-result-edit-json-cancel))
              ('finish (clutch-result-edit-json-finish)))))
        (should-not (buffer-live-p json-buf))
        (should-not (buffer-live-p parent-buf))
        (with-current-buffer result-buf
          (should-not clutch--active-edit-cell)
          (if (eq case 'finish)
              (should (equal clutch--pending-edits
                             (list
                              (cons (cons (vector "{\"ok\":true}") 0)
                                    "{\"ok\":false}"))))
            (should-not clutch--pending-edits)))))))

(ert-deftest clutch-test-edit-set-current-time-replaces-existing-value ()
  "The edit-buffer current-time helper should replace the current value with now."
  (with-temp-buffer
    (insert "2020-01-01 00:00:00")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--column-name "opened_at"
                clutch-result-edit--column-def '(:name "opened_at" :type-category datetime)
                clutch-result-edit--column-detail '(:name "opened_at" :type "datetime"))
    (cl-letf (((symbol-function 'current-time)
               (lambda () (encode-time 30 45 13 12 3 2026))))
      (clutch-result-edit-set-current-time)
      (should (equal (buffer-string) "2026-03-12 13:45:30")))))

;;;; Edit — insert buffer

(ert-deftest clutch-test-insert-buffer-navigation ()
  "Insert buffer TAB and RET navigation should jump between field values."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("id" "name" "created_at")
         :column-defs '((:name "id") (:name "name") (:name "created_at"))
         :source-table "users")
      (clutch-result-insert--open-buffer
       "users" result-buf '(("name" . "alice")))
      (with-current-buffer insert-buf
        (clutch-test--goto-insert-field-value "id" t)
        (clutch-result-insert-next-field)
        (should (equal (clutch-result-insert--current-field-name) "name"))
        (should (= (point) (line-end-position)))
        (clutch-result-insert-next-field)
        (should (string-prefix-p "created_at" (thing-at-point 'line t)))
        (clutch-result-insert-prev-field)
        (should (equal (clutch-result-insert--current-field-name) "name"))
        (clutch-test--goto-insert-field-value "id")
        (call-interactively (key-binding (kbd "RET")))
        (should (equal (clutch-result-insert--current-field-name) "name"))))))

(ert-deftest clutch-test-pending-insert-render-contract ()
  "Staged insert rows should show insert markers and metadata placeholders."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name" "created_at" "notes")
                clutch--result-source-table "users"
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text)
                                             (:name "created_at" :type-category datetime)
                                             (:name "notes" :type-category text))
                clutch--pending-inserts '((("name" . "alice"))))
    (let ((row-positions (make-vector 1 nil))
          render-state)
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (&rest _)
                   (error "render should not synchronously load column details")))
                ((symbol-function 'clutch--cached-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :generated t)
                         (list :name "name")
                         (list :name "created_at" :default "CURRENT_TIMESTAMP")
                         (list :name "notes")))))
        (setq render-state (clutch--build-render-state))
        (clutch--insert-pending-insert-rows '(0 1 2 3) [12 12 12 12] 3 0 row-positions
                                            render-state)
        (let ((rendered (buffer-string)))
          (should (string-prefix-p "│I I1 " rendered))
          (should (string-match-p "<generated>" rendered))
          (should (string-match-p "<default>" rendered))
          (should (string-match-p "alice" rendered)))))))

(ert-deftest clutch-test-pending-insert-placeholders-skip-metadata-without-inserts ()
  "Result rendering should not request insert placeholder metadata without inserts."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--result-source-table "users"
                clutch--pending-inserts nil)
    (cl-letf (((symbol-function 'clutch--cached-column-details)
               (lambda (&rest _)
                 (error "column details cache should not be consulted")))
              ((symbol-function 'clutch--ensure-column-details-async)
               (lambda (&rest _)
                 (error "column details should not be queued"))))
      (should-not (plist-get (clutch--build-render-state)
                             :insert-placeholders)))))

(ert-deftest clutch-test-pending-insert-placeholders-queue-metadata-and-render-empty ()
  "Staged insert rendering should keep column shape while metadata loads."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--result-source-table "users"
                clutch--pending-inserts '((("name" . "alice"))))
    (let (queued)
      (cl-letf (((symbol-function 'clutch--cached-column-details)
                 (lambda (&rest _) nil))
                ((symbol-function 'clutch--ensure-column-details-async)
                 (lambda (_conn table)
                   (setq queued table))))
        (let ((render-state (clutch--build-render-state)))
          (should (equal queued "users"))
          (should (equal (plist-get render-state :insert-placeholders)
                         '(nil nil)))
          (should (equal (clutch--pending-insert-render-rows render-state)
                         '((nil "alice")))))))))

(ert-deftest clutch-test-insert-fill-current-time-respects-column-type ()
  "The insert buffer time-filling helper should use result column metadata."
  (clutch-test--with-insert-result-buffer result-buf
      (:columns '("due_on" "created_at" "name")
       :column-defs '((:name "due_on" :type-category date)
                      (:name "created_at" :type-category datetime)
                      (:name "name" :type-category text)))
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (clutch-result-insert--open-buffer
       "users" result-buf
       '(("due_on" . "2024-01-01")
         ("created_at" . "2024-01-01 00:00:00")
         ("name" . "alice")))
      (with-current-buffer insert-buf
      (cl-letf (((symbol-function 'current-time)
                 (lambda () (encode-time 30 45 13 12 3 2026))))
        (goto-char (point-min))
        (clutch-result-insert-fill-current-time)
        (should (equal
                 (plist-get (clutch-result-insert--field-state "due_on") :value)
                 "2026-03-12"))
        (forward-line 1)
        (clutch-result-insert-fill-current-time)
        (should (equal
                 (plist-get (clutch-result-insert--field-state "created_at") :value)
                 "2026-03-12 13:45:30"))
        (forward-line 1)
        (should-error (clutch-result-insert-fill-current-time)
                      :type 'user-error))))))

(ert-deftest clutch-test-insert-buffer-labels-show_field_metadata ()
  "Insert buffer labels should show field metadata without changing parsed names."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("id" "severity" "postmortem" "is_ship_blocked" "opened_at")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "severity" :type-category text)
                        (:name "postmortem" :type-category json)
                        (:name "is_ship_blocked" :type-category numeric)
                        (:name "opened_at" :type-category datetime))
         :connection (make-clutch-test-conn :table "shipping_incidents"
                                            :columns '((:name "id" :type "int" :generated t :nullable nil)
                                                       (:name "severity" :type "enum('low','medium')" :nullable nil)
                                                       (:name "postmortem" :type "json" :nullable t)
                                                       (:name "is_ship_blocked" :type "tinyint(1)" :default "0" :nullable nil)
                                                       (:name "opened_at" :type "datetime" :nullable nil))))
      (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
      (with-current-buffer insert-buf
        (let ((rendered (buffer-string)))
          (should (string-match-p "^id[ ]+\\[generated\\]: $" rendered))
          (should (string-match-p "^severity[ ]+\\[enum required\\]: $" rendered))
          (should (string-match-p "^postmortem[ ]+\\[json\\]: $" rendered))
          (should (string-match-p "^is_ship_blocked \\[default=0 bool\\]: $" rendered))
          (should (string-match-p "^opened_at[ ]+\\[datetime required\\]: $" rendered)))
        (goto-char (point-min))
        (should (get-text-property (point) 'read-only))
        (should (eq (get-text-property (point) 'face)
                    'clutch-field-name-face))
        (search-forward "[generated]")
        (should (eq (get-text-property (1- (point)) 'face)
                    'clutch-insert-field-tag-face))
        (clutch-test--goto-insert-field-value "severity" t)
        (insert "low")
        (goto-char (point-min))
        (let ((fields (clutch-result-insert--parse-fields)))
          (should (equal fields '(("severity" . "low")))))))))

(ert-deftest clutch-test-insert-buffer-shows-all-fields-by-default ()
  "Insert buffers should render every field without a sparse toggle."
  :tags '(:smoke)
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("id" "severity" "owner" "created_at")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "severity" :type-category text)
                        (:name "owner" :type-category text)
                        (:name "created_at" :type-category datetime))
         :connection (make-clutch-test-conn :table "shipping_incidents"
                                            :columns '((:name "id" :type "int" :generated t :nullable nil)
                                                       (:name "severity" :type "enum('low','medium','high')" :nullable nil)
                                                       (:name "owner" :type "varchar(64)" :default "system" :nullable t)
                                                       (:name "created_at" :type "datetime" :default "CURRENT_TIMESTAMP" :nullable t)))
         :source-table "shipping_incidents")
      (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
      (with-current-buffer insert-buf
        (should (string-match-p "C-c \\. Set current time"
                                (substring-no-properties
                                 (clutch-result-insert--header-line))))
        (should (string-match-p "^id[ ]+\\[generated\\]: $" (buffer-string)))
        (should (string-match-p "^severity[ ]+\\[enum required\\]: $" (buffer-string)))
        (should (string-match-p "^owner[ ]+\\[default=system\\]: $" (buffer-string)))
        (should (string-match-p "^created_at[ ]+\\[default=CURRENT_TIMESTAMP datetime\\]: "
                                (buffer-string)))
        (should-not (lookup-key clutch--result-insert-major-mode-map
                                (kbd "C-c C-a")))
        (goto-char (point-min))
        (re-search-forward "^owner.*: " nil t)
        (insert "bob")
        (should (string-match-p "^owner[ ]+\\[default=system\\]: bob$"
                                (buffer-string)))))))

(ert-deftest clutch-test-clone-row-to-insert-prefills-effective-result-values ()
  "Cloning a result row should reuse visible row values and staged edits."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection (make-clutch-test-conn :table "shipping_incidents"
                                            :columns '((:name "id" :type "int" :generated t :nullable nil)
                                                       (:name "severity" :type "enum('low','medium','high')" :nullable nil)
                                                       (:name "owner" :type "varchar(64)" :nullable t)
                                                       (:name "created_at" :type "datetime" :default "CURRENT_TIMESTAMP" :nullable t)))
         :connection-params '(:backend mysql)
         :last-query "SELECT * FROM shipping_incidents"
         :source-table "shipping_incidents"
         :columns '("id" "severity" "owner" "created_at")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "severity" :type-category text)
                        (:name "owner" :type-category text)
                        (:name "created_at" :type-category datetime))
         :rows '((1 "low" "alice" "2026-03-01 10:00:00"))
         :row-identity (clutch-test--primary-row-identity
                        "shipping_incidents" '("id") '(0))
         :pending-edits `((([1] . 1) . "high")
                          (([1] . 3) . ,clutch--cell-default-placeholder)))
      (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                 (lambda () 0)))
        (with-current-buffer result-buf
          (clutch-clone-row-to-insert)))
      (with-current-buffer insert-buf
        (should (string-match-p "C-c \\. Set current time"
                                (substring-no-properties
                                 (clutch-result-insert--header-line))))
        (should (string-match-p "^id[ ]+\\[generated\\]: $" (buffer-string)))
        (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                (buffer-string)))
        (should (string-match-p "^owner[ ]*: alice$" (buffer-string)))
        (should (string-match-p "^created_at .*: $" (buffer-string)))
        (should-not (string-match-p "2026-03-01 10:00:00"
                                    (buffer-string)))))))

(ert-deftest clutch-test-clone-row-to-insert-from-record-buffer ()
  "Cloning from a record buffer should prefill the current visible record row."
  (dolist (case '((unfiltered ((7 "carol")) nil nil "carol" nil)
                  (filtered ((1 "alice") (2 "bob")) "bob" ((2 "bob"))
                            "bob" "alice")))
    (pcase-let ((`(,label ,rows ,filter ,filtered-rows ,expected ,rejected)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((record-buf (generate-new-buffer "*clutch-record*")))
          (unwind-protect
              (clutch-test--with-pop-to-buffer-capture insert-buf
                (clutch-test--with-result-state-buffer result-buf
                    (:connection (make-clutch-test-conn :table "shipping_incidents"
                                                        :columns '((:name "id" :type "int" :generated t :nullable nil)
                                                                   (:name "owner" :type "varchar(64)" :nullable t)))
                     :connection-params '(:backend mysql)
                     :last-query "SELECT * FROM shipping_incidents"
                     :source-table "shipping_incidents"
                     :columns '("id" "owner")
                     :column-defs '((:name "id" :type-category numeric)
                                    (:name "owner" :type-category text))
                     :rows rows
                     :filter-pattern filter
                     :filtered-rows filtered-rows)
                  (with-current-buffer record-buf
                    (clutch-record-mode)
                    (setq-local clutch-record--result-buffer result-buf
                                clutch-record--row-idx 0))
                  (with-current-buffer record-buf
                    (clutch-clone-row-to-insert))
                  (with-current-buffer insert-buf
                    (should (string-match-p "^id[ ]+\\[generated\\]: $"
                                            (buffer-string)))
                    (should (string-match-p
                             (format "^owner[ ]*: %s$" expected)
                             (buffer-string)))
                    (when rejected
                      (should-not (string-match-p rejected
                                                  (buffer-string)))))))
            (when (buffer-live-p record-buf)
              (kill-buffer record-buf))))))))

(ert-deftest clutch-test-clone-row-to-insert-leaves-primary-key-empty ()
  "Clone-to-insert should render primary-key fields without prefilling them."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection (make-clutch-test-conn :table "incident_codes"
                                            :columns '((:name "id" :type "int" :primary-key t :nullable nil)
                                                       (:name "label" :type "varchar(64)" :nullable nil)))
         :connection-params '(:backend mysql)
         :last-query "SELECT * FROM incident_codes"
         :source-table "incident_codes"
         :columns '("id" "label")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "label" :type-category text))
         :rows '((42 "duplicate me")))
      (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                 (lambda () 0)))
        (with-current-buffer result-buf
          (clutch-clone-row-to-insert)))
      (with-current-buffer insert-buf
        (should (string-match-p "^id[ ]+\\[required\\]: $" (buffer-string)))
        (should (string-match-p "^label[ ]+\\[required\\]: duplicate me$"
                                (buffer-string)))))))

(ert-deftest clutch-test-insert-import-delimited-preserves-clipboard-errors ()
  "Clipboard failures must not be misreported as an empty kill ring."
  (with-temp-buffer
    (insert "existing form text")
    (let ((kill-ring '("owner,severity\nbob,high\n"))
          (interprogram-paste-function
           (lambda () (error "Clipboard provider failed"))))
      (should (equal
               (should-error
                (call-interactively #'clutch-result-insert-import-delimited))
               '(error "Clipboard provider failed"))))
    (let ((kill-ring nil)
          (interprogram-paste-function nil))
      (should (string-match-p
               "Kill ring is empty"
               (error-message-string
                (should-error
                 (call-interactively #'clutch-result-insert-import-delimited))))))
    (should (equal (buffer-string) "existing form text"))))

(ert-deftest clutch-test-insert-import-delimited-parses-quoted-csv-row ()
  "Single-row CSV import should prefill the current insert form."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("owner" "severity")
         :column-defs '((:name "owner" :type-category text)
                        (:name "severity" :type-category text))
         :connection (make-clutch-test-conn :table "shipping_incidents"
                                            :columns '((:name "owner" :type "varchar(64)" :nullable t)
                                                       (:name "severity" :type "enum('low','high')" :nullable nil)))
         :source-table "shipping_incidents")
      (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
      (with-current-buffer insert-buf
        (clutch-result-insert-import-delimited
         "owner,severity\n\"Bob, Jr.\",high\n")
        (should (string-match-p "^owner[ ]*: Bob, Jr\\.$" (buffer-string)))
        (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                (buffer-string)))))))

(ert-deftest clutch-test-insert-import-delimited-stages-multi-row-header-mapping ()
  "Multi-row delimited import should stage inserts by header names."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("severity" "owner" "created_at")
         :column-defs '((:name "severity" :type-category text)
                        (:name "owner" :type-category text)
                        (:name "created_at" :type-category datetime))
         :connection (make-clutch-test-conn :table "shipping_incidents"
                                            :columns '((:name "severity" :type "enum('low','high')" :nullable nil)
                                                       (:name "owner" :type "varchar(64)" :nullable t)
                                                       (:name "created_at" :type "datetime" :default "CURRENT_TIMESTAMP" :nullable t)))
         :source-table "shipping_incidents")
      (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore))
        (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
        (with-current-buffer insert-buf
          (clutch-result-insert-import-delimited
           "owner\tseverity\nbob\thigh\nann\tlow\n")
          (should (equal (with-current-buffer result-buf
                           clutch--pending-inserts)
                         '((("owner" . "bob") ("severity" . "high"))
                           (("owner" . "ann") ("severity" . "low")))))
          (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                  (buffer-string))))))))

(ert-deftest clutch-test-insert-import-refuses-while-a-query-runs ()
  "Importing rows should be refused while a statement runs, as staging is.
The page that statement brings would replace the staged rows."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("severity" "owner")
         :column-defs '((:name "severity" :type-category text)
                        (:name "owner" :type-category text))
         :connection (make-clutch-test-conn :table "shipping_incidents"
                                            :columns '((:name "severity" :type "text" :nullable t)
                                                       (:name "owner" :type "text" :nullable t)))
         :source-table "shipping_incidents")
      (let ((clutch--running-queries (make-hash-table :test 'eq)))
        (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore))
          (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
          (puthash (buffer-local-value 'clutch-connection result-buf)
                   (list :buffer result-buf) clutch--running-queries)
          (with-current-buffer insert-buf
            (should-error (clutch-result-insert-import-delimited
                           "owner\tseverity\nbob\thigh\nann\tlow\n")
                          :type 'user-error))
          (should-not (with-current-buffer result-buf
                        clutch--pending-inserts)))))))

;;;; Edit — staged mutations (row identity)

(ert-deftest clutch-test-insert-stage-replaces-existing-pending-insert ()
  "Staging a re-edited insert should replace the pending entry in place."
  (clutch-test--with-result-state-buffer result-buf
      (:connection nil
       :pending-inserts '((("severity" . "low"))))
    (let (replaced insert-buf)
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (error "existing insert update should use row replacement")))
                ((symbol-function 'clutch--replace-row-at-index)
                 (lambda (ridx)
                   (setq replaced ridx)))
                ((symbol-function 'quit-window) #'ignore))
        (cl-letf (((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) (setq insert-buf buf) buf)))
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("severity")
                        clutch--result-column-defs '((:name "severity"))
                        clutch--result-source-table "incidents"))
          (clutch-result-insert--open-buffer
           "incidents" result-buf '(("severity" . "low")) 0)
          (with-current-buffer insert-buf
            (clutch-test--set-insert-field-value "severity" "high")
            (clutch-result-insert-stage))))
      (when (buffer-live-p insert-buf) (kill-buffer insert-buf))
      (should (= replaced 2))
      (should (equal (with-current-buffer result-buf
                       clutch--pending-inserts)
                     '((("severity" . "high"))))))))

(ert-deftest clutch-test-insert-stage-appends-new-pending-insert-locally ()
  "Staging a new insert should append one ghost row without full redraw."
  (clutch-test--with-result-state-buffer result-buf
      (:connection nil
       :pending-inserts nil)
    (let (appended insert-buf)
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (error "new insert should use row append")))
                ((symbol-function 'clutch--append-pending-insert-row)
                 (lambda (iidx)
                   (setq appended iidx)))
                ((symbol-function 'quit-window) #'ignore))
        (cl-letf (((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) (setq insert-buf buf) buf)))
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("name")
                        clutch--result-column-defs '((:name "name"))
                        clutch--result-source-table "users"))
          (clutch-result-insert--open-buffer "users" result-buf)
          (with-current-buffer insert-buf
            (clutch-test--set-insert-field-value "name" "carol")
            (clutch-result-insert-stage))))
      (when (buffer-live-p insert-buf) (kill-buffer insert-buf))
      (should (= appended 0))
      (should (equal (with-current-buffer result-buf
                       clutch--pending-inserts)
                     '((("name" . "carol"))))))))

(ert-deftest clutch-test-insert-stage-rejects-stale-result-table-before-closing ()
  "Insert staging should keep the form open when the parent result table changed."
  (let (closed)
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (clutch-test--with-insert-result-buffer result-buf
          (:columns '("name")
           :column-defs '((:name "name" :type-category text))
           :connection (make-clutch-test-conn :table "users"
                                              :columns '())
           :source-table "users")
        (clutch-result-insert--open-buffer "users" result-buf)
        (with-current-buffer insert-buf
          (clutch-test--set-insert-field-value "name" "alice"))
        (with-current-buffer result-buf
          (setq-local clutch--result-source-table "orders"))
        (with-current-buffer insert-buf
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&rest _) (setq closed t)))
                    ((symbol-function 'clutch--refresh-display) #'ignore))
            (let ((err (should-error (clutch-result-insert-stage)
                                     :type 'user-error)))
              (should (string-match-p "Result table changed"
                                      (error-message-string err))))))
        (should-not closed)
        (should-not (with-current-buffer result-buf
                      clutch--pending-inserts))))))

(ert-deftest clutch-test-apply-edit-errors-clearly-without-row-identity ()
  "Edit staging should explain why update/delete are disabled."
  (clutch-test--with-result-state
      (:connection-params '(:backend mysql)
       :last-query "SELECT * FROM users"
       :source-table "users"
       :rows '((1 "before")))
    (let ((err (should-error (clutch-result--apply-edit
                              0 1 "after"
                              (list :identity [1]
                                    :original "before"
                                    :original-state '(nil . "before")))
                             :type 'user-error)))
      (should (string-match-p
               "no primary, unique, or row locator identity available for table users"
               (error-message-string err))))))

(ert-deftest clutch-test-apply-edit-with-filter-noop-uses-visible-row ()
  "Edit staging should compare against the filtered display row."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :last-query "SELECT id, name FROM users"
       :source-table "users"
       :rows '((1 "alpha") (2 "beta"))
       :filter-pattern "beta"
       :filtered-rows '((2 "beta"))
       :row-identity (clutch-test--primary-row-identity "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
              ((symbol-function 'clutch--refresh-footer-line) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-result--apply-edit
       0 1 "beta" (list :identity [2]
                        :original "beta"
                        :original-state '(nil . "beta")))
      (should-not clutch--pending-edits))))

(ert-deftest clutch-test-delete-rows-errors-clearly-without-row-identity ()
  "Delete staging should explain why update/delete are disabled."
  (clutch-test--with-result-state
      (:connection-params '(:backend mysql)
       :last-query "SELECT * FROM users"
       :source-table "users"
       :rows '((1 "before")))
    (cl-letf (((symbol-function 'clutch--selected-row-indices) (lambda () '(0)))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (let ((err (should-error (clutch-result-delete-rows)
                               :type 'user-error)))
        (should (string-match-p
                 "no primary, unique, or row locator identity available for table users"
                 (error-message-string err)))))))

(ert-deftest clutch-test-discard-pending-at-point-contract ()
  "Discarding at point should remove matching delete, insert, or edit state."
  (let ((identity (clutch-test--primary-row-identity "users" '("id") '(0))))
    (dolist (case
             (list
              (list :label "delete"
                    :row 0
                    :state 'clutch--pending-deletes
                    :spec (list :columns '("id" "name")
                                :rows '((42 "alice"))
                                :row-identity identity
                                :pending-deletes (list (vector 42))))
              (list :label "insert"
                    :row 1
                    :state 'clutch--pending-inserts
                    :spec (list :columns '("id" "name")
                                :rows '((1 "x"))
                                :pending-inserts
                                (list '(("id" . "99") ("name" . "new")))))
              (list :label "edit"
                    :row 0
                    :column 1
                    :state 'clutch--pending-edits
                    :spec (list :columns '("id" "name")
                                :rows '((42 "alice"))
                                :row-identity identity
                                :pending-edits
                                (list (cons (cons (vector 42) 1)
                                            "carol"))))))
      (ert-info ((format "case: %s" (plist-get case :label)))
        (with-temp-buffer
          (clutch-test--init-result-state (plist-get case :spec))
          (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                     (lambda () (plist-get case :row)))
                    ((symbol-function 'clutch--col-idx-at-point)
                     (lambda () (plist-get case :column)))
                    ((symbol-function 'clutch--refresh-display) #'ignore))
            (clutch-result-discard-pending-at-point)
            (should-not (symbol-value (plist-get case :state)))))))))

(ert-deftest clutch-test-check-pending-changes-blocks-when-deletes-pending ()
  "`clutch-result--check-pending-changes' should signal when discard is declined."
  (let ((buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (with-current-buffer buf
          (setq-local clutch--pending-deletes (list (vector 1)))
          (setq-local clutch--pending-edits nil)
          (setq-local clutch--pending-inserts nil)
          (cl-letf (((symbol-function 'get-buffer)
                     (lambda (_name) buf))
                    ((symbol-function 'yes-or-no-p) (lambda (_) nil)))
            (should-error (clutch-result--check-pending-changes)
                          :type 'user-error)))
      (kill-buffer buf))))

(defconst clutch-test--each-kind-sql
  (concat "INSERT INTO t (\"id\", \"name\") VALUES ('3', 'c');\n"
          "UPDATE t SET \"name\" = 'a2' WHERE \"id\" = 1;\n"
          "DELETE FROM t WHERE \"id\" = 2;\n")
  "The SQL that `clutch-test--with-each-kind-staged' stages, as copied.")

(defmacro clutch-test--with-each-kind-staged (bindings &rest body)
  "Run BODY in a SQLite result with one change of each kind staged.
BINDINGS is (CONN).  Rows 1 and 2 of table t are shown; row 3 is
staged for insertion, row 1's name is staged as a2 and row 2 is staged
for deletion."
  (declare (indent 1) (debug ((symbolp) body)))
  `(clutch-test--with-sqlite-result (,(car bindings) _result)
       '("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"
         "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
       "SELECT id, name FROM t ORDER BY id"
     (setq-local clutch--pending-inserts '((("id" . "3") ("name" . "c"))))
     (let ((row (car clutch--result-rows)))
       (clutch-result--apply-edit
        0 1 "a2"
        (list :identity (clutch-db-row-identity-values row clutch--row-identity)
              :original (nth 1 row)
              :original-state (cons nil (nth 1 row)))))
     (goto-char (aref clutch--row-start-positions 1))
     (clutch-result-delete-rows)
     ,@body))

(ert-deftest clutch-test-submit-orders-insert-update-delete ()
  "Submit executes INSERT before UPDATE before DELETE in one atomic batch."
  (clutch-test--with-each-kind-staged (conn)
    (let (atomic executed)
      (cl-letf (((symbol-function 'yes-or-no-p) #'always))
        (advice-add 'clutch-db-call-with-atomic-batch :before
                    (lambda (&rest _) (setq atomic t)) '((name . test-atomic)))
        (advice-add 'clutch--run-db-query :before
                    (lambda (_conn sql &rest _) (push sql executed))
                    '((name . test-order)))
        (unwind-protect
            (clutch-result-submit)
          (advice-remove 'clutch-db-call-with-atomic-batch 'test-atomic)
          (advice-remove 'clutch--run-db-query 'test-order)))
      (should atomic)
      (should (equal (seq-keep (lambda (sql)
                                 (car (member (car (split-string sql))
                                              '("INSERT" "UPDATE" "DELETE"))))
                               (reverse executed))
                     '("INSERT" "UPDATE" "DELETE")))
      (should (equal (clutch-db-result-rows
                      (clutch-db-query conn "SELECT id, name FROM t ORDER BY id"))
                     '((1 "a2") (3 "c"))))
      ;; The result shows the submitted rows, with nothing left staged.
      (should (equal (mapcar (lambda (row) (seq-take row 2)) clutch--result-rows)
                     '((1 "a2") (3 "c"))))
      (should-not (or clutch--pending-inserts clutch--pending-edits
                      clutch--pending-deletes)))))

(ert-deftest clutch-test-submit-validates-before-auto-commit ()
  "Auto submit should roll back when row-count validation fails."
  (clutch-test--with-each-kind-staged (conn)
    (setq-local clutch--pending-inserts nil
                clutch--pending-deletes nil)
    (let ((pending clutch--pending-edits))
      (cl-letf (((symbol-function 'yes-or-no-p) #'always))
        ;; The UPDATE runs, but reports two rows, as an identity that is
        ;; not unique after all would.
        (advice-add 'clutch--run-db-query :filter-return
                    (lambda (result)
                      (setf (clutch-db-result-affected-rows result) 2)
                      result)
                    '((name . test-two-rows)))
        (unwind-protect
            (let ((err (should-error (clutch-result-submit) :type 'user-error)))
              (should (string-match-p "Mutation matched 2 rows"
                                      (error-message-string err))))
          (advice-remove 'clutch--run-db-query 'test-two-rows)))
      (should (equal (clutch-db-result-rows
                      (clutch-db-query conn "SELECT id, name FROM t ORDER BY id"))
                     '((1 "a") (2 "b"))))
      (should (equal (mapcar (lambda (row) (seq-take row 2)) clutch--result-rows)
                     '((1 "a") (2 "b"))))
      (should (equal clutch--pending-edits pending)))))

(ert-deftest clutch-test-submit-refuses-while-a-query-runs ()
  "Submitting staged changes should be refused while a statement runs.
JDBC opens the batch with a synchronous call that waits for the running
statement, so the refusal has to come before the prompt and the batch."
  (clutch-test--with-each-kind-staged (conn)
    (let ((clutch--running-queries (make-hash-table :test 'eq))
          (pending clutch--pending-edits)
          prompted batched)
      (puthash conn (list :buffer (current-buffer)) clutch--running-queries)
      (cl-letf (((symbol-function 'yes-or-no-p) (lambda (_) (setq prompted t))))
        (advice-add 'clutch-db-call-with-atomic-batch :before
                    (lambda (&rest _) (setq batched t)) '((name . test-batch)))
        (unwind-protect
            (should (string-match-p
                     "A query is running"
                     (error-message-string
                      (should-error (clutch-result-submit) :type 'user-error))))
          (advice-remove 'clutch-db-call-with-atomic-batch 'test-batch)))
      (should-not prompted)
      (should-not batched)
      (should (equal (clutch-db-result-rows
                      (clutch-db-query conn "SELECT id, name FROM t ORDER BY id"))
                     '((1 "a") (2 "b"))))
      (should (equal clutch--pending-edits pending)))))

(ert-deftest clutch-test-result-refuses-writes-after-its-source-moved ()
  "A result should refuse staging and submitting once its connection moved.
A staged edit names the table as the query did and ran wherever the
connection was when it was submitted, so after `clutch-switch-schema' it
updated the table of the same name in the other database.  A table the
query qualified is no exception: in DuckDB a schema still resolves in
the current catalog."
  (let ((here '("a" "a"))
        now batched)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
              ((symbol-function 'clutch-db-resolution-context)
               (lambda (_conn) now))
              ((symbol-function 'clutch-db-sql-surface-p) #'always)
              ((symbol-function 'clutch-result--build-update-statements)
               (lambda ()
                 '(("UPDATE t SET v = ? WHERE id = ?" . ("x" 1)))))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn value) (format "'%s'" value)))
              ((symbol-function 'yes-or-no-p) #'always)
              ((symbol-function 'clutch-db-call-with-atomic-batch)
               (lambda (&rest _) (setq batched t)))
              ((symbol-function 'clutch-result-rerun) #'ignore))
      (clutch-test--with-result-state
          (:pending-edits '(edit) :source-table "t")
        (setq-local clutch--result-resolution-context here)
        (ert-info ("the connection moved: staging and submitting are refused")
          (setq now '("b" "b"))
          (should (string-match-p
                   "run the query again"
                   (error-message-string
                    (should-error (clutch-result-submit) :type 'user-error))))
          (should-not batched)
          (should-error (clutch-edit--require-sql-staged-mutation "Stage delete")
                        :type 'user-error)
          (setq-local clutch--result-source-schema "a")
          (should-error (clutch-edit--require-sql-staged-mutation "Stage delete")
                        :type 'user-error)
          (setq-local clutch--result-source-schema nil))
        (ert-info ("a context Clutch could not read is refused")
          (setq now here)
          (setq-local clutch--result-resolution-context 'unknown)
          (should-error (clutch-edit--require-sql-staged-mutation "Stage delete")
                        :type 'user-error))
        (ert-info ("a failure to read the context is reported as the server gave it")
          (setq-local clutch--result-resolution-context here)
          (cl-letf (((symbol-function 'clutch-db-resolution-context)
                     (lambda (_conn)
                       (signal 'clutch-db-error
                               '("current transaction is aborted")))))
            (should (string-match-p
                     "current transaction is aborted"
                     (error-message-string
                      (should-error
                       (clutch-edit--require-sql-staged-mutation "Stage delete")
                       :type 'user-error))))))
        (ert-info ("a connection that is not live is left to fail on its own")
          (cl-letf (((symbol-function 'clutch--connection-alive-p) #'ignore))
            (clutch-edit--require-sql-staged-mutation "Stage delete")))
        (ert-info ("the same context allows both")
          (setq-local clutch--result-resolution-context here)
          (clutch-edit--require-sql-staged-mutation "Stage delete")
          (clutch-result-submit)
          (should batched))))))

(ert-deftest clutch-test-result-refuses-to-load-more-after-its-source-moved ()
  "A result should not load pages, a count or an export once its source moved.
They run the result's query again, wherever the connection is, and would
mix another table's rows into the result."
  (let ((here '("app" "{pg_catalog,s1,s2}"))
        now ran)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
              ((symbol-function 'clutch-db-resolution-context)
               (lambda (_conn) now))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql &rest _) sql))
              ((symbol-function 'clutch-db-build-count-sql)
               (lambda (_conn sql) (concat "SELECT COUNT(*) FROM (" sql ") c")))
              ((symbol-function 'clutch-result--run-query)
               (lambda (&rest _) (setq ran t))))
      (clutch-test--with-result-state
          (:source-table "t" :base-query "SELECT id, v FROM t"
           :last-query "SELECT id, v FROM t"
           :server-pageable t :server-rewritable t)
        (setq-local clutch--result-resolution-context here)
        (setq now '("app" "{pg_catalog,s1,s3}"))
        (should-error (clutch-result--execute-page 1) :type 'user-error)
        (should-error (clutch-result-count-total) :type 'user-error)
        (should-error (clutch-result--map-export-batches #'ignore #'ignore)
                      :type 'user-error)
        (should-not ran)
        (setq now here)
        (clutch-result--execute-page 1)
        (should ran)))))

(ert-deftest clutch-test-result-records-the-context-its-query-ran-in ()
  "A result should record the context its query ran in, once it has run.
A context that cannot be read is recorded as `unknown'."
  (let ((clutch--source-window (selected-window))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
        (result-name "*clutch-test-result*")
        ran context-error)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql &rest _) sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (setq ran t)
                 (make-clutch-db-result :columns '((:name "id")) :rows '((1)))))
              ((symbol-function 'clutch-db-resolution-context)
               (lambda (_conn)
                 (when context-error
                   (signal 'clutch-db-error '("connection lost")))
                 (if ran 'after 'before))))
      (clutch-test--with-result-buffer (result-name)
        (clutch-test--execute-and-present "SELECT id FROM users" 'fake-conn)
        (with-current-buffer result-name
          (should (eq clutch--result-resolution-context 'after)))
        (setq context-error t)
        (clutch-test--execute-and-present "SELECT id FROM users" 'fake-conn)
        (with-current-buffer result-name
          (should (eq clutch--result-resolution-context 'unknown)))))))

(ert-deftest clutch-test-submit-manual-batch-uses-atomic-backend-boundary ()
  "Manual staged submit should be atomic without committing the user transaction."
  (let ((clutch--tx-state-cache (make-hash-table :test 'eq)))
    (clutch-test--with-result-state
        (:pending-inserts '(first second))
      (let (atomic executed notice)
      (setq-local revert-buffer-function
                  (lambda (&rest _args) nil))
      (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
                ((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT first") ("INSERT second"))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'yes-or-no-p) (lambda (_) t))
                ((symbol-function 'clutch-db-manual-commit-p) (lambda (_) t))
                ((symbol-function 'clutch--tx-dirty-p) (lambda (_) t))
                ((symbol-function 'clutch-db-call-with-atomic-batch)
                 (lambda (_conn function)
                   (setq atomic t)
                   (funcall function)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql &optional _params _defer-transaction-state)
                   (push sql executed)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (setq notice (apply #'format format-string args)))))
        (clutch-result-submit)
        (should (equal (nreverse executed) '("INSERT first" "INSERT second")))
        (should atomic)
        (should (equal notice "2 changes submitted"))
        (should-not clutch--pending-inserts))))))

(ert-deftest clutch-test-submit-handles-known-session-restore-outcomes ()
  "Pending state must follow the known outcome when restoring Auto mode fails."
  (dolist (outcome '(committed rolled-back))
    (clutch-test--with-result-state
        (:pending-inserts '(first second))
      (let (refreshed reverted)
        (setq-local revert-buffer-function
                    (lambda (&rest _args) (setq reverted t)))
        (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
                  ((symbol-function 'clutch-result--build-pending-insert-statements)
                   (lambda () '(("INSERT first") ("INSERT second"))))
                  ((symbol-function 'clutch-db-escape-literal)
                   (lambda (_conn value) (format "'%s'" value)))
                  ((symbol-function 'yes-or-no-p) (lambda (_) t))
                  ((symbol-function 'clutch--refresh-transaction-ui)
                   (lambda (_) (setq refreshed t)))
                  ((symbol-function 'clutch-db-call-with-atomic-batch)
                   (lambda (&rest _)
                     (signal
                      'clutch-db-session-restore-error
                      (list "Restoring auto-commit failed"
                            :outcome outcome)))))
          (should-error (clutch-result-submit) :type 'user-error)
          (should refreshed)
          (if (eq outcome 'committed)
              (should-not clutch--pending-inserts)
            (should (equal clutch--pending-inserts '(first second))))
          (should (eq (not (null reverted))
                      (eq outcome 'committed))))))))

(ert-deftest clutch-test-submit-marks-unknown-batch-outcome-uncertain ()
  "An uncertain batch should retain staging and require transaction recovery."
  (let ((clutch--tx-state-cache (make-hash-table :test 'eq)))
    (clutch-test--with-result-state
        (:pending-inserts '(first second))
      (let (reverted)
        (setq-local revert-buffer-function
                    (lambda (&rest _args) (setq reverted t)))
        (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
                  ((symbol-function 'clutch-result--build-pending-insert-statements)
                   (lambda () '(("INSERT first") ("INSERT second"))))
                  ((symbol-function 'clutch-db-escape-literal)
                   (lambda (_conn value) (format "'%s'" value)))
                  ((symbol-function 'yes-or-no-p) (lambda (_) t))
                  ((symbol-function 'clutch-db-manual-commit-supported-p)
                   (lambda (_) t))
                  ((symbol-function 'clutch-db-manual-commit-p) (lambda (_) t))
                  ((symbol-function 'clutch-db-call-with-atomic-batch)
                   (lambda (_conn function)
                     (funcall function)
                     (signal
                      'clutch-db-batch-outcome-uncertain
                      '("commit outcome is uncertain"
                        :phase commit))))
                  ((symbol-function 'clutch--run-db-query)
                   (lambda (&rest _)
                     (make-clutch-db-result :affected-rows 1)))
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
          (let ((err (should-error (clutch-result-submit) :type 'user-error)))
            (should (string-match-p
                     "roll back or reconnect"
                     (error-message-string err))))
          (should (clutch--tx-uncertain-p clutch-connection))
          (cl-letf (((symbol-function 'clutch--connection-alive-p) #'ignore))
            (should (string-match-p
                     "Transaction state is uncertain"
                     (error-message-string
                      (should-error (clutch-result-submit) :type 'user-error)))))
          (should (equal clutch--pending-inserts '(first second)))
          (should-not reverted))))))

(ert-deftest clutch-test-rollback-to-a-savepoint-keeps-the-transaction-dirty ()
  "Only a rollback of the whole transaction should clear its uncommitted work.
A rollback to a savepoint keeps the work done before the savepoint, which a
disconnect then lost without asking.  SQL Server's ROLLBACK TRANSACTION with
a name may name a savepoint, so it keeps the work too, also when the name is
a word that ends a whole rollback, such as CHAIN."
  (pcase-dolist (`(,sql ,state)
                 '(("ROLLBACK TO SAVEPOINT s" dirty)
                   ("rollback to s;" dirty)
                   ("ROLLBACK WORK TO SAVEPOINT s" dirty)
                   ("ROLLBACK TRANSACTION TO SAVEPOINT s" dirty)
                   ("ROLLBACK TRAN s" dirty)
                   ("ROLLBACK TRANSACTION chain" dirty)
                   ("ROLLBACK TRAN release" dirty)
                   ("ROLLBACK TRAN no" dirty)
                   ("ROLLBACK" nil)
                   ("rollback work;" nil)
                   ("ROLLBACK TRANSACTION" nil)
                   ("ROLLBACK TRANSACTION AND CHAIN" nil)
                   ("ROLLBACK AND NO CHAIN" nil)
                   ("ROLLBACK WORK AND CHAIN NO RELEASE" nil)
                   ("-- done\nROLLBACK" nil)
                   ("COMMIT" nil)))
    (ert-info (sql)
      (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
            (clutch--running-queries (make-hash-table :test 'eq)))
        (cl-letf (((symbol-function 'clutch-db-manual-commit-p) (lambda (_) t))
                  ((symbol-function 'clutch-db-query)
                   (lambda (&rest _) (make-clutch-db-result :affected-rows 1)))
                  ((symbol-function 'clutch--clear-connection-problem-capture)
                   #'ignore))
          (dolist (statement (list "UPDATE t SET n = 1" "SAVEPOINT s"
                                   "UPDATE t SET n = 2" sql))
            (clutch--run-db-query 'tx-conn statement))
          (should (eq (clutch--tx-state 'tx-conn) state)))))))

(ert-deftest clutch-test-copy-and-save-pending-sql-write-the-batch ()
  "Copying and saving staged SQL should produce the batch submit runs."
  (let ((path (make-temp-file "clutch-pending-" nil ".sql")))
    (unwind-protect
        (clutch-test--with-each-kind-staged (_conn)
          (let (kill-ring kill-ring-yank-pointer)
            (clutch-result-copy-pending-sql)
            (should (equal (current-kill 0) clutch-test--each-kind-sql)))
          (cl-letf (((symbol-function 'read-file-name)
                     (lambda (&rest _args) path)))
            (clutch-result-save-pending-sql))
          (should (equal (with-temp-buffer
                           (insert-file-contents path)
                           (buffer-string))
                         clutch-test--each-kind-sql)))
      (delete-file path))))

;;;; Edit — validation

(ert-deftest clutch-test-insert-local-validation-updates-inline-error ()
  "Insert field live validation should show and clear inline errors."
  (clutch-test--with-insert-result-buffer result-buf
      (:columns '("impact_score")
       :column-defs '((:name "impact_score" :type-category numeric))
       :connection (make-clutch-test-conn :table "shipping_incidents"
                                          :columns '((:name "impact_score" :type "decimal(5,1)"))))
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
      (with-current-buffer insert-buf
        (clutch-test--goto-insert-field-value "impact_score")
        (insert "x")
        (clutch-result-insert--run-idle-validation insert-buf)
        (let* ((field (clutch-result-insert--field-state "impact_score"))
               (after (overlay-get (plist-get field :error-overlay)
                                   'after-string)))
          (should (equal (plist-get field :error-message)
                         "Field impact_score expects a numeric value"))
          (should (overlayp (plist-get field :error-overlay)))
          (should (string-match-p "\\[invalid numeric\\]" after))
          (should-not (string-prefix-p "\n" after))
          (clutch-test--goto-insert-field-value "impact_score")
          (delete-region (point) (line-end-position))
          (insert "1.5")
          (clutch-result-insert--run-idle-validation insert-buf)
          (setq field (clutch-result-insert--field-state "impact_score"))
          (should-not (plist-get field :error-message))
          (should-not (plist-get field :error-overlay)))))))

(ert-deftest clutch-test-insert-idle-validation-covers-all-changed-fields ()
  "Idle validation should validate every field changed since the last run."
  (clutch-test--with-insert-result-buffer result-buf
      (:columns '("impact_score" "severity")
       :column-defs '((:name "impact_score" :type-category numeric)
                      (:name "severity" :type-category numeric))
       :connection (make-clutch-test-conn :table "shipping_incidents"
                                          :columns '((:name "impact_score" :type "decimal(5,1)")
                                                     (:name "severity" :type "int"))))
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
      (with-current-buffer insert-buf
        (clutch-test--goto-insert-field-value "impact_score")
        (insert "x")
        (clutch-test--goto-insert-field-value "severity")
        (insert "1")
        (let ((timer (buffer-local-value 'clutch-result-insert--validation-timer
                                         insert-buf)))
          (apply (timer--function timer) (timer--args timer)))
        (should (equal (plist-get (clutch-result-insert--field-state "impact_score")
                                  :error-message)
                       "Field impact_score expects a numeric value"))
        (should-not (plist-get (clutch-result-insert--field-state "severity")
                               :error-message))
        (clutch-test--goto-insert-field-value "impact_score")
        (delete-region (point) (line-end-position))
        (insert "1.5")
        (clutch-test--goto-insert-field-value "severity" t)
        (insert "2")
        (let ((timer (buffer-local-value 'clutch-result-insert--validation-timer
                                         insert-buf)))
          (apply (timer--function timer) (timer--args timer)))
        (let ((field (clutch-result-insert--field-state "impact_score")))
          (should-not (plist-get field :error-message))
          (should-not (plist-get field :error-overlay)))))))

(ert-deftest clutch-test-json-validation-is-scheduled-on-idle ()
  "JSON insert and edit buffers should defer local validation until idle."
  (dolist (case '(insert edit))
    (ert-info ((format "case: %s" case))
      (let (scheduled)
        (cl-letf (((symbol-function 'run-with-idle-timer)
                   (lambda (secs _repeat fn &rest args)
                     (setq scheduled (list secs fn args))
                     'fake-timer)))
          (pcase case
            ('insert
             (clutch-test--with-insert-result-buffer result-buf
                 (:columns '("postmortem")
                  :column-defs '((:name "postmortem" :type-category json))
                  :connection 'fake-conn)
               (clutch-test--with-pop-to-buffer-capture insert-buf
                 (cl-letf (((symbol-function 'clutch--ensure-column-details)
                            (lambda (_conn _table)
                              (list (list :name "postmortem" :type "json")))))
                   (clutch-result-insert--open-buffer
                    "shipping_incidents" result-buf)
                   (with-current-buffer insert-buf
                     (clutch-test--goto-insert-field-value "postmortem")
                     (insert "{")
                     (should scheduled)
                     (should (= (car scheduled)
                                clutch-insert-validation-idle-delay))
                     (should (eq (cadr scheduled)
                                 #'clutch-result-insert--run-idle-validation))
                     (should (equal (caddr scheduled)
                                    (list (current-buffer)))))))))
            ('edit
             (with-temp-buffer
               (clutch--result-edit-mode 1)
               (setq-local clutch-result-edit--column-name "payload"
                           clutch-result-edit--column-def
                           '(:name "payload" :type-category json)
                           clutch-result-edit--column-detail
                           '(:name "payload" :type "json"))
               (clutch-result-edit--schedule-validation)
               (should scheduled)
               (should (= (car scheduled) clutch-insert-validation-idle-delay))
               (should (eq (cadr scheduled)
                           #'clutch-result-edit--run-idle-validation))
               (should (equal (caddr scheduled)
                              (list (current-buffer))))))))))))

(ert-deftest clutch-test-edit-live-validation-updates-header ()
  "Edit buffers should update the compact live-validation token."
  (with-temp-buffer
    (insert "xx")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--column-name "impact_score"
                clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)"))
    (clutch-result-edit--refresh-header-line)
    (clutch-result-edit--validate-live)
    (should (equal clutch-result-edit--error-message
                   "Field impact_score expects a numeric value"))
    (should (string-match-p "\\[invalid numeric\\]"
                            (format "%s" header-line-format)))
    (erase-buffer)
    (insert "1.5")
    (clutch-result-edit--validate-live)
    (should-not clutch-result-edit--error-message)
    (should-not (string-match-p "\\[invalid numeric\\]"
                                (format "%s" header-line-format)))))

(ert-deftest clutch-test-edit-finish-validates-before-stage ()
  "Edit staging should reject invalid values before calling the edit callback."
  (dolist (case '((numeric "xx" "impact_score"
                   (:name "impact_score" :type-category numeric)
                   (:name "impact_score" :type "decimal(5,1)")
                   "Field impact_score expects a numeric value")
                  (enum "urgent" "severity"
                   (:name "severity" :type-category text)
                   (:name "severity" :type "enum('low','medium','high')")
                   "Field severity must be one of: low, medium, high")
                  (json "{oops}" "payload"
                   (:name "payload" :type-category json)
                   (:name "payload" :type "json")
                   "Field payload expects valid JSON")))
    (pcase-let ((`(,label ,value ,column-name ,column-def
                   ,column-detail ,message) case))
      (ert-info ((format "case: %s" label))
        (let (staged-value quit-called err)
          (clutch-test--with-result-edit-buffer edit-buf value
            (setq-local clutch-result-edit--column-name column-name
                        clutch-result-edit--column-def column-def
                        clutch-result-edit--column-detail column-detail
                        clutch-result--edit-callback
                        (lambda (staged) (setq staged-value staged)))
            (cl-letf (((symbol-function 'quit-window)
                       (lambda (&rest _args) (setq quit-called t))))
              (setq err (should-error (clutch-result-edit-finish)
                                      :type 'user-error))
              (should (string-match-p (regexp-quote message)
                                      (error-message-string err)))
              (should-not quit-called)
              (should-not staged-value)
              (should (buffer-live-p edit-buf)))))))))

(ert-deftest clutch-test-edit-set-null-stages-nil ()
  "The explicit NULL command should stage a nil value."
  (let ((staged-value :not-called)
        quit-called)
    (clutch-test--with-result-edit-buffer _edit-buf "12.5"
      (setq-local clutch-result-edit--column-name "impact_score"
                  clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                  clutch-result-edit--column-detail
                  '(:name "impact_score" :type "decimal(5,1)" :nullable t)
                  clutch-result--edit-callback
                  (lambda (value) (setq staged-value (list value))))
      (cl-letf (((symbol-function 'quit-window)
                 (lambda (&rest _args) (setq quit-called t))))
        (clutch-result-edit-set-null)
        (clutch-result-edit-finish)
        (should quit-called)
        (should (equal staged-value '(nil)))))))

(ert-deftest clutch-test-edit-set-default-stages-explicit-sentinel ()
  "The explicit DEFAULT command should stage the database-default sentinel."
  (let ((staged-value :not-called)
        quit-called)
    (clutch-test--with-result-edit-buffer _edit-buf "manual"
      (setq-local clutch-result-edit--column-name "status"
                  clutch-result-edit--column-def
                  '(:name "status" :type-category text)
                  clutch-result-edit--column-detail
                  '(:name "status" :type "text" :nullable t :default "'new'")
                  clutch-result-edit--default-supported-p t
                  clutch-result--edit-callback
                  (lambda (value) (setq staged-value value)))
      (cl-letf (((symbol-function 'quit-window)
                 (lambda (&rest _args) (setq quit-called t))))
        (should (eq (lookup-key clutch--result-edit-mode-map (kbd "C-c C-d"))
                    #'clutch-result-edit-set-default))
        (clutch-result-edit-set-default)
        (should (eq clutch-result-edit--special-value 'default))
        (should (equal (buffer-string) ""))
        (should (equal
                 (substring-no-properties
                  (overlay-get clutch-result-edit--special-placeholder-overlay
                               'after-string))
                 "<default>"))
        (should (eq (get-text-property
                     0 'face
                     (overlay-get clutch-result-edit--special-placeholder-overlay
                                  'after-string))
                    'clutch-null-face))
        (clutch-result-edit-finish)
        (should quit-called)
        (should (eq staged-value clutch--cell-default-placeholder))))))

(ert-deftest clutch-test-edit-finish-preserves-literal-special-strings ()
  "Typing NULL or DEFAULT should stage literal text, not a special value."
  (dolist (text '("NULL" "DEFAULT"))
    (let (staged-value)
      (clutch-test--with-result-edit-buffer _edit-buf text
        (setq-local clutch-result-edit--column-name "note"
                    clutch-result-edit--column-def
                    '(:name "note" :type-category text)
                    clutch-result-edit--column-detail
                    '(:name "note" :type "text" :nullable t :default "'x'")
                    clutch-result-edit--default-supported-p t
                    clutch-result--edit-callback
                    (lambda (value) (setq staged-value value)))
        (cl-letf (((symbol-function 'quit-window) #'ignore))
          (clutch-result-edit-finish)
          (should (equal staged-value text)))))))

(ert-deftest clutch-test-edit-finish-restores-result-cell-position ()
  "Finishing a cell edit should restore point without shifting the viewport."
  (save-window-excursion
    (let ((result-buf (generate-new-buffer "*clutch-result-test*"))
          edit-buf)
      (unwind-protect
          (progn
            (switch-to-buffer result-buf)
            (clutch-test--init-result-state
             (list :columns '("id" "name" "city" "note" "flag")
                   :rows '((1 "alpha" "oslo" "before" "x")
                           (2 "bravo" "rome" "target" "y"))
                   :page-total-rows 2
                   :column-widths [3 18 18 18 18]
                   :render t
                   :row-identity
                   (clutch-test--primary-row-identity "users" '("id") '(0))))
            (setq-local clutch-connection (make-clutch-test-conn :table "users"
                                                                 :columns '((:name "id")
                                                                            (:name "name")
                                                                            (:name "city")
                                                                            (:name "note")
                                                                            (:name "flag")))
                        clutch--connection-params '(:backend mysql)
                        clutch--result-source-table "users")
            (clutch--goto-cell 1 4)
            (set-window-hscroll (selected-window) 40)
            (cl-letf (((symbol-function 'window-body-width)
                       (lambda (&rest _) 40)))
              (clutch-result-edit-cell))
            (setq edit-buf (current-buffer))
            (erase-buffer)
            (insert "z")
            (cl-letf (((symbol-function 'quit-window)
                       (lambda (&rest _args)
                         (switch-to-buffer result-buf)
                         (set-window-hscroll (selected-window) 0)))
                      ((symbol-function 'window-body-width)
                       (lambda (&rest _) 40)))
              (clutch-result-edit-finish))
            (should (eq (current-buffer) result-buf))
            (should (= (get-text-property (point) 'clutch-row-idx) 1))
            (should (= (get-text-property (point) 'clutch-col-idx) 4))
            (should (= (window-hscroll) 40)))
        (when (buffer-live-p result-buf)
          (kill-buffer result-buf))
        (when (buffer-live-p edit-buf)
          (kill-buffer edit-buf))))))

(ert-deftest clutch-test-edit-finish-errors-when-result-buffer-is-dead ()
  "Finishing an edit should fail cleanly when the parent result buffer is gone."
  (let ((result-buf (generate-new-buffer "*clutch-result-test*"))
        edit-buf)
    (unwind-protect
        (with-current-buffer result-buf
          (clutch-result-mode)
          (setq-local clutch-connection (make-clutch-test-conn :table "users"
                                                               :columns '((:name "name")))
                      clutch--result-columns '("name")
                      clutch--connection-params '(:backend mysql)
                      clutch--result-column-defs
                      '((:name "name" :type-category text
                         :source-column "name"))
                      clutch--result-rows '(("before"))
                      clutch--last-query "SELECT * FROM users"
                      clutch--result-source-table "users"
                      clutch--row-identity (clutch-test--primary-row-identity
                                            "users" '("name") '(0)))
          (let ((inhibit-read-only t))
            (insert "before")
            (add-text-properties (point-min) (point-max)
                                 '(clutch-row-idx 0 clutch-col-idx 0
                                                  clutch-full-value "before")))
          (goto-char (point-min))
          (cl-letf (((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq edit-buf buf)
                       buf)))
            (clutch-result-edit-cell))
          (kill-buffer result-buf)
          (with-current-buffer edit-buf
            (erase-buffer)
            (insert "after")
            (cl-letf (((symbol-function 'quit-window) #'ignore))
              (should-error (clutch-result-edit-finish) :type 'user-error))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (when (buffer-live-p edit-buf)
        (kill-buffer edit-buf)))))

(ert-deftest clutch-test-insert-stage-validates-fields-before-stage ()
  "Insert staging should reject invalid enum, bool, JSON, temporal, and numeric values."
  (dolist (case
           `((("severity" "is_ship_blocked" "postmortem")
              ((:name "severity" :type-category text)
               (:name "is_ship_blocked" :type-category numeric)
               (:name "postmortem" :type-category json))
              (("severity" . "nope")
               ("is_ship_blocked" . "7")
               ("postmortem" . "not-json"))
              ((:name "severity" :type "enum('low','medium')")
               (:name "is_ship_blocked" :type "tinyint(1)")
               (:name "postmortem" :type "json"))
              nil)
             (("opened_at" "due_on" "starts_at")
              ((:name "opened_at" :type-category datetime)
               (:name "due_on" :type-category date)
               (:name "starts_at" :type-category time))
              (("opened_at" . "ss")
               ("due_on" . "2026-02-30")
               ("starts_at" . "25:61"))
              ((:name "opened_at" :type "datetime")
               (:name "due_on" :type "date")
               (:name "starts_at" :type "time"))
              "Field opened_at expects YYYY-MM-DD HH:MM\\[:SS\\]")
             (("impact_score")
              ((:name "impact_score" :type-category numeric))
              (("impact_score" . "xx"))
              ((:name "impact_score" :type "decimal(5,1)"))
              "Field impact_score expects a numeric value")))
    (pcase-let ((`(,columns ,column-defs ,fields ,details ,expected-message) case))
      (clutch-test--with-insert-result-buffer result-buf
          (:columns columns
           :column-defs column-defs
           :connection (make-clutch-test-conn :table "shipping_incidents"
                                              :columns details)
           :source-table "shipping_incidents"
           :pending-inserts nil)
        (clutch-test--with-pop-to-buffer-capture insert-buf
          (clutch-result-insert--open-buffer
           "shipping_incidents" result-buf fields)
          (with-current-buffer insert-buf
            (let ((err (should-error (clutch-result-insert-stage)
                                     :type 'user-error)))
              (when expected-message
                (should (string-match-p expected-message
                                        (error-message-string err)))))))
        (should (buffer-live-p result-buf))
        (should-not (with-current-buffer result-buf clutch--pending-inserts))))))

;;;; Edit — JSON sub-editor

(ert-deftest clutch-test-json-editor-mode-uses-js-mode-without-json-ts-grammar ()
  "JSON editors should use `js-mode' when the tree-sitter grammar is unavailable."
  (let (selected-mode)
    (with-temp-buffer
      (cl-letf (((symbol-function 'json-ts-mode)
                 (lambda () (ert-fail "json-ts-mode should not run without a JSON grammar")))
                ((symbol-function 'treesit-language-available-p)
                 (lambda (_language &optional _quiet) nil))
                ((symbol-function 'js-mode)
                 (lambda () (setq selected-mode 'js-mode))))
        (clutch-result-insert--json-editor-mode)))
    (should (eq selected-mode 'js-mode))))

(ert-deftest clutch-test-json-sub-editor-preserves-serialization-errors ()
  "JSON save commands distinguish invalid input from serialization failure."
  (dolist (command '(clutch-result-edit-json-finish
                     clutch-result-insert-json-finish))
    (ert-info ((symbol-name command))
      (with-temp-buffer
        (setq-local clutch-result-edit-json--field-name "payload"
                    clutch-result-insert-json--field-name "payload")
        (insert "{")
        (should (equal
                 (should-error (call-interactively command) :type 'user-error)
                 '(user-error "Field payload expects valid JSON")))
        (erase-buffer)
        (insert "{\"ok\":true}")
        (cl-letf (((symbol-function 'json-serialize)
                   (lambda (&rest _) (error "Serializer failed"))))
          (should (equal
                   (should-error (call-interactively command)
                                 :type 'clutch-db-error)
                   '(clutch-db-error
                     "Cannot serialize value as JSON: Serializer failed"))))
        (should (equal (buffer-string) "{\"ok\":true}"))))))

(ert-deftest clutch-test-json-sub-editor-editing-contract ()
  "JSON child editors should validate parents, save JSON, and cancel cleanly."
  (dolist (case
           '((save
              ""
              "{\n  \"severity\": \"high\",\n  \"ship_blocked\": true\n}"
              clutch-result-insert-json-finish
              "{\"severity\":\"high\",\"ship_blocked\":true}")
             (cancel
              "{\"ok\":true}"
              "{\"ok\":false}"
              clutch-result-insert-json-cancel
              "{\"ok\":true}")))
    (pcase-let ((`(,label ,parent-value ,editor-text ,command ,expected) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-pop-to-buffer-capture editor-buf
          (clutch-test--with-insert-result-buffer result-buf
              (:columns '("postmortem")
               :column-defs '((:name "postmortem" :type-category json))
               :connection 'fake-conn)
            (let (insert-buf)
              (cl-letf (((symbol-function 'clutch--ensure-column-details)
                         (lambda (_conn _table)
                           (list (list :name "postmortem" :type "json")))))
                (clutch-result-insert--open-buffer
                 "shipping_incidents" result-buf
                 `(("postmortem" . ,parent-value)))
                (setq insert-buf editor-buf)
                (with-current-buffer insert-buf
                  (clutch-test--goto-insert-field-value "postmortem")
                  (clutch-result-insert-edit-json-field)))
              (with-current-buffer editor-buf
                (erase-buffer)
                (insert editor-text)
                (cl-letf (((symbol-function 'clutch--ensure-column-details)
                           (lambda (_conn _table)
                             (list (list :name "postmortem" :type "json"))))
                          ((symbol-function 'quit-window)
                           (lambda (&rest _args) nil))
                          ((symbol-function 'pop-to-buffer)
                           (lambda (buf &rest _args) buf)))
                  (funcall command)))
              (with-current-buffer insert-buf
                (should (equal
                         (plist-get
                          (clutch-result-insert--field-state "postmortem")
                          :value)
                         expected)))
              (when (buffer-live-p insert-buf)
                (kill-buffer insert-buf))))))))
  (ert-info ("insert editor rejects invalid parent field text")
    (clutch-test--with-pop-to-buffer-capture editor-buf
      (clutch-test--with-insert-result-buffer result-buf
          (:columns '("postmortem")
           :column-defs '((:name "postmortem" :type-category json))
           :connection 'fake-conn)
        (let (insert-buf)
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "postmortem" :type "json")))))
            (clutch-result-insert--open-buffer
             "shipping_incidents" result-buf '(("postmortem" . "hello")))
            (setq insert-buf editor-buf)
            (setq editor-buf nil)
            (with-current-buffer insert-buf
              (clutch-test--goto-insert-field-value "postmortem")
              (let ((err (should-error (clutch-result-insert-edit-json-field)
                                       :type 'user-error)))
                (should (string-match-p "Field postmortem expects valid JSON"
                                        (error-message-string err))))))
          (should-not editor-buf)
          (with-current-buffer insert-buf
            (should (equal
                     (plist-get (clutch-result-insert--field-state "postmortem")
                                :value)
                     "hello")))
          (when (buffer-live-p insert-buf)
            (kill-buffer insert-buf))))))
  (ert-info ("edit editor saves normalized JSON")
    (clutch-test--with-pop-to-buffer-capture json-buf
      (clutch-test--with-result-edit-buffer parent-buf "{\"a\":1}"
        (setq-local clutch-result-edit--column-name "payload"
                    clutch-result-edit--column-def
                    '(:name "payload" :type-category json)
                    clutch-result-edit--column-detail
                    '(:name "payload" :type "json"))
        (clutch-result-edit-json-field)
        (with-current-buffer json-buf
          (erase-buffer)
          (insert "{\"a\":2}")
          (clutch-result-edit-json-finish))
        (should (equal (with-current-buffer parent-buf (buffer-string))
                       "{\"a\":2}")))))
  (ert-info ("manual edit editor cancel returns to parent edit buffer")
    (let (json-buf popped-buf)
      (clutch-test--with-result-edit-buffer parent-buf "{\"a\":1}"
        (setq-local clutch-result-edit--column-name "payload"
                    clutch-result-edit--column-def
                    '(:name "payload" :type-category json)
                    clutch-result-edit--column-detail
                    '(:name "payload" :type "json"))
        (cl-letf (((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args)
                     (setq json-buf buf)
                     buf)))
          (clutch-result-edit-json-field))
        (should (buffer-live-p json-buf))
        (with-current-buffer json-buf
          (should-not clutch-result-edit-json--whole-edit-p))
        (cl-letf (((symbol-function 'quit-window)
                   (lambda (&optional kill _window)
                     (when kill
                       (kill-buffer (current-buffer)))))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args)
                     (setq popped-buf buf)
                     buf)))
          (with-current-buffer json-buf
            (clutch-result-edit-json-cancel)))
        (should-not (buffer-live-p json-buf))
        (should (buffer-live-p parent-buf))
        (should (eq popped-buf parent-buf))
        (should (equal (with-current-buffer parent-buf (buffer-string))
                       "{\"a\":1}")))))
  (ert-info ("edit editor rejects invalid parent text")
    (clutch-test--with-result-edit-buffer _parent-buf "hello"
      (setq-local clutch-result-edit--column-name "payload"
                  clutch-result-edit--column-def
                  '(:name "payload" :type-category json)
                  clutch-result-edit--column-detail
                  '(:name "payload" :type "json"))
      (cl-letf (((symbol-function 'pop-to-buffer)
                 (lambda (&rest _args)
                   (ert-fail "JSON editor should not open"))))
        (let ((err (should-error (clutch-result-edit-json-field)
                                 :type 'user-error)))
          (should (string-match-p "Field payload expects valid JSON"
                                  (error-message-string err))))))))

;;;; Edit — qualified sources, clones and previews

(ert-deftest clutch-test-clone-preserves-qualified-relation-and-values ()
  "Clone/import/stage/execute preserves the source schema and submitted values."
  (require 'clutch-db-sqlite)
  (dolist (value '("NULL" "null" "" nil))
    (dolist (import '(nil "payload\nreplacement\n"))
      (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
            result-buf insert-buf)
        (unwind-protect
            (progn
              (dolist (sql '("ATTACH DATABASE ':memory:' AS other"
                             "CREATE TABLE main.audit (id INTEGER PRIMARY KEY, payload TEXT DEFAULT 'fallback')"
                             "CREATE TABLE other.audit (id INTEGER PRIMARY KEY, payload TEXT DEFAULT 'fallback')"))
                (clutch-db-query conn sql))
              (clutch-db-execute-params
               conn "INSERT INTO other.audit (payload) VALUES (?)" (list value))
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn)
                (let ((source (current-buffer)))
                  (clutch-test--execute-and-present "SELECT * FROM other.audit" conn)
                  (setq result-buf
                        (buffer-local-value 'clutch--last-result-buffer source)))
                (with-current-buffer result-buf
                  (clutch--goto-cell 0 0)
                  (clutch-clone-row-to-insert))
                (setq insert-buf (get-buffer "*clutch-insert: audit*"))
                (with-current-buffer insert-buf
                  (when import (clutch-result-insert-import-delimited import))
                  (clutch-result-insert-stage))
                (with-current-buffer result-buf
                  (clutch-result--submit-mutation-batch
                   (clutch-result--build-pending-insert-statements) nil nil)
                  (setq clutch--pending-inserts nil)))
              (should (equal (clutch-db-result-rows
                              (clutch-db-query conn "SELECT payload FROM other.audit ORDER BY id"))
                             (list (list value) (list (if import "replacement" value)))))
              (should-not (clutch-db-result-rows
                           (clutch-db-query conn "SELECT * FROM main.audit"))))
          (dolist (buf (list insert-buf result-buf))
            (when (buffer-live-p buf) (kill-buffer buf)))
          (clutch-db-disconnect conn))))))

(ert-deftest clutch-test-qualified-sqlite-result-changes-its-own-table ()
  "Every change staged on a result of aux.people should go to aux.people.
Its key and columns come from aux.people, not from main.people, which
SQLite finds first for the bare name, has another key and lacks a column.
A server-side filter, which wraps the query, keeps that table."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        result-buf)
    (unwind-protect
        (progn
          (dolist (sql '("CREATE TABLE main.people (id TEXT PRIMARY KEY, name TEXT)"
                         "ATTACH DATABASE ':memory:' AS aux"
                         "CREATE TABLE aux.people (pk INTEGER PRIMARY KEY, id TEXT, name TEXT, nick TEXT)"
                         "INSERT INTO aux.people VALUES (1, 'same', 'Ann', 'a'), (2, 'same', 'Bob', 'b')"))
            (clutch-db-query conn sql))
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection conn)
            (let ((source (current-buffer)))
              (clutch-test--execute-and-present
               "SELECT * FROM aux.people ORDER BY pk" conn)
              (setq result-buf
                    (buffer-local-value 'clutch--last-result-buffer source))))
          (with-current-buffer result-buf
            (should (equal (plist-get clutch--row-identity :columns) '("pk")))
            (clutch-test--with-minibuffer-answers '("id" "= 'same'")
              (clutch-result-apply-filter))
            (clutch-test--await-queries)
            (let ((rows clutch--result-rows))
              (clutch-result--apply-edit
               0 (cl-position "nick" clutch--result-columns :test #'string=) "A"
               (list :identity (clutch-db-row-identity-values
                                (nth 0 rows) clutch--row-identity)
                     :original "a"
                     :original-state (cons nil "a")))
              (setq-local clutch--pending-deletes
                          (list (clutch-db-row-identity-values
                                 (nth 1 rows) clutch--row-identity))
                          clutch--pending-inserts
                          '((("pk" . "3") ("id" . "x") ("name" . "Cy") ("nick" . "c")))))
            (should (equal (clutch-result--pending-sql-statements)
                           '("INSERT INTO aux.people (\"pk\", \"id\", \"name\", \"nick\") VALUES ('3', 'x', 'Cy', 'c')"
                             "UPDATE aux.people SET \"nick\" = 'A' WHERE \"pk\" = 1"
                             "DELETE FROM aux.people WHERE \"pk\" = 2")))
            (cl-letf (((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
              (clutch-result-submit)
              (clutch-test--await-queries)))
          (should (equal (clutch-db-result-rows
                          (clutch-db-query conn "SELECT * FROM aux.people ORDER BY pk"))
                         '((1 "same" "Ann" "A") (3 "x" "Cy" "c"))))
          (should-not (clutch-db-result-rows
                       (clutch-db-query conn "SELECT * FROM main.people"))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-changes-name-the-table-as-the-query-did ()
  "Staged and exported changes should name the table as the query did.
A staged INSERT, UPDATE and DELETE and an exported INSERT of one result
name its table alike, whether the query wrote it in another case, with an
alias, quoted, qualified or through a CTE."
  (cl-flet ((target (sql)
              (and (string-match (concat "\\`\\(?:INSERT INTO\\|UPDATE\\|DELETE FROM\\) "
                                         "\\(.+?\\) \\(?:(\\|SET \\|WHERE \\)")
                                 sql)
                   (match-string 1 sql))))
    (pcase-dolist (`(,query ,table)
                   '(("SELECT * FROM people" "people")
                     ("SELECT p.id, p.name FROM People p" "People")
                     ("SELECT * FROM \"my people\"" "\"my people\"")
                     ("SELECT * FROM aux.people" "aux.people")
                     ("WITH c AS (SELECT * FROM aux.people) SELECT * FROM c"
                      "aux.people")))
      (clutch-test--with-sqlite-result (conn result)
          '("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)"
            "CREATE TABLE \"my people\" (id INTEGER PRIMARY KEY, name TEXT)"
            "ATTACH DATABASE ':memory:' AS aux"
            "CREATE TABLE aux.people (id INTEGER PRIMARY KEY, name TEXT)"
            "INSERT INTO people VALUES (1, 'a'), (2, 'b')"
            "INSERT INTO \"my people\" VALUES (1, 'a'), (2, 'b')"
            "INSERT INTO aux.people VALUES (1, 'a'), (2, 'b')")
          query
        (let ((rows clutch--result-rows))
          (clutch-result--apply-edit
           0 (cl-position "name" clutch--result-columns :test #'string=) "a2"
           (list :identity (clutch-db-row-identity-values
                            (nth 0 rows) clutch--row-identity)
                 :original "a"
                 :original-state (cons nil "a")))
          (setq-local clutch--pending-deletes
                      (list (clutch-db-row-identity-values
                             (nth 1 rows) clutch--row-identity))
                      clutch--pending-inserts '((("id" . "3") ("name" . "c"))))
          (should (equal (cons query
                               (mapcar #'target
                                       (append
                                        (clutch-result--pending-sql-statements)
                                        (clutch-result--copy-lines
                                         'insert (list (car rows))
                                         (clutch--visible-columns)))))
                         (cons query (make-list 4 table)))))))))

(ert-deftest clutch-test-json-cancel-retains-null-on-clone ()
  "Opening and cancelling a JSON editor preserves SQL NULL on submission."
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        result-buf insert-buf json-buf)
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, payload JSON DEFAULT '{\"fallback\":true}')")
          (clutch-db-query conn "INSERT INTO items(name,payload) VALUES('kept',NULL)")
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection conn)
            (let ((source (current-buffer)))
              (clutch-test--execute-and-present "SELECT * FROM items" conn)
              (setq result-buf (buffer-local-value 'clutch--last-result-buffer source)))
            (with-current-buffer result-buf
              (clutch--goto-cell 0 0)
              (clutch-clone-row-to-insert))
            (setq insert-buf (get-buffer "*clutch-insert: items*"))
            (with-current-buffer insert-buf
              (goto-char (plist-get (clutch-result-insert--field-state "payload") :value-marker))
              (should (equal (assoc "payload" (clutch-result-insert--parse-fields)) '("payload")))
              (setq json-buf (clutch-result-insert-edit-json-field)))
            (with-current-buffer json-buf (clutch-result-insert-json-cancel))
            (with-current-buffer insert-buf
              (clutch-result-insert-stage))
            (with-current-buffer result-buf
              (clutch-result--submit-mutation-batch
               (clutch-result--build-pending-insert-statements) nil nil)
              (setq clutch--pending-inserts nil)))
          (let ((rows (clutch-db-result-rows
                       (clutch-db-query conn "SELECT name,payload FROM items ORDER BY id"))))
            (should (equal rows '(("kept" nil) ("kept" nil))))))
      (dolist (buf (list json-buf insert-buf result-buf))
        (when (buffer-live-p buf) (kill-buffer buf)))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-jdbc-preview-cannot-be-written-or-exported ()
  "A legacy agent's incomplete preview never becomes writable text."
  (require 'clutch-db-jdbc)
  (let* ((preview (make-string 256 ?a))
         (value (car (clutch-jdbc--normalize-row
                      (list (list :__type "clob" :length 1000 :preview preview)))))
         (clutch--result-columns '("body"))
         (clutch--result-column-defs '((:name "body" :type-category text))))
    (should-not (stringp value))
    (should (= (clutch-db-value-preview-length value) 1000))
    (should (equal (clutch-db-value-preview-text value) preview))
    (should (string-match-p "CLOB preview; 1000 total"
                            (plist-get (clutch--view-spec value nil) :content)))
    (should-error (clutch-result--editable-field-string
                   value '(:type-category text) '(:type "CLOB"))
                  :type 'user-error)
    (dolist (format '(csv tsv org-table insert))
      (should-error (clutch-result--copy-lines format (list (list value)) '(0))
                    :type 'user-error))
    (dolist (export '(clutch--export-csv-content clutch--export-tsv-content))
      (should-error (funcall export (list (list value))) :type 'user-error))))

(ert-deftest clutch-test-insert-explicit-values-survive-validation-and-reopen ()
  "NULL, empty text, default and literal text remain distinct in the form."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("payload")
         :column-defs '((:name "payload" :type-category text))
         :connection (make-clutch-test-conn :table "audit"
                                            :columns '((:name "payload" :type "text" :nullable t :default "fallback"))) :source-table "audit")
      (clutch-result-insert--open-buffer "audit" result-buf)
      (dolist (case '((clutch-result-insert-set-null (("payload")))
                      (clutch-result-insert-set-empty (("payload" . "")))
                      (clutch-result-insert-set-default nil)))
        (with-current-buffer insert-buf
          (clutch-test--goto-insert-field-value "payload")
          (funcall (car case))
          (clutch-result-insert--run-idle-validation insert-buf)
          (should (equal (clutch-result-insert--parse-fields) (cadr case))))
        (clutch-result-insert--open-buffer "audit" result-buf (cadr case))
        (with-current-buffer insert-buf
          (should (equal (clutch-result-insert--parse-fields) (cadr case)))
          (clutch-result-insert-import-delimited "payload\nimported\n")
          (should (equal (clutch-result-insert--parse-fields)
                         '(("payload" . "imported"))))
          (should-not (plist-get (clutch-result-insert--field-state "payload")
                                 :special-value))))
      (with-current-buffer insert-buf
        (clutch-test--goto-insert-field-value "payload")
        (clutch-result-insert-set-null)
        (insert "NULL")
        (should (equal (clutch-result-insert--parse-fields)
                       '(("payload" . "NULL"))))))))

(ert-deftest clutch-test-clone-skips-generated-previews-before-conversion ()
  "Omitted generated columns do not prevent cloning the writable fields."
  (let ((clutch-connection (make-clutch-test-conn :table "audit"
                                                  :columns '((:name "id" :primary-key t)
                                                             (:name "generated_body" :generated t)
                                                             (:name "name" :type "text"))))
        (clutch--result-columns '("id" "generated_body" "name"))
        (clutch--result-column-defs
         '((:name "id") (:name "generated_body") (:name "name")))
        (clutch--row-identity
         (clutch-test--primary-row-identity "audit" '("id") '(0))))
    (should (equal (clutch-result-insert--clone-fields-from-row-values
                    "audit" (list 1 (make-clutch-db-value-preview
                                     :type 'clob :length 1000 :text "preview")
                                  "kept"))
                   '(("name" . "kept"))))))

(provide 'clutch-test-edit)

;;; clutch-test-edit.el ends here
