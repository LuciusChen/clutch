;;; clutch-test-object.el --- Object workflow ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Object browse, describe, jump, action, warmup, and schema-switch tests.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-document))

;;;; Object test helpers

(defmacro clutch-test-object--with-browse-console (spec &rest body)
  "Run BODY in a temporary object browse console.
SPEC is (INITIAL CONN ESCAPE-FN).  INITIAL is inserted before BODY, CONN
becomes `clutch-connection', and ESCAPE-FN, when non-nil, replaces
`clutch-db-escape-identifier'."
  (declare (indent 1) (debug ((form form form) body)))
  (pcase-let ((`(,initial ,conn ,escape-fn) spec))
    `(let ((console (generate-new-buffer " *clutch-test-object-browse*")))
       (unwind-protect
           (with-current-buffer console
             (insert ,initial)
             (setq-local clutch-connection ,conn)
             (setq-local clutch--query-buffer-local-p t)
             (cl-letf (((symbol-function 'derived-mode-p)
                        (lambda (&rest modes) (memq 'clutch-mode modes)))
                       ((symbol-function 'pop-to-buffer)
                        (lambda (buf &rest _args)
                          (set-buffer buf)
                          buf))
                       ,@(when escape-fn
                           `(((symbol-function 'clutch-db-escape-identifier)
                              ,escape-fn))))
               ,@body))
         (when (buffer-live-p console)
           (kill-buffer console))))))

;;;; Object — browse and actions

(ert-deftest clutch-test-object-browse-errors-without-console ()
  "Object browse should error when no matching query console is open."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--find-console-for-conn)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) name)))
      (should-error (clutch-object-browse '(:name "users" :type "TABLE"))
                    :type 'user-error))))

(ert-deftest clutch-test-object-browse-errors-on-non-table-like-type ()
  "Browse should reject object types that do not expose rows."
  (should-error
   (clutch-object-browse '(:name "order_idx" :type "INDEX"))
   :type 'user-error
   :exclude-subtypes nil))

(ert-deftest clutch-test-object-browse-inserts-sql-into-console ()
  "Object browse should insert escaped SELECT in the target console buffer."
  (clutch-test-object--with-browse-console
      ("SELECT 1;" 'fake-conn
       (lambda (_conn name) (format "\"%s\"" name)))
    (clutch-object-browse '(:name "order-items" :type "TABLE"))
    (should (string-suffix-p
             "\n\nSELECT * FROM \"order-items\";"
             (buffer-string)))))

(ert-deftest clutch-test-object-browse-preserves-schema-qualification ()
  "Object browse should keep schema-qualified object names."
  (clutch-test-object--with-browse-console
      ("" 'fake-conn
       (lambda (_conn name) (format "\"%s\"" name)))
    (clutch-object-browse
     '(:name "background_schedule_pool_log"
       :type "TABLE"
       :source-schema "system"))
    (should (equal (buffer-string)
                   "SELECT * FROM \"system\".\"background_schedule_pool_log\";"))))

(ert-deftest clutch-test-object-browse-uses-object-schema-before-source-schema ()
  "Object browse should prefer the object's real schema over discovery schema."
  (clutch-test-object--with-browse-console
      ("" 'fake-conn
       (lambda (_conn name) (format "\"%s\"" name)))
    (clutch-object-browse
     '(:name "ORDERS"
       :type "SYNONYM"
       :schema "DATA_OWNER"
       :source-schema "APP"))
    (should (equal (buffer-string)
                   "SELECT * FROM \"DATA_OWNER\".\"ORDERS\";"))))

(ert-deftest clutch-test-object-browse-clickhouse-uses-unquoted-identifiers ()
  "ClickHouse browse SQL should avoid double quotes for simple identifiers."
  (require 'clutch-db-jdbc)
  (let ((conn (make-clutch-jdbc-conn :params '(:driver clickhouse))))
    (clutch-test-object--with-browse-console ("" conn nil)
      (clutch-object-browse
       '(:name "background_schedule_pool_log"
         :type "TABLE"
         :source-schema "system"))
      (should (equal (buffer-string)
                     "SELECT * FROM system.background_schedule_pool_log;")))))

(ert-deftest clutch-test-object-browse-inserts-at-first-line-in-empty-console ()
  "Object browse should insert at line 1 when the console has no content."
  (clutch-test-object--with-browse-console
      ("\n\n" 'fake-conn
       (lambda (_conn name) (format "\"%s\"" name)))
    (clutch-object-browse '(:name "order-items" :type "TABLE"))
    (should (equal (buffer-string)
                   "SELECT * FROM \"order-items\";"))))

(ert-deftest clutch-test-object-browse-inserts-around-current-point-in-console ()
  "Object browse should insert around point with one blank line on each side."
  (clutch-test-object--with-browse-console
      ("SELECT 1;\nSELECT 2;" 'fake-conn
       (lambda (_conn name) (format "\"%s\"" name)))
    (goto-char (point-min))
    (forward-line 1)
    (clutch-object-browse '(:name "order-items" :type "TABLE"))
    (should (equal (buffer-string)
                   (concat "SELECT 1;\n\n"
                           "SELECT * FROM \"order-items\";\n\n"
                           "SELECT 2;")))))

(ert-deftest clutch-test-object-browse-does-not-add-extra-blank-lines ()
  "Object browse should reuse an existing blank-line separator."
  (clutch-test-object--with-browse-console
      ("SELECT 1;\n\n" 'fake-conn
       (lambda (_conn name) (format "\"%s\"" name)))
    (clutch-object-browse '(:name "order-items" :type "TABLE"))
    (should (equal (buffer-string)
                   "SELECT 1;\n\nSELECT * FROM \"order-items\";"))))

(ert-deftest clutch-test-object-default-action-routes-table-like-types-to-browse ()
  "Default action should browse rows for table-like objects."
  (let (browse-entry show-entry)
    (cl-letf (((symbol-function 'clutch-object-browse)
               (lambda (entry) (setq browse-entry entry)))
              ((symbol-function 'clutch-object-show-ddl-or-source)
               (lambda (entry) (setq show-entry entry))))
      (clutch-object-default-action '(:name "orders" :type "TABLE"))
      (should (equal browse-entry '(:name "orders" :type "TABLE")))
      (setq browse-entry nil)
      (clutch-object-default-action '(:name "users" :type "COLLECTION"))
      (should (equal browse-entry '(:name "users" :type "COLLECTION")))
      (should-not show-entry))))

(ert-deftest clutch-test-object-default-action-routes-non-table-types-to-definition ()
  "Default action should show DDL/source for non-table objects."
  (let (browse-entry show-entry)
    (cl-letf (((symbol-function 'clutch-object-browse)
               (lambda (entry) (setq browse-entry entry)))
              ((symbol-function 'clutch-object-show-ddl-or-source)
               (lambda (entry) (setq show-entry entry))))
      (clutch-object-default-action '(:name "process_order" :type "PROCEDURE"))
      (should-not browse-entry)
      (should (equal show-entry '(:name "process_order" :type "PROCEDURE"))))))

(ert-deftest clutch-test-object-jump-target-resolves-index-table ()
  "Jump target should follow index target-table metadata."
  (let (described)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--object-matches-by-name)
                 (lambda (_conn name &optional table-like-only)
                   (should table-like-only)
                   (should (equal name "ORDERS"))
                   '((:name "ORDERS" :type "TABLE" :schema "APP" :source-schema "APP"))))
                ((symbol-function 'clutch-object-describe)
                 (lambda (entry) (setq described entry))))
        (clutch-object-jump-target
         '(:name "ORDER_IDX" :type "INDEX"
           :schema "APP" :source-schema "APP"
           :target-table "ORDERS"))
        (should (equal described
                       '(:name "ORDERS" :type "TABLE"
                         :schema "APP" :source-schema "APP")))))))

(ert-deftest clutch-test-object-jump-target-prefers-synonym-target-schema ()
  "Jump target should prefer synonym matches in the declared target schema."
  (let (described)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--object-matches-by-name)
                 (lambda (_conn name &optional _table-like-only)
                   (should (equal name "ORDERS"))
                   '((:name "ORDERS" :type "TABLE"
                      :schema "ARCHIVE" :source-schema "ARCHIVE")
                     (:name "ORDERS" :type "TABLE"
                      :schema "APP" :source-schema "APP"))))
                ((symbol-function 'clutch-object-describe)
                 (lambda (entry) (setq described entry))))
        (clutch-object-jump-target
         '(:name "ORDERS_SYM" :type "SYNONYM"
           :target-name "ORDERS" :target-schema "APP"))
        (should (equal described
                       '(:name "ORDERS" :type "TABLE"
                         :schema "APP" :source-schema "APP")))))))

(ert-deftest clutch-test-object-jump-target-errors-on-unsupported-type ()
  "Forward jump should reject object types without target semantics."
  (should-error
   (clutch-object-jump-target '(:name "ORDER_SEQ" :type "SEQUENCE"))
   :type 'user-error
   :exclude-subtypes nil))

(ert-deftest clutch-test-act-dwim-opens-transient-with-current-entry ()
  "Act-dwim should resolve the current object and open the shared action UI."
  (let (setup-command)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
                 (lambda (&rest _)
                   '(:name "PROCESS_ORDER" :type "PROCEDURE")))
                ((symbol-function 'transient-setup)
                 (lambda (command &rest _args)
                   (setq setup-command command))))
        (let ((clutch--object-action-entry nil))
          (clutch-act-dwim)
          (should (eq setup-command 'clutch-object-actions-menu))
          (should (equal clutch--object-action-entry
                         '(:name "PROCESS_ORDER" :type "PROCEDURE"))))))))

(ert-deftest clutch-test-act-dwim-errors-without-action-ui ()
  "Act-dwim should not silently run the default action when no UI exists."
  (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
             (lambda (&rest _)
               '(:name "ORDERS" :type "TABLE")))
            ((symbol-function 'clutch--present-object-actions-natively)
             (lambda (_entry) nil)))
    (should-error (clutch-act-dwim) :type 'user-error)))

(ert-deftest clutch-test-act-dwim-with-explicit-entry-opens-transient ()
  "Act-dwim should still work when called programmatically with ENTRY."
  (let (setup-command)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'transient-setup)
                 (lambda (command &rest _args)
                   (setq setup-command command))))
        (let ((clutch--object-action-entry nil))
          (clutch-act-dwim '(:name "ORDERS" :type "TABLE"))
          (should (eq setup-command 'clutch-object-actions-menu))
          (should (equal clutch--object-action-entry
                         '(:name "ORDERS" :type "TABLE"))))))))

(ert-deftest clutch-test-object-action-inapt-flags-reflect-target-type ()
  "Object action transient flags should reflect the current target type."
  (let ((clutch--object-action-entry '(:name "ORDERS" :type "TABLE")))
    (should (clutch--object-act-jump-target-inapt-p))
    (should (clutch--object-act-mongodb-collection-inapt-p)))
  (let ((clutch--object-action-entry '(:name "ORDER_IDX" :type "INDEX")))
    (should-not (clutch--object-act-jump-target-inapt-p))
    (should (clutch--object-act-mongodb-collection-inapt-p)))
  (let ((clutch--object-action-entry '(:name "users" :type "COLLECTION")))
    (should (clutch--object-act-jump-target-inapt-p))
    (should-not (clutch--object-act-mongodb-collection-inapt-p))))

(ert-deftest clutch-test-object-list-indexes-executes-mongodb-helper ()
  "MongoDB collection index action should execute listIndexes helper syntax."
  (let (captured)
    (with-temp-buffer
      (setq-local clutch-connection 'mongo-conn
                  clutch--connection-params nil
                  clutch--conn-sql-product nil)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'mongodb))
                ((symbol-function 'clutch--execute)
                 (lambda (sql conn &optional result-context)
                   (setq captured (list sql conn result-context))))
                ((symbol-function 'clutch--clear-connection-problem-capture)
                 #'ignore)
                ((symbol-function 'clutch--remember-current-object)
                 #'ignore))
        (clutch-object-list-indexes '(:name "users" :type "COLLECTION"))))
    (should (equal captured
                   '("db.getCollection(\"users\").listIndexes();"
                     mongo-conn nil)))))

(ert-deftest clutch-test-object-explain-sample-executes-mongodb-helper ()
  "MongoDB collection explain action should execute a sample explain helper."
  (let (captured)
    (with-temp-buffer
      (setq-local clutch-connection 'mongo-conn
                  clutch--connection-params nil
                  clutch--conn-sql-product nil)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'mongodb))
                ((symbol-function 'clutch--execute)
                 (lambda (sql conn &optional result-context)
                   (setq captured (list sql conn result-context))))
                ((symbol-function 'clutch--clear-connection-problem-capture)
                 #'ignore)
                ((symbol-function 'clutch--remember-current-object)
                 #'ignore))
        (clutch-object-explain-sample-query
         '(:name "users" :type "COLLECTION"))))
    (should (equal captured
                   '("db.getCollection(\"users\").find({}).limit(1).explain(\"executionStats\");"
                     mongo-conn nil)))))

(ert-deftest clutch-test-copy-object-fqname-prompts-for-fqname ()
  "Copy-fqname should use an fqname-specific prompt."
  (let (prompt)
    (cl-letf (((symbol-function 'clutch--resolve-object-entry)
               (lambda (arg &rest _)
                 (setq prompt arg)
                 '(:name "ORDERS" :type "TABLE" :schema "APP")))
              ((symbol-function 'kill-new) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-copy-object-fqname)
      (should (equal prompt "Copy object fqname: ")))))

(ert-deftest clutch-test-browse-table-inserts-escaped-select ()
  "Browse-table compatibility wrapper should still escape identifiers."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (insert "-- tail")
    (cl-letf (((symbol-function 'clutch-object-browse)
               (lambda (entry)
                 (let ((sql (format "SELECT * FROM %s;"
                                    (clutch-db-escape-identifier
                                     clutch-connection
                                     (plist-get entry :name)))))
                   (goto-char (point-max))
                   (unless (bolp) (insert "\n"))
                   (insert "\n" sql))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn tbl) (format "\"%s\"" tbl))))
      (clutch-browse-table "order-items"))
    (should (string-suffix-p
             "\n\nSELECT * FROM \"order-items\";"
             (buffer-string)))))

(ert-deftest clutch-test-browse-table-interactive-preserves-entry-schema ()
  "Interactive browse-table should preserve schema-qualified entries."
  (let (captured-entry)
    (cl-letf (((symbol-function 'clutch-object-read)
               (lambda (&rest _args)
                 '(:name "background_schedule_pool_log"
                   :type "TABLE"
                   :source-schema "system")))
              ((symbol-function 'clutch-object-browse)
               (lambda (entry)
                 (setq captured-entry entry))))
      (call-interactively #'clutch-browse-table))
    (should (equal captured-entry
                   '(:name "background_schedule_pool_log"
                     :type "TABLE"
                     :source-schema "system")))))

;;;; Object — describe and DDL

(ert-deftest clutch-test-describe-dwim-prompts-when-point-has-no-object ()
  "Describe-dwim should resolve an object and then open its describe view."
  (let (resolved-prompt captured)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'thing-at-point) (lambda (&rest _) nil))
                ((symbol-function 'clutch--resolve-object-dwim)
                 (lambda (prompt &rest _args)
                   (setq resolved-prompt prompt)
                   '(:name "IDX_A" :type "INDEX")))
                ((symbol-function 'clutch-object-describe)
                 (lambda (entry)
                   (setq captured entry))))
        (clutch-describe-dwim)
        (should (equal resolved-prompt "Describe object: "))
        (should (equal captured '(:name "IDX_A" :type "INDEX")))))))

(ert-deftest clutch-test-render-object-describe-keeps-fqname-in-body-not-header ()
  "Describe buffers should keep the fqname in the body title, not duplicate it in the header-line."
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch--bind-connection-context) #'ignore)
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[desc]"))
              ((symbol-function 'clutch--object-describe-text)
               (lambda (_conn _entry)
                 "PUBLIC.orders (TABLE)\n\nSummary\n  Name  orders")))
      (clutch--render-object-describe
       'fake-conn
       '(:name "orders" :type "TABLE" :source-schema "PUBLIC"))
      (should (string-match-p "PUBLIC.orders (TABLE)" (buffer-string)))
      (should (string-match-p "\\[desc\\]" clutch-describe--header-base))
      (should (string-match-p "show definition" clutch-describe--header-base))
      (should-not (string-match-p "PUBLIC.orders" clutch-describe--header-base)))))

(ert-deftest clutch-test-render-object-describe-fontifies-short-standalone-index-name ()
  "Short standalone index names in describe buffers should be highlighted."
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch--bind-connection-context) #'ignore)
              ((symbol-function 'clutch--icon-with-face) (lambda (&rest _) "[desc]"))
              ((symbol-function 'clutch--ensure-table-comment) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-column-details) (lambda (&rest _) nil))
              ((symbol-function 'clutch-db-list-columns) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-related-entries)
               (lambda (_conn _entry type)
                 (pcase type
                   ("INDEX"
                    (list (list :name "IDX_ORDER_RECEIPT_CODE" :unique nil)
                          (list :name "idx_ord" :unique nil)))
                   (_ nil)))))
      (clutch--render-object-describe
       'fake-conn
       '(:name "order_items" :type "TABLE" :source-schema "sales"))
      (goto-char (point-min))
      (re-search-forward "^  \\(idx_ord\\)")
      (should (eq (get-text-property (match-beginning 1) 'face)
                  'font-lock-variable-name-face)))))

(ert-deftest clutch-test-object-entry-label-uses-lowercase-type ()
  "Object-entry labels should keep schema uppercase and type lowercase."
  (should (equal (clutch--object-entry-label
                  '(:name "USER_TABLES" :type "PUBLIC SYNONYM"
                    :schema "SYS" :source-schema "PUBLIC"))
                 "PUBLIC/synonym"))
  (should (equal (clutch--object-entry-label
                  '(:name "MONTHLY_REPORT" :type "VIEW"
                    :schema "DATA_OWNER" :source-schema "DATA_OWNER"))
                 "DATA_OWNER/view")))

(ert-deftest clutch-test-object-sql-name-rejects-current-schema-placeholder ()
  "Browse SQL must never contain literal \"current_schema\" as schema qualifier.
Reproduces the bug where jumping to a PG table generated
SELECT * FROM \"current_schema\".\"orders_large\" which PostgreSQL rejects.
The fix is in the PG backend: entries carry the real schema (e.g. \"public\"),
so `clutch--object-sql-name' produces \"public\".\"orders_large\"."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    ;; An entry with a real schema produces correct qualified SQL.
    (let* ((entry '(:name "orders_large" :type "TABLE"
                    :schema "public" :source-schema "public"))
           (sql-name (clutch--object-sql-name 'fake entry)))
      (should (equal sql-name "\"public\".\"orders_large\""))
      (should-not (string-match-p "current_schema" sql-name)))))

(ert-deftest clutch-test-object-sql-name-never-produces-current-schema-literal ()
  "Ensure `clutch--object-sql-name' never emits \"current_schema\" as a schema qualifier."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    ;; With a real schema name, should produce "public"."orders"
    (should (equal (clutch--object-sql-name 'fake '(:name "orders" :type "TABLE"
                                                    :schema "public"))
                   "\"public\".\"orders\""))
    ;; Without schema, should produce bare "orders"
    (should (equal (clutch--object-sql-name 'fake '(:name "orders" :type "TABLE"))
                   "\"orders\""))))

(ert-deftest clutch-test-object-show-ddl-or-source-derives-oracle-sql-product-from-backend ()
  "Object definition buffers should use Oracle font-lock when backend is oracle."
  (let (captured-product)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-show-create-table)
               (lambda (_conn _table) "CREATE TABLE demo_tasks (id NUMBER)"))
              ((symbol-function 'sql-mode) #'ignore)
              ((symbol-function 'sql-set-product)
               (lambda (product) (setq captured-product product)))
              ((symbol-function 'font-lock-ensure) #'ignore)
              ((symbol-function 'pop-to-buffer) (lambda (&rest _args) nil)))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (setq-local clutch--connection-params '(:backend oracle))
        (setq-local clutch--conn-sql-product nil)
        (clutch-object-show-ddl-or-source '(:name "DEMO_TASKS" :type "TABLE"))))
    (should (eq captured-product 'oracle))))

(ert-deftest clutch-test-object-show-ddl-or-source-populates-problem-record-and-debug-trace ()
  "Definition/source failures should feed the shared problem/debug workflow."
  (let ((details '(:backend jdbc
                   :summary "ORA-04043: object does not exist"
                   :diag (:category "metadata"
                          :op "get-object-ddl"
                          :conn-id 7
                          :raw-message "ORA-04043: object does not exist"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch-db-show-create-table)
                   (lambda (&rest _args)
                     (signal 'clutch-db-error
                             (list "ORA-04043: object does not exist" details))))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (should-error
           (clutch-object-show-ddl-or-source '(:name "DEMO_TASKS" :type "TABLE"))
           :type 'clutch-db-error)
          (should clutch--buffer-error-details)
          (should (string-match-p "ORA-04043"
                                  (plist-get clutch--buffer-error-details :summary)))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: show-definition" text))
            (should (string-match-p "Phase: error" text))
            (should (string-match-p "DEMO_TASKS" text))))))))

(ert-deftest clutch-test-object-describe-propagates-detail-errors ()
  "Describe rendering should not silently swallow object detail failures."
  (cl-letf (((symbol-function 'clutch-db-object-details)
             (lambda (_conn _entry)
               (signal 'clutch-db-error '("detail boom")))))
    (should-error
     (clutch--object-describe-text 'fake-conn '(:name "IDX_USERS" :type "INDEX"))
     :type 'clutch-db-error)))

(ert-deftest clutch-test-object-describe-table-propagates-column-detail-errors ()
  "Table describe should not hide column-detail failures behind list-columns."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (detail-calls 0)
        (list-columns-calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn _table)
                 (cl-incf detail-calls)
                 (signal 'clutch-db-error '("column detail boom"))))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (&rest _args)
                 (cl-incf list-columns-calls)
                 '("id" "name")))
              ((symbol-function 'clutch--ensure-table-comment)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--object-related-entries)
               (lambda (&rest _args) nil)))
      (should-error
       (clutch--object-describe-text 'fake-conn '(:name "USERS" :type "TABLE"))
       :type 'clutch-db-error)
      (should-error
       (clutch--object-describe-text 'fake-conn '(:name "USERS" :type "TABLE"))
       :type 'clutch-db-error)
      (should (= detail-calls 1))
      (should (= list-columns-calls 0)))))

(ert-deftest clutch-test-object-describe-collection-shows-fields-and-indexes ()
  "Collection describe should expose sampled fields and MongoDB indexes."
  (cl-letf (((symbol-function 'clutch--ensure-column-details)
             (lambda (_conn collection &optional strict)
               (should (equal collection "orders"))
               (should strict)
               '((:name "_id" :type "BSON" :type-category json :nullable t)
                 (:name "status" :type "BSON" :type-category json :nullable t))))
            ((symbol-function 'clutch-db-list-columns)
             (lambda (&rest _args)
               (ert-fail "Column fallback should not run when details exist")))
            ((symbol-function 'clutch--object-related-entries)
             (lambda (_conn entry type &optional refresh)
               (should (equal entry '(:name "orders" :type "COLLECTION")))
               (should (equal type "INDEX"))
               (should refresh)
               '((:name "_id_" :type "INDEX" :target-table "orders")
                 (:name "status_idx" :type "INDEX"
                  :target-table "orders" :unique t)))))
    (let ((text (clutch--object-describe-text
                 'fake-conn '(:name "orders" :type "COLLECTION"))))
      (should (string-match-p "orders (COLLECTION)" text))
      (should (string-match-p "Fields (2)" text))
      (should (string-match-p "_id[[:space:]]+BSON" text))
      (should (string-match-p "status[[:space:]]+BSON" text))
      (should (string-match-p "Indexes (2)" text))
      (should (string-match-p "_id_" text))
      (should (string-match-p "status_idx[[:space:]]+UNIQUE" text)))))

(ert-deftest clutch-test-object-describe-populates-problem-record-in-source-buffer ()
  "Describe failures should populate a problem record in the invoking buffer."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (details '(:backend jdbc
                   :summary "ORA-12592: TNS:bad packet"
                   :diag (:category "metadata"
                          :op "get-columns"
                          :conn-id 7
                          :raw-message "ORA-12592: TNS:bad packet"))))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "dev-key"))
                ((symbol-function 'clutch-db-error-details)
                 (lambda (_conn) details))
                ((symbol-function 'clutch-db-column-details)
                 (lambda (_conn _table)
                   (signal 'clutch-db-error
                           (list "ORA-12592: TNS:bad packet" details))))
                ((symbol-function 'clutch--ensure-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--object-related-entries)
                 (lambda (&rest _args) nil))
                ((symbol-function 'pop-to-buffer)
                 (lambda (buf &rest _args) buf)))
        (should-error
         (clutch-object-describe '(:name "USERS" :type "TABLE"))
         :type 'clutch-db-error)
        (let* ((problem clutch--buffer-error-details)
               (diag (plist-get problem :diag)))
          (should problem)
          (should (string-match-p "ORA-12592" (plist-get problem :summary)))
          (should (equal (plist-get diag :op) "get-columns")))))))

(ert-deftest clutch-test-object-describe-populates-debug-trace-when-enabled ()
  "Describe failures should contribute debug trace entries when debug mode is on."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (details '(:backend jdbc
                   :summary "ORA-12592: TNS:bad packet"
                   :diag (:category "metadata"
                          :op "get-columns"
                          :conn-id 7
                          :raw-message "ORA-12592: TNS:bad packet"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "dev-key"))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch-db-error-details)
                   (lambda (_conn) details))
                  ((symbol-function 'clutch-db-column-details)
                   (lambda (_conn _table)
                     (signal 'clutch-db-error
                             (list "ORA-12592: TNS:bad packet" details))))
                  ((symbol-function 'clutch--ensure-table-comment)
                   (lambda (&rest _args) nil))
                  ((symbol-function 'clutch--object-related-entries)
                   (lambda (&rest _args) nil))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (should-error
           (clutch-object-describe '(:name "USERS" :type "TABLE"))
           :type 'clutch-db-error)
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: describe" text))
            (should (string-match-p "Phase: error" text))
            (should (string-match-p "USERS" text))))))))

(ert-deftest clutch-test-object-describe-success-clears-stale-problem-records ()
  "Successful describe should clear older failure state for the same connection."
  (let ((source (generate-new-buffer " *clutch-describe-source*")))
    (unwind-protect
        (with-current-buffer source
          (setq-local clutch-connection 'fake-conn
                      clutch--buffer-error-details '(:summary "old"))
          (puthash 'fake-conn '(:summary "old") clutch--problem-records-by-conn)
          (cl-letf (((symbol-function 'clutch--remember-current-object) #'ignore)
                    ((symbol-function 'clutch--object-fqname)
                     (lambda (_entry) "USERS"))
                    ((symbol-function 'clutch--object-describe-text)
                     (lambda (_conn _entry) "ok"))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (clutch-object-describe '(:name "USERS" :type "TABLE"))
            (should-not clutch--buffer-error-details)
            (should-not (gethash 'fake-conn clutch--problem-records-by-conn))))
      (kill-buffer source))))

(ert-deftest clutch-test-describe-refresh-success-clears-stale-problem-records ()
  "Refreshing a describe buffer should clear stale failure state on success."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--describe-object-entry '(:name "USERS" :type "TABLE")
                clutch--buffer-error-details '(:summary "old"))
    (puthash 'fake-conn '(:summary "old") clutch--problem-records-by-conn)
    (cl-letf (((symbol-function 'clutch--refresh-current-schema) #'ignore)
              ((symbol-function 'clutch--render-object-describe) #'ignore))
      (clutch-describe-refresh)
      (should-not clutch--buffer-error-details)
      (should-not (gethash 'fake-conn clutch--problem-records-by-conn)))))

(ert-deftest clutch-test-object-describe-propagates-related-object-errors ()
  "Describe rendering should not silently swallow related-object lookup failures."
  (cl-letf (((symbol-function 'clutch--ensure-table-comment) (lambda (&rest _) nil))
            ((symbol-function 'clutch--ensure-column-details) (lambda (&rest _) nil))
            ((symbol-function 'clutch-db-list-columns) (lambda (&rest _) '("id")))
            ((symbol-function 'clutch--object-entries)
             (lambda (_conn)
               (signal 'clutch-db-error '("related boom")))))
    (should-error
     (clutch--object-describe-text 'fake-conn '(:name "USERS" :type "TABLE"))
     :type 'clutch-db-error)))

;;;; Object — jump, resolve, and warmup

(ert-deftest clutch-test-object-read-annotates-mixed-object-types ()
  "Object reader should expose warmed non-table metadata in the flat picker."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        captured)
    (let ((by-type (make-hash-table :test 'equal)))
      (puthash "INDEX"
               '((:name "ORDER_IDX" :type "INDEX"
                  :schema "APP" :source-schema "APP"))
               by-type)
      (puthash "PROCEDURE"
               '((:name "PROCESS_ORDER" :type "PROCEDURE"
                  :schema "APP" :source-schema "APP"
                  :status "VALID"))
               by-type)
      (puthash "fake-key"
               (list :entries '((:name "ORDER_IDX" :type "INDEX"
                                 :schema "APP" :source-schema "APP")
                                (:name "PROCESS_ORDER" :type "PROCEDURE"
                                 :schema "APP" :source-schema "APP"
                                 :status "VALID"))
                     :by-type by-type
                     :loaded-categories '(indexes procedures))
               clutch--object-cache))
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch--warn-schema-cache-state) #'ignore)
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-list-table-entries)
               (lambda (_conn)
                 '((:name "ORDERS" :type "TABLE" :schema "APP" :source-schema "APP")
                   (:name "V_ORDERS" :type "VIEW" :schema "APP" :source-schema "APP"))))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix)
                 '((:name "USER_TABLES" :type "PUBLIC SYNONYM"
                    :schema "SYS" :source-schema "PUBLIC"))))
              ((symbol-function 'clutch-db-list-objects)
               (lambda (&rest _args)
                 (ert-fail "default flat picker should not synchronously fetch uncached object categories")))
              ((symbol-function 'run-with-idle-timer)
               (lambda (&rest _args) 'fake-timer))
              ((symbol-function 'completing-read)
               (lambda (_prompt collection &rest _args)
                 (let* ((metadata (funcall collection "" nil 'metadata))
                        (meta-alist (cdr metadata))
                        (annotation-fn (alist-get 'annotation-function meta-alist))
                        (group-fn (alist-get 'group-function meta-alist))
                        (base (all-completions "" collection nil)))
                   (setq captured
                         (list :category (alist-get 'category meta-alist)
                               :base base
                               :orders-group (funcall group-fn "ORDERS" nil)
                               :proc-group (funcall group-fn "PROCESS_ORDER" nil)
                               :proc-ann (funcall annotation-fn "PROCESS_ORDER")))
                   "PROCESS_ORDER"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (equal (clutch-object-read "Object: ")
                       '(:name "PROCESS_ORDER" :type "PROCEDURE"
                         :schema "APP" :source-schema "APP" :status "VALID"))))
      (should (eq (plist-get captured :category) 'clutch-object))
      (should (equal (plist-get captured :base)
                     '("ORDERS" "V_ORDERS" "USER_TABLES" "ORDER_IDX" "PROCESS_ORDER")))
      (should (equal (plist-get captured :orders-group) "Tables"))
      (should (equal (plist-get captured :proc-group) "Procedures"))
      (should (equal (plist-get captured :proc-ann) "  APP/procedure")))))

(ert-deftest clutch-test-jump-resolves-then-runs-default-action ()
  "Jump should resolve the object once and run the shared default action."
  (let (resolved-prompt resolved-category resolved-types action-call presented-entry)
    (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
               (lambda (prompt &optional _table-like-only category allowed-types)
                 (setq resolved-prompt prompt)
                 (setq resolved-category category)
                 (setq resolved-types allowed-types)
                 '(:name "ORDERS" :type "TABLE")))
              ((symbol-function 'clutch--run-object-action)
               (lambda (entry action-id)
                 (setq action-call (list entry action-id))))
              ((symbol-function 'clutch--present-object-actions-natively)
               (lambda (entry)
                 (setq presented-entry entry)
                 t)))
      (clutch-jump)
      (should (equal resolved-prompt "Jump to object: "))
      (should-not resolved-category)
      (should (equal resolved-types clutch-primary-object-types))
      (should (equal action-call
                     '((:name "ORDERS" :type "TABLE") browse)))
      (should-not presented-entry))))

(ert-deftest clutch-test-jump-falls-back-to-default-action-when-ui-is-unavailable ()
  "Jump should still run the default action when no action UI is available."
  (let (resolved-prompt action-call)
    (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
               (lambda (prompt &optional _table-like-only _category _allowed-types)
                 (setq resolved-prompt prompt)
                 '(:name "PROCESS_ORDER" :type "PROCEDURE")))
              ((symbol-function 'clutch--run-object-action)
                 (lambda (entry action-id)
                   (setq action-call (list entry action-id)))))
      (clutch-jump)
      (should (equal resolved-prompt "Jump to object: "))
      (should (equal action-call
                     '((:name "PROCESS_ORDER" :type "PROCEDURE")
                       show-definition))))))

(ert-deftest clutch-test-jump-on-table-at-point-in-console-prompts-before-browse ()
  "Jump should prompt before browsing when point is on a table-like name."
  (with-temp-buffer
    (clutch-mode)
    (insert "users")
    (goto-char (point-min))
    (setq-local clutch-connection 'fake-conn)
    (let (read-args resolved-called action-call)
      (cl-letf (((symbol-function 'clutch-object-at-point)
                 (lambda () '(:name "users" :type "TABLE")))
                ((symbol-function 'thing-at-point)
                 (lambda (&rest _args) "users"))
                ((symbol-function 'clutch-object-read)
                 (lambda (prompt &optional table-like-only initial-input category allowed-types)
                   (setq read-args (list prompt table-like-only initial-input category allowed-types))
                   '(:name "users" :type "TABLE")))
                ((symbol-function 'clutch--resolve-object-dwim)
                 (lambda (&rest _args)
                   (setq resolved-called t)
                   '(:name "users" :type "TABLE")))
                ((symbol-function 'clutch--run-object-action)
                 (lambda (entry action-id)
                   (setq action-call (list entry action-id)))))
        (clutch-jump)
        (should read-args)
        (should (equal (nth 0 read-args) "Jump to object: "))
        (should-not (nth 1 read-args))
        (should (equal (nth 2 read-args) "users"))
        (should-not (nth 3 read-args))
        (should (equal (nth 4 read-args) clutch-primary-object-types))
        (should-not resolved-called)
        (should (equal action-call
                       '((:name "users" :type "TABLE") browse)))))))

(ert-deftest clutch-test-jump-on-collection-at-point-in-mongodb-console-browses ()
  "Jump should treat MongoDB collections as primary browseable objects."
  (let ((clutch--object-cache (make-hash-table :test 'equal)))
    (with-temp-buffer
      (clutch-mongodb-mode)
      (insert "db.users.find()")
      (search-backward "users")
      (setq-local clutch-connection 'fake-mongodb-conn)
      (let (read-args action-call)
        (cl-letf (((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "mongodb-test"))
                  ((symbol-function 'clutch-db-browseable-object-entries)
                   (lambda (_conn)
                     '((:name "users" :schema "app" :type "COLLECTION")
                       (:name "orders" :schema "app" :type "COLLECTION"))))
                  ((symbol-function 'clutch--schedule-object-warmup)
                   #'ignore)
                  ((symbol-function 'clutch--resolve-object-dwim)
                   (lambda (&rest _args)
                     (ert-fail "MongoDB collection at point should resolve directly")))
                  ((symbol-function 'clutch-object-read)
                   (lambda (prompt &optional table-like-only initial-input category allowed-types)
                     (setq read-args
                           (list prompt table-like-only initial-input category allowed-types))
                     '(:name "users" :schema "app" :type "COLLECTION")))
                  ((symbol-function 'clutch--run-object-action)
                   (lambda (entry action-id)
                     (setq action-call (list entry action-id)))))
          (clutch-jump)
          (should (equal read-args
                         (list "Jump to object: "
                               nil
                               "users"
                               nil
                               clutch-primary-object-types)))
          (should (equal action-call
                         '((:name "users" :schema "app" :type "COLLECTION")
                           browse))))))))

(ert-deftest clutch-test-object-read-filters-by-allowed-types ()
  "Object reader should honor an explicit allowed type set."
  (let (captured)
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn)
                 '((:name "ORDERS" :type "TABLE")
                   (:name "ORDER_IDX" :type "INDEX")
                   (:name "PROCESS_ORDER" :type "PROCEDURE"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &optional _initial _category)
                 (setq captured entries)
                 (car entries))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (equal (clutch-object-read "Object: " nil nil nil '("TABLE" "VIEW"))
                       '(:name "ORDERS" :type "TABLE"))))
      (should (equal captured '((:name "ORDERS" :type "TABLE")))))))

(ert-deftest clutch-test-symbol-has-local-completions-p ()
  "Prefix helper should match case-insensitively."
  (let ((entries '((:name "ORDERS" :type "TABLE")
                   (:name "ORDER_ITEMS" :type "TABLE"))))
    (should (clutch--symbol-has-local-completions-p "ord" entries))
    (should (clutch--symbol-has-local-completions-p "ORD" entries))
    (should-not (clutch--symbol-has-local-completions-p "ZZZ" entries))))

(ert-deftest clutch-test-resolve-entry-prefills-when-symbol-has-local-completions ()
  "When symbol at point prefix-matches local entries, pre-fill the picker."
  (let (captured-initial)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "ORD"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh)
                 '((:name "ORDERS" :type "TABLE")
                   (:name "ORDER_ITEMS" :type "TABLE"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional initial _cat)
                 (setq captured-initial initial)
                 '(:name "ORDERS" :type "TABLE"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (equal captured-initial "ORD"))))))

(ert-deftest clutch-test-resolve-entry-single-remote-hit-returns-directly ()
  "When on-demand search finds exactly one hit, return it directly."
  (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
            ((symbol-function 'clutch--ensure-connection) (lambda () t))
            ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
            ((symbol-function 'thing-at-point) (lambda (&rest _) "RARE_TABLE"))
            ((symbol-function 'clutch--object-entries)
             (lambda (_conn &optional _refresh) nil))
            ((symbol-function 'clutch-db-search-table-entries)
             (lambda (_conn _prefix)
               '((:name "RARE_TABLE" :type "TABLE"))))
            ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) t)))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--resolve-object-entry "Describe: ")
                     '(:name "RARE_TABLE" :type "TABLE"))))))

(ert-deftest clutch-test-resolve-entry-multiple-remote-hits-opens-picker ()
  "When on-demand search finds multiple hits, open picker with those hits."
  (let (captured-entries)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "AUDIT"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh) nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix)
                 '((:name "AUDIT_LOG" :type "TABLE")
                   (:name "AUDIT_TRAIL" :type "TABLE"))))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) t))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &optional _initial _cat)
                 (setq captured-entries entries)
                 (car entries))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (= (length captured-entries) 2))))))

(ert-deftest clutch-test-resolve-entry-no-match-anywhere-opens-full-picker ()
  "When nothing matches, show message and open picker with no pre-fill."
  (let (captured-initial messages)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "preferential_price"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh)
                 '((:name "ORDERS" :type "TABLE"))))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix) nil))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (push (apply #'format fmt args) messages)))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional initial _cat)
                 (setq captured-initial initial)
                 '(:name "ORDERS" :type "TABLE"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (null captured-initial))
        (should (cl-some (lambda (m) (string-match-p "preferential_price" m))
                         messages))))))

(ert-deftest clutch-test-resolve-entry-sync-refresh-finds-non-table-object ()
  "Sync refresh should discover non-table objects when cache is incomplete."
  (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
            ((symbol-function 'clutch--ensure-connection) (lambda () t))
            ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
            ((symbol-function 'thing-at-point) (lambda (&rest _) "PROCESS_ORDER"))
            ((symbol-function 'clutch--object-entries)
             (lambda (_conn &optional refresh)
               (if refresh
                   '((:name "PROCESS_ORDER" :type "PROCEDURE"))
                 nil)))
            ((symbol-function 'clutch-db-search-table-entries)
             (lambda (_conn _prefix) nil))
            ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) nil)))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--resolve-object-entry "Describe: ")
                     '(:name "PROCESS_ORDER" :type "PROCEDURE"))))))

(ert-deftest clutch-test-resolve-entry-merges-table-and-non-table-hits ()
  "On-demand search should merge table search and sync-refresh results."
  (let (captured-entries)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "ORDER"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional refresh)
                 (if refresh
                     '((:name "ORDERS" :type "TABLE")
                       (:name "ORDER_PROC" :type "PROCEDURE"))
                   nil)))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix)
                 '((:name "ORDERS" :type "TABLE"))))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) nil))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &optional _initial _cat)
                 (setq captured-entries entries)
                 (car entries))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        ;; Should have both TABLE and PROCEDURE hits
        (should (>= (length captured-entries) 2))
        (should (cl-some (lambda (e) (equal (plist-get e :type) "PROCEDURE"))
                         captured-entries))))))

(ert-deftest clutch-test-resolve-entry-prefix-matches-non-table-objects ()
  "Local prefix match should work for non-table objects too."
  (let (captured-initial)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "GET_"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh)
                 '((:name "GET_ORDER" :type "FUNCTION")
                   (:name "GET_CUSTOMER" :type "FUNCTION"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional initial _cat)
                 (setq captured-initial initial)
                 '(:name "GET_ORDER" :type "FUNCTION"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (equal captured-initial "GET_"))))))

(ert-deftest clutch-test-resolve-entry-table-like-only-skips-non-table-refresh ()
  "When table-like-only is set, on-demand search should not sync-refresh."
  (let (refresh-called)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "UNKNOWN_TBL"))
              ((symbol-function 'clutch--browseable-object-entries)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix) nil))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional refresh)
                 (when refresh (setq refresh-called t))
                 nil))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) nil))
              ((symbol-function 'message) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional _initial _cat) nil)))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Table: " t)
        (should-not refresh-called)))))

(ert-deftest clutch-test-object-at-point-prefers-table-over-public-synonym ()
  "Symbol resolution should prefer concrete schema objects over PUBLIC synonyms."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'thing-at-point)
               (lambda (&rest _) "ORDER_QUEUE"))
              ((symbol-function 'clutch--object-matches-by-name)
               (lambda (_conn name &optional _table-like-only _allowed-types)
                 (should (equal name "ORDER_QUEUE"))
                 '((:name "ORDER_QUEUE" :type "PUBLIC SYNONYM"
                    :schema "SYS" :source-schema "PUBLIC")
                   (:name "ORDER_QUEUE" :type "TABLE"
                    :schema "sales" :source-schema "sales")))))
      (should (equal (clutch-object-at-point)
                     '(:name "ORDER_QUEUE" :type "TABLE"
                       :schema "sales" :source-schema "sales"))))))

(ert-deftest clutch-test-object-entries-use-fast-partial-cache-on-first-open ()
  "Unified object entries should avoid synchronous full metadata scans."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        scheduled)
    (let ((by-type (make-hash-table :test 'equal)))
      (puthash "PROCEDURE"
               '((:name "PROCESS_ORDER" :type "PROCEDURE"
                  :schema "APP" :source-schema "APP"))
               by-type)
      (puthash "fake-key"
               (list :entries '((:name "PROCESS_ORDER" :type "PROCEDURE"
                                 :schema "APP" :source-schema "APP"))
                     :by-type by-type
                     :loaded-categories '(procedures))
               clutch--object-cache))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--browseable-object-entries)
               (lambda (_conn)
                 '((:name "ORDERS" :type "TABLE"
                    :schema "APP" :source-schema "APP"))))
              ((symbol-function 'clutch-db-list-objects)
               (lambda (&rest _args)
                 (ert-fail "should not synchronously load uncached object categories")))
              ((symbol-function 'run-with-idle-timer)
               (lambda (secs repeat fn)
                 (setq scheduled (list secs repeat fn))
                 'fake-timer)))
      (should (equal (clutch--object-entries 'fake-conn)
                     '((:name "ORDERS" :type "TABLE"
                        :schema "APP" :source-schema "APP")
                       (:name "PROCESS_ORDER" :type "PROCEDURE"
                        :schema "APP" :source-schema "APP"))))
      (when scheduled
        (should (equal (car scheduled) clutch-object-warmup-idle-delay-seconds))))))

(ert-deftest clutch-test-object-warmup-prefers-async-object-loading-when-available ()
  "Warmup should use backend async object loading when supported."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        async-call sync-called timer-fn)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (_conn category callback &optional _errback)
                 (setq async-call category)
                 (funcall callback
                          '((:name "ORDER_IDX" :type "INDEX"
                             :schema "APP" :source-schema "APP")))
                 t))
              ((symbol-function 'clutch--store-object-cache-type-entries)
               (lambda (_conn type entries)
                 (setq async-call (list async-call type entries))))
              ((symbol-function 'clutch--object-type-entries)
               (lambda (&rest _args)
                 (setq sync-called t)
                 nil)))
      (clutch--schedule-object-warmup 'fake-conn)
      (should timer-fn)
      (funcall timer-fn)
      (should (equal async-call
                     '(indexes "INDEX" ((:name "ORDER_IDX" :type "INDEX"
                                 :schema "APP" :source-schema "APP")))))
      (should-not sync-called))))

(ert-deftest clutch-test-object-warmup-reschedules-when-background-work-should-defer ()
  "Warmup should reschedule while the connection is busy or foreground-active."
  (dolist (case '(busy foreground-active))
    (ert-info ((format "case: %s" case))
      (let ((clutch--object-cache (make-hash-table :test 'equal))
            (clutch--object-warmup-timers (make-hash-table :test 'equal))
            (clutch--object-warmup-generations (make-hash-table :test 'equal))
            (clutch-db--foreground-connections (make-hash-table :test 'eq))
            timer-fns async-called sync-called)
        (when (eq case 'foreground-active)
          (puthash 'fake-conn t clutch-db--foreground-connections))
        (cl-letf (((symbol-function 'clutch--object-cache-key)
                   (lambda (_conn) "fake-key"))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--object-cache-loaded-categories)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) (eq case 'busy)))
                  ((symbol-function 'run-with-idle-timer)
                   (lambda (_secs _repeat fn)
                     (push fn timer-fns)
                     (intern (format "fake-timer-%d" (length timer-fns)))))
                  ((symbol-function 'clutch-db-list-objects-async)
                   (lambda (&rest _args)
                     (setq async-called t)
                     t))
                  ((symbol-function 'clutch--object-type-entries)
                   (lambda (&rest _args)
                     (setq sync-called t)
                     nil)))
          (clutch--schedule-object-warmup 'fake-conn)
          (should (= (length timer-fns) 1))
          (funcall (car timer-fns))
          (should (= (length timer-fns) 2))
          (should-not async-called)
          (should-not sync-called))))))

(ert-deftest clutch-test-object-warmup-reschedules-on-recoverable-db-errors ()
  "Warmup should reschedule when background metadata fetch hits `clutch-db-error'."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fns
        warned)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--warn-completion-metadata-error-once)
               (lambda (message-text)
                 (setq warned message-text)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (push fn timer-fns)
                 (intern (format "fake-timer-%d" (length timer-fns)))))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (&rest _args)
                 nil))
              ((symbol-function 'clutch--object-type-entries)
               (lambda (&rest _args)
                 (signal 'clutch-db-error '("warmup boom")))))
      (clutch--schedule-object-warmup 'fake-conn)
      (should (= (length timer-fns) 1))
      (funcall (car timer-fns))
      (should (= (length timer-fns) 2))
      (should (string-match-p "warmup boom" warned)))))

(ert-deftest clutch-test-object-warmup-records-debug-event-on-recoverable-db-errors ()
  "Warmup metadata failures should feed the shared debug trace when enabled."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fns)
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--object-cache-key)
                   (lambda (_conn) "fake-key"))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--object-cache-loaded-categories)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch--warn-completion-metadata-error-once)
                   #'ignore)
                  ((symbol-function 'run-with-idle-timer)
                   (lambda (_secs _repeat fn)
                     (push fn timer-fns)
                     (intern (format "fake-timer-%d" (length timer-fns)))))
                  ((symbol-function 'clutch-db-list-objects-async)
                   (lambda (&rest _args)
                     nil))
                  ((symbol-function 'clutch--object-type-entries)
                   (lambda (&rest _args)
                     (signal 'clutch-db-error '("warmup boom"))))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (clutch--clear-debug-capture)
          (clutch--schedule-object-warmup 'fake-conn)
          (should (= (length timer-fns) 1))
          (funcall (car timer-fns))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: object-warmup" text))
            (should (string-match-p "Phase: warning" text))
            (should (string-match-p "warmup boom" text))))))))

(ert-deftest clutch-test-object-warmup-warns-on-connection-liveness-errors ()
  "Warmup should warn instead of silently hiding recoverable liveness failures."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        warned
        scheduled)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn)
                 (signal 'clutch-db-error '("alive boom"))))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--warn-completion-metadata-error-once)
               (lambda (message-text)
                 (setq warned message-text)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (&rest _args)
                 (setq scheduled t)
                 'fake-timer)))
      (clutch--schedule-object-warmup 'fake-conn)
      (should (string-match-p "alive boom" warned))
      (should-not scheduled))))

(ert-deftest clutch-test-safe-completion-call-records-debug-event-on-db-errors ()
  "Recoverable completion metadata errors should surface in the debug buffer."
  (with-temp-buffer
    (let ((clutch-debug-mode t))
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'oracle))
                ((symbol-function 'clutch--warn-completion-metadata-error-once)
                 #'ignore)
                ((symbol-function 'pop-to-buffer)
                 (lambda (buf &rest _args) buf)))
        (clutch--clear-debug-capture)
        (should-not
         (clutch--safe-completion-call
          (lambda ()
            (signal 'clutch-db-error '("completion boom")))))
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Operation: completion" text))
          (should (string-match-p "Phase: warning" text))
          (should (string-match-p "completion boom" text)))))))

(ert-deftest clutch-test-object-warmup-propagates-non-db-errors ()
  "Warmup should still expose programming errors that are not db-runtime races."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fn)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (&rest _args)
                 nil))
              ((symbol-function 'clutch--object-type-entries)
               (lambda (&rest _args)
                 (error "Warmup boom"))))
      (clutch--schedule-object-warmup 'fake-conn)
      (should timer-fn)
      (should-error (funcall timer-fn) :type 'error))))

(ert-deftest clutch-test-object-warmup-ignores-stale-async-callbacks ()
  "Warmup should ignore async object results after invalidation."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fn
        async-callback
        stored)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (_conn _category callback &optional _errback)
                 (setq async-callback callback)
                 t))
              ((symbol-function 'clutch--store-object-cache-type-entries)
               (lambda (_conn _type entries)
                 (setq stored entries))))
      (clutch--schedule-object-warmup 'fake-conn)
      (should timer-fn)
      (funcall timer-fn)
      (should async-callback)
      (clutch--invalidate-object-warmup 'fake-conn)
      (funcall async-callback
               '((:name "ORDER_IDX" :type "INDEX"
                  :schema "APP" :source-schema "APP")))
      (should-not stored))))

(ert-deftest clutch-test-object-warmup-records-submit-success-and-stale-debug-events ()
  "Async object warmup should record submit, success, and stale-drop phases."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fn
        async-callback)
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--object-cache-key)
                   (lambda (_conn) "fake-key"))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'mysql))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--object-cache-loaded-categories)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'run-with-idle-timer)
                   (lambda (_secs _repeat fn)
                     (setq timer-fn fn)
                     'fake-timer))
                  ((symbol-function 'clutch-db-list-objects-async)
                   (lambda (_conn _category callback &optional _errback)
                     (setq async-callback callback)
                     t))
                  ((symbol-function 'clutch--store-object-cache-type-entries)
                   (lambda (&rest _args) nil))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (clutch--clear-debug-capture)
          (clutch--schedule-object-warmup 'fake-conn)
          (should timer-fn)
          (funcall timer-fn)
          (should async-callback)
          (funcall async-callback
                   '((:name "ORDER_IDX" :type "INDEX"
                      :schema "APP" :source-schema "APP")))
          (clutch--schedule-object-warmup 'fake-conn)
          (funcall timer-fn)
          (clutch--invalidate-object-warmup 'fake-conn)
          (funcall async-callback
                   '((:name "STALE_IDX" :type "INDEX"
                      :schema "APP" :source-schema "APP")))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: object-warmup" text))
            (should (string-match-p "Phase: submit" text))
            (should (string-match-p "Phase: success" text))
            (should (string-match-p "Phase: stale-drop" text))))))))

(ert-deftest clutch-test-object-cache-key-propagates-connection-key-errors ()
  "Object cache key should not silently hide connection-key failures."
  (cl-letf (((symbol-function 'clutch--connection-key)
             (lambda (_conn)
               (signal 'error '("cache-key boom")))))
    (should-error (clutch--object-cache-key 'fake-conn) :type 'error)))

(ert-deftest clutch-test-object-cache-key-falls-back-on-nil-connection-key ()
  "Object cache key should still fall back when connection-key returns nil."
  (cl-letf (((symbol-function 'clutch--connection-key)
             (lambda (_conn) nil)))
    (should (equal (clutch--object-cache-key 'fake-conn)
                   "fake-conn"))))

(ert-deftest clutch-test-browseable-object-entries-skip-empty-search-for-oracle-jdbc ()
  "Oracle JDBC browseable entries should not issue an extra empty-prefix search."
  (cl-letf (((symbol-function 'clutch-db-browseable-object-entries)
             (lambda (_conn)
               '((:name "ORDERS" :type "TABLE")))))
    (should (equal (clutch--browseable-object-entries 'fake-conn)
                   '((:name "ORDERS" :type "TABLE"))))))

(ert-deftest clutch-test-object-entry-reader-keeps-overloaded-object-names ()
  "Object reader should keep duplicate names distinct when identity differs."
  (let (captured)
    (cl-letf (((symbol-function 'completing-read)
               (lambda (_prompt collection &rest _args)
                 (setq captured (funcall collection "" nil t))
                 (car captured))))
      (should (equal (clutch--object-entry-reader
                      'fake-conn
                      "Object: "
                      '((:name "process_order" :type "FUNCTION"
                         :schema "public" :source-schema "public"
                         :identity "OID:1")
                        (:name "process_order" :type "FUNCTION"
                         :schema "public" :source-schema "public"
                         :identity "OID:2")))
                     '(:name "process_order" :type "FUNCTION"
                       :schema "public" :source-schema "public"
                       :identity "OID:1")))
      (should (= (length captured) 2))
      (should-not (equal (car captured) (cadr captured))))))

(ert-deftest clutch-test-object-entry-annotation-shows-detail-only-for-duplicates ()
  "Object entry detail should only appear when duplicate names need disambiguation."
  (let ((duplicate-counts (make-hash-table :test 'equal)))
    (puthash "ORDERS" 1 duplicate-counts)
    (should (equal (substring-no-properties
                    (clutch--object-entry-annotation
                     '(:name "ORDERS" :type "SYNONYM"
                       :schema "DATA_OWNER" :source-schema "APP")
                     duplicate-counts))
                   "  APP/synonym"))
    (puthash "ORDERS" 2 duplicate-counts)
    (should (equal (substring-no-properties
                    (clutch--object-entry-annotation
                     '(:name "ORDERS" :type "SYNONYM"
                       :schema "DATA_OWNER" :source-schema "APP")
                     duplicate-counts))
                   "  APP/synonym  DATA_OWNER"))))

(ert-deftest clutch-test-object-resolve-prefers-definition-buffer-object-over-symbol-at-point ()
  "Definition buffers should use the displayed object before `symbol-at-point'."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch-browser-current-object '(:name "ORDER_QUEUE" :type "TABLE"))
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'thing-at-point)
               (lambda (&rest _) "ORDER_QUEUE_ARCHIVE"))
              ((symbol-function 'clutch--object-matches-by-name)
               (lambda (_conn name &optional _table-like-only _allowed-types)
                 (should (equal name "ORDER_QUEUE_ARCHIVE"))
                 '((:name "ORDER_QUEUE_ARCHIVE" :type "TABLE")))))
      (should (equal (clutch--resolve-object-entry "Referenced by: ")
                     '(:name "ORDER_QUEUE" :type "TABLE"))))))

;;;; Object — Embark integration

(ert-deftest clutch-test-embark-object-target-uses-object-at-point ()
  "Embark target finder should expose the object at point."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local major-mode 'clutch-mode)
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-object-at-point)
               (lambda () '(:name "orders" :type "TABLE")))
              ((symbol-function 'bounds-of-thing-at-point)
               (lambda (_thing) '(1 . 7))))
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "orders" :type "TABLE") 1 7))))))

(ert-deftest clutch-test-embark-object-target-prefers-definition-buffer-object ()
  "Embark target finder should use the buffer's current object in definition buffers."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local major-mode 'sql-mode)
    (setq-local clutch-browser-current-object '(:name "PROCESS_ORDER" :type "PROCEDURE"))
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-object-at-point)
               (lambda () nil)))
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "PROCESS_ORDER" :type "PROCEDURE")))))))

(ert-deftest clutch-test-embark-object-target-prefers-dispatch-entry ()
  "Embark target finder should use the explicit dispatch entry when present."
  (let ((clutch--object-dispatch-entry '(:name "ORDERS" :type "TABLE")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "ORDERS" :type "TABLE")))))))

(ert-deftest clutch-test-embark-object-target-uses-target-capable-type ()
  "Embark target finder should expose a distinct target type for target-capable objects."
  (let ((clutch--object-dispatch-entry
         '(:name "ORDER_IDX" :type "INDEX" :target-table "ORDERS")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--embark-object-target)
                     '(clutch-target-object
                       (:name "ORDER_IDX" :type "INDEX" :target-table "ORDERS")))))))

(ert-deftest clutch-test-embark-around-hook-resolves-candidate ()
  "Around-action hook should resolve target string to entry via completion map."
  (let* ((entry '(:name "xxx_detail" :type "TABLE" :schema "public"))
         (clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx_detail" entry clutch--object-completion-entry-map)
    ;; Simulate what Embark passes to around hooks.
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "xxx_detail"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (equal resolved entry)))))

(ert-deftest clutch-test-embark-around-hook-clears-map ()
  "Around-action hook should clear the completion map after resolution."
  (let ((clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx" '(:name "xxx" :type "TABLE") clutch--object-completion-entry-map)
    (clutch--embark-with-resolved-entry
     :target "xxx"
     :run (lambda (&rest _) nil))
    ;; The global should be nil after the hook returns.
    (should (null (symbol-value 'clutch--object-completion-entry-map)))))

(ert-deftest clutch-test-embark-around-hook-nil-for-unknown-target ()
  "Around-action hook should leave dispatch entry nil for unknown targets."
  (let* ((clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx" '(:name "xxx" :type "TABLE") clutch--object-completion-entry-map)
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "yyy"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (null resolved)))))

(ert-deftest clutch-test-embark-around-hook-nil-without-map ()
  "Around-action hook should be a no-op when no completion map exists."
  (let ((clutch--object-completion-entry-map nil))
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "xxx"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (null resolved)))))

(ert-deftest clutch-test-resolve-object-entry-prefers-dispatch-entry ()
  "Resolution should prefer dispatch entry over point matches."
  (let ((clutch--object-dispatch-entry '(:name "xxx_detail" :type "TABLE")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      ;; Insert a different object name at point to prove dispatch wins.
      (insert "xxx")
      (goto-char (point-min))
      (should (equal (clutch--resolve-object-entry "Test: ")
                     '(:name "xxx_detail" :type "TABLE"))))))

(ert-deftest clutch-test-embark-action-specs-hide-default-actions ()
  "Embark menus should omit actions that duplicate `RET'."
  (should (equal (mapcar (lambda (spec) (plist-get spec :id))
                         (clutch--embark-action-specs
                          (lambda (spec)
                            (not (eq (plist-get spec :id) 'jump-target)))))
                 '(describe show-definition list-indexes explain-sample
                            copy-name copy-fqname))))

(ert-deftest clutch-test-embark-target-action-specs-keep-jump-target ()
  "Target-capable Embark menus should keep jump-target."
  (should (equal (mapcar (lambda (spec) (plist-get spec :id))
                         (clutch--embark-action-specs))
                 '(describe show-definition list-indexes explain-sample
                            jump-target copy-name copy-fqname))))

(ert-deftest clutch-test-embark-command-label-uses-shared-label ()
  "Embark command labels should reuse the shared object action wording."
  (should (equal (clutch--embark-command-label 'clutch-object-show-ddl-or-source)
                 "Show definition"))
  (should (equal (clutch--embark-command-label 'clutch-object-list-indexes)
                 "List indexes"))
  (should (equal (clutch--embark-command-label
                  'clutch-object-explain-sample-query)
                 "Explain sample query"))
  (should (equal (clutch--embark-command-label 'clutch-object-default-action)
                 "Default action")))

;;;; Object — schema switching

(ert-deftest clutch-test-switch-schema-uses-selected-schema-and-refreshes ()
  "Schema switching should update params, clear caches, and refresh metadata."
  (let ((conn 'fake-conn)
        switched refresh-called message-text)
    (with-temp-buffer
      (setq-local clutch-connection conn
                  clutch--connection-params '(:driver oracle :schema "SALES"))
      (cl-letf (((symbol-function 'clutch-db-list-schemas)
                 (lambda (_conn) '("SALES" "ANALYTICS")))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "user@host:1521/ORCL"))
                ((symbol-function 'clutch-db-current-schema)
                 (lambda (_conn) "SALES"))
                ((symbol-function 'completing-read)
                 (lambda (&rest _args) "ANALYTICS"))
                ((symbol-function 'clutch-db-set-current-schema)
                 (lambda (_conn schema) (setq switched schema) schema))
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn &optional _key) (setq refresh-called 'cleared)))
                ((symbol-function 'clutch--refresh-current-schema)
                 (lambda (&optional quiet)
                   (setq refresh-called (list refresh-called quiet))
                   t))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq message-text (apply #'format fmt args)))))
        (clutch-switch-schema)
        (should (equal switched "ANALYTICS"))
        (should (equal clutch--connection-params '(:driver oracle :schema "ANALYTICS")))
        (should (equal refresh-called '(cleared t)))
        (should (equal message-text "Current schema: ANALYTICS"))))))

(ert-deftest clutch-test-switch-schema-failure-populates-problem-record-and-debug-trace ()
  "Schema-switch failures should feed the shared problem/debug workflow."
  (let ((details '(:backend oracle
                   :summary "ORA-12592: TNS:bad packet"
                   :diag (:category "metadata"
                          :op "set-current-schema"
                          :conn-id 7
                          :raw-message "ORA-12592: TNS:bad packet"
                          :context (:generated-sql
                                    "ALTER SESSION SET CURRENT_SCHEMA = \"ANALYTICS\"")))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn
                    clutch--connection-params '(:driver oracle :schema "SALES"))
        (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "user@host:1521/ORCL"))
                  ((symbol-function 'clutch-db-list-schemas)
                   (lambda (_conn) '("SALES" "ANALYTICS")))
                  ((symbol-function 'clutch-db-current-schema)
                   (lambda (_conn) "SALES"))
                  ((symbol-function 'completing-read)
                   (lambda (&rest _args) "ANALYTICS"))
                  ((symbol-function 'clutch-db-set-current-schema)
                   (lambda (_conn _schema)
                     (signal 'clutch-db-error
                             (list "ORA-12592: TNS:bad packet" details)))))
          (condition-case err
              (progn
                (clutch-switch-schema)
                (should nil))
            (user-error
             (should (string-match-p "ORA-12592" (cadr err)))
             (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                     (cadr err)))))
          (let ((problem clutch--buffer-error-details))
            (should problem)
            (should (equal (plist-get problem :summary) "ORA-12592: TNS:bad packet"))
            (should (eq (plist-get problem :backend) 'oracle)))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "set-current-schema" text))
            (should (string-match-p "Operation: schema-switch" text))
            (should (string-match-p "Phase: error" text))
            (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" text))))))))

(ert-deftest clutch-test-switch-schema-updates-mysql-database-param ()
  "MySQL schema switching should persist the selected database in buffer params."
  (let ((conn 'fake-conn)
        switched cleared-keys)
    (with-temp-buffer
      (setq-local clutch-connection conn
                  clutch--connection-params '(:backend mysql :database "sales"))
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (_conn)
                   (if (equal clutch--connection-params '(:backend mysql :database "sales"))
                       "user@host:3306/sales"
                     "user@host:3306/analytics")))
                ((symbol-function 'clutch-db-list-schemas)
                 (lambda (_conn) '("sales" "analytics")))
                ((symbol-function 'clutch-db-current-schema)
                 (lambda (_conn) "sales"))
                ((symbol-function 'completing-read)
                 (lambda (&rest _args) "analytics"))
                ((symbol-function 'clutch-db-set-current-schema)
                 (lambda (_conn schema) (setq switched schema) schema))
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn &optional key)
                   (push (or key "current") cleared-keys)))
                ((symbol-function 'clutch--refresh-current-schema)
                 (lambda (&optional _quiet) t))
                ((symbol-function 'message) (lambda (&rest _args) nil)))
        (clutch-switch-schema)
        (should (equal switched "analytics"))
        (should (equal clutch--connection-params '(:backend mysql :database "analytics")))
        (should (equal cleared-keys '("current" "user@host:3306/sales")))))))

(ert-deftest clutch-test-switch-schema-clickhouse-reconnects-to-selected-database ()
  "ClickHouse switching should reconnect with the selected database."
  (let ((old-conn 'old-conn)
        (new-conn 'new-conn)
        built-params disconnected primed tx-cleared cleared-keys message-text)
    (with-temp-buffer
      (setq-local clutch-connection old-conn
                  clutch--connection-params '(:backend clickhouse
                                              :host "127.0.0.1"
                                              :port 8123
                                              :database "default"
                                              :user "default"))
      (cl-letf (((symbol-function 'clutch--list-clickhouse-databases)
                 (lambda (_conn) '("default" "demo")))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 (lambda (&rest _) t))
                ((symbol-function 'completing-read)
                 (lambda (&rest _args) "demo"))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built-params params)
                   new-conn))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (conn) (setq disconnected conn)))
                ((symbol-function 'clutch--prime-schema-cache)
                 (lambda (conn) (setq primed conn)))
                ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                ((symbol-function 'clutch--clear-tx-dirty)
                 (lambda (conn) (setq tx-cleared conn)))
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn &optional key)
                   (push (or key "current") cleared-keys)))
                ((symbol-function 'clutch--connection-key)
                 (lambda (conn)
                   (if (eq conn old-conn)
                       "default-key"
                     "demo-key")))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq message-text (apply #'format fmt args)))))
        (clutch-switch-schema)
        (should (eq clutch-connection new-conn))
        (should (equal built-params '(:backend clickhouse
                                      :host "127.0.0.1"
                                      :port 8123
                                      :database "demo"
                                      :user "default")))
        (should (equal clutch--connection-params '(:backend clickhouse
                                                   :host "127.0.0.1"
                                                   :port 8123
                                                   :database "demo"
                                                   :user "default")))
        (should (eq disconnected old-conn))
        (should (eq primed new-conn))
        (should (eq tx-cleared old-conn))
        (should (equal cleared-keys '("current" "default-key")))
        (should (equal message-text "Current database: demo"))))))

(ert-deftest clutch-test-switch-schema-header-line-shows-current-schema ()
  "Connection header line should display a non-default effective schema."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[schema]"))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) nil))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "Oracle"))
              ((symbol-function 'clutch--connection-display-key) (lambda (_conn) "scott@dbhost"))
              ((symbol-function 'clutch-db-user) (lambda (_conn) "SCOTT"))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "ORCL"))
              ((symbol-function 'clutch-db-current-schema) (lambda (_conn) "SALES"))
              ((symbol-function 'clutch--schema-status-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (clutch--build-connection-header-line)))
        (should (string-match-p "scott@dbhost" line))
        (should (string-match-p "\\[schema\\] SALES" line))
        (should-not (string-match-p "Schema:" line))))))


(provide 'clutch-test-object)

;;; clutch-test-object.el ends here
