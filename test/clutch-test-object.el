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

(defmacro clutch-test-object--with-warmup-state (&rest body)
  "Run BODY with fresh object cache and warmup scheduler state."
  (declare (indent 0) (debug (body)))
  `(let ((clutch--object-cache (make-hash-table :test 'equal))
         (clutch--object-warmup-timers (make-hash-table :test 'equal))
         (clutch--object-warmup-generations (make-hash-table :test 'equal)))
     ,@body))

;;;; Object — browse and actions

(defun clutch-test-object--render-action-menu (entry &optional conn supported-actions)
  "Return rendered object action menu text for ENTRY.
CONN is bound as the current connection.  SUPPORTED-ACTIONS, when non-nil,
controls backend-specific object action availability."
  (when-let* ((buf (get-buffer " *transient*")))
    (kill-buffer buf))
  (unwind-protect
      (with-temp-buffer
        (setq-local clutch-connection conn)
        (cl-letf (((symbol-function 'clutch-db-object-action-supported-p)
                   (lambda (_conn _entry action-id)
                     (memq action-id supported-actions))))
          (let ((clutch--object-action-entry entry))
            (transient-setup 'clutch-object-actions-menu)
            (with-current-buffer " *transient*"
              (buffer-string)))))
    (when-let* ((buf (get-buffer " *transient*")))
      (kill-buffer buf))))

(defun clutch-test-object--capture-collection-action
    (command action-id payload &optional conn backend)
  "Run collection COMMAND and return captured object text display arguments.
ACTION-ID controls capability support.  PAYLOAD is the raw metadata text the
backend action returns.  CONN and BACKEND default to a MongoDB-flavored native
document connection."
  (let ((conn (or conn 'mongo-conn))
        (backend (or backend 'mongodb))
        captured)
    (with-temp-buffer
      (setq-local clutch-connection conn
                  clutch--connection-params nil
                  clutch--conn-sql-product nil)
      (clutch-test--with-connection-data-model (conn backend 'document)
        (cl-letf (((symbol-function 'clutch-db-object-action-supported-p)
                   (lambda (actual-conn entry candidate)
                     (should (eq actual-conn conn))
                     (should (equal (plist-get entry :name) "users"))
                     (eq candidate action-id)))
                  ((symbol-function 'clutch-db-object-action-metadata)
                   (lambda (actual-conn entry candidate)
                     (should (eq actual-conn conn))
                     (should (equal (plist-get entry :name) "users"))
                     (should (eq candidate action-id))
                     payload))
                  ((symbol-function 'clutch--show-object-text-buffer)
                   (lambda (actual-conn entry text
                                        &optional params product title-suffix)
                     (setq captured
                           (list actual-conn entry text
                                 params product title-suffix))))
                  ((symbol-function 'clutch--clear-connection-problem-capture)
                   #'ignore)
                  ((symbol-function 'clutch--remember-current-object)
                   #'ignore))
          (funcall command '(:name "users" :type "COLLECTION")))))
    captured))

(ert-deftest clutch-test-object-browse-contract ()
  "Object browse should validate entries and insert SELECT in the console."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--find-console-for-conn)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) name)))
      (should-error (clutch-object-browse '(:name "users" :type "TABLE"))
                    :type 'user-error)))
  (should-error
   (clutch-object-browse '(:name "order_idx" :type "INDEX"))
   :type 'user-error
   :exclude-subtypes nil)
  (dolist (case
           (list
            (list :label "append after existing SQL"
                  :initial "SELECT 1;"
                  :entry '(:name "order-items" :type "TABLE")
                  :suffix "\n\nSELECT * FROM \"order-items\";")
            (list :label "source schema qualification"
                  :entry '(:name "background_schedule_pool_log"
                           :type "TABLE"
                           :source-schema "system")
                  :expected
                  "SELECT * FROM \"system\".\"background_schedule_pool_log\";")
            (list :label "object schema before discovery schema"
                  :entry '(:name "ORDERS"
                           :type "SYNONYM"
                           :schema "DATA_OWNER"
                           :source-schema "APP")
                  :expected "SELECT * FROM \"DATA_OWNER\".\"ORDERS\";")
            (list :label "clickhouse simple identifiers"
                  :conn (make-clutch-jdbc-conn :params '(:driver clickhouse))
                  :entry '(:name "background_schedule_pool_log"
                           :type "TABLE"
                           :source-schema "system")
                  :expected
                  "SELECT * FROM system.background_schedule_pool_log;")
            (list :label "empty console first line"
                  :initial "\n\n"
                  :entry '(:name "order-items" :type "TABLE")
                  :expected "SELECT * FROM \"order-items\";")
            (list :label "insert around point"
                  :initial "SELECT 1;\nSELECT 2;"
                  :point-line 1
                  :entry '(:name "order-items" :type "TABLE")
                  :expected (concat "SELECT 1;\n\n"
                                    "SELECT * FROM \"order-items\";\n\n"
                                    "SELECT 2;"))
            (list :label "reuse existing blank separator"
                  :initial "SELECT 1;\n\n"
                  :entry '(:name "order-items" :type "TABLE")
                  :expected "SELECT 1;\n\nSELECT * FROM \"order-items\";")))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (let ((console (generate-new-buffer " *clutch-test-object-browse*"))
            (escape-fn (unless (plist-get case :conn)
                         (lambda (_conn name) (format "\"%s\"" name))))
            (orig-escape (symbol-function 'clutch-db-escape-identifier)))
        (unwind-protect
            (with-current-buffer console
              (insert (or (plist-get case :initial) ""))
              (setq-local clutch-connection (or (plist-get case :conn)
                                                'fake-conn))
              (setq-local clutch--query-buffer-local-p t)
              (when (plist-member case :point-line)
                (goto-char (point-min))
                (forward-line (plist-get case :point-line)))
              (cl-letf (((symbol-function 'derived-mode-p)
                         (lambda (&rest modes) (memq 'clutch-mode modes)))
                        ((symbol-function 'pop-to-buffer)
                         (lambda (buf &rest _args)
                           (set-buffer buf)
                           buf))
                        ((symbol-function 'clutch-db-escape-identifier)
                         (or escape-fn orig-escape)))
                (clutch-object-browse (plist-get case :entry))
                (if-let* ((expected (plist-get case :expected)))
                    (should (equal (buffer-string) expected))
                  (should (string-suffix-p (plist-get case :suffix)
                                           (buffer-string))))))
          (when (buffer-live-p console)
            (kill-buffer console)))))))

(ert-deftest clutch-test-object-default-action-routing-contract ()
  "Default action should browse table-like entries and show definitions otherwise."
  (dolist (case '((:entry (:name "orders" :type "TABLE")
                   :action browse)
                  (:entry (:name "users" :type "COLLECTION")
                   :action browse)
                  (:entry (:name "process_order" :type "PROCEDURE")
                   :action show-definition)))
    (ert-info ((plist-get (plist-get case :entry) :type))
      (let (browse-entry show-entry)
        (cl-letf (((symbol-function 'clutch-object-browse)
                   (lambda (entry) (setq browse-entry entry)))
                  ((symbol-function 'clutch-object-show-ddl-or-source)
                   (lambda (entry) (setq show-entry entry))))
          (clutch-object-default-action (plist-get case :entry))
          (pcase (plist-get case :action)
            ('browse
             (should (equal browse-entry (plist-get case :entry)))
             (should-not show-entry))
            ('show-definition
             (should-not browse-entry)
             (should (equal show-entry (plist-get case :entry))))))))))

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

(ert-deftest clutch-test-act-dwim-opens-transient-contract ()
  "Act-dwim should resolve or accept entries and open the shared action UI."
  (dolist (case '((:label "current object"
                   :resolved (:name "PROCESS_ORDER" :type "PROCEDURE"))
                  (:label "explicit entry"
                   :entry (:name "ORDERS" :type "TABLE"))))
    (ert-info ((plist-get case :label))
      (let (setup-command)
        (with-temp-buffer
          (setq-local clutch-connection 'fake-conn)
          (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
                     (lambda (&rest _)
                       (or (plist-get case :resolved)
                           (ert-fail "explicit entry should not resolve"))))
                    ((symbol-function 'transient-setup)
                     (lambda (command &rest _args)
                       (setq setup-command command))))
            (let ((clutch--object-action-entry nil))
              (if-let* ((entry (plist-get case :entry)))
                  (clutch-act-dwim entry)
                (clutch-act-dwim))
              (should (eq setup-command 'clutch-object-actions-menu))
              (should (equal clutch--object-action-entry
                             (or (plist-get case :entry)
                                 (plist-get case :resolved)))))))))))

(ert-deftest clutch-test-act-dwim-errors-without-action-ui ()
  "Act-dwim should not silently run the default action when no UI exists."
  (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
             (lambda (&rest _)
               '(:name "ORDERS" :type "TABLE")))
            ((symbol-function 'clutch--present-object-actions-natively)
             (lambda (_entry) nil)))
    (should-error (clutch-act-dwim) :type 'user-error)))

(ert-deftest clutch-test-object-action-inapt-flags-reflect-target-type ()
  "Object action transient flags should reflect the current target type."
  (with-temp-buffer
    (setq-local clutch-connection 'document-conn)
    (cl-letf (((symbol-function 'clutch-db-object-action-supported-p)
               (lambda (_conn entry action-id)
                 (and (equal (plist-get entry :type) "COLLECTION")
                      (eq action-id 'show-stats)))))
      (let ((clutch--object-action-entry '(:name "ORDERS" :type "TABLE")))
        (should-not (clutch--object-act-jump-target-p))
        (should (clutch--object-act-jump-target-inapt-p))
        (should-not (clutch--object-act-document-actions-p))
        (should (clutch--object-act-show-stats-inapt-p)))
      (let ((clutch--object-action-entry '(:name "ORDER_IDX" :type "INDEX")))
        (should (clutch--object-act-jump-target-p))
        (should-not (clutch--object-act-jump-target-inapt-p))
        (should-not (clutch--object-act-document-actions-p))
        (should (clutch--object-act-show-stats-inapt-p)))
      (let ((clutch--object-action-entry '(:name "users" :type "COLLECTION")))
        (should-not (clutch--object-act-jump-target-p))
        (should (clutch--object-act-jump-target-inapt-p))
        (should (clutch--object-act-document-actions-p))
        (should-not (clutch--object-act-show-stats-inapt-p))))))

(ert-deftest clutch-test-object-key-is-browseable-but-not-document-action-target ()
  "Redis KEY objects should browse without becoming document collections."
  (let ((entry '(:name "session:1" :type "KEY")))
    (should (clutch--table-like-entry-p entry))
    (should-not (clutch--document-collection-entry-p entry))))

(ert-deftest clutch-test-object-action-menu-hides-inapplicable-groups ()
  "Object action menu should hide groups that do not apply to the target."
  (let ((table-menu
         (clutch-test-object--render-action-menu
          '(:name "ORDERS" :type "TABLE")))
        (collection-menu
         (clutch-test-object--render-action-menu
          '(:name "users" :type "COLLECTION")
          'document-conn
          '(show-stats)))
        (index-menu
         (clutch-test-object--render-action-menu
          '(:name "idx_users" :type "INDEX" :target-table "users"))))
    (should-not (string-match-p "Document" table-menu))
    (should-not (string-match-p "Navigate" table-menu))
    (should (string-match-p "Document" collection-menu))
    (should (string-match-p "Show stats" collection-menu))
    (should-not (string-match-p "Navigate" collection-menu))
    (should (string-match-p "Navigate" index-menu))
    (should-not (string-match-p "Document" index-menu))))

(ert-deftest clutch-test-object-collection-actions-display-metadata ()
  "Collection metadata actions should display backend-provided JSON payloads."
  (dolist (case
           (list
            (list :label "index insight"
                  :command #'clutch-object-show-index-insight
                  :action-id 'index-insight
                  :payload "{\"indexes\":[{\"name\":\"_id_\"}]}"
                  :expected-text
                  "{\n  \"indexes\": [\n    {\n      \"name\": \"_id_\"\n    }\n  ]\n}"
                  :title-suffix "index insight")
            (list :label "explain"
                  :command #'clutch-object-explain-sample-query
                  :action-id 'explain-sample
                  :payload "{\"summary\":{\"collectionScan\":true}}"
                  :expected-text
                  "{\n  \"summary\": {\n    \"collectionScan\": true\n  }\n}"
                  :title-suffix "explain")
            (list :label "validation"
                  :command #'clutch-object-show-validation
                  :action-id 'show-validation
                  :payload "{\"configured\":true}"
                  :expected-text "{\n  \"configured\": true\n}"
                  :title-suffix "validation")
            (list :label "stats"
                  :command #'clutch-object-show-stats
                  :action-id 'show-stats
                  :payload "{\"count\":3,\"storageSize\":20480}"
                  :conn 'document-conn
                  :backend 'couchdb
                  :expected-text
                  "{\n  \"count\": 3,\n  \"storageSize\": 20480\n}"
                  :title-suffix "stats")))
    (ert-info ((plist-get case :label))
      (should
       (equal
        (clutch-test-object--capture-collection-action
         (plist-get case :command)
         (plist-get case :action-id)
         (plist-get case :payload)
         (plist-get case :conn)
         (plist-get case :backend))
        (list (or (plist-get case :conn) 'mongo-conn)
              '(:name "users" :type "COLLECTION")
              (plist-get case :expected-text)
              nil nil (plist-get case :title-suffix)))))))

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
               (lambda (_conn _entry &optional _params)
                 "PUBLIC.orders (TABLE)\n\nSummary\n  Name  orders")))
      (clutch--render-object-describe
       'fake-conn
       '(:name "orders" :type "TABLE" :source-schema "PUBLIC"))
      (should (string-match-p "PUBLIC.orders (TABLE)" (buffer-string)))
      (should (string-match-p "\\[desc\\]" clutch-describe--header-base))
      (should (string-match-p "show definition" clutch-describe--header-base))
      (should-not (string-match-p "PUBLIC.orders" clutch-describe--header-base)))))

(ert-deftest clutch-test-object-sql-name-never-produces-current-schema-literal ()
  "Browse SQL must use real schema metadata, never current_schema placeholders."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (case
             '(((:name "orders_large" :type "TABLE"
                 :schema "public" :source-schema "public")
                "\"public\".\"orders_large\"")
               ((:name "orders" :type "TABLE" :schema "public")
                "\"public\".\"orders\"")
               ((:name "orders" :type "TABLE")
                "\"orders\"")))
      (pcase-let ((`(,entry ,expected) case))
        (let ((sql-name (clutch--object-sql-name 'fake entry)))
          (should (equal sql-name expected))
          (should-not (string-match-p "current_schema" sql-name)))))))

(ert-deftest clutch-test-object-show-ddl-or-source-derives-oracle-sql-product-from-backend ()
  "Object definition buffers should use Oracle font-lock when backend is oracle."
  (let (captured-product)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-object-definition)
               (lambda (_conn _entry) "CREATE TABLE demo_tasks (id NUMBER)"))
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

(ert-deftest clutch-test-object-show-ddl-or-source-pretty-prints-mongodb-json ()
  "MongoDB object definition buffers should display formatted JSON metadata."
  (let (captured)
    (with-temp-buffer
      (setq-local clutch-connection 'mongo-conn
                  clutch--connection-params nil
                  clutch--conn-sql-product nil)
      (cl-letf (((symbol-function 'clutch-db-backend-key)
                 (lambda (_conn) 'mongodb))
                ((symbol-function 'clutch-backend-data-model)
                 (lambda (_backend) 'document))
                ((symbol-function 'clutch-db-object-definition)
                 (lambda (conn entry)
                   (should (eq conn 'mongo-conn))
                   (should (equal entry '(:name "users" :type "COLLECTION")))
                   "{\"name\":\"users\",\"type\":\"collection\"}"))
                ((symbol-function 'clutch--show-object-text-buffer)
                 (lambda (conn entry text &optional params product title-suffix)
                   (setq captured
                         (list conn entry text params product title-suffix))))
                ((symbol-function 'clutch--clear-connection-problem-capture)
                 #'ignore)
                ((symbol-function 'clutch--remember-current-object)
                 #'ignore))
        (clutch-object-show-ddl-or-source
         '(:name "users" :type "COLLECTION"))))
    (should (equal captured
                   '(mongo-conn
                     (:name "users" :type "COLLECTION")
                     "{\n  \"name\": \"users\",\n  \"type\": \"collection\"\n}"
                     nil nil nil)))))

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
                  ((symbol-function 'clutch-db-object-definition)
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

(ert-deftest clutch-test-render-object-describe-refreshes-table-metadata-cache ()
  "Describe rendering should reload table metadata instead of reusing stale fields."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--columns-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--table-comment-status-cache (make-hash-table :test 'equal))
        (detail-calls 0))
    (let ((schema (make-hash-table :test 'equal))
          (details (make-hash-table :test 'equal))
          (details-status (make-hash-table :test 'equal))
          (columns-status (make-hash-table :test 'equal))
          (comments (make-hash-table :test 'equal)))
      (puthash "USERS" '("id" "old_col") schema)
      (puthash "dev-key" schema clutch--schema-cache)
      (puthash "USERS" '((:name "id" :type "int")
                         (:name "old_col" :type "text"))
               details)
      (puthash "dev-key" details clutch--column-details-cache)
      (puthash "USERS" '(:state failed :error "old failure") details-status)
      (puthash "dev-key" details-status clutch--column-details-status-cache)
      (puthash "USERS" '(:state failed :error "old columns") columns-status)
      (puthash "dev-key" columns-status clutch--columns-status-cache)
      (puthash "USERS" "old comment" comments)
      (puthash "dev-key" comments clutch--table-comment-cache)
      (puthash "dev-key" '("USERS" "ORDERS")
               clutch--column-details-queue-cache)
      (puthash "dev-key" (cons "USERS" 7)
               clutch--column-details-active-cache)
      (cl-letf (((symbol-function 'clutch--command-connection-context)
                 (lambda () (list :connection clutch-connection)))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "dev-key"))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--clear-connection-problem-capture)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--ensure-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch-db-column-details)
                 (lambda (_conn table)
                   (should (equal table "USERS"))
                   (cl-incf detail-calls)
                   '((:name "id" :type "int")
                     (:name "new_col" :type "text"))))
                ((symbol-function 'clutch-db-list-columns)
                 (lambda (&rest _args)
                   (ert-fail "Describe should use fresh column details")))
                ((symbol-function 'clutch--object-related-entries)
                 (lambda (&rest _args) nil))
                ((symbol-function 'pop-to-buffer)
                 (lambda (buffer &rest _args)
                   (set-buffer buffer)
                   buffer)))
        (unwind-protect
            (with-temp-buffer
              (setq-local clutch-connection 'fake-conn)
              (clutch-object-describe '(:name "USERS" :type "TABLE"))
              (with-current-buffer "*clutch describe: USERS*"
                (let ((text (buffer-string)))
                  (should (string-match-p "new_col" text))
                  (should-not (string-match-p "old_col" text)))))
          (when-let* ((buf (get-buffer "*clutch describe: USERS*")))
            (kill-buffer buf)))
        (should (= detail-calls 1))
        (should-not (gethash "dev-key" clutch--column-details-active-cache))
        (should (equal (gethash "dev-key" clutch--column-details-queue-cache)
                       '("ORDERS")))
        (should-not (clutch--column-details-status 'fake-conn "USERS"))
        (should (eq (gethash "USERS" schema 'missing) nil))))))

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

(ert-deftest clutch-test-object-describe-mongodb-collection-renders-json ()
  "Native MongoDB collection describe should display formatted JSON metadata."
  (cl-letf (((symbol-function 'clutch-db-backend-key)
             (lambda (_conn) 'mongodb))
            ((symbol-function 'clutch-backend-data-model)
             (lambda (_backend) 'document))
            ((symbol-function 'clutch-db-collection-profile)
             (lambda (_conn collection)
               (should (equal collection "orders"))
               (concat
                "{\"collection\":\"orders\",\"database\":\"app\","
                "\"fields\":[{\"path\":\"_id\",\"typeCategory\":\"json\"},"
                "{\"path\":\"status\",\"comment\":\"present in 2/3 sampled documents\"}],"
                "\"indexes\":[{\"name\":\"_id_\",\"unique\":true},"
                "{\"name\":\"status_idx\",\"unique\":false}]}"))))
    (let ((text (clutch--object-describe-text
                 'mongo-conn
                 '(:name "orders" :type "COLLECTION" :schema "app"))))
      (should (string-prefix-p "{\n" text))
      (should (string-match-p "\"collection\": \"orders\"" text))
      (should (string-match-p "\"database\": \"app\"" text))
      (should (string-match-p "\"fields\": \\[" text))
      (should (string-match-p "\"typeCategory\": \"json\"" text))
      (should (string-match-p "present in 2/3 sampled documents" text))
      (should (string-match-p "\"indexes\": \\[" text))
      (should (string-match-p "\"unique\": true" text))
      (should (string-match-p "\"unique\": false" text))
      (should-not (string-match-p "^Fields" text)))))

(ert-deftest clutch-test-object-describe-mongodb-buffer-uses-json-mode ()
  "Native MongoDB describe buffers should use JSON display mode."
  (let (selected-mode fontified)
    (with-temp-buffer
      (cl-letf (((symbol-function 'clutch-db-backend-key)
                 (lambda (_conn) 'mongodb))
                ((symbol-function 'clutch-backend-data-model)
                 (lambda (_backend) 'document))
                ((symbol-function 'json-ts-mode)
                 (lambda () (ert-fail "json-ts-mode should not run without a JSON grammar")))
                ((symbol-function 'treesit-language-available-p)
                 (lambda (_language) nil))
                ((symbol-function 'json-mode)
                 (lambda () (setq selected-mode 'json-mode)))
                ((symbol-function 'clutch--bind-connection-context)
                 #'ignore)
                ((symbol-function 'clutch--icon-with-face)
                 (lambda (&rest _args) ""))
                ((symbol-function 'clutch--object-describe-text)
                 (lambda (_conn _entry &optional _params)
                   "{\n  \"name\": \"users\"\n}"))
                ((symbol-function 'clutch--fontify-object-describe)
                 (lambda () (ert-fail
                             "MongoDB JSON describe should not use table fontification")))
                ((symbol-function 'font-lock-ensure)
                 (lambda () (setq fontified t))))
        (clutch--render-object-describe
         'mongo-conn '(:name "users" :type "COLLECTION"))
        (should (eq selected-mode 'json-mode))
        (should fontified)
        (should buffer-read-only)
        (should (eq (lookup-key (current-local-map) (kbd "s"))
                    #'clutch-object-show-ddl-or-source))
        (should (eq (lookup-key (current-local-map) (kbd "g"))
                    #'clutch-describe-refresh))
        (should (eq (lookup-key (current-local-map) (kbd "C-c C-d"))
                    #'clutch-describe-dwim))
        (should (eq (lookup-key (current-local-map) (kbd "C-c C-o"))
                    #'clutch-act-dwim))))))

(ert-deftest clutch-test-object-describe-failure-records-problem-and-debug-trace ()
  "Describe failures should record source-buffer problems and optional debug trace."
  (dolist (debug-p '(nil t))
    (ert-info ((format "debug: %s" debug-p))
      (let ((clutch--column-details-cache (make-hash-table :test 'equal))
            (clutch--column-details-status-cache (make-hash-table :test 'equal))
            (details '(:backend jdbc
                       :summary "ORA-12592: TNS:bad packet"
                       :diag (:category "metadata"
                              :op "get-columns"
                              :conn-id 7
                              :raw-message "ORA-12592: TNS:bad packet"))))
        (with-temp-buffer
          (let ((clutch-debug-mode debug-p))
            (when debug-p
              (clutch--clear-debug-capture))
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
                                 (list "ORA-12592: TNS:bad packet"
                                       details))))
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
                (should (string-match-p "ORA-12592"
                                        (plist-get problem :summary)))
                (should (equal (plist-get diag :op) "get-columns")))
              (when debug-p
                (let ((text (clutch-test--debug-buffer-string)))
                  (should (string-match-p "Operation: describe" text))
                  (should (string-match-p "Phase: error" text))
                  (should (string-match-p "USERS" text)))))))))))

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
                     (lambda (_conn _entry &optional _params) "ok"))
                    ((symbol-function 'clutch--refresh-object-describe-metadata)
                     (lambda (&rest _args) nil))
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
              ((symbol-function 'clutch--refresh-object-describe-metadata)
               (lambda (&rest _args) nil))
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

(ert-deftest clutch-test-jump-default-action-contract ()
  "Jump should resolve once and run the shared default action."
  (ert-info ("table default browse")
    (let (resolved-prompt resolved-category resolved-types
                          action-call presented-entry)
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
  (ert-info ("procedure default definition")
    (let (resolved-prompt action-call)
      (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
                 (lambda (prompt &optional _table-like-only
                                  _category _allowed-types)
                   (setq resolved-prompt prompt)
                   '(:name "PROCESS_ORDER" :type "PROCEDURE")))
                ((symbol-function 'clutch--run-object-action)
                 (lambda (entry action-id)
                   (setq action-call (list entry action-id)))))
        (clutch-jump)
        (should (equal resolved-prompt "Jump to object: "))
        (should (equal action-call
                       '((:name "PROCESS_ORDER" :type "PROCEDURE")
                         show-definition)))))))

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

(ert-deftest clutch-test-resolve-entry-local-prefix-prefills-picker ()
  "Local prefix matches should pre-fill the object picker."
  (dolist (case '(("tables"
                   "ORD"
                   ((:name "ORDERS" :type "TABLE")
                    (:name "ORDER_ITEMS" :type "TABLE"))
                   (:name "ORDERS" :type "TABLE"))
                  ("non-table objects"
                   "GET_"
                   ((:name "GET_ORDER" :type "FUNCTION")
                    (:name "GET_CUSTOMER" :type "FUNCTION"))
                   (:name "GET_ORDER" :type "FUNCTION"))))
    (pcase-let ((`(,label ,symbol ,entries ,selected) case))
      (ert-info ((format "case: %s" label))
        (let (captured-initial)
          (cl-letf (((symbol-function 'clutch--buffer-current-object)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--object-matches-at-point)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--ensure-connection) (lambda () t))
                    ((symbol-function 'clutch--warn-schema-cache-state)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--object-connection-alive-p)
                     (lambda (_) t))
                    ((symbol-function 'thing-at-point)
                     (lambda (&rest _) symbol))
                    ((symbol-function 'clutch--object-entries)
                     (lambda (_conn &optional _refresh) entries))
                    ((symbol-function 'clutch--object-entry-reader)
                     (lambda (_conn _prompt _entries &optional initial _cat)
                       (setq captured-initial initial)
                       selected)))
            (with-temp-buffer
              (setq-local clutch-connection 'fake-conn)
              (clutch--resolve-object-entry "Describe: ")
              (should (equal captured-initial symbol)))))))))

(ert-deftest clutch-test-resolve-entry-remote-table-search-contract ()
  "Remote table search should return direct hits or open the right picker."
  (dolist (case '(("single hit"
                   "RARE_TABLE"
                   nil
                   ((:name "RARE_TABLE" :type "TABLE"))
                   (:name "RARE_TABLE" :type "TABLE")
                   nil nil)
                  ("multiple hits"
                   "AUDIT"
                   nil
                   ((:name "AUDIT_LOG" :type "TABLE")
                    (:name "AUDIT_TRAIL" :type "TABLE"))
                   nil 2 nil)
                  ("no hit"
                   "preferential_price"
                   ((:name "ORDERS" :type "TABLE"))
                   nil
                   nil nil message)))
    (pcase-let ((`(,label ,symbol ,local-entries ,remote-entries
                          ,direct-result ,picker-count ,expect-message)
                 case))
      (ert-info ((format "case: %s" label))
        (let (captured-entries captured-initial messages)
          (cl-letf (((symbol-function 'clutch--buffer-current-object)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--object-matches-at-point)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--ensure-connection) (lambda () t))
                    ((symbol-function 'clutch--warn-schema-cache-state)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--object-connection-alive-p)
                     (lambda (_) t))
                    ((symbol-function 'thing-at-point)
                     (lambda (&rest _) symbol))
                    ((symbol-function 'clutch--object-entries)
                     (lambda (_conn &optional _refresh) local-entries))
                    ((symbol-function 'clutch-db-search-table-entries)
                     (lambda (_conn _prefix) remote-entries))
                    ((symbol-function 'clutch--object-cache-complete-p)
                     (lambda (_) t))
                    ((symbol-function 'message)
                     (lambda (fmt &rest args)
                       (push (apply #'format fmt args) messages)))
                    ((symbol-function 'clutch--object-entry-reader)
                     (lambda (_conn _prompt entries &optional initial _cat)
                       (setq captured-entries entries
                             captured-initial initial)
                       (car entries))))
            (with-temp-buffer
              (setq-local clutch-connection 'fake-conn)
              (let ((result (clutch--resolve-object-entry "Describe: ")))
                (when direct-result
                  (should (equal result direct-result))
                  (should-not captured-entries))
                (when picker-count
                  (should (= (length captured-entries) picker-count)))
                (when expect-message
                  (should (null captured-initial))
                  (should (cl-some
                           (lambda (m) (string-match-p symbol m))
                           messages)))))))))))

(ert-deftest clutch-test-resolve-entry-incomplete-cache-refresh-contract ()
  "Incomplete object caches should sync-refresh non-table candidates."
  (dolist (case '(("single refreshed procedure"
                   "PROCESS_ORDER"
                   nil
                   ((:name "PROCESS_ORDER" :type "PROCEDURE"))
                   (:name "PROCESS_ORDER" :type "PROCEDURE")
                   nil)
                  ("merged table and procedure hits"
                   "ORDER"
                   ((:name "ORDERS" :type "TABLE"))
                   ((:name "ORDERS" :type "TABLE")
                    (:name "ORDER_PROC" :type "PROCEDURE"))
                   nil
                   procedure)))
    (pcase-let ((`(,label ,symbol ,remote-entries ,refresh-entries
                          ,direct-result ,expect-picker)
                 case))
      (ert-info ((format "case: %s" label))
        (let (captured-entries)
          (cl-letf (((symbol-function 'clutch--buffer-current-object)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--object-matches-at-point)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--ensure-connection) (lambda () t))
                    ((symbol-function 'clutch--warn-schema-cache-state)
                     (lambda (&rest _) nil))
                    ((symbol-function 'clutch--object-connection-alive-p)
                     (lambda (_) t))
                    ((symbol-function 'thing-at-point)
                     (lambda (&rest _) symbol))
                    ((symbol-function 'clutch--object-entries)
                     (lambda (_conn &optional refresh)
                       (and refresh refresh-entries)))
                    ((symbol-function 'clutch-db-search-table-entries)
                     (lambda (_conn _prefix) remote-entries))
                    ((symbol-function 'clutch--object-cache-complete-p)
                     (lambda (_) nil))
                    ((symbol-function 'clutch--object-entry-reader)
                     (lambda (_conn _prompt entries &optional _initial _cat)
                       (setq captured-entries entries)
                       (car entries))))
            (with-temp-buffer
              (setq-local clutch-connection 'fake-conn)
              (let ((result (clutch--resolve-object-entry "Describe: ")))
                (when direct-result
                  (should (equal result direct-result)))
                (when expect-picker
                  (should (>= (length captured-entries) 2))
                  (should (cl-some
                           (lambda (e)
                             (equal (plist-get e :type) "PROCEDURE"))
                           captured-entries)))))))))))

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
  (clutch-test-object--with-warmup-state
   (let (timer-fn)
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
       (should-error (funcall timer-fn) :type 'error)))))

(ert-deftest clutch-test-object-warmup-async-callback-contract ()
  "Async object warmup should store live results, ignore stale callbacks, and trace phases."
  (clutch-test-object--with-warmup-state
   (let (timer-fn async-callback stored)
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
                    (lambda (_conn _type entries)
                      (setq stored entries)))
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
           (should (equal stored
                          '((:name "ORDER_IDX" :type "INDEX"
				   :schema "APP" :source-schema "APP"))))
           (setq stored nil)
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
             (should (string-match-p "Phase: stale-drop" text))
             (should-not stored))))))))

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

(ert-deftest clutch-test-object-entry-reader-affixation-enriches-bounded-metadata ()
  "Object reader affixation should enrich a bounded candidate batch."
  (let (metadata-calls)
    (let ((clutch--object-affixation-metadata-limit 1))
      (cl-letf (((symbol-function 'clutch-db-object-entry-metadata)
                 (lambda (_conn entry)
                   (push (plist-get entry :name) metadata-calls)
                   (plist-put (copy-sequence entry) :value-type "STRING")))
                ((symbol-function 'completing-read)
                 (lambda (_prompt collection &rest _args)
                   (let* ((metadata (funcall collection "" nil 'metadata))
                          (meta-alist (cdr metadata))
                          (annotation-fn
                           (alist-get 'annotation-function meta-alist))
                          (affixation-fn
                           (alist-get 'affixation-function meta-alist))
                          (candidates (funcall collection "" nil t))
                          (affixed (funcall affixation-fn candidates)))
                     (should (equal metadata-calls '("alpha")))
                     (should (string-match-p
                              "string"
                              (substring-no-properties (nth 2 (car affixed)))))
                     (should (string-match-p
                              "key"
                              (substring-no-properties (nth 2 (cadr affixed)))))
                     (should (equal (substring-no-properties
                                     (funcall annotation-fn "beta"))
                                    "  string"))
                     (should (equal metadata-calls '("beta" "alpha")))
                     "alpha"))))
        (should (equal (clutch--object-entry-reader
                        'fake-conn
                        "Object: "
                        '((:name "alpha" :type "KEY" :schema "0")
                          (:name "beta" :type "KEY" :schema "0")))
                       '(:name "alpha" :type "KEY" :schema "0")))))))

(ert-deftest clutch-test-object-entry-reader-metadata-errors-surface ()
  "Display metadata errors should surface through object completion."
  (cl-letf (((symbol-function 'clutch-db-object-entry-metadata)
             (lambda (&rest _args)
               (signal 'clutch-db-error '("metadata boom"))))
            ((symbol-function 'completing-read)
             (lambda (_prompt collection &rest _args)
               (let* ((metadata (funcall collection "" nil 'metadata))
                      (meta-alist (cdr metadata))
                      (annotation-fn
                       (alist-get 'annotation-function meta-alist)))
                 (funcall annotation-fn "cache:1")
                 "cache:1"))))
    (should-error
     (clutch--object-entry-reader
      'fake-conn
      "Object: "
      '((:name "cache:1" :type "KEY" :schema "0")))
     :type 'clutch-db-error)))

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

(ert-deftest clutch-test-embark-object-target-contract ()
  "Embark target finder should choose point, buffer, or dispatch entries."
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
                     '(clutch-object (:name "orders" :type "TABLE") 1 7)))))
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local major-mode 'sql-mode)
    (setq-local clutch-browser-current-object '(:name "PROCESS_ORDER" :type "PROCEDURE"))
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-object-at-point)
               (lambda () nil)))
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "PROCESS_ORDER" :type "PROCEDURE"))))))
  (let ((clutch--object-dispatch-entry '(:name "ORDERS" :type "TABLE")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "ORDERS" :type "TABLE"))))))
  (let ((clutch--object-dispatch-entry
         '(:name "ORDER_IDX" :type "INDEX" :target-table "ORDERS")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--embark-object-target)
                     '(clutch-target-object
                       (:name "ORDER_IDX" :type "INDEX" :target-table "ORDERS")))))))

(ert-deftest clutch-test-embark-around-hook-contract ()
  "Around-action hook should resolve candidates and clear completion state."
  (let* ((entry '(:name "xxx_detail" :type "TABLE" :schema "public"))
         (clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx_detail" entry clutch--object-completion-entry-map)
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "xxx_detail"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (equal resolved entry)))
    (should (null (symbol-value 'clutch--object-completion-entry-map))))
  (let ((clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx" '(:name "xxx" :type "TABLE") clutch--object-completion-entry-map)
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "yyy"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (null resolved))))
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

(ert-deftest clutch-test-embark-action-specs-contract ()
  "Embark menus should hide default duplicates unless target actions need them."
  (should (equal (mapcar (lambda (spec) (plist-get spec :id))
                         (clutch--embark-action-specs
                          (lambda (spec)
                            (not (eq (plist-get spec :id) 'jump-target)))))
                 '(describe show-definition index-insight explain-sample
                            show-validation show-stats copy-name
                            copy-fqname)))
  (should (equal (mapcar (lambda (spec) (plist-get spec :id))
                         (clutch--embark-action-specs))
                 '(describe show-definition index-insight explain-sample
                            show-validation show-stats jump-target copy-name
                            copy-fqname))))

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

(provide 'clutch-test-object)

;;; clutch-test-object.el ends here
