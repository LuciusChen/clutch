;;; clutch-test-object.el --- Object workflow ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Object browse, describe, jump, action, warmup, and schema-switch tests.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-document)
  (require 'clutch-db-sqlite))

;;;; Object test helpers

(defmacro clutch-test-object--with-warmup-state (&rest body)
  "Run BODY with fresh object cache and warmup scheduler state."
  (declare (indent 0) (debug (body)))
  `(let ((clutch--object-cache (make-hash-table :test 'eq))
         (clutch--object-warmup-timers (make-hash-table :test 'eq))
         (clutch--object-warmup-generations (make-hash-table :test 'eq)))
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
        (should-not (clutch--object-act-document-actions-p))
        (should-not (clutch--object-act-backend-action-p 'show-stats)))
      (let ((clutch--object-action-entry '(:name "ORDER_IDX" :type "INDEX")))
        (should (clutch--object-act-jump-target-p))
        (should-not (clutch--object-act-document-actions-p))
        (should-not (clutch--object-act-backend-action-p 'show-stats)))
      (let ((clutch--object-action-entry '(:name "users" :type "COLLECTION")))
        (should-not (clutch--object-act-jump-target-p))
        (should (clutch--object-act-document-actions-p))
        (should (clutch--object-act-backend-action-p 'show-stats))))))

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
  (let ((default-product (default-value 'sql-product))
        object-buffer)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-object-definition)
                   (lambda (_conn _entry)
                     "SELECT NVL(name, 0) FROM DUAL"))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args)
                     (setq object-buffer buf)
                     buf)))
          (with-temp-buffer
            (setq-local clutch-connection 'fake-conn)
            (setq-local clutch--connection-params '(:backend oracle))
            (setq-local clutch--conn-sql-product nil)
            (clutch-object-show-ddl-or-source
             '(:name "DEMO_TASKS" :type "TABLE")))
          (with-current-buffer object-buffer
            (should (local-variable-p 'sql-product))
            (should (eq sql-product 'oracle))
            (should (equal mode-name "SQL[Oracle]"))
            (goto-char (point-min))
            (search-forward "NVL")
            (should (eq (get-text-property (match-beginning 0) 'face)
                        'font-lock-builtin-face)))
          (should (eq (default-value 'sql-product) default-product)))
      (when (buffer-live-p object-buffer)
        (kill-buffer object-buffer)))))

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
  (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
        (details '(:backend jdbc
                   :summary "ORA-04043: object does not exist"
                   :diag (:category "metadata"
                          :op "get-object-ddl"
                          :conn-id 7
                          :raw-message "ORA-04043: object does not exist"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection conn)
        (cl-letf (((symbol-function 'clutch-db-backend-key)
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
  (let ((clutch--table-metadata-cache (make-hash-table :test 'eq))
        (detail-calls 0)
        (list-columns-calls 0))
    (cl-letf (((symbol-function 'clutch-db-column-details)
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
  (let ((clutch--schema-cache (make-hash-table :test 'eq))
        (clutch--table-metadata-cache (make-hash-table :test 'eq))
        (clutch--column-details-queue-cache (make-hash-table :test 'eq))
        (clutch--column-details-active-cache (make-hash-table :test 'eq))
        (detail-calls 0))
    (let ((conn (list 'fake-conn))
          (schema (make-hash-table :test 'equal))
          (metadata (make-hash-table :test 'equal)))
      (puthash "USERS" '("id" "old_col") schema)
      (puthash conn schema clutch--schema-cache)
      (puthash "USERS"
               '(:column-details ((:name "id" :type "int")
                                  (:name "old_col" :type "text"))
                 :column-details-status (:state failed :error "old failure")
                 :columns-status (:state failed :error "old columns"))
               metadata)
      (puthash '("APP" . "USERS") '(:comment "old comment") metadata)
      (puthash conn metadata clutch--table-metadata-cache)
      (puthash conn '("USERS" "ORDERS")
               clutch--column-details-queue-cache)
      (puthash conn (cons "USERS" 7)
               clutch--column-details-active-cache)
      (cl-letf (((symbol-function 'clutch--command-connection-context)
                 (lambda () (list :connection clutch-connection)))
                ((symbol-function 'clutch-db-live-p)
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
              (setq-local clutch-connection conn)
              (clutch-object-describe '(:name "USERS" :type "TABLE"))
              (with-current-buffer "*clutch describe: USERS*"
                (let ((text (buffer-string)))
                  (should (string-match-p "new_col" text))
                  (should-not (string-match-p "old_col" text)))))
          (when-let* ((buf (get-buffer "*clutch describe: USERS*")))
            (kill-buffer buf)))
        (should (= detail-calls 1))
        (should-not (gethash conn clutch--column-details-active-cache))
        (should (equal (gethash conn clutch--column-details-queue-cache)
                       '("ORDERS")))
        (should-not (clutch--column-details-status conn "USERS"))
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
                 (lambda (_language &optional _quiet) nil))
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
      (let ((clutch--table-metadata-cache (make-hash-table :test 'eq))
            (conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
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
            (setq-local clutch-connection conn)
            (cl-letf (((symbol-function 'clutch-db-backend-key)
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
          (puthash 'fake-conn
                   '(:buffer nil :problem (:summary "old"))
                   clutch--problem-records-by-conn)
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
  "Manual and automatic describe refreshes should preserve their boundaries."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--describe-object-entry '(:name "USERS" :type "TABLE")
                clutch--buffer-error-details '(:summary "old"))
    (puthash 'fake-conn
             '(:buffer nil :problem (:summary "old"))
             clutch--problem-records-by-conn)
    (let ((refreshes 0)
          (invalidations 0)
          (renders 0))
      (cl-letf (((symbol-function 'clutch--refresh-current-schema)
                 (lambda (&rest _) (cl-incf refreshes)))
                ((symbol-function 'clutch--refresh-object-describe-metadata)
                 (lambda (&rest _) (cl-incf invalidations)))
                ((symbol-function 'clutch--render-object-describe)
                 (lambda (&rest _) (cl-incf renders))))
        (clutch-describe-refresh t t)
        (should (= refreshes 0))
        (should (= invalidations 0))
        (should (= renders 1))
        (clutch-describe-refresh)
        (should (= refreshes 1))
        (should (= invalidations 1))
        (should (= renders 2))
        (should-not clutch--buffer-error-details)
        (should-not (gethash 'fake-conn clutch--problem-records-by-conn))))))

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
  (let ((conn (list 'fake-conn))
        (clutch--object-cache (make-hash-table :test 'eq))
        captured)
    (puthash conn
             (list :entries '((:name "ORDER_IDX" :type "INDEX"
                               :schema "APP" :source-schema "APP")
                              (:name "PROCESS_ORDER" :type "PROCEDURE"
                               :schema "APP" :source-schema "APP"
                               :status "VALID"))
                   :loaded-categories '(indexes procedures))
             clutch--object-cache)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch--warn-schema-cache-state) #'ignore)
              ((symbol-function 'clutch-db-live-p)
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
        (setq-local clutch-connection conn)
        (should (equal (clutch-object-read "Object: ")
                       '(:name "PROCESS_ORDER" :type "PROCEDURE"
                         :schema "APP" :source-schema "APP" :status "VALID"))))
      (should (eq (plist-get captured :category) 'clutch-object))
      (should (equal (plist-get captured :base)
                     '("ORDERS" "V_ORDERS" "USER_TABLES" "ORDER_IDX" "PROCESS_ORDER")))
      (should (equal (plist-get captured :orders-group) "Tables"))
      (should (equal (plist-get captured :proc-group) "Procedures"))
      (should (equal (plist-get captured :proc-ann) "  APP/procedure")))))

(ert-deftest clutch-test-object-cache-primes-table-entry-comments ()
  "Object cache storage should prime table comments carried by entries."
  (let ((clutch--object-cache (make-hash-table :test 'eq))
        (clutch--table-metadata-cache (make-hash-table :test 'eq)))
    (cl-letf (((symbol-function 'clutch-db-list-table-entries)
               (lambda (_conn)
                 '((:name "ORDERS" :type "TABLE" :schema "APP"
                    :source-schema "APP" :comment "订单")
                   (:name "ORDERS" :type "TABLE" :schema "ARCHIVE"
                    :source-schema "ARCHIVE" :comment "历史订单")
                   (:name "AUDIT_LOG" :type "TABLE" :schema "APP"
                    :source-schema "APP" :comment nil))))
              ((symbol-function 'clutch-db-current-schema)
               (lambda (_conn) "APP"))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix) nil)))
      (clutch--store-object-cache-type-entries 'fake-conn "INDEX" nil)
      (should (equal (clutch--cached-table-comment 'fake-conn "ORDERS")
                     "订单"))
      (should (clutch--table-comment-cached-p 'fake-conn "AUDIT_LOG"))
      (should-not (clutch--cached-table-comment 'fake-conn "AUDIT_LOG")))))

(ert-deftest clutch-test-object-cache-lifecycle-isolated-by-connection-identity ()
  "Schema invalidation should clear only the exact connection's object state."
  (clutch-test-object--with-warmup-state
    (let* ((conn-a (list :display-key "shared"))
           (conn-b (list :display-key "shared"))
           (entries-a '((:name "ORDERS" :type "TABLE")))
           (entries-b '((:name "AUDIT_LOG" :type "TABLE"))))
      (should (equal conn-a conn-b))
      (should-not (eq conn-a conn-b))
      (clutch--store-object-cache conn-a entries-a)
      (clutch--store-object-cache conn-b entries-b)
      (should (equal (clutch--object-cache-entries conn-a) entries-a))
      (should (equal (clutch--object-cache-entries conn-b) entries-b))
      (let ((clutch--schema-cache-updated-hook
             '(clutch--handle-schema-cache-updated)))
        (clutch--notify-schema-cache-updated conn-a 'invalidated))
      (should-not (clutch--object-cache-entry conn-a))
      (should (equal (clutch--object-cache-entries conn-b) entries-b))
      (should (= (clutch--object-warmup-generation conn-a) 1))
      (should (= (clutch--object-warmup-generation conn-b) 0)))))

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
  (let ((clutch--object-cache (make-hash-table :test 'eq)))
    (puthash 'fake-mongodb-conn
             (list :entries nil
                   :loaded-categories (copy-sequence clutch--object-categories))
             clutch--object-cache)
    (with-temp-buffer
      (clutch-mongodb-mode)
      (insert "db.users.find()")
      (search-backward "users")
      (setq-local clutch-connection 'fake-mongodb-conn)
      (let (read-args action-call)
        (cl-letf (((symbol-function 'clutch-db-live-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch-db-browseable-object-entries)
                   (lambda (_conn)
                     '((:name "users" :schema "app" :type "COLLECTION")
                       (:name "orders" :schema "app" :type "COLLECTION"))))
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

(ert-deftest clutch-test-object-resolution-plan-contract ()
  "Object resolution policy should be a pure decision table."
  (let* ((local '((:name "ORDERS" :type "TABLE")))
         (single '((:name "REMOTE_ONLY" :type "TABLE")))
         (multiple '((:name "AUDIT_LOG" :type "TABLE")
                     (:name "AUDIT_TRAIL" :type "TABLE")))
         (full '((:name "ORDERS" :type "TABLE")
                 (:name "PROCESS_ORDER" :type "PROCEDURE"))))
    (dolist
        (case
         (list
          (list "no symbol" nil local nil (list 'read local nil))
          (list "case-insensitive local prefix" "ord" local nil
                (list 'read local "ord"))
          (list "remote search required" "REMOTE" local nil '(search))
          (list "single remote hit" "REMOTE" local
                (list :attempted t :hits single)
                (list 'return (car single)))
          (list "multiple remote hits" "AUDIT" local
                (list :attempted t :hits multiple)
                (list 'read multiple "AUDIT"))
          (list "no remote hit" "MISSING" local '(:attempted t)
                (list 'missing local))
          (list "refreshed fallback" "MISSING" local
                (list :attempted t :full-entries full)
                (list 'missing full))))
      (pcase-let ((`(,label ,symbol ,entries ,search ,expected) case))
        (ert-info ((format "case: %s" label))
          (should (equal (clutch--object-resolution-plan
                          symbol entries search)
                         expected)))))))

(ert-deftest clutch-test-on-demand-object-search-contract ()
  "On-demand search should merge a permitted refresh only when needed."
  (let ((remote '((:name "ORDERS" :type "TABLE")))
        (refreshed '((:name "ORDERS" :type "TABLE")
                     (:name "ORDER_PROC" :type "PROCEDURE")
                     (:name "ORDER_TRIGGER" :type "TRIGGER")
                     (:name "OTHER_PROC" :type "PROCEDURE")))
        (refresh-count 0))
    (cl-letf (((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn prefix)
                 (should (equal prefix "ORDER"))
                 remote))
              ((symbol-function 'clutch--object-cache-complete-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional refresh)
                 (should refresh)
                 (cl-incf refresh-count)
                 refreshed)))
      (let ((result (clutch--on-demand-object-search
                     'fake-conn "ORDER" nil '("TABLE" "PROCEDURE"))))
        (should (plist-get result :attempted))
        (should (equal (plist-get result :hits)
                       '((:name "ORDERS" :type "TABLE")
                         (:name "ORDER_PROC" :type "PROCEDURE"))))
        (should (equal (plist-get result :full-entries)
                       '((:name "ORDERS" :type "TABLE")
                         (:name "ORDER_PROC" :type "PROCEDURE")
                         (:name "OTHER_PROC" :type "PROCEDURE")))))
      (let ((result (clutch--on-demand-object-search
                     'fake-conn "ORDER" t '("TABLE"))))
        (should (equal (plist-get result :hits) remote))
        (should-not (plist-get result :full-entries)))
      (should (= refresh-count 1)))))

(ert-deftest clutch-test-resolve-object-entry-effect-boundaries ()
  "The real resolver should avoid remote I/O for local and dead paths."
  (let ((entries '((:name "ORDERS" :type "TABLE")
                   (:name "ORDER_ITEMS" :type "TABLE")))
        object-alive reader-call remote-called messages)
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) object-alive))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh) entries))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (&rest _args) (setq remote-called t)))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (conn prompt candidates &optional initial category)
                 (setq reader-call
                       (list conn prompt candidates initial category))
                 (car candidates)))
              ((symbol-function 'message)
               (lambda (format-string &rest args)
                 (push (apply #'format format-string args) messages))))
      (with-temp-buffer
        (insert "ord")
        (setq-local clutch-connection 'fake-conn)
        (setq object-alive t)
        (should (equal (clutch--resolve-object-entry "Describe: ")
                       (car entries)))
        (should (equal reader-call
                       (list 'fake-conn "Describe: " entries "ord"
                             'clutch-object)))
        (should (equal clutch-browser-current-object (car entries)))
        (should-not remote-called))
      (setq object-alive nil reader-call nil messages nil)
      (with-temp-buffer
        (insert "MISSING")
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (equal (nth 3 reader-call) nil))
        (should (string-match-p "MISSING" (car messages)))
        (should-not remote-called)))))

(ert-deftest clutch-test-resolve-object-entry-search-errors-surface ()
  "Backend search failures should reach the real resolver's caller."
  (cl-letf (((symbol-function 'clutch--connection-alive-p)
             (lambda (_conn) t))
            ((symbol-function 'clutch-db-live-p)
             (lambda (_conn) t))
            ((symbol-function 'clutch--object-entries)
             (lambda (&rest _args) nil))
            ((symbol-function 'clutch-db-search-table-entries)
             (lambda (&rest _args)
               (signal 'clutch-db-error '("remote search failed")))))
    (with-temp-buffer
      (insert "REMOTE_ONLY")
      (setq-local clutch-connection 'fake-conn)
      (should-error (clutch--resolve-object-entry "Describe: ")
                    :type 'clutch-db-error))))

(ert-deftest clutch-test-object-at-point-prefers-table-over-public-synonym ()
  "Symbol resolution should prefer concrete schema objects over PUBLIC synonyms."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-db-live-p)
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
  (let ((conn (list 'fake-conn))
        (clutch--object-cache (make-hash-table :test 'eq))
        scheduled)
    (puthash conn
             (list :entries '((:name "PROCESS_ORDER" :type "PROCEDURE"
                               :schema "APP" :source-schema "APP"))
                   :loaded-categories '(procedures))
             clutch--object-cache)
    (cl-letf (((symbol-function 'clutch-db-live-p)
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
      (should (equal (clutch--object-entries conn)
                     '((:name "ORDERS" :type "TABLE"
                        :schema "APP" :source-schema "APP")
                       (:name "PROCESS_ORDER" :type "PROCEDURE"
                        :schema "APP" :source-schema "APP"))))
      (when scheduled
        (should (equal (car scheduled) clutch-object-warmup-idle-delay-seconds))))))

(ert-deftest clutch-test-object-entries-refresh-uses-browseable-contract ()
  "Full refresh should preserve backend-specific browseable snapshot bounds."
  (let ((browseable-calls 0)
        categories
        stored)
    (cl-letf (((symbol-function 'clutch-db-browseable-object-entries)
               (lambda (_conn)
                 (cl-incf browseable-calls)
                 '((:name "cache:1" :type "KEY"))))
              ((symbol-function 'clutch-db-list-table-entries)
               (lambda (&rest _)
                 (ert-fail "refresh bypassed browseable object contract")))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (&rest _)
                 (ert-fail "refresh repeated empty-prefix object search")))
              ((symbol-function 'clutch-db-list-objects)
               (lambda (_conn category)
                 (push category categories)
                 nil))
              ((symbol-function 'clutch--store-object-cache)
               (lambda (_conn entries)
                 (setq stored entries))))
      (should (equal (clutch--object-entries 'fake-conn t)
                     '((:name "cache:1" :type "KEY"))))
      (should (= browseable-calls 1))
      (should (equal (nreverse categories) clutch--object-categories))
      (should (equal stored '((:name "cache:1" :type "KEY")))))))

(ert-deftest clutch-test-safe-completion-call-records-debug-event-on-db-errors ()
  "Recoverable completion metadata errors should surface in the debug buffer."
  (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db")))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (setq-local clutch-connection conn)
      (cl-letf (((symbol-function 'clutch-db-backend-key)
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
          (should (string-match-p "completion boom" text))))))))

(ert-deftest clutch-test-object-warmup-propagates-non-db-errors ()
  "Warmup should still expose programming errors that are not db-runtime races."
  (clutch-test-object--with-warmup-state
   (let (timer-fn)
     (cl-letf (((symbol-function 'clutch-db-live-p)
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
   (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
         timer-fn async-callback stored)
     (with-temp-buffer
       (let ((clutch-debug-mode t))
         (setq-local clutch-connection conn)
         (cl-letf (((symbol-function 'clutch-db-backend-key)
                    (lambda (_conn) 'mysql))
                   ((symbol-function 'clutch-db-live-p)
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
           (clutch--schedule-object-warmup conn)
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
           (clutch--schedule-object-warmup conn)
           (funcall timer-fn)
           (clutch--invalidate-object-warmup conn)
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

(ert-deftest clutch-test-object-entry-reader-resolves-exact-key-value-input ()
  "Key-value readers should validate an exact key beyond their snapshot once."
  (let (searched)
    (cl-letf (((symbol-function 'clutch-db-backend-key)
               (lambda (_conn) 'redis))
              ((symbol-function 'clutch-backend-data-model)
               (lambda (_backend) 'key-value))
              ((symbol-function 'clutch-db-find-table-entry)
               (lambda (_conn name)
                 (push name searched)
                 '(:name "cache:9000" :type "KEY" :schema "0")))
              ((symbol-function 'completing-read)
               (lambda (_prompt collection _predicate require-match &rest _args)
                 (should-not require-match)
                 (should (member "cache:1" (funcall collection "cache:" nil t)))
                 (should-not searched)
                 "cache:9000")))
      (should
       (equal
        (clutch--object-entry-reader
         'fake-conn "Object: " '((:name "cache:1" :type "KEY" :schema "0")))
        '(:name "cache:9000" :type "KEY" :schema "0")))
      (should (equal searched '("cache:9000"))))))

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
    (cl-letf (((symbol-function 'clutch-db-live-p)
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
    (cl-letf (((symbol-function 'clutch-db-live-p)
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
    (cl-letf (((symbol-function 'clutch-db-live-p)
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

(ert-deftest clutch-test-embark-resolves-exact-key-value-input ()
  "Embark should resolve a free exact key instead of falling back to point."
  (let ((clutch--object-completion-entry-map
         (make-hash-table :test 'equal))
        (entry '(:name "remote:key" :type "KEY" :schema "0"))
        resolved
        looked-up)
    (puthash :clutch-resolver
             (lambda (name)
               (setq looked-up (list 'redis-conn name))
               entry)
             clutch--object-completion-entry-map)
    (clutch--embark-with-resolved-entry
     :target "remote:key"
     :run (lambda (&rest _)
            (setq resolved clutch--object-dispatch-entry)))
    (should (equal looked-up '(redis-conn "remote:key")))
    (should (equal resolved entry))
    (should-not clutch--object-completion-entry-map)))

(ert-deftest clutch-test-embark-rejects-missing-key-value-input ()
  "Embark should reject a free key that its session resolver cannot find."
  (let ((clutch--object-completion-entry-map
         (make-hash-table :test 'equal))
        ran)
    (puthash :clutch-resolver (lambda (_name) nil)
             clutch--object-completion-entry-map)
    (should-error
     (clutch--embark-with-resolved-entry
      :target "missing:key"
      :run (lambda (&rest _) (setq ran t)))
     :type 'user-error)
    (should-not ran)
    (should-not clutch--object-completion-entry-map)))

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

(ert-deftest clutch-test-resolve-object-entry-scopes-action-target-to-transient ()
  "A closed object menu should not override normal object resolution."
  (let ((clutch--object-action-entry '(:name "OLD" :type "TABLE")))
    (cl-letf (((symbol-function 'clutch--buffer-current-object)
               (lambda (&rest _) '(:name "CURRENT" :type "TABLE")))
              ((symbol-function 'clutch--object-matches-at-point)
               (lambda (&rest _) nil)))
      (let ((transient-current-command nil))
        (should (equal (clutch--resolve-object-entry "Test: ")
                       '(:name "CURRENT" :type "TABLE"))))
      (let ((transient-current-command 'clutch-object-actions-menu))
        (should (equal (clutch--resolve-object-entry "Test: ")
                       '(:name "OLD" :type "TABLE")))))))

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

(provide 'clutch-test-object)

;;; clutch-test-object.el ends here
