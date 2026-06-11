;;; clutch-test-debug.el --- Debug workflow ERT tests -*- lexical-binding: t; -*-

;;; Commentary:

;; Error humanization, debug capture, and problem-record tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common))

;;;; Debug — error humanization

(ert-deftest clutch-test-humanize-db-error-known-patterns ()
  "Known backend errors should be cleaned and hinted."
  (dolist (case `((clickhouse-update
                   ,(concat "Code: 48. DB::Exception: Lightweight updates are not supported. "
                            "Lightweight updates are supported only for tables with materialized "
                            "_block_number column. (NOT_IMPLEMENTED) "
                            "(version 26.2.5.45 (official build))  "
                            "(queryId= 92961bbf-e430-4c89-95c3-0deb471daec6)")
                   ("enable lightweight update")
                   ("queryId" "version 26"))
                  (clickhouse-delete-singular
                   "Lightweight delete is not supported"
                   ("enable lightweight delete")
                   nil)
                  (clickhouse-delete-plural
                   "Lightweight deletes are not supported"
                   ("enable lightweight delete")
                   nil)
                  (oracle-ora00942
                   "ORA-00942: table or view does not exist"
                   ("ORA-00942" "table or view does not exist")
                   nil)
                  (connection-refused
                   "Connection refused (host=db.example.com, port=3306)"
                   ("check host and port")
                   nil)
                  (jdbc-driver-missing
                   "SQLException [SQLState=08001]: No suitable driver found for jdbc:oracle:thin:@//db:1521/ORCL"
                   ("No suitable driver found"
                    "clutch-jdbc-install-driver RET oracle")
                   nil)))
    (pcase-let ((`(,label ,msg ,present-patterns ,absent-patterns) case))
      (ert-info ((format "case: %s" label))
        (let ((result (clutch--humanize-db-error msg)))
          (dolist (pattern present-patterns)
            (should (string-match-p pattern result)))
          (dolist (pattern absent-patterns)
            (should-not (string-match-p pattern result))))))))

(ert-deftest clutch-test-humanize-db-error-parts-keep-jdbc-state-in-summary ()
  "Structured error rendering should not treat JDBC SQLState as a display hint."
  (let ((parts (clutch--humanize-db-error-parts
                "SQLException [SQLState=99999]: unexpected driver error")))
    (should (string-match-p "\\[SQLState=99999\\]"
                            (plist-get parts :summary)))
    (should-not (plist-get parts :hint))))

(ert-deftest clutch-test-humanize-db-error-strips-noise ()
  "Unknown and noisy errors should keep the useful summary only."
  (dolist (case `((unknown
                   "Something totally unexpected happened"
                   "Something totally unexpected happened"
                   nil
                   ("\\["))
                  (clickhouse-suffix
                   "Some error (version 24.1.1.1 (official build)) (queryId= abc-123)"
                   nil
                   ("Some error")
                   ("queryId" "version 24"))
                  (java-stack
                   ,(concat "Connection failed: timeout\n"
                            "\tat java.base/java.net.Socket.connect(Socket.java:633)\n"
                            "\tat com.clickhouse.client.Http.open(Http.java:42)")
                   nil
                   ("Connection failed")
                   ("java\\.base" "Socket"))
                  (database-prefix
                   "Database error: ORA-00942: table does not exist"
                   nil
                   ("ORA-00942")
                   ("^Database error:"))))
    (pcase-let ((`(,label ,msg ,expected ,present-patterns ,absent-patterns)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((result (clutch--humanize-db-error msg)))
          (when expected
            (should (equal result expected)))
          (dolist (pattern present-patterns)
            (should (string-match-p pattern result)))
          (dolist (pattern absent-patterns)
            (should-not (string-match-p pattern result))))))))

;;;; Debug — problem records and buffer

(ert-deftest clutch-test-debug-buffer-removes-old-details-commands ()
  "The old error-details workflow should be deleted outright."
  (should-not (fboundp 'clutch-show-last-error-details))
  (should-not (fboundp 'clutch-show-error-details))
  (should-not (fboundp 'clutch-debug-buffer))
  (should-not (fboundp 'clutch-error-details-refresh))
  (should-not (fboundp 'clutch-error-details-copy-message))
  (should-not (fboundp 'clutch-error-details-copy-all))
  (should-not (boundp 'clutch-error-details-mode-map)))

(ert-deftest clutch-test-debug-mode-creates-dedicated-buffer ()
  "Enabling debug mode should create the dedicated debug buffer immediately."
  (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
    (kill-buffer buf))
  (let ((clutch-debug-mode nil))
    (unwind-protect
        (progn
          (clutch-debug-mode 1)
          (let ((buf (get-buffer clutch-debug-buffer-name)))
            (should (buffer-live-p buf))
            (with-current-buffer buf
              (should (derived-mode-p 'clutch--debug-buffer-mode))
              (should (string-match-p "Clutch Debug"
                                      (buffer-string))))))
      (clutch-debug-mode -1)
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-buffer-appends-current-buffer-failure ()
  "Problem records should be appended to the dedicated debug buffer."
  (let ((details '(:backend jdbc
                   :summary "Connection failed [check host and port]"
                   :diag (:category "connect"
                          :op "connect"
                          :request-id 71
                          :exception-class "java.sql.SQLNonTransientConnectionException"
                          :sql-state "08071"
                          :context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>"
                                    :generated-sql "ALTER SESSION SET CURRENT_SCHEMA = \"REPORTING\""
                                    :property-keys ("http_header_COOKIE" "socket_timeout"))
                          :cause-chain ((:exception-class "java.sql.SQLNonTransientConnectionException"
                                         :message "reason-71")
                                        (:exception-class "java.net.ConnectException"
                                         :message "root-71")))
                   :stderr-tail "Connect request 71 failed")))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (clutch--remember-problem-record
         :buffer (current-buffer)
         :problem details)
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Connection failed" text))
          (should (string-match-p "08071" text))
          (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" text))
          (should (string-match-p "Connect request 71 failed" text)))))))

(ert-deftest clutch-test-debug-buffer-appends-connection-problem-record ()
  "Connection-scoped problem records should also be appended to the debug buffer."
  (let ((problem '(:backend oracle
                   :summary "Metadata failed"
                   :diag (:category "metadata"
                          :op "get-columns"
                          :conn-id 88
                          :raw-message "ORA-12592: TNS:bad packet"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn)
        (clutch--remember-problem-record
         :connection 'fake-conn
         :problem problem)
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Metadata failed" text))
          (should (string-match-p "get-columns" text)))))))

(ert-deftest clutch-test-debug-buffer-appends-debug-trace-when-enabled ()
  "Debug mode should append recent captured events to the debug buffer."
  (let ((details '(:backend jdbc
                   :summary "Query failed"
                   :diag (:category "query"
                          :op "execute"
                          :raw-message "ORA-00942"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch--buffer-error-details details)
        (clutch--remember-debug-event
         :op "execute"
         :phase "error"
         :backend 'oracle
         :summary "Query failed"
         :sql "SELECT * FROM missing_table")
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Trace Event" text))
          (should (string-match-p "Operation: execute" text))
          (should (string-match-p "Phase: error" text))
          (should (string-match-p "Query failed" text))
          (should (string-match-p "SELECT \\* FROM missing_table" text)))))))

(ert-deftest clutch-test-run-db-query-success-clears-problems-across-connection-buffers ()
  "Successful queries should clear stale failure state for the whole connection."
  (let ((conn 'fake-conn)
        (source (generate-new-buffer " *clutch-debug-source*"))
        (peer (generate-new-buffer " *clutch-debug-peer*"))
        cleared)
    (unwind-protect
        (progn
          (with-current-buffer source
            (setq-local clutch-connection conn)
            (clutch--remember-problem-record
             :buffer source
             :connection conn
             :problem '(:summary "old")))
          (with-current-buffer peer
            (setq-local clutch-connection conn)
            (cl-letf (((symbol-function 'clutch-db-query)
                       (lambda (_conn _sql) 'ok))
                      ((symbol-function 'clutch-db-manual-commit-p)
                       (lambda (_conn) nil))
                      ((symbol-function 'clutch-db-clear-error-details)
                       (lambda (clear-conn)
                         (setq cleared clear-conn))))
              (should (eq (clutch--run-db-query conn "SELECT 1") 'ok))))
          (with-current-buffer source
            (should-not clutch--buffer-error-details)
            (should-not (gethash conn clutch--problem-records-by-conn)))
          (should-not (gethash conn clutch--problem-records-by-conn))
          (should (eq cleared conn)))
      (kill-buffer source)
      (kill-buffer peer))))

(ert-deftest clutch-test-debug-mode-enable-replays-stored-problem-records ()
  "Enabling debug mode should replay already-stored problems as history."
  (let ((source (generate-new-buffer " *clutch-debug-replay*"))
        (summary "pre-debug failure"))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch-test--clear-problem-capture)
          (with-current-buffer source
            (clutch--remember-problem-record
             :buffer source
             :problem `(:backend oracle :summary ,summary)))
          (clutch-debug-mode 1)
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Historical Problems" text))
            (should (string-match-p summary text))))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-mode-enable-does-not-clear-problem-records ()
  "Enabling debug mode should keep already-stored problem records."
  (let ((source (generate-new-buffer " *clutch-debug-problem*")))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch-test--clear-problem-capture)
          (with-current-buffer source
            (clutch--remember-problem-record
             :buffer source
             :problem '(:summary "pre-debug")))
          (clutch-debug-mode 1)
          (with-current-buffer source
            (should (equal (plist-get clutch--buffer-error-details :summary)
                           "pre-debug"))))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-mode-enable-clears-debug-events-but-keeps-problems ()
  "Starting a new debug capture should clear events but keep problems."
  (let ((conn 'fake-conn)
        (source (generate-new-buffer " *clutch-debug-capture*")))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch-test--clear-problem-capture)
          (with-current-buffer source
            (setq-local clutch-connection conn)
            (clutch-debug-mode 1)
            (clutch--remember-problem-record
             :buffer source
             :connection conn
             :problem '(:summary "pre-debug"))
            (clutch--remember-debug-event
             :buffer source
             :connection conn
             :op "execute"
             :phase "error"
             :summary "boom")
            (should clutch--debug-events)
            (should clutch--buffer-error-details)
            (clutch-debug-mode -1)
            (clutch-debug-mode 1)
            (should-not clutch--debug-events)
            (should clutch--buffer-error-details)))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-execute-error-populates-problem-record ()
  "SQL execution failures should populate the current buffer problem record."
  (let ((source (generate-new-buffer " *clutch-error-source*")))
    (unwind-protect
        (with-current-buffer source
          (set-window-buffer (selected-window) source)
          (setq-local clutch-connection 'fake-conn
                      clutch--source-window (selected-window))
          (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_conn sql _page-num _page-size
                                    &optional _order-by _page-offset)
                       sql))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn _sql)
                       (signal 'clutch-db-error
                               (list "ORA-00942: table or view does not exist"))))
                    ((symbol-function 'clutch-db-error-details)
                     (lambda (_conn) nil))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (catch 'clutch--execution-aborted
              (clutch--execute-select "SELECT * FROM missing_table" 'fake-conn))
            (let* ((details clutch--buffer-error-details)
                   (diag (plist-get details :diag)))
              (should details)
              (should (equal (plist-get details :summary)
                             (clutch--humanize-db-error
                              "ORA-00942: table or view does not exist")))
              (should (equal (plist-get diag :raw-message)
                             "ORA-00942: table or view does not exist"))
              (should (equal (plist-get (plist-get diag :context) :sql)
                             "SELECT * FROM missing_table")))))
      (kill-buffer source))))

(provide 'clutch-test-debug)

;;; clutch-test-debug.el ends here
