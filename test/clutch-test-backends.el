;;; clutch-test-backends.el --- Backend matrix for tests -*- lexical-binding: t; -*-

;;; Commentary:

;; Shared backend capability matrix used by unit and live tests.

;;; Code:

(require 'cl-lib)

(defvar clutch-test-backend)
(defvar clutch-test-url)

(defconst clutch-test-live-backends
  '((:id mysql
     :backend mysql
     :display-name "MySQL"
     :capabilities (:result-workflow :updateable-workflow :object-describe
                    :duplicate-column-join))
    (:id pg
     :backend pg
     :display-name "PostgreSQL"
     :capabilities (:result-workflow :updateable-workflow :object-describe
                    :ctid-row-identity))
    (:id sqlserver
     :backend sqlserver
     :display-name "SQL Server"
     :capabilities (:result-workflow :updateable-workflow))
    (:id oracle
     :backend oracle
     :display-name "Oracle"
     :capabilities (:result-workflow :updateable-workflow
                    :uppercase-identifiers))
    (:id clickhouse
     :backend clickhouse
     :display-name "ClickHouse"
     :capabilities (:result-workflow :clickhouse-engine))
    (:id duckdb
     :backend jdbc
     :display-name "DuckDB"
     :url-prefix "jdbc:duckdb:"
     :capabilities (:result-workflow :updateable-workflow))
    (:id jdbc
     :backend jdbc
     :display-name "Generic JDBC"
     :capabilities nil))
  "Backend descriptors used by live workflow tests.

Each descriptor names the backend identity and behavioral capabilities that
`clutch-test-live.el' can select against.")

(defun clutch-test-live-backend-descriptor (&optional backend url)
  "Return the live workflow descriptor for BACKEND and URL.

When BACKEND is nil, use `clutch-test-backend'.  When URL is nil, use
`clutch-test-url'."
  (let ((backend (or backend clutch-test-backend))
        (url (or url clutch-test-url)))
    (cl-loop for descriptor in clutch-test-live-backends
             for descriptor-backend = (plist-get descriptor :backend)
             for prefix = (plist-get descriptor :url-prefix)
             when (and (eq descriptor-backend backend)
                       (if prefix
                           (and (stringp url)
                                (string-prefix-p prefix url))
                         t))
             return descriptor)))

(defun clutch-test-live-backend-id (&optional backend url)
  "Return the live workflow backend id for BACKEND and URL."
  (or (plist-get (clutch-test-live-backend-descriptor backend url) :id)
      (or backend clutch-test-backend)))

(defun clutch-test-live-backend-capability-p (capability
                                             &optional backend url)
  "Return non-nil when the live backend supports CAPABILITY."
  (when-let* ((descriptor (clutch-test-live-backend-descriptor backend url)))
    (memq capability (plist-get descriptor :capabilities))))

(defun clutch-test-capability-skip-message (capability)
  "Return an ERT skip message for tests that require CAPABILITY."
  (format "Live test covers %s"
          (mapconcat #'identity
                     (cl-loop for descriptor in clutch-test-live-backends
                              when (memq capability
                                         (plist-get descriptor :capabilities))
                              collect (plist-get descriptor :display-name))
                     "/")))

(provide 'clutch-test-backends)

;;; clutch-test-backends.el ends here
