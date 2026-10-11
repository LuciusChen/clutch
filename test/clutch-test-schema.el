;;; clutch-test-schema.el --- Schema cache ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Schema cache refresh, status, column details and metadata tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; Schema cache — refresh and status

(ert-deftest clutch-test-metadata-cache-uses-connection-identity ()
  "Structurally equal connection tokens must keep distinct metadata state."
  (clutch-test--with-isolated-metadata-caches
   (let ((conn-a (list 'same-connection-shape))
         (conn-b (list 'same-connection-shape)))
     (should (equal conn-a conn-b))
     (should-not (eq conn-a conn-b))
     (clutch--set-table-metadata conn-a "users" :column-details '(id))
     (should (equal (plist-get (clutch--table-metadata conn-a "users")
                               :column-details)
                    '(id)))
     (should-not (clutch--table-metadata conn-b "users")))))

(ert-deftest clutch-test-object-warmup-generations-do-not-own-connections ()
  "Warmup freshness tracking must not keep retired connections alive."
  (should (eq (hash-table-weakness clutch--object-warmup-generations)
              'key)))

(ert-deftest clutch-test-object-warmup-error-advances-past-failed-category ()
  "A permanent category error should not retry forever or starve later work."
  (let ((clutch--object-cache (make-hash-table :test 'eq))
        (conn 'warmup-conn)
        scheduled)
    (cl-letf (((symbol-function 'clutch--object-warmup-current-p)
               (lambda (_conn _generation) t))
              ((symbol-function 'clutch--object-warmup-debug-event) #'ignore)
              ((symbol-function 'clutch--browseable-object-entries)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--cache-table-entry-comments) #'ignore)
              ((symbol-function 'clutch--schedule-object-warmup)
               (lambda (_conn) (setq scheduled t))))
      (clutch--object-warmup-error
       conn 3 'indexes "permission denied")
      (should (memq 'indexes
                    (clutch--object-cache-loaded-categories conn)))
      (should scheduled))))

(ert-deftest clutch-test-refresh-schema-cache-records-ready-status ()
  "Schema refresh entry points should record ready state and table count."
  (dolist (mode '(sync async))
    (ert-info ((format "mode: %s" mode))
      (clutch-test--with-isolated-metadata-caches
       (cl-letf (((symbol-function 'clutch-db-list-tables)
                  (lambda (_conn) '("users" "orders")))
                 ((symbol-function 'clutch-db-live-p)
                  (lambda (_conn) t))
                 ((symbol-function 'clutch-db-refresh-schema-async)
                  (lambda (_conn callback &optional _errback _idle-delay)
                    (funcall callback '("users" "orders"))
                    t)))
         (should (pcase mode
                   ('sync (clutch--refresh-schema-cache 'fake-conn))
                   ('async (clutch--refresh-schema-cache-async 'fake-conn))))
         (let ((status (gethash 'fake-conn clutch--schema-status-cache)))
           (should (eq (plist-get status :state) 'ready))
           (should (= (plist-get status :tables) 2))))))))

(ert-deftest clutch-test-refresh-schema-cache-propagates-programmer-errors ()
  "Synchronous schema refresh should not hide non-database failures."
  (clutch-test--with-isolated-metadata-caches
   (cl-letf (((symbol-function 'clutch-db-list-tables)
              (lambda (_conn)
                (signal 'wrong-type-argument '(integerp broken-state)))))
     (should-error (clutch--refresh-schema-cache 'fake-conn)
                   :type 'wrong-type-argument))))

(ert-deftest clutch-test-refresh-schema-cache-async-callback-contract ()
  "Async schema refresh should ignore stale callbacks and trace callback phases."
  (clutch-test--with-isolated-metadata-caches
   (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
         first-callback second-callback)
     (with-temp-buffer
       (let ((clutch-debug-mode t))
         (setq-local clutch-connection conn)
         (cl-letf (((symbol-function 'clutch-db-live-p)
                    (lambda (_conn) t))
                   ((symbol-function 'clutch-db-backend-key)
                    (lambda (_conn) 'mysql))
                   ((symbol-function 'clutch-db-refresh-schema-async)
                    (lambda (_conn callback &optional _errback _idle-delay)
                      (if first-callback
                          (setq second-callback callback)
                        (setq first-callback callback))
                      t))
                   ((symbol-function 'pop-to-buffer)
                    (lambda (buf &rest _args) buf)))
           (clutch--clear-debug-capture)
           (should (clutch--refresh-schema-cache-async conn))
           (should (clutch--refresh-schema-cache-async conn))
           (funcall first-callback '("stale_users"))
           (should-not (gethash conn clutch--schema-cache))
           (funcall second-callback '("users" "orders"))
           (should (= (hash-table-count
                       (gethash conn clutch--schema-cache))
                      2))
           (should (eq (plist-get
                        (gethash conn clutch--schema-status-cache)
                                  :state)
                       'ready))
           (let ((text (clutch-test--debug-buffer-string)))
             (should (string-match-p "Operation: schema-refresh" text))
             (should (string-match-p "Phase: submit" text))
             (should (string-match-p "Phase: success" text))
             (should (string-match-p "Phase: stale-drop" text)))))))))

(ert-deftest clutch-test-refresh-schema-cache-async-records-current-closed-error ()
  "Async schema refresh should finish current closed-connection errors."
  (clutch-test--with-isolated-metadata-caches
   (let ((alive t)
         errback
         problem)
     (cl-letf (((symbol-function 'clutch-db-live-p)
                (lambda (_conn) alive))
               ((symbol-function 'clutch--remember-problem-record)
                (lambda (&rest args) (setq problem args)))
               ((symbol-function 'clutch-db-refresh-schema-async)
                (lambda (_conn _callback captured-errback &optional _idle-delay)
                  (setq errback captured-errback)
                  t)))
       (should (clutch--refresh-schema-cache-async 'fake-conn))
       (should (eq (plist-get
                    (gethash 'fake-conn clutch--schema-status-cache)
                              :state)
                   'refreshing))
       (setq alive nil)
       (funcall errback "Connection closed")
       (let ((status (gethash 'fake-conn clutch--schema-status-cache)))
         (should (eq (plist-get status :state) 'failed))
         (should (equal (plist-get status :error) "Connection closed")))
       (should problem)))))

(ert-deftest clutch-test-console-buffer-name-reflects-schema-status ()
  "Console buffer names should expose schema status."
  (let ((clutch--schema-status-cache (make-hash-table :test 'eq)))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "dev"
                  clutch-connection 'fake-conn)
      (puthash 'fake-conn '(:state stale) clutch--schema-status-cache)
      (clutch--update-console-buffer-name)
      (should (equal (buffer-name) "*clutch: dev* [schema~]"))
      (puthash 'fake-conn '(:state refreshing) clutch--schema-status-cache)
      (clutch--update-console-buffer-name)
      (should (equal (buffer-name) "*clutch: dev* [schema...]"))
      (puthash 'fake-conn '(:state ready :tables 42)
               clutch--schema-status-cache)
      (clutch--update-console-buffer-name)
      (should (equal (buffer-name) "*clutch: dev* [schema 42t]")))))

(ert-deftest clutch-test-schema-state-header-line-segment ()
  "Schema states should produce the correct header-line segment text."
  (should (equal (clutch--schema-state-header-line-segment 'stale)
                 (propertize "schema~" 'face 'warning)))
  (should (equal (clutch--schema-state-header-line-segment 'failed)
                 (propertize "schema!" 'face 'error)))
  (should (equal (clutch--schema-state-header-line-segment 'refreshing)
                 (propertize "schema…" 'face 'shadow))))

(ert-deftest clutch-test-refresh-current-schema-background-contract ()
  "Manual schema refresh should pick the background or synchronous path.
Lazy backends refresh in the background and fall back to a synchronous
refresh, while the `clutch-refresh-schema' command always refreshes
synchronously."
  (dolist (case
           (list
            (list "background" (lambda () (clutch--refresh-current-schema))
                  t t nil "started in background")
            (list "fallback" (lambda () (clutch--refresh-current-schema))
                  nil t t "Schema refreshed (2 tables)")
            (list "command entry point forces sync"
                  (lambda () (clutch-refresh-schema))
                  t nil t "Schema refreshed (2 tables)")))
    (pcase-let ((`(,label ,entry-point ,async-result ,expect-async
                   ,expect-sync ,message-fragment)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--schema-status-cache (make-hash-table :test 'eq))
              seen-message
              sync-called
              async-called)
          (cl-letf (((symbol-function 'clutch-db-live-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch-db-eager-schema-refresh-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'clutch--refresh-schema-cache-async)
                     (lambda (_conn)
                       (setq async-called t)
                       async-result))
                    ((symbol-function 'clutch--refresh-schema-cache)
                     (lambda (_conn)
                       (setq sync-called t)
                       (puthash 'fake-conn '(:state ready :tables 2)
                                clutch--schema-status-cache)
                       t))
                    ((symbol-function 'message)
                     (lambda (fmt &rest args)
                       (setq seen-message (apply #'format fmt args)))))
            (with-temp-buffer
              (setq-local clutch-connection 'fake-conn)
              (should (funcall entry-point))
              (if expect-async
                  (should async-called)
                (should-not async-called))
              (if expect-sync
                  (should sync-called)
                (should-not sync-called))
              (should (string-match-p (regexp-quote message-fragment)
                                      seen-message)))))))))

(ert-deftest clutch-test-refresh-schema-failure-message-shows-error-as-is ()
  "A failed schema refresh should show the error text as it is.
MySQL access errors name the host pattern, as in \\='u\\='@\\='%\\='."
  (let ((clutch--schema-status-cache (make-hash-table :test 'eq))
        (error-text "Access denied for user 'u'@'%' to database 'zj'")
        seen-message)
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (conn)
                 (puthash conn (list :state 'failed :error error-text)
                          clutch--schema-status-cache)
                 nil))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format-message fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should-not (clutch-refresh-schema))
        (should (equal seen-message
                       (concat "Schema refresh failed: " error-text)))))))

(ert-deftest clutch-test-describe-dwim-warns-when-schema-cache-is-stale ()
  "Object prompts should surface stale-schema recovery hints."
  (let ((clutch--schema-status-cache (make-hash-table :test 'eq))
        hinted
        described)
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn)
                 '((:name "users" :type "TABLE"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &rest _)
                 (car entries)))
              ((symbol-function 'clutch-object-describe)
               (lambda (entry)
                 (setq described entry)))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq hinted (apply #'format fmt args)))))
      (puthash 'fake-conn '(:state stale) clutch--schema-status-cache)
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (call-interactively #'clutch-describe-dwim))
      (should (equal described '(:name "users" :type "TABLE")))
      (should (string-match-p "Schema cache is stale" hinted)))))

;;;; Schema cache — column details and metadata

(ert-deftest clutch-test-result-column-info-works-on-cell-padding ()
  "Column info should resolve from padded whitespace inside a data cell."
  (with-temp-buffer
    (setq-local clutch-column-padding 1
                clutch--result-columns '("name")
                clutch--result-column-defs '((:name "name" :type-category text))
                clutch--result-column-details
                (list (list :name "name" :type "VARCHAR(255)" :nullable t)))
    (insert (clutch--render-row '("alice") 0 '(0) [8] nil))
    (goto-char 2)
    (let (seen)
      (cl-letf (((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq seen (apply #'format fmt args)))))
        (clutch-result-column-info)
        (should (string-match-p "name" seen))
        (should (string-match-p "Type: VARCHAR(255)" seen))))))

(ert-deftest clutch-test-metadata-sync-failures-are-memoized ()
  "Repeated sync metadata failures should not reissue the same failing RPC."
  (clutch-test--with-isolated-metadata-caches
   (let ((schema (make-hash-table :test 'equal)) (column-calls 0)
         (detail-calls 0))
     (puthash "users" nil schema)
     (cl-letf (((symbol-function 'clutch-db-list-columns)
		(lambda (_conn _table)
                  (cl-incf column-calls)
                  (signal 'clutch-db-error '("column load failed"))))
               ((symbol-function 'clutch-db-column-details)
		(lambda (_conn _table)
                  (cl-incf detail-calls)
                  (signal 'clutch-db-error '("detail load failed")))))
       (dotimes (_ 2)
         (should-not (clutch--ensure-columns 'fake-conn schema "users"))
         (should-not (clutch--ensure-column-details 'fake-conn "users")))
       (should (= column-calls detail-calls 1))
       (dolist (property '(:columns-status :column-details-status))
         (should (eq (plist-get (clutch--metadata-status
                                 'fake-conn "users" property) :state)
                     'failed)))))))

(ert-deftest clutch-test-transient-metadata-errors-are-not-cached ()
  "Transient metadata failures should stay observable and retryable."
  (clutch-test--with-isolated-metadata-caches
   (let ((calls (make-hash-table :test 'eq))
         warnings)
     (cl-letf (((symbol-function 'clutch-db-table-comment)
		(lambda (_conn _table &optional _schema)
                  (cl-incf (gethash 'comment calls 0))
                  (if (= (gethash 'comment calls) 1)
                      (signal 'clutch-db-error '("comment boom"))
                    "Orders table")))
               ((symbol-function 'clutch-db-symbol-help)
		(lambda (_conn _symbol)
                  (cl-incf (gethash 'help calls 0))
                  (if (= (gethash 'help calls) 1)
                      (signal 'clutch-db-error '("help boom"))
                    '(:sig "ABS(X)" :desc "Returns absolute value."))))
               ((symbol-function 'clutch--remember-recoverable-metadata-warning)
                (lambda (_conn op _err &optional context)
                  (push (list op context) warnings))))
       (dolist (case `((comment
			,(lambda ()
                           (clutch--ensure-table-comment 'fake-conn "orders"))
			"Orders table"
			equal)
                       (help
			,(lambda ()
                           (clutch--ensure-help-doc 'fake-conn "abs"))
			"ABS(X)"
			string-match-p)))
         (pcase-let ((`(,label ,load ,expected ,match) case))
           (ert-info ((format "case: %s" label))
             (should-not (funcall load))
             (should (funcall match expected (funcall load)))
             (should (= (gethash label calls) 2)))))
       (should (equal (sort warnings
                            (lambda (a b) (string< (car a) (car b))))
                      '(("symbol help" (:symbol "abs"))
                        ("table comment" (:table "orders" :schema nil)))))))))

(ert-deftest clutch-test-column-details-async-callback-lifecycle ()
  "Async details should reject invalid callbacks and retain empty results."
  (dolist (case '(stale-ticket cleared-active dead-connection empty-result))
    (clutch-test--with-isolated-metadata-caches
     (let ((alive t) callback (calls 0))
       (cl-letf (((symbol-function 'clutch-db-live-p)
                  (lambda (_conn) alive))
                 ((symbol-function 'clutch-db-column-details-async)
                  (lambda (_conn _table cb &optional _errback)
                    (cl-incf calls)
                    (setq callback cb)
                    t)))
         (clutch--ensure-column-details-async 'fake-conn "users")
         (pcase case
           ((or 'stale-ticket 'cleared-active)
            (clutch--ensure-column-details-async 'fake-conn "orders")
            (if (eq case 'stale-ticket)
                (clutch--set-metadata-status
                 'fake-conn "users" :column-details-status 'loading nil
                 (clutch--begin-metadata-ticket))
              (clutch--clear-table-metadata-caches 'fake-conn "users")))
           ('dead-connection (setq alive nil)))
         (funcall callback
                  (unless (eq case 'empty-result)
                    '((:name "ignored" :type "int"))))
         (if (eq case 'empty-result)
             (progn
               (should (clutch--column-details-cached-p 'fake-conn "users"))
               (should-not (clutch--cached-column-details 'fake-conn "users"))
               (clutch--ensure-column-details-async 'fake-conn "users")
               (should (= calls 1)))
           (should-not (clutch--column-details-cached-p 'fake-conn "users")))
         (when (memq case '(stale-ticket cleared-active))
           (should (equal (car (clutch--column-details-active 'fake-conn))
                          "orders"))))))))

(ert-deftest clutch-test-load-fk-info-is-cache-first-and-async ()
  "Result FK display metadata should not synchronously hit the backend."
  (clutch-test--with-isolated-metadata-caches
   (with-temp-buffer
     (clutch-result-mode)
     (setq-local clutch-connection 'fake-conn
                 clutch--result-source-table "users"
                 clutch--result-columns '("id" "account_id"))
     (let (queued)
       (cl-letf (((symbol-function 'clutch-db-foreign-keys)
                  (lambda (&rest _)
                    (error "foreign keys should not load synchronously")))
                 ((symbol-function 'clutch--ensure-foreign-keys-async)
                  (lambda (_conn table)
                    (setq queued table))))
         (clutch--load-fk-info)
         (should (equal queued "users"))
         (should-not clutch--fk-info))))))

(ert-deftest clutch-test-foreign-keys-async-caches-and-refreshes-results ()
  "Foreign-key async callbacks should cache metadata and notify listeners."
  (clutch-test--with-isolated-metadata-caches
   (let (callback notified)
     (let ((clutch--table-metadata-updated-hook
            (list (lambda (conn table kind)
                    (should-not (clutch--metadata-status conn table :foreign-keys-status))
                    (setq notified (list conn table kind))))))
       (cl-letf (((symbol-function 'clutch-db-live-p)
                  (lambda (_conn) t))
                 ((symbol-function 'clutch-db-foreign-keys-async)
                  (lambda (_conn _table cb &optional _errback)
                    (setq callback cb)
                    t)))
         (clutch--ensure-foreign-keys-async 'fake-conn "users")
         (funcall callback '(("account_id" :ref-table "accounts"
                              :ref-column "id")))
         (should (equal (clutch--cached-foreign-keys 'fake-conn "users")
                        '(("account_id" :ref-table "accounts"
                           :ref-column "id"))))
         (should (equal notified '(fake-conn "users" foreign-keys)))
         (should-not (clutch--metadata-status 'fake-conn "users" :foreign-keys-status)))))))

(ert-deftest clutch-test-foreign-keys-async-unsupported-caches-empty-result ()
  "Backends without async foreign-key metadata should not be retried on each render."
  (clutch-test--with-isolated-metadata-caches
   (let ((calls 0))
     (cl-letf (((symbol-function 'clutch-db-foreign-keys-async)
                (lambda (&rest _args)
                  (cl-incf calls)
                  nil)))
       (clutch--ensure-foreign-keys-async 'fake-conn "users")
       (clutch--ensure-foreign-keys-async 'fake-conn "users")
       (should (= calls 1))
       (should (clutch--foreign-keys-cached-p 'fake-conn "users"))
       (should-not (clutch--cached-foreign-keys 'fake-conn "users"))
       (should-not (clutch--metadata-status 'fake-conn "users" :foreign-keys-status))))))

(ert-deftest clutch-test-refresh-result-metadata-buffers-updates-only-matching-results ()
  "Result metadata refresh should match connection identity, not its label."
  (let ((conn-a (list 'same-connection-shape))
        (conn-b (list 'same-connection-shape))
        (buf-a (generate-new-buffer " *clutch-result-a*"))
        (buf-b (generate-new-buffer " *clutch-result-b*"))
        (details '((:name "id" :type "int"))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "same-label"))
                  ((symbol-function 'clutch--result-column-details)
                   (lambda (_conn _table _col-names)
                     details)))
          (should (equal conn-a conn-b))
          (should-not (eq conn-a conn-b))
          (with-current-buffer buf-a
            (clutch-result-mode)
            (setq-local clutch-connection conn-a)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--result-source-table "users")
            (setq-local clutch--last-query "select * from users"))
          (with-current-buffer buf-b
            (clutch-result-mode)
            (setq-local clutch-connection conn-b)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--result-source-table "users")
            (setq-local clutch--last-query "select * from users"))
          (clutch--handle-table-metadata-updated conn-a "users" 'column-details)
          (with-current-buffer buf-a
            (should (equal clutch--result-column-details details)))
          (with-current-buffer buf-b
            (should-not clutch--result-column-details)))
      (when (buffer-live-p buf-a)
        (kill-buffer buf-a))
      (when (buffer-live-p buf-b)
        (kill-buffer buf-b)))))

(ert-deftest clutch-test-metadata-update-refreshes-only-its-qualified-table ()
  "Column details of aux.people should refresh results of aux.people only."
  (let ((plain (generate-new-buffer " *clutch-result-plain*"))
        (aux (generate-new-buffer " *clutch-result-aux*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--result-column-details)
                   (lambda (_conn table _col-names) (list table))))
          (dolist (spec (list (list plain nil) (list aux "aux")))
            (with-current-buffer (car spec)
              (clutch-result-mode)
              (setq-local clutch-connection 'conn
                          clutch--result-columns '("id")
                          clutch--result-source-table "people"
                          clutch--result-source-schema (cadr spec)
                          clutch--last-query "select * from people")))
          (clutch--handle-table-metadata-updated
           'conn '(nil "aux" "people") 'column-details)
          (with-current-buffer aux
            (should (equal clutch--result-column-details
                           '((nil "aux" "people")))))
          (with-current-buffer plain
            (should-not clutch--result-column-details)))
      (kill-buffer plain)
      (kill-buffer aux))))

(ert-deftest clutch-test-column-details-refresh-redraws-pending-insert-placeholders ()
  "Async column details should redraw staged insert metadata placeholders."
  (clutch-test--with-isolated-metadata-caches
   (let ((buf (generate-new-buffer " *clutch-result-pending-insert*"))
         callback)
     (unwind-protect
         (let ((clutch--table-metadata-updated-hook
                (list #'clutch--handle-table-metadata-updated)))
           (cl-letf (((symbol-function 'clutch-db-live-p)
                      (lambda (_conn) t))
                     ((symbol-function 'clutch-db-column-details-async)
                      (lambda (_conn _table cb &optional _errback)
                        (setq callback cb)
                        t)))
             (with-current-buffer buf
               (clutch-test--init-result-state
                (list :connection 'fake-conn
                      :columns '("id" "name")
                      :column-defs '((:name "id" :type-category numeric)
                                     (:name "name" :type-category text))
                      :rows nil
                      :source-table "users"
                      :pending-inserts '((("name" . "alice")))
                      :column-widths [12 12]
                      :render t))
               (should-not (string-match-p "<generated>" (buffer-string))))
             (should callback)
             (funcall callback '((:name "id" :generated t)
                                 (:name "name")))
             (with-current-buffer buf
               (should (string-match-p "<generated>" (buffer-string)))
               (should (string-match-p "alice" (buffer-string))))))
       (when (buffer-live-p buf)
         (kill-buffer buf))))))

(ert-deftest clutch-test-foreign-key-refresh-redraws-only-marked-results ()
  "Async foreign keys should redraw a result only when they mark its columns."
  (dolist (case '(unmarked marked))
    (clutch-test--with-isolated-metadata-caches
     (let ((buf (generate-new-buffer " *clutch-result-fk*"))
           (render (symbol-function 'clutch--render-result))
           (renders 0)
           callback)
       (unwind-protect
           (let ((clutch--table-metadata-updated-hook
                  (list #'clutch--handle-table-metadata-updated)))
             (cl-letf (((symbol-function 'clutch-db-live-p)
                        (lambda (_conn) t))
                       ((symbol-function 'clutch-db-foreign-keys-async)
                        (lambda (_conn _table cb &optional _errback)
                          (setq callback cb)
                          t)))
               (with-current-buffer buf
                 (clutch-test--init-result-state
                  (list :connection 'fake-conn
                        :columns '("id" "account_id")
                        :rows '((1 7))
                        :source-table "users"
                        :column-widths [12 12]
                        :render t))
                 (clutch--load-fk-info))
               (should callback)
               (cl-letf (((symbol-function 'clutch--render-result)
                          (lambda ()
                            (cl-incf renders)
                            (funcall render))))
                 (funcall callback
                          (and (eq case 'marked)
                               '(("account_id" :ref-table "accounts"
                                  :ref-column "id")))))
               (with-current-buffer buf
                 (should (= renders (if (eq case 'marked) 1 0)))
                 (let ((line (clutch-test--rendered-line-at 0)))
                   (should (eq (get-text-property (string-match "7" line)
                                                  'face line)
                               (and (eq case 'marked) 'clutch-fk-face)))))))
         (when (buffer-live-p buf)
           (kill-buffer buf)))))))

(ert-deftest clutch-test-column-info-string-contract ()
  "Column info strings should format detail text, faces, and missing metadata."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-column-details
                (list (list :name "id" :type "INT" :nullable nil
                            :default "42" :comment "Primary key")))
    (let* ((info (clutch--column-info-string 0))
           (one-line (clutch--column-info-message-string info))
           (name-pos (string-match-p "\\bid\\b" one-line))
           (type-pos (string-match-p "INT" one-line))
           (sep-pos (string-match-p "  •  " one-line)))
      (should (string-match-p "Type: INT" info))
      (should (string-match-p "Nullable: NO" info))
      (should (string-match-p "Default: 42" info))
      (should (string-match-p "Primary key" info))
      (should name-pos)
      (should type-pos)
      (should sep-pos)
      (should (eq (get-text-property name-pos 'face one-line)
                  'clutch-field-name-face))
      (should (eq (get-text-property type-pos 'face one-line)
                  'font-lock-type-face))
      (should (eq (get-text-property sep-pos 'face one-line)
                  'font-lock-comment-face)))
    (setq-local clutch--result-column-details
                (list nil
                      (list :name "name" :type "VARCHAR(255)" :nullable t
                            :default "unnamed")))
    (let ((info (clutch--column-info-string 1)))
      (should (string-match-p "Type: VARCHAR(255)" info))
      (should (string-match-p "Nullable: YES" info))
      (should (string-match-p "Default: unnamed" info)))
    (setq-local clutch--result-column-details nil)
    (should-not (clutch--column-info-string 0))))

(ert-deftest clutch-test-result-column-details-contract ()
  "Detail resolution should map result columns by name and skip missing tables."
  (cl-letf (((symbol-function 'clutch--cached-column-details)
             (lambda (_conn _table)
               (list (list :name "ID" :type "INT" :nullable nil)
                     (list :name "NAME" :type "VARCHAR" :nullable t)))))
    (let ((result (clutch--result-column-details
                   'dummy-conn "users" '("id" "name"))))
      (should (= (length result) 2))
      (should (equal (plist-get (nth 0 result) :type) "INT"))
      (should (equal (plist-get (nth 1 result) :type) "VARCHAR"))))
  (should-not (clutch--result-column-details
               'dummy nil '("col1"))))

(provide 'clutch-test-schema)

;;; clutch-test-schema.el ends here
