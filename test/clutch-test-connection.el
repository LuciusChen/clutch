;;; clutch-test-connection.el --- Connection workflow ERT tests -*- lexical-binding: t; -*-

;;; Commentary:

;; Connection setup, lifecycle, transport, query-console, and transaction tests.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-document)
  (require 'clutch-redis))

(defvar mysql-tls-verify-server)
(defvar tramp-rpc-use-controlmaster)
(defvar clutch-test-backend)
(defvar clutch-test-host)
(defvar clutch-test-port)
(defvar clutch-test-user)
(defvar clutch-test-password)
(defvar clutch-test-database)
(defvar clutch-test-url)
(defvar clutch-test-display-name)
(defvar clutch-test-props)

(declare-function make-clutch-jdbc-conn "clutch-db-jdbc" (&rest slot-value-pairs))
(declare-function make-clutch-db-sqlite-conn "clutch-db-sqlite" (&rest slot-value-pairs))
(declare-function make-mysql-conn "mysql" (&rest args))
(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))

;;;; Connection — build, timeout, and lifecycle

(ert-deftest clutch-test-backend-key-from-conn-returns-nil-for-opaque-connections ()
  "Backend key detection should tolerate opaque connections."
  (should-not (clutch--backend-key-from-conn 'fake-conn)))

(ert-deftest clutch-test-backend-key-from-conn-propagates-backend-errors ()
  "Backend key detection should expose invalid backend state."
  (cl-letf (((symbol-function 'clutch-db-backend-key)
             (lambda (_conn)
               (signal 'clutch-db-error '("backend key boom")))))
    (should-error (clutch--backend-key-from-conn 'broken-conn)
                  :type 'clutch-db-error)))

(ert-deftest clutch-test-backend-key-from-params-prefers-concrete-backend ()
  "Concrete :backend should win over generic JDBC :driver metadata."
  (should (eq (clutch--backend-key-from-params
               '(:backend oracle :driver jdbc))
              'oracle))
  (should (eq (clutch--backend-key-from-params
               '(:backend jdbc :driver oracle))
              'oracle))
  (should (eq (clutch--backend-key-from-params
               '(:backend jdbc :driver jdbc :display-name "KingbaseES"))
              'jdbc)))

(ert-deftest clutch-test-connection-key ()
  :tags '(:smoke)
  "Test connection key generation."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost" :port 3306
                                    :user "root" :database "test")))
    (let ((key (clutch--connection-key conn)))
      (should (stringp key))
      (should (string-match-p "localhost" key))
      (should (string-match-p "3306" key))
      (should (string-match-p "root" key))
      (should (string-match-p "test" key)))))

(ert-deftest clutch-test-connection-display-key-formats-endpoints ()
  "Connection display labels should cover local, remote, and SQLite endpoints."
  (dolist (case
           '((remote-ssh pg "alice" "127.0.0.1" 40123 "appdb"
              (:host "db.internal" :port 5432 :ssh-host "bastion-prod")
              "alice@db.internal:5432/appdb"
              "alice@db.internal via bastion-prod")
             (remote-tramp pg "alice" "127.0.0.1" 40123 "appdb"
              (:host "db" :port 5432
               :tramp-default-directory "/ssh:devbox:/workspace/")
              "alice@db:5432/appdb"
              "alice@db via devbox")
             (default-port oracle "scott" "dbhost" 1521 nil nil nil
              "scott@dbhost")
             (nondefault-port oracle "scott" "dbhost" 1522 nil nil nil
              "scott@dbhost:1522")
             (empty-user mongodb nil "127.0.0.1" 27017 "app" nil
              "127.0.0.1:27017/app" "127.0.0.1")
             (sqlite-file sqlite nil nil nil "/tmp/bookmarks.db" nil
              "sqlite:/tmp/bookmarks.db" "bookmarks.db")
             (sqlite-memory sqlite nil nil nil ":memory:" nil nil
              ":memory:")))
    (pcase-let ((`(,label ,backend ,user ,host ,port ,database
                          ,remote ,expected-key ,expected-display)
                 case))
      (ert-info ((symbol-name label))
        (let ((clutch--connection-remote-params-cache
               (make-hash-table :test 'eq))
              (conn 'fake-conn))
          (when remote
            (puthash conn remote clutch--connection-remote-params-cache))
          (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                     (lambda (_conn) backend))
                    ((symbol-function 'clutch-db-user)
                     (lambda (_conn) user))
                    ((symbol-function 'clutch-db-host)
                     (lambda (_conn) host))
                    ((symbol-function 'clutch-db-port)
                     (lambda (_conn) port))
                    ((symbol-function 'clutch-db-database)
                     (lambda (_conn) database)))
            (when expected-key
              (should (equal (clutch--connection-key conn) expected-key)))
            (should (equal (clutch--connection-display-key conn)
                           expected-display))))))))

(ert-deftest clutch-test-prepare-origin-params-contract ()
  "TRAMP source origin inference should obey policy and transport constraints."
  (dolist (case
           '((:label "auto ssh source"
              :policy auto
              :source "/ssh:devbox:/workspace/"
              :params (:backend pg :host "db" :port 5432 :user "app")
              :expected (:backend pg :host "db" :port 5432 :user "app"
                         :tramp-default-directory "/ssh:devbox:/workspace/"))
             (:label "auto container source"
              :policy auto
              :source "/docker:vscode@app:/workspace/"
              :params (:backend pg :host "db" :port 5432 :user "app")
              :expected (:backend pg :host "db" :port 5432 :user "app"
                         :tramp-default-directory "/docker:vscode@app:/workspace/"))
             (:label "ask declined"
              :policy ask
              :source "/ssh:devbox:/workspace/"
              :params (:backend pg :host "db" :port 5432 :user "app")
              :ask nil
              :expect-asked t
              :expected (:backend pg :host "db" :port 5432 :user "app"))
             (:label "explicit ssh transport wins"
              :policy auto
              :source "/ssh:devbox:/workspace/"
              :params (:backend pg :host "db" :port 5432 :user "app"
                       :ssh-host "bastion-prod")
              :expected (:backend pg :host "db" :port 5432 :user "app"
                         :ssh-host "bastion-prod"))
             (:label "sqlite is not forwardable"
              :policy auto
              :source "/ssh:devbox:/workspace/"
              :params (:backend sqlite :database "/tmp/app.db")
              :absent :tramp-default-directory)
             (:label "raw url is not forwardable"
              :policy auto
              :source "/ssh:devbox:/workspace/"
              :params (:backend oracle :url "jdbc:oracle:thin:@//db:1521/ORCL")
              :absent :tramp-default-directory)))
    (ert-info ((plist-get case :label))
      (let ((clutch-tramp-context-policy (plist-get case :policy))
            asked)
        (cl-letf (((symbol-function 'y-or-n-p)
                   (lambda (_prompt)
                     (setq asked t)
                     (plist-get case :ask))))
          (let ((result (clutch--prepare-connection-origin-params
                         (plist-get case :params)
                         (plist-get case :source))))
            (if-let* ((absent (plist-get case :absent)))
                (should-not (plist-member result absent))
              (should (equal result (plist-get case :expected))))
            (when (plist-get case :expect-asked)
              (should asked))))))))

(ert-deftest clutch-test-prepare-connection-params-rejects-removed-tramp-key ()
  "The removed :tramp key should fail instead of being ignored."
  (should-error
   (clutch-prepare-connection-params
    '(:backend pg :host "db" :port 5432 :user "app"
      :tramp "/ssh:devbox:/workspace/"))
   :type 'user-error))

(ert-deftest clutch-test-build-conn-leaves-timeout-defaults-to-backends ()
  "Connection building should not synthesize backend timeout defaults."
  (let (captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
      (dolist (params '((:backend mysql :host "127.0.0.1" :port 3306 :user "u")
                        (:backend pg :host "127.0.0.1" :port 5432 :user "u")
                        (:backend oracle :host "db" :port 1521 :user "u")
                        (:backend sqlite :database ":memory:")))
        (setq captured nil)
        (clutch--build-conn params)
        (should-not (plist-member captured :connect-timeout))
        (should-not (plist-member captured :read-idle-timeout))
        (should-not (plist-member captured :query-timeout))
        (should-not (plist-member captured :rpc-timeout))))))

(ert-deftest clutch-test-build-conn-rewrites-endpoints-through-ssh-tunnel ()
  "SSH-backed host/port connections should target the local forwarded port."
  (dolist (case
           (list
            (list :label "SQL backend"
                  :conn 'fake-conn
                  :local-port 40123
                  :params '(:backend pg
                            :host "db.internal"
                            :port 5432
                            :user "alice"
                            :database "appdb"
                            :ssh-host "bastion-prod"))
            (list :label "MongoDB backend"
                  :conn 'fake-mongo-conn
                  :local-port 47017
                  :params '(:backend mongodb
                            :host "mongo.internal"
                            :port 27017
                            :database "app"
                            :ssh-host "bastion-prod"))))
    (ert-info ((format "SSH endpoint rewrite: %s" (plist-get case :label)))
      (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
            (clutch--connection-transport-cache (make-hash-table :test 'eq))
            (input (plist-get case :params))
            captured)
        (cl-letf (((symbol-function 'clutch--resolve-password)
                   (lambda (_params) nil))
                  ((symbol-function 'clutch--start-ssh-tunnel)
                   (lambda (params)
                     (should (eq (plist-get params :backend)
                                 (plist-get input :backend)))
                     (should (equal (plist-get params :host)
                                    (plist-get input :host)))
                     (should (= (plist-get params :port)
                                (plist-get input :port)))
                     (should (equal (plist-get params :ssh-host) "bastion-prod"))
                     (list :kind 'ssh
                           :process 'fake-proc
                           :local-port (plist-get case :local-port)
                           :ssh-host "bastion-prod")))
                  ((symbol-function 'clutch-db-connect)
                   (lambda (backend params)
                     (should (eq backend (plist-get input :backend)))
                     (setq captured params)
                     (plist-get case :conn)))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t)))
          (let ((conn (plist-get case :conn)))
            (should (eq (clutch--build-conn input) conn))
            (should (equal (plist-get captured :host) "127.0.0.1"))
            (should (= (plist-get captured :port) (plist-get case :local-port)))
            (should-not (plist-member captured :ssh-host))
            (should (equal (plist-get
                            (gethash conn clutch--connection-remote-params-cache)
                            :host)
                           (plist-get input :host)))
            (should (equal (plist-get
                            (gethash conn clutch--connection-transport-cache)
                            :ssh-host)
                           "bastion-prod"))))))))

(ert-deftest clutch-test-build-conn-direct-first-selects-direct-or-ssh ()
  "Direct-first SSH mode should probe direct TCP before tunneling."
  (dolist (direct-open '(t nil))
    (ert-info ((format "direct-open=%S" direct-open))
      (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
            (clutch--connection-transport-cache (make-hash-table :test 'eq))
            captured probed tunnel-params attempts restored)
        (cl-letf (((symbol-function 'clutch--resolve-password)
                   (lambda (_params) nil))
                  ((symbol-function 'clutch--tcp-endpoint-open-p)
                   (lambda (host port timeout)
                     (setq probed (list host port timeout))
                     direct-open))
                  ((symbol-function 'clutch--start-ssh-tunnel)
                   (lambda (params)
                     (setq tunnel-params params)
                     '(:process fake-proc :local-port 40123
                       :ssh-host "bastion-prod")))
                  ((symbol-function 'clutch-db-connect)
                   (lambda (_backend params)
                     (setq attempts (append attempts (list params)))
                     (setq captured params)
                     'fake-conn))
                  ((symbol-function 'clutch-db--restore-connection-timeouts)
                   (lambda (conn params)
                     (setq restored (list conn params))))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t)))
          (should (eq (clutch--build-conn
                       '(:backend pg
                         :host "db.internal"
                         :port 5432
                         :user "alice"
                         :database "appdb"
                         :ssh-host "bastion-prod"
                         :ssh-tunnel direct-first))
                      'fake-conn))
          (should (equal probed
                         (list "db.internal" 5432
                               clutch--ssh-direct-first-probe-timeout)))
          (should-not (plist-member captured :ssh-tunnel))
          (should (= (length attempts) 1))
          (if direct-open
              (progn
                (should (equal (plist-get captured :host) "db.internal"))
                (should (= (plist-get captured :port) 5432))
                (should (= (plist-get captured :connect-timeout)
                           clutch--ssh-direct-first-connect-timeout))
                (should (= (plist-get captured :read-idle-timeout)
                           clutch--ssh-direct-first-connect-timeout))
                (should-not (plist-member captured :rpc-timeout))
                (should (equal (car restored) 'fake-conn))
                (should (equal (plist-get (cadr restored) :host)
                               "db.internal"))
                (should-not tunnel-params)
                (should-not (gethash 'fake-conn
                                     clutch--connection-transport-cache)))
            (should (equal (plist-get tunnel-params :ssh-host) "bastion-prod"))
            (should (equal (plist-get captured :host) "127.0.0.1"))
            (should (= (plist-get captured :port) 40123))
            (should-not (plist-member captured :connect-timeout))
            (should-not (plist-member captured :read-idle-timeout))
            (should-not restored)
            (should (equal (plist-get
                            (gethash 'fake-conn
                                     clutch--connection-transport-cache)
                            :ssh-host)
                           "bastion-prod"))))))))

(ert-deftest clutch-test-build-conn-direct-first-falls-back-after-db-error ()
  "Direct-first should require a successful direct database connection."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        attempts tunnel-params restored)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--tcp-endpoint-open-p)
               (lambda (_host _port _timeout) t))
              ((symbol-function 'clutch--start-ssh-tunnel)
               (lambda (params)
                 (setq tunnel-params params)
                 '(:process fake-proc :local-port 40123
                   :ssh-host "bastion-prod")))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq attempts (append attempts (list params)))
                 (if (equal (plist-get params :host) "db.internal")
                     (signal 'clutch-db-error
                             '("Connection closed by server"))
                   'fake-conn)))
              ((symbol-function 'clutch-db--restore-connection-timeouts)
               (lambda (conn params)
                 (setq restored (list conn params))))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
      (should (eq (clutch--build-conn
                   '(:backend oracle
                     :host "db.internal"
                     :port 1521
                     :user "alice"
                     :database "ORCL"
                     :ssh-host "bastion-prod"
                     :ssh-tunnel direct-first))
                  'fake-conn))
      (should (= (length attempts) 2))
      (should (equal (plist-get (nth 0 attempts) :host) "db.internal"))
      (should (= (plist-get (nth 0 attempts) :connect-timeout) 1))
      (should (= (plist-get (nth 0 attempts) :rpc-timeout) 2))
      (should-not (plist-member (nth 0 attempts) :read-idle-timeout))
      (should (equal (plist-get (nth 1 attempts) :host) "127.0.0.1"))
      (should (= (plist-get (nth 1 attempts) :port) 40123))
      (should-not (plist-member (nth 1 attempts) :connect-timeout))
      (should-not (plist-member (nth 1 attempts) :read-idle-timeout))
      (should (equal (plist-get tunnel-params :ssh-host) "bastion-prod"))
      (should-not restored)
      (should (equal (plist-get (gethash 'fake-conn
                                          clutch--connection-transport-cache)
                                :ssh-host)
                     "bastion-prod")))))

(ert-deftest clutch-test-db-mysql-restore-connection-timeouts ()
  "Restoring MySQL timeout state should undo provisional direct-connect limits."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :read-idle-timeout 1)))
    (clutch-db--restore-connection-timeouts
     conn '(:host "db.internal" :user "alice" :read-idle-timeout 42))
    (should (= (mysql-conn-read-idle-timeout conn) 42))))

(ert-deftest clutch-test-build-conn-rewrites-endpoints-through-tramp-forward ()
  "TRAMP-backed host/port connections should target the local forwarded port."
  (dolist (case
           (list
            (list :label "SQL backend"
                  :conn 'fake-conn
                  :local-port 40124
                  :params '(:backend pg
                            :host "db"
                            :port 5432
                            :user "alice"
                            :database "appdb"
                            :tramp-default-directory "/ssh:devbox:/workspace/"))
            (list :label "MongoDB backend"
                  :conn 'fake-mongo-conn
                  :local-port 47018
                  :params '(:backend mongodb
                            :host "mongo.internal"
                            :port 27017
                            :database "app"
                            :tramp-default-directory "/ssh:devbox:/workspace/"))))
    (ert-info ((format "TRAMP endpoint rewrite: %s" (plist-get case :label)))
      (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
            (clutch--connection-transport-cache (make-hash-table :test 'eq))
            (input (plist-get case :params))
            captured)
        (cl-letf (((symbol-function 'clutch--resolve-password)
                   (lambda (_params) nil))
                  ((symbol-function 'clutch--start-tramp-tcp-forward)
                   (lambda (params)
                     (should (eq (plist-get params :backend)
                                 (plist-get input :backend)))
                     (should (equal (plist-get params :host)
                                    (plist-get input :host)))
                     (should (= (plist-get params :port)
                                (plist-get input :port)))
                     (should (equal (plist-get params :tramp-default-directory)
                                    "/ssh:devbox:/workspace/"))
                     (list :kind 'tramp
                           :process 'fake-listener
                           :local-port (plist-get case :local-port)
                           :tramp-default-directory "/ssh:devbox:/workspace/")))
                  ((symbol-function 'clutch-db-connect)
                   (lambda (backend params)
                     (should (eq backend (plist-get input :backend)))
                     (setq captured params)
                     (plist-get case :conn)))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t)))
          (let ((conn (plist-get case :conn)))
            (should (eq (clutch--build-conn input) conn))
            (should (equal (plist-get captured :host) "127.0.0.1"))
            (should (= (plist-get captured :port) (plist-get case :local-port)))
            (should-not (plist-member captured :tramp-default-directory))
            (should (equal (plist-get
                            (gethash conn clutch--connection-remote-params-cache)
                            :host)
                           (plist-get input :host)))
            (should (equal (plist-get
                            (gethash conn clutch--connection-remote-params-cache)
                            :tramp-default-directory)
                           "/ssh:devbox:/workspace/"))
            (should (eq (plist-get
                         (gethash conn clutch--connection-transport-cache)
                         :kind)
                        'tramp))))))))

(ert-deftest clutch-test-open-connection-supports-tramp-default-directory ()
  "The public connection API should support TRAMP default-directory origin."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-tramp-tcp-forward)
               (lambda (params)
                 (should (equal (plist-get params :tramp-default-directory)
                                "/ssh:devbox:/workspace/"))
                 '(:kind tramp
                   :process fake-listener
                   :local-port 40124
                   :tramp-default-directory "/ssh:devbox:/workspace/")))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
      (should (eq (clutch-open-connection
                   '(:backend pg
                     :host "db"
                     :port 5432
                     :user "alice"
                     :database "appdb"
                     :tramp-default-directory "/ssh:devbox:/workspace/"))
                  'fake-conn))
      (should (equal (plist-get captured :host) "127.0.0.1"))
      (should (= (plist-get captured :port) 40124))
      (should-not (plist-member captured :tramp-default-directory)))))

(ert-deftest clutch-test-prepare-connect-params-rejects-ambiguous-transports ()
  "A connection must not combine SSH and TRAMP transports."
  (should-error
   (clutch--prepare-connect-params
    '(:backend pg
      :host "db"
      :port 5432
      :ssh-host "bastion-prod"
      :tramp-default-directory "/ssh:devbox:/workspace/"))
   :type 'user-error))

(ert-deftest clutch-test-prepare-connect-params-validates-ssh-tunnel-mode ()
  "SSH tunnel mode should be explicit and tied to :ssh-host."
  (should-error
   (clutch--prepare-connect-params
    '(:backend pg :host "db" :port 5432 :ssh-tunnel direct-first))
   :type 'user-error)
  (should-error
   (clutch--prepare-connect-params
    '(:backend pg :host "db" :port 5432 :ssh-host "bastion-prod"
      :ssh-tunnel sometimes))
   :type 'user-error))

(ert-deftest clutch-test-build-conn-stops-ssh-tunnel-when-db-connect-fails ()
  "Failed DB connect should tear down the SSH tunnel it just opened."
  (let (stopped)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-ssh-tunnel)
               (lambda (_params)
                 '(:process fake-proc :local-port 40123 :ssh-host "bastion-prod")))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'delete-process)
               (lambda (proc) (setq stopped proc)))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend _params)
                 (signal 'clutch-db-error '("db connect failed")))))
      (should-error
       (clutch--build-conn
        '(:backend pg
          :host "db.internal"
          :port 5432
          :user "alice"
          :database "appdb"
          :ssh-host "bastion-prod"))
       :type 'user-error)
      (should (eq stopped 'fake-proc)))))

(ert-deftest clutch-test-start-ssh-tunnel-rejects-opaque-url-profiles ()
  "SSH tunneling should reject URL-only profiles before opening transports."
  (dolist (case
           (list
            (list :label "raw JDBC URL"
                  :params '(:backend oracle
                            :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
                            :user "scott"
                            :ssh-host "bastion-prod"))
            (list :label "MongoDB URL"
                  :params '(:backend mongodb
                            :url "mongodb://mongo.internal:27017/app"
                            :ssh-host "bastion-prod")
                  :message (concat "Structured forwarding via SSH tunnels "
                                   "requires :host/:port params, not :url"))))
    (ert-info ((format "SSH URL rejection: %s" (plist-get case :label)))
      (let (connect-called process-called)
        (cl-letf (((symbol-function 'clutch--resolve-password)
                   (lambda (_params) nil))
                  ((symbol-function 'executable-find)
                   (lambda (_name) "/usr/bin/ssh"))
                  ((symbol-function 'clutch-db-connect)
                   (lambda (&rest _args)
                     (setq connect-called t)
                     'unexpected))
                  ((symbol-function 'make-process)
                   (lambda (&rest _args)
                     (setq process-called t)
                     'unexpected)))
          (let ((err (should-error
                      (clutch--start-ssh-tunnel (plist-get case :params))
                      :type 'user-error)))
            (when-let* ((message (plist-get case :message)))
              (should (equal (cadr err) message))))
          (should-not connect-called)
          (should-not process-called))))))

(ert-deftest clutch-test-start-ssh-tunnel-starts-batch-tunnel-directly ()
  "SSH tunnel startup should rely on the real batch tunnel command."
  (let (process-file-called make-process-args)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'process-file)
               (lambda (&rest _args)
                 (setq process-file-called t)
                 0))
              ((symbol-function 'clutch--allocate-local-port)
               (lambda () 40123))
              ((symbol-function 'make-process)
                (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
              ((symbol-function 'clutch--wait-for-ssh-tunnel)
               (lambda (&rest _args) t)))
      (let ((tunnel (clutch--start-ssh-tunnel
                     '(:backend pg
                       :host "db.internal"
                       :port 5432
                       :ssh-host "bastion-prod"))))
        (should-not process-file-called)
        (should (eq (plist-get tunnel :process) 'fake-proc))
        (should (equal (plist-get make-process-args :command)
                       '("ssh"
                         "-N"
                         "-o" "BatchMode=yes"
                         "-o" "ExitOnForwardFailure=yes"
                         "-L" "127.0.0.1:40123:db.internal:5432"
                         "bastion-prod")))))))

(ert-deftest clutch-test-start-tramp-forward-rejects-opaque-url-profiles ()
  "TRAMP forwarding should reject URL-only profiles before opening transports."
  (dolist (case
           (list
            (list :label "raw JDBC URL"
                  :params '(:backend oracle
                            :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
                            :user "scott"
                            :tramp-default-directory "/ssh:devbox:/workspace/"))
            (list :label "MongoDB URL"
                  :params '(:backend mongodb
                            :url "mongodb://mongo.internal:27017/app"
                            :tramp-default-directory "/ssh:devbox:/workspace/")
                  :message (concat "Structured forwarding via TRAMP forwarding "
                                   "requires :host/:port params, not :url"))))
    (ert-info ((format "TRAMP URL rejection: %s" (plist-get case :label)))
      (let (process-called)
        (cl-letf (((symbol-function 'make-process)
                   (lambda (&rest _args)
                     (setq process-called t)
                     'unexpected)))
          (let ((err (should-error
                      (clutch--start-tramp-tcp-forward (plist-get case :params))
                      :type 'user-error)))
            (when-let* ((message (plist-get case :message)))
              (should (equal (cadr err) message))))
          (should-not process-called))))))

(ert-deftest clutch-test-start-tramp-forward-starts-direct-ssh-forward ()
  "Direct ssh TRAMP forward startup should use OpenSSH -L."
  (let (make-process-args query-flag waited)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "ssh") "/usr/bin/ssh")))
              ((symbol-function 'clutch--allocate-local-port)
               (lambda () 40124))
              ((symbol-function 'make-process)
               (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'set-process-query-on-exit-flag)
               (lambda (proc flag)
                 (setq query-flag (list proc flag))))
              ((symbol-function 'clutch--wait-for-ssh-tunnel)
               (lambda (proc port params buffer timeout)
                 (setq waited (list proc port params buffer timeout)))))
      (let ((transport (clutch--start-tramp-tcp-forward
                        '(:backend pg
                          :host "db"
                          :port 5432
                          :tramp-default-directory "/ssh:devbox:/workspace/"))))
        (should (equal (plist-get make-process-args :command)
                       '("ssh"
                         "-N"
                         "-o" "BatchMode=yes"
                         "-o" "ExitOnForwardFailure=yes"
                         "-L" "127.0.0.1:40124:db:5432"
                         "devbox")))
        (should (equal query-flag '(fake-proc nil)))
        (should (eq (plist-get transport :kind) 'tramp))
        (should (eq (plist-get transport :process) 'fake-proc))
        (should (= (plist-get transport :local-port) 40124))
        (should (equal (plist-get transport :tramp-default-directory)
                       "/ssh:devbox:/workspace/"))
        (should (equal (nth 0 waited) 'fake-proc))
        (should (= (nth 1 waited) 40124))
        (should (equal (plist-get (nth 2 waited) :ssh-host) "devbox"))))))

(ert-deftest clutch-test-start-tramp-forward-starts-rpc-forward ()
  "tramp-rpc directories should be mapped to OpenSSH -L."
  (let ((old-bound (boundp 'tramp-rpc-use-controlmaster))
        (old-value (and (boundp 'tramp-rpc-use-controlmaster)
                        (symbol-value 'tramp-rpc-use-controlmaster)))
        make-process-args)
    (unwind-protect
        (progn
          (set 'tramp-rpc-use-controlmaster t)
          (cl-letf (((symbol-function 'executable-find)
                     (lambda (program)
                       (when (equal program "ssh") "/usr/bin/ssh")))
                    ((symbol-function 'clutch--allocate-local-port)
                     (lambda () 40124))
                    ((symbol-function 'make-process)
                     (lambda (&rest args)
                       (setq make-process-args args)
                       'fake-proc))
                    ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
                    ((symbol-function 'clutch--wait-for-ssh-tunnel) #'ignore)
                    ((symbol-function 'tramp-rpc-controlmaster-options)
                     (lambda (vec)
                       (should (equal (tramp-file-name-method vec) "rpc"))
                       '("-o" "ControlMaster=auto"
                         "-o" "ControlPath=/tmp/tramp-rpc.sock"))))
            (clutch--start-tramp-tcp-forward
             '(:backend pg
               :host "127.0.0.1"
               :port 55433
               :tramp-default-directory "/rpc:devbox:/workspace/"))
            (should (equal (plist-get make-process-args :command)
                           '("ssh"
                             "-N"
                             "-o" "BatchMode=yes"
                             "-o" "ExitOnForwardFailure=yes"
                             "-L" "127.0.0.1:40124:127.0.0.1:55433"
                             "-o" "ControlMaster=auto"
                             "-o" "ControlPath=/tmp/tramp-rpc.sock"
                             "devbox")))))
      (if old-bound
          (set 'tramp-rpc-use-controlmaster old-value)
        (makunbound 'tramp-rpc-use-controlmaster)))))

(ert-deftest clutch-test-start-tramp-forward-maps-ssh-like-hops-to-proxyjump ()
  "SSH-like TRAMP hops should become an OpenSSH ProxyJump option."
  (let (make-process-args)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "ssh") "/usr/bin/ssh")))
              ((symbol-function 'clutch--allocate-local-port)
               (lambda () 40124))
              ((symbol-function 'make-process)
               (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
              ((symbol-function 'clutch--wait-for-ssh-tunnel) #'ignore))
      (clutch--start-tramp-tcp-forward
       '(:backend pg
         :host "db"
         :port 5432
         :tramp-default-directory "/rpc:jump|rpc:devbox:/workspace/"))
      (should (equal (plist-get make-process-args :command)
                     '("ssh"
                       "-N"
                       "-o" "BatchMode=yes"
                       "-o" "ExitOnForwardFailure=yes"
                       "-L" "127.0.0.1:40124:db:5432"
                       "-J" "jump"
                       "devbox"))))))

(ert-deftest clutch-test-start-tramp-forward-starts-docker-container-relay ()
  "Docker TRAMP forward startup should create a local relay listener."
  (let (network-args puts query-flag)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "docker") "/usr/bin/docker")))
              ((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq network-args args)
                 'fake-listener))
              ((symbol-function 'process-contact)
               (lambda (_process &optional key _no-block)
                 (when (eq key :service) 40125)))
              ((symbol-function 'process-put)
               (lambda (process key value)
                 (push (list process key value) puts)))
              ((symbol-function 'set-process-query-on-exit-flag)
               (lambda (proc flag)
                 (setq query-flag (list proc flag)))))
      (let ((transport (clutch--start-tramp-tcp-forward
                        '(:backend pg
                          :host "db"
                          :port 5432
                          :tramp-default-directory
                          "/docker:vscode@app:/workspace/"))))
        (should (equal (plist-get network-args :name)
                       "clutch-tramp-container-db:5432"))
        (should (eq (plist-get network-args :server) t))
        (should (equal (plist-get network-args :host) "127.0.0.1"))
        (should (eq (plist-get network-args :service) t))
        (should (eq (plist-get network-args :coding) 'no-conversion))
        (should (equal query-flag '(fake-listener nil)))
        (should (member
                 (list 'fake-listener
                       :clutch-container-command
                       (list "docker" "exec" "-i" "-u" "vscode" "app"
                             "sh" "-lc" clutch--container-relay-script
                             "clutch-container-relay" "db" "5432"))
                 puts))
        (should (eq (plist-get transport :kind) 'tramp))
        (should (eq (plist-get transport :process) 'fake-listener))
        (should (= (plist-get transport :local-port) 40125))
        (should (equal (plist-get transport :tramp-default-directory)
                       "/docker:vscode@app:/workspace/"))))))

(ert-deftest clutch-test-start-tramp-forward-starts-remote-podman-container-relay ()
  "Container TRAMP paths behind SSH hops should run the runtime remotely."
  (let (network-args puts)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "ssh") "/usr/bin/ssh")))
              ((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq network-args args)
                 'fake-listener))
              ((symbol-function 'process-contact)
               (lambda (_process &optional key _no-block)
                 (when (eq key :service) 40126)))
              ((symbol-function 'process-put)
               (lambda (process key value)
                 (push (list process key value) puts)))
              ((symbol-function 'set-process-query-on-exit-flag) #'ignore))
      (clutch--start-tramp-tcp-forward
       '(:backend pg
         :host "db"
         :port 5432
         :tramp-default-directory "/ssh:devbox|podman:app:/workspace/"))
      (should (equal (plist-get network-args :name)
                     "clutch-tramp-container-db:5432"))
      (should (member
               (list 'fake-listener
                     :clutch-container-command
                     (append
                      (list "ssh" "-T" "-o" "BatchMode=yes" "devbox")
                      (mapcar
                       #'shell-quote-argument
                       (list "podman" "exec" "-i" "app" "sh" "-lc"
                             clutch--container-relay-script
                             "clutch-container-relay" "db" "5432"))))
               puts)))))

(ert-deftest clutch-test-container-forward-relays-bytes ()
  "Container relay listener should bridge bytes between client and relay."
  (skip-unless (executable-find "cat"))
  (let (server client received)
    (unwind-protect
        (progn
          (setq server
                (condition-case err
                    (make-network-process
                     :name "clutch-test-container-forward"
                     :server t
                     :host "127.0.0.1"
                     :service t
                     :family 'ipv4
                     :coding 'no-conversion
                     :filter #'clutch--container-forward-client-filter
                     :sentinel #'clutch--container-forward-client-sentinel
                     :noquery t)
                  (file-error
                   (let ((message (error-message-string err)))
                     (if (string-match-p
                          "\\`Cannot bind server socket: Operation not permitted\\'"
                          message)
                         (ert-skip
                          (format "Cannot bind localhost server socket: %s"
                                  message))
                       (signal (car err) (cdr err)))))))
          (set-process-query-on-exit-flag server nil)
          (process-put server :clutch-container-command (list "cat"))
          (process-put server :clutch-container-listener server)
          (process-put server :clutch-container-children nil)
          (setq client
                (make-network-process
                 :name "clutch-test-container-client"
                 :host "127.0.0.1"
                 :service (process-contact server :service)
                 :family 'ipv4
                 :coding 'no-conversion
                 :filter (lambda (_proc string)
                           (setq received (concat received string)))
                 :noquery t))
          (set-process-query-on-exit-flag client nil)
          (process-send-string client "ping")
          (let ((deadline (+ (float-time) 2)))
            (while (and (< (float-time) deadline)
                        (not (equal received "ping")))
              (accept-process-output nil 0.05)))
          (should (equal received "ping")))
      (when (and client (process-live-p client))
        (delete-process client))
      (when server
        (clutch--stop-connection-transport (list :process server))))))

(ert-deftest clutch-test-ssh-diagnose-output-hints-common-auth-failures ()
  "SSH diagnosis should point users at the right interactive recovery path."
  (dolist (case `((locked-key
                   ,(concat "sign_and_send_pubkey: signing failed for ED25519 "
                            "\"~/.ssh/id_ed25519\" from agent: agent refused operation\n")
                   ("locked" "clutch-prepare-ssh-host"))
                  (authorized-keys
                   "lucius@db: Permission denied (publickey,password).\n"
                   ("clutch-prepare-ssh-host" "authorized_keys"))))
    (pcase-let ((`(,label ,output ,patterns) case))
      (ert-info ((format "case: %s" label))
        (let ((message (clutch--ssh-diagnose-output "bastion-prod" output)))
          (dolist (pattern patterns)
            (should (string-match-p pattern message))))))))

(ert-deftest clutch-test-prepare-ssh-host-starts-interactive-session ()
  "Interactive SSH prepare should open a comint session for the alias."
  (let (started puts sentinel-set query-flag popped message-text proc-lookups)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'make-comint-in-buffer)
               (lambda (name buffer program startfile &rest switches)
                 (setq started (list name buffer program startfile switches))
                 (get-buffer-create buffer)))
              ((symbol-function 'get-buffer-process)
               (lambda (_buffer)
                 (prog1 (if proc-lookups 'fake-proc nil)
                   (setq proc-lookups t))))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'process-put)
               (lambda (proc key value)
                 (push (list proc key value) puts)))
              ((symbol-function 'set-process-query-on-exit-flag)
               (lambda (proc flag)
                 (setq query-flag (list proc flag))))
              ((symbol-function 'set-process-sentinel)
               (lambda (proc fn)
                 (setq sentinel-set (list proc fn))))
              ((symbol-function 'pop-to-buffer)
               (lambda (buffer &rest _args)
                 (setq popped buffer)
                 buffer))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq message-text (apply #'format fmt args)))))
      (clutch-prepare-ssh-host "bastion-prod")
      (should (equal (car started) "clutch-ssh-prepare-bastion-prod"))
      (should (equal (buffer-name (cadr started))
                     "*clutch-ssh-prepare bastion-prod*"))
      (should (equal (cddr started)
                     '("ssh"
                       nil
                       ("bastion-prod" "exit"))))
      (should (equal query-flag '(fake-proc nil)))
      (should (equal (car puts) '(fake-proc :clutch-ssh-host "bastion-prod")))
      (should (eq (car sentinel-set) 'fake-proc))
      (should (eq (cadr sentinel-set) #'clutch--ssh-prepare-sentinel))
      (should (equal (buffer-name popped) "*clutch-ssh-prepare bastion-prod*"))
      (should (string-match-p "Complete any SSH prompts" message-text)))))

(ert-deftest clutch-test-ssh-prepare-sentinel-nonzero-suggests-retry ()
  "Non-zero SSH prepare exit should not imply preparation was useless."
  (let ((buffer (get-buffer-create "*clutch-ssh-prepare bastion-prod*"))
        message-text)
    (unwind-protect
        (cl-letf (((symbol-function 'process-status)
                   (lambda (_proc) 'exit))
                  ((symbol-function 'process-exit-status)
                   (lambda (_proc) 255))
                  ((symbol-function 'process-get)
                   (lambda (_proc key)
                     (when (eq key :clutch-ssh-host) "bastion-prod")))
                  ((symbol-function 'process-buffer)
                   (lambda (_proc) buffer))
                  ((symbol-function 'message)
                   (lambda (fmt &rest args)
                     (setq message-text (apply #'format fmt args)))))
          (clutch--ssh-prepare-sentinel 'fake-proc "exited")
          (should (string-match-p "retry clutch-connect" message-text))
          (should (string-match-p "inspect" message-text)))
      (kill-buffer buffer))))

(ert-deftest clutch-test-build-conn-errors-early-when-jdbc-pass-entry-is-unresolved ()
  "JDBC connections should fail fast when :pass-entry resolves to no password."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'fake-conn)))
      (should-error
       (clutch--build-conn
       '(:backend oracle :host "db" :port 1521 :user "u" :sid "orcl"
          :pass-entry "prod-oracle"))
       :type 'user-error)
      (should-not connect-called))))

(ert-deftest clutch-test-build-conn-pass-entry-contract ()
  "Connection build should strip or materialize native pass-entry credentials."
  (dolist (case
           '((:label "passwordless mongodb"
              :resolved nil
              :params (:backend mongodb
                       :host "127.0.0.1"
                       :port 27017
                       :database "app"
                       :pass-entry "mongdb")
              :backend mongodb
              :expected (:host "127.0.0.1"
                         :port 27017
                         :database "app"))
             (:label "passwordless redis"
              :resolved nil
              :params (:backend redis
                       :host "127.0.0.1"
                       :port 6379
                       :database 0
                       :pass-entry "redis-local")
              :backend redis
              :expected (:host "127.0.0.1"
                         :port 6379
                         :database 0))
             (:label "authenticated mongodb"
              :resolved "secret"
              :params (:backend mongodb
                       :host "127.0.0.1"
                       :port 27017
                       :database "app"
                       :auth-database "admin"
                       :user "root"
                       :pass-entry "mongodb-root")
              :backend mongodb
              :expected (:host "127.0.0.1"
                         :port 27017
                         :database "app"
                         :auth-database "admin"
                         :user "root"
                         :password "secret"))))
    (ert-info ((plist-get case :label))
      (let (captured-backend captured-params)
        (cl-letf (((symbol-function 'clutch--resolve-password)
                   (lambda (_params) (plist-get case :resolved)))
                  ((symbol-function 'clutch-db-connect)
                   (lambda (backend params)
                     (setq captured-backend backend
                           captured-params params)
                     'fake-conn))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t)))
          (should (eq (clutch--build-conn (plist-get case :params))
                      'fake-conn))
          (should (eq captured-backend (plist-get case :backend)))
          (should (equal captured-params
                         (plist-get case :expected))))))))

(ert-deftest clutch-test-resolve-password-errors-when-pass-entry-cannot-be-read ()
  "Unreadable pass entries should fail before the backend sees auth."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch--pass-entry-by-suffix)
               (lambda (_suffix) "mysql/app-prod"))
              ((symbol-function 'auth-source-pass-parse-entry)
               (lambda (_entry) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'fake-conn)))
      (let ((err (should-error
                  (clutch--build-conn
                   '(:backend mysql :host "db" :port 3306 :user "u"
                     :database "app" :pass-entry "app-prod"))
                  :type 'user-error)))
        (should (equal
                 (cadr err)
                 "Database password lookup failed for pass entry mysql/app-prod. Unlock pass/auth-source-pass and retry")))
      (should-not connect-called))))

(ert-deftest clutch-test-resolve-password-falls-back-when-pass-store-missing ()
  "Missing pass stores should not block host/user/port auth-source lookup."
  (let (auth-source-args)
    (cl-letf (((symbol-function 'auth-source-pass-entries)
               (lambda ()
                 (signal 'file-missing
                         '("Opening directory" "No such file or directory"
                           "/home/user/.password-store"))))
              ((symbol-function 'auth-source-pass-parse-entry)
               (lambda (_entry)
                 (ert-fail "No pass entry should be parsed when listing fails")))
              ((symbol-function 'auth-source-search)
               (lambda (&rest args)
                 (setq auth-source-args args)
                 (list (list :secret "postgres")))))
      (should (equal (clutch--resolve-password
                      '(:host "127.0.0.1"
                        :user "postgres"
                        :port 5432
                        :pass-entry "local-pg"))
                     "postgres"))
      (should (equal auth-source-args
                     '(:host "127.0.0.1"
                       :user "postgres"
                       :port 5432
                       :max 1))))))

(ert-deftest clutch-test-resolve-password-skips-auth-source-without-target ()
  "Password lookup should not search all auth-source entries without a target."
  (cl-letf (((symbol-function 'auth-source-search)
             (lambda (&rest _args)
               (ert-fail "auth-source should not be queried without host/user/port"))))
    (should-not (clutch--resolve-password
                 '(:backend sqlite :database "/tmp/app.db")))))

(ert-deftest clutch-test-profile-entry-loads-pass-connection-fields ()
  "Pass profiles should fill connection params before backend setup."
  (cl-letf (((symbol-function 'clutch--pass-entry-by-suffix)
             (lambda (entry)
               (and (equal entry "mysql/prod")
                    "mysql/prod")))
            ((symbol-function 'auth-source-pass-parse-entry)
             (lambda (_entry)
               '((secret . "secret")
                 ("backend" . "mysql")
                 ("host" . "db.example.com")
                 ("port" . "3306")
                 ("user" . "app_user")
                 ("database" . "app_db")
                 ("ssh-host" . "arch")
                 ("ssh-tunnel" . "direct-first")
                 ("connect-timeout" . "8")))))
    (let ((params (clutch--canonicalize-connection-params
                   '(:profile-entry "mysql/prod"))))
      (should (eq (plist-get params :backend) 'mysql))
      (should (equal (plist-get params :host) "db.example.com"))
      (should (= (plist-get params :port) 3306))
      (should (equal (plist-get params :user) "app_user"))
      (should (equal (plist-get params :database) "app_db"))
      (should (equal (plist-get params :ssh-host) "arch"))
      (should (eq (plist-get params :ssh-tunnel) 'direct-first))
      (should (= (plist-get params :connect-timeout) 8))
      (should (equal (plist-get params :password) "secret"))
      (should-not (plist-member params :profile-entry)))))

(ert-deftest clutch-test-profile-entry-loads-authinfo-connection-fields ()
  "Authinfo profiles should use machine as profile id and db-host as host."
  (cl-letf (((symbol-function 'clutch--pass-profile-params)
             (lambda (_entry &optional _default-backend) nil))
            ((symbol-function 'auth-source-search)
             (lambda (&rest args)
               (should (equal args '(:host "mysql/reporting"
                                     :type netrc
                                     :max 1)))
               (list (list :host "mysql/reporting"
                           :backend "mysql"
                           :db-host "db.example.com"
                           :port "3306"
                           :user "report_user"
                           :database "reporting"
                           :ssh-host "arch"
                           :ssh-tunnel "direct-first"
                           :secret (lambda () "secret"))))))
    (let ((params (clutch--canonicalize-connection-params
                   '(:profile-entry "mysql/reporting"))))
      (should (eq (plist-get params :backend) 'mysql))
      (should (equal (plist-get params :host) "db.example.com"))
      (should (= (plist-get params :port) 3306))
      (should (equal (plist-get params :user) "report_user"))
      (should (equal (plist-get params :database) "reporting"))
      (should (equal (plist-get params :ssh-host) "arch"))
      (should (eq (plist-get params :ssh-tunnel) 'direct-first))
      (should (equal (plist-get params :password) "secret"))
      (should-not (plist-member params :profile-entry)))))

(ert-deftest clutch-test-profile-entry-authinfo-parse-errors-surface ()
  "Authinfo profile field errors should not be wrapped as lookup failures."
  (cl-letf (((symbol-function 'clutch--pass-profile-params)
             (lambda (_entry &optional _default-backend) nil))
            ((symbol-function 'auth-source-search)
             (lambda (&rest _args)
               (list (list :host "mysql/bad"
                           :backend "mysql"
                           :db-host "db.example.com"
                           :port "bad")))))
    (let ((err (should-error
                (clutch--canonicalize-connection-params
                 '(:profile-entry "mysql/bad"))
                :type 'user-error)))
      (should (equal
               (cadr err)
               "Connection profile field :port must be a non-negative integer, got \"bad\"")))))

(ert-deftest clutch-test-profile-entry-explicit-fields-win ()
  "Saved connection fields should override profile defaults."
  (cl-letf (((symbol-function 'clutch--profile-entry-params)
             (lambda (_entry &optional _default-backend)
               '(:backend pg
                 :host "db.example.com"
                 :port 3306
                 :user "profile_user"
                 :database "profile_db"
                 :password "profile-secret"
                 :ssh-host "arch"))))
    (let ((params (clutch--canonicalize-connection-params
                   '(:backend mysql
                     :profile-entry "mysql/prod"
                     :database "readonly"
                     :pass-entry "override-pass"))))
      (should (eq (plist-get params :backend) 'mysql))
      (should (equal (plist-get params :host) "db.example.com"))
      (should (= (plist-get params :port) 3306))
      (should (equal (plist-get params :user) "profile_user"))
      (should (equal (plist-get params :database) "readonly"))
      (should (equal (plist-get params :pass-entry) "override-pass"))
      (should (equal (plist-get params :ssh-host) "arch"))
      (should-not (plist-member params :password))
      (should-not (plist-member params :profile-entry)))))

(ert-deftest clutch-test-profile-entry-explicit-backend-guides-profile-parsing ()
  "Explicit backend should guide backend-specific profile value parsing."
  (cl-letf (((symbol-function 'clutch--pass-entry-by-suffix)
             (lambda (entry)
               (and (equal entry "redis/prod")
                    "redis/prod")))
            ((symbol-function 'auth-source-pass-parse-entry)
             (lambda (_entry)
               '(("host" . "127.0.0.1")
                 ("port" . "6379")
                 ("database" . "0")))))
    (let ((params (clutch--canonicalize-connection-params
                   '(:backend redis :profile-entry "redis/prod"))))
      (should (eq (plist-get params :backend) 'redis))
      (should (equal (plist-get params :host) "127.0.0.1"))
      (should (= (plist-get params :port) 6379))
      (should (= (plist-get params :database) 0)))))

(ert-deftest clutch-test-profile-entry-is-not-passed-to-backend ()
  "Profile-entry is a Clutch-level field and should not reach adapters."
  (let (captured-backend captured-params)
    (cl-letf (((symbol-function 'clutch--profile-entry-params)
               (lambda (_entry &optional _default-backend)
                 '(:backend mysql
                   :host "db.example.com"
                   :port 3306
                   :user "u"
                   :database "app"
                   :password "secret")))
              ((symbol-function 'clutch-db-connect)
               (lambda (backend params)
                 (setq captured-backend backend
                       captured-params params)
                 'fake-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
      (should (eq (clutch--build-conn '(:profile-entry "clutch/app"))
                  'fake-conn))
      (should (eq captured-backend 'mysql))
      (should (equal captured-params
                     '(:host "db.example.com"
                       :port 3306
                       :user "u"
                       :database "app"
                       :password "secret"))))))

(ert-deftest clutch-test-resolve-password-errors-when-auth-source-secret-fails ()
  "auth-source secret retrieval failures should surface directly."
  (let ((err (cl-letf (((symbol-function 'clutch--pass-entry-by-suffix)
                        (lambda (_suffix) nil))
                       ((symbol-function 'auth-source-search)
                        (lambda (&rest _args)
                          (list (list :secret (lambda ()
                                                (error "bad decrypt")))))))
               (should-error
                (clutch--resolve-password
                 '(:host "db.example.com" :port 3306 :user "scott"))
                :type 'user-error))))
    (should (equal
             (cadr err)
             "Database password lookup failed via auth-source for scott@db.example.com:3306: bad decrypt"))))

(ert-deftest clutch-test-build-conn-relays-db-errors-without-duplicate-prefixes ()
  "User-facing connect errors should not duplicate nested connection-failed wrappers."
  (let ((err (cl-letf (((symbol-function 'clutch--resolve-password)
                        (lambda (_params) nil))
                       ((symbol-function 'clutch-db-connect)
                        (lambda (&rest _args)
                          (signal 'clutch-db-error
                                  '("Connection failed (oracle): Connection attempt timed out")))))
               (should-error
                (clutch--build-conn '(:backend oracle :host "db" :port 1521 :user "u"))
                :type 'user-error))))
    (should (string-match-p
             "Connection failed [(]oracle[)]: Connection attempt timed out"
             (cadr err)))
    (should (string-match-p "clutch-debug-mode" (cadr err)))
    (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                            (cadr err)))))

(ert-deftest clutch-test-build-conn-wraps-ssh-setup-errors-before-user-display ()
  "SSH setup errors should be normalized before they reach the minibuffer."
  (let ((err (cl-letf (((symbol-function 'clutch--materialize-connection-params)
                        (lambda (params) params))
                       ((symbol-function 'clutch--prepare-connect-params)
                        (lambda (_params)
                          (signal 'clutch-db-error
                                  '("SSH tunnel to arch failed: SSH authentication to arch was rejected")))))
               (should-error
                (clutch--build-conn '(:backend mysql :host "db" :port 3306 :ssh-host "arch"))
                :type 'user-error))))
    (should (string-match-p
             "SSH tunnel to arch failed: SSH authentication to arch was rejected"
             (cadr err)))
    (should-not (string-match-p "^let\\*:" (cadr err)))
    (should-not (string-match-p "^if:" (cadr err)))
    (should-not (string-match-p "^when-let\\*:" (cadr err)))))

(ert-deftest clutch-test-build-conn-enriches-buffer-error-details ()
  "Connect failures should keep enriched `current-buffer' error details."
  (with-temp-buffer
    (let ((raw-message "Connection refused (host=db.example.com, port=3306)"))
      (cl-letf (((symbol-function 'clutch--resolve-password)
                 (lambda (_params) nil))
                ((symbol-function 'clutch-db-connect)
                 (lambda (&rest _args)
                   (signal 'clutch-db-error (list raw-message)))))
        (let ((signaled
               (should-error
                (clutch--build-conn
                 '(:backend oracle :host "db.example.com" :port 1521
                   :user "scott" :database "orcl"))
                :type 'user-error)))
          (should (equal (cadr signaled)
                         (clutch--debug-workflow-message
                          (clutch--humanize-db-error raw-message))))
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (context (plist-get diag :context)))
            (should details)
            (should (eq (plist-get details :backend) 'oracle))
            (should (equal (plist-get details :summary)
                           (clutch--humanize-db-error raw-message)))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get context :host) "db.example.com"))
            (should (equal (plist-get context :port) 1521))
            (should (equal (plist-get context :database) "orcl"))
            (should (eq (plist-get context :backend) 'oracle))))))))

(ert-deftest clutch-test-build-conn-skips-timeouts-for-sqlite ()
  "Test that `clutch--build-conn' does not pass network timeout keys to sqlite."
  (let ((clutch-connect-timeout-seconds 11)
        (clutch-read-idle-timeout-seconds 42)
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
      (clutch--build-conn '(:backend sqlite :database ":memory:"))
      (should-not (plist-member captured :connect-timeout))
      (should-not (plist-member captured :read-idle-timeout))
      (should-not (plist-member captured :query-timeout))
      (should-not (plist-member captured :rpc-timeout)))))

(ert-deftest clutch-test-build-conn-rejects-removed-read-timeout ()
  "Test that removed timeout keys fail fast with a clear error."
  (should-error
   (clutch--build-conn '(:backend mysql :host "127.0.0.1" :port 3306
                         :user "u" :read-timeout 5))
   :type 'user-error))

(ert-deftest clutch-test-effective-sql-product-contract ()
  "Connection SQL products should come from explicit or backend metadata."
  (dolist (case '(((:host "127.0.0.1" :port 3306 :user "u" :database "db")
                   nil)
                  ((:backend pg) postgres)
                  ((:backend oracle) oracle)
                  ((:backend clickhouse :sql-product mysql) mysql)))
    (pcase-let ((`(,params ,expected) case))
      (should (eq (clutch--effective-sql-product params) expected)))))

(ert-deftest clutch-test-build-conn-errors-without-backend ()
  "Building a connection should fail fast when :backend is missing."
  (let ((err (should-error
              (clutch--build-conn
               '(:host "127.0.0.1" :port 3306 :user "u" :database "db"))
              :type 'user-error)))
    (should (string-match-p ":backend" (cadr err)))))

(ert-deftest clutch-test-open-connection-rejects-oracle-url-with-generic-jdbc ()
  "Public connection setup should reject Oracle URLs using generic JDBC."
  (require 'clutch-db-jdbc)
  (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites)
             (lambda (&rest _args)
               (ert-fail "Oracle backend validation ran after JDBC setup"))))
    (let ((err (should-error
                (clutch-open-connection
                 '(:backend jdbc
                   :url "jdbc:oracle:thin:@db.example.com:1521:orcl"
                   :driver-class "oracle.jdbc.OracleDriver"
                   :user "scott"
                   :password "tiger"))
                :type 'user-error)))
      (should (string-match-p ":backend oracle"
                              (error-message-string err))))))

(ert-deftest clutch-test-default-connect-timeout-is-10-seconds ()
  "Project default connect timeout should stay at 10 seconds."
  (should (= clutch-connect-timeout-seconds 10)))

(ert-deftest clutch-test-build-conn-failure-debug-guidance ()
  "Connect failures should adapt their debug guidance to debug-mode state."
  (dolist (case '((:label "debug capture off"
                   :debug-mode nil
                   :mentions-enable t)
                  (:label "debug capture on"
                   :debug-mode t
                   :mentions-enable nil)))
    (ert-info ((format "build-conn debug guidance: %s" (plist-get case :label)))
      (let ((clutch-debug-mode (plist-get case :debug-mode)))
        (cl-letf (((symbol-function 'clutch-db-connect)
                   (lambda (_backend _params)
                     (signal 'clutch-db-error
                             '("Connection refused (host=db.example.com, port=3306)")))))
          (let ((err (should-error
                      (clutch--build-conn
                       '(:backend mysql :host "db.example.com" :port 3306))
                      :type 'user-error)))
            (should (string-match-p "check host and port" (cadr err)))
            (if (plist-get case :mentions-enable)
                (should (string-match-p "clutch-debug-mode" (cadr err)))
              (should-not (string-match-p "clutch-debug-mode" (cadr err))))
            (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                    (cadr err)))))))))

(ert-deftest clutch-test-build-conn-jdbc-driver-missing-points-to-install-command ()
  "Missing JDBC driver should point to install-driver, not the debug workflow."
  (let ((err (cl-letf (((symbol-function 'clutch-db-connect)
                        (lambda (_backend _params)
                          (signal 'clutch-db-error
                                  '("SQLException [SQLState=08001]: No suitable driver found for jdbc:oracle:thin:@//db:1521/ORCL")))))
               (should-error
                (clutch--build-conn '(:backend oracle :driver jdbc :host "db" :port 1521))
                :type 'user-error))))
    (should (string-match-p "clutch-jdbc-install-driver RET oracle" (cadr err)))
    (should-not (string-match-p "clutch-debug-mode" (cadr err)))
    (should-not (string-match-p (regexp-quote clutch-debug-buffer-name)
                                (cadr err)))))

(ert-deftest clutch-test-build-conn-agent-missing-points-to-ensure-agent ()
  "Missing JDBC agent should point to ensure-agent, not the debug workflow."
  (let ((err (cl-letf (((symbol-function 'clutch-db-connect)
                        (lambda (_backend _params)
                          (signal 'clutch-db-error
                                  '("JDBC agent jar not found: /tmp/clutch-jdbc-agent.jar\nRun M-x clutch-jdbc-ensure-agent")))))
               (should-error
                (clutch--build-conn '(:backend oracle :driver jdbc :host "db" :port 1521))
                :type 'user-error))))
    (should (string-match-p "Run M-x clutch-jdbc-ensure-agent" (cadr err)))
    (should-not (string-match-p "clutch-debug-mode" (cadr err)))
    (should-not (string-match-p (regexp-quote clutch-debug-buffer-name)
                                (cadr err)))))

(ert-deftest clutch-test-readers-load-clutch-entrypoint ()
  "Saved-connection readers should load `clutch' before checking profiles."
  (dolist (case `((clutch--read-connection-params
                   (:backend mysql :database "app_a" :pass-entry "alpha"))
                  (clutch--read-query-console-target
                   "alpha")))
    (pcase-let ((`(,reader ,expected) case))
      (ert-info ((symbol-name reader))
        (let ((clutch-connection-alist nil)
              required
              (orig-featurep (symbol-function 'featurep))
              (orig-require (symbol-function 'require)))
          (cl-letf (((symbol-function 'featurep)
                     (lambda (feature &optional subfeature)
                       (if (eq feature 'clutch)
                           nil
                         (funcall orig-featurep feature subfeature))))
                    ((symbol-function 'require)
                     (lambda (feature &optional filename noerror)
                       (if (eq feature 'clutch)
                           (progn
                             (setq required t
                                   clutch-connection-alist
                                   '(("alpha" . (:backend mysql :database "app_a"))))
                             t)
                         (funcall orig-require feature filename noerror))))
                    ((symbol-function 'completing-read)
                     (lambda (&rest _args) "alpha")))
            (should (equal (funcall reader) expected))
            (should required)))))))

(ert-deftest clutch-test-read-connection-params-no-match-prompts-manual-when-saved ()
  "No-match connect choices should collect temporary connection params."
  (dolist (choice '("" "new-sqlite"))
    (ert-info ((format "choice %S" choice))
      (let ((clutch-connection-alist
             '(("alpha" . (:backend mysql :database "app_a"))))
            connection-candidates
            connection-require-match
            connection-default
            backend-require-match)
        (cl-letf (((symbol-function 'completing-read)
                   (lambda (prompt collection &optional _predicate require-match
                                   _initial-input _hist def _inherit)
                     (pcase prompt
                       ("Connection: "
                        (setq connection-candidates collection
                              connection-require-match require-match
                              connection-default def)
                        choice)
                       ("Backend: "
                        (setq backend-require-match require-match)
                        "sqlite")
                       (_ (error "Unexpected prompt: %s" prompt)))))
                  ((symbol-function 'read-string)
                   (lambda (prompt &optional _initial _history default-value _inherit)
                     (pcase prompt
                       ("Database (:memory:): " (or default-value ""))
                       (_ (error "Unexpected prompt: %s" prompt))))))
          (should (equal (clutch--read-connection-params)
                         '(:backend sqlite :database ":memory:")))
          (should (equal connection-candidates '("alpha")))
          (should-not connection-require-match)
          (should (equal connection-default ""))
          (should backend-require-match))))))

(ert-deftest clutch-test-read-connection-params-manual-backend-contract ()
  "Manual connection prompts should collect backend-specific defaults."
  (dolist (case '((:label "pg"
                   :backend "pg"
                   :host "db.example.com"
                   :user "alice"
                   :ssh-host "bastion-prod"
                   :database "app_db"
                   :port 5544
                   :default-port 5432
                   :password "secret"
                   :expected (:backend pg
                              :host "db.example.com"
                              :port 5544
                              :user "alice"
                              :ssh-host "bastion-prod"
                              :password "secret"
                              :database "app_db"))
                  (:label "clickhouse"
                   :backend "clickhouse"
                   :host "127.0.0.1"
                   :user "default"
                   :ssh-host ""
                   :database "default"
                   :port 8123
                   :default-port 8123
                   :password ""
                   :expected (:backend clickhouse
                              :host "127.0.0.1"
                              :port 8123
                              :user "default"
                              :password ""
                              :database "default"))
                  (:label "mongodb"
                   :backend "mongodb"
                   :host "cluster0.a.query.mongodb.net"
                   :user "reporter"
                   :ssh-host ""
                   :database "analytics"
                   :port 27017
                   :default-port 27017
                   :password "secret"
                   :expected (:backend mongodb
                              :host "cluster0.a.query.mongodb.net"
                              :port 27017
                              :user "reporter"
                              :password "secret"
                              :database "analytics"))))
    (ert-info ((format "case: %s" (plist-get case :label)))
      (let ((clutch-connection-alist nil)
            backend-prompt
            port-default)
        (cl-letf (((symbol-function 'completing-read)
                   (lambda (prompt _collection &rest _args)
                     (setq backend-prompt prompt)
                     (plist-get case :backend)))
                  ((symbol-function 'read-string)
                   (lambda (prompt &optional _initial _history
                                   default-value _inherit)
                     (pcase prompt
                       ("Host (127.0.0.1): " (plist-get case :host))
                       ("User: " (plist-get case :user))
                       ("SSH host from ~/.ssh/config (optional): "
                        (plist-get case :ssh-host))
                       ("Database (optional): " (plist-get case :database))
                       (_ (or default-value "")))))
                  ((symbol-function 'read-number)
                   (lambda (_prompt default)
                     (setq port-default default)
                     (plist-get case :port)))
                  ((symbol-function 'clutch--resolve-password)
                   (lambda (_params) nil))
                  ((symbol-function 'read-passwd)
                   (lambda (&rest _args) (plist-get case :password))))
          (should (equal (clutch--read-connection-params)
                         (plist-get case :expected)))
          (should (string-match-p "Backend" backend-prompt))
          (should (= port-default (plist-get case :default-port))))))))

(ert-deftest clutch-test-canonicalize-connection-params-normalizes-mongodb-surfaces ()
  "MongoDB connection params should normalize backend aliases and surfaces."
  (dolist (case '(((:backend mongodb :database "analytics")
                   (:backend mongodb :database "analytics"))
                  ((:backend mongodb :surface "sql-interface" :database "analytics")
                   (:backend mongodb :surface sql-interface :database "analytics"))))
    (should (equal (clutch--canonicalize-connection-params (car case))
                   (cadr case)))))

(ert-deftest clutch-test-canonicalize-connection-params-rejects-mongodb-driver-option ()
  "MongoDB should not accept a public driver option."
  (dolist (driver '(mongodb mongodb jdbc))
    (should-error
     (clutch--canonicalize-connection-params
      `(:backend mongodb :driver ,driver :database "analytics"))
     :type 'user-error)))

;;;; Connection — transaction and auto-commit

(ert-deftest clutch-test-tx-header-line-and-update ()
  "Tx header-line should distinguish auto, manual, and dirty states."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq)))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch-db-display-name)
                 (lambda (_conn) "Oracle"))
                ((symbol-function 'clutch--connection-display-key)
                 (lambda (_conn) "scott@db"))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-supported-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--schema-status-header-line-segment)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--icon)
                 (lambda (_spec &rest _fb) "[lock]")))
        ;; Mode-line is now just the mode name.
        (clutch--update-mode-line)
        (should (equal mode-name "clutch"))
        (should (equal header-line-format
                       '((:eval (clutch--build-connection-header-line)))))
        ;; Auto-commit.
        (cl-letf (((symbol-function 'clutch-db-manual-commit-p)
                   (lambda (_conn) nil)))
          (should (string-match-p
                   "Tx: Auto"
                   (clutch--tx-header-line-segment clutch-connection))))
        ;; header-line-format is an (:eval ...) form; evaluate it to get content.
        (let ((hl (clutch--build-connection-header-line)))
          ;; Clean manual-commit: shows Tx: Manual (no asterisk).
          (should (string-match-p "Tx: Manual" hl))
          (should-not (string-match-p "Tx: Manual\\*" hl)))
        ;; Dirty: header-line shows Tx: Manual*.
        (puthash clutch-connection t clutch--tx-dirty-cache)
        (should (string-match-p "Tx: Manual\\*"
                                (clutch--build-connection-header-line)))))))

(ert-deftest clutch-test-update-mode-line-shows-spinner-when-executing ()
  "Busy buffers should show the current spinner frame in `mode-name'."
  (with-temp-buffer
    (clutch-mode)
    (let ((clutch--executing-p t)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (clutch--update-mode-line)
      (should (string-prefix-p "clutch " mode-name))
      (should (> (length mode-name) (length "clutch ")))
      (should-not (string-match-p "\\[\\.\\.\\.\\]" mode-name)))))

(ert-deftest clutch-test-result-footer-spinner-contract ()
  "Result footer timing slot should show spinner only while executing."
  (dolist (case '((busy-no-elapsed t nil)
                  (busy-with-elapsed t 0.042)
                  (idle-with-elapsed nil 0.042)))
    (pcase-let ((`(,label ,executing ,elapsed) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (clutch-result-mode)
          (let ((clutch--footer-base-string "Σ 1 of ? rows")
                (clutch--query-elapsed elapsed)
                (clutch--executing-p executing)
                (clutch--spinner-timer t)
                (clutch--spinner-index 2))
            (cl-letf (((symbol-function 'clutch--format-elapsed)
                       (lambda (_seconds) "42ms")))
              (clutch--refresh-footer-display)
              (let ((footer (substring-no-properties
                             (clutch--footer-mode-line-display))))
                (should (eq (not (null (string-match-p
                                        (regexp-quote
                                         (aref clutch--spinner-frames 2))
                                        footer)))
                            executing))
                (should (eq (not (null (string-match-p "42ms" footer)))
                            (and elapsed (not executing))))))))))))

(ert-deftest clutch-test-spinner-tick-stops-when-no-busy-buffers ()
  "Spinner timer should stop itself when no buffers are busy."
  (with-temp-buffer
    (let ((clutch--spinner-timer 'fake-timer)
          (clutch--spinner-index 0)
          cancelled)
      (cl-letf (((symbol-function 'buffer-list)
                 (lambda (&optional _frame) (list (current-buffer))))
                ((symbol-function 'cancel-timer)
                 (lambda (_timer) (setq cancelled t))))
        (clutch--spinner-tick)
        (should cancelled)
        (should-not clutch--spinner-timer)
        (should (zerop clutch--spinner-index))))))

(ert-deftest clutch-test-run-db-query-updates-manual-commit-dirty-state ()
  "Query execution should update dirty state from SQL and backend DDL semantics."
  (dolist (case '((dml "UPDATE demo SET x = 1" nil nil t)
                  (transactional-ddl "CREATE TABLE demo (id int)" dirty nil t)
                  (autocommit-ddl "CREATE TABLE demo (id int)" clear t nil)
                  (commit "COMMIT" nil t nil)
                  (unknown-ddl "ALTER TABLE demo ADD x NUMBER" nil t t)))
    (pcase-let ((`(,label ,sql ,schema-effect ,initial-dirty ,expected-dirty)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
              (conn 'fake-conn))
          (when initial-dirty
            (puthash conn t clutch--tx-dirty-cache))
          (cl-letf (((symbol-function 'clutch-db-query)
                     (lambda (_conn _sql) '(:ok t)))
                    ((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch-db-schema-transaction-effect)
                     (lambda (_conn _sql) schema-effect))
                    ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
            (clutch--run-db-query conn sql)
            (should (eq (not (null (clutch--tx-dirty-p conn)))
                        expected-dirty))))))))

(ert-deftest clutch-test-transaction-commands-error-in-autocommit-mode ()
  "Commit and rollback should error when the connection is not in manual mode."
  (dolist (command '(clutch-commit clutch-rollback))
    (ert-info ((format "command: %s" command))
      (let ((clutch-connection 'fake-conn))
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-manual-commit-supported-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch-db-manual-commit-p)
                   (lambda (_conn) nil)))
          (should-error (funcall command) :type 'user-error))))))

(ert-deftest clutch-test-transaction-commands-call-rpc-and-clear-dirty ()
  "Commit and rollback should fire their RPC and clear the dirty cache."
  (dolist (case '((commit clutch-commit clutch-db-commit)
                  (rollback clutch-rollback clutch-db-rollback)))
    (pcase-let ((`(,label ,command ,rpc) case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
              (clutch-connection 'fake-conn)
              called)
          (puthash clutch-connection t clutch--tx-dirty-cache)
          (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                    ((symbol-function 'clutch-db-manual-commit-supported-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_conn) t))
                    ((symbol-function rpc)
                     (lambda (_conn) (setq called t)))
                    ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
            (funcall command)
            (should called)
            (should-not (clutch--tx-dirty-p clutch-connection))))))))

(defun clutch-test--make-dml-result-buf (conn)
  "Create a temporary DML result buffer associated with CONN for testing."
  (let ((buf (generate-new-buffer " *clutch-dml-test*")))
    (with-current-buffer buf
      (clutch-result-mode)
      (setq-local clutch-connection conn)
      (setq-local clutch--dml-result t)
      (setq-local header-line-format nil))
    buf))

(ert-deftest clutch-test-terminal-actions-mark-dml-result-buffers ()
  "Commit, rollback, and disconnect should mark open DML result buffers."
  (dolist (case '((rollback "rolled back")
                  (commit "committed")
                  (disconnect "closed")))
    (pcase-let ((`(,action ,banner) case))
      (ert-info ((format "action: %s" action))
        (let* ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
               (clutch-connection 'fake-conn)
               (buf (clutch-test--make-dml-result-buf 'fake-conn)))
          (when (eq action 'rollback)
            (puthash clutch-connection t clutch--tx-dirty-cache))
          (unwind-protect
              (progn
                (pcase action
                  ('rollback
                   (cl-letf (((symbol-function 'clutch--ensure-connection)
                              #'ignore)
                             ((symbol-function
                               'clutch-db-manual-commit-supported-p)
                              (lambda (_conn) t))
                             ((symbol-function 'clutch-db-manual-commit-p)
                              (lambda (_conn) t))
                             ((symbol-function 'clutch-db-rollback) #'ignore)
                             ((symbol-function 'clutch--refresh-transaction-ui)
                              #'ignore))
                     (clutch-rollback)))
                  ('commit
                   (cl-letf (((symbol-function 'clutch--ensure-connection)
                              #'ignore)
                             ((symbol-function
                               'clutch-db-manual-commit-supported-p)
                              (lambda (_conn) t))
                             ((symbol-function 'clutch-db-manual-commit-p)
                              (lambda (_conn) t))
                             ((symbol-function 'clutch-db-commit) #'ignore)
                             ((symbol-function 'clutch--refresh-transaction-ui)
                              #'ignore))
                     (clutch-commit)))
                  ('disconnect
                   (cl-letf (((symbol-function 'clutch--connection-alive-p)
                              (lambda (_c) t))
                             ((symbol-function
                               'clutch--confirm-disconnect-transaction-loss)
                              #'ignore)
                             ((symbol-function 'clutch-db-disconnect) #'ignore)
                             ((symbol-function 'clutch--refresh-transaction-ui)
                              #'ignore)
                             ((symbol-function
                               'clutch--clear-connection-metadata-caches)
                              #'ignore)
                             ((symbol-function
                               'clutch--update-console-buffer-name)
                              #'ignore)
                             ((symbol-function 'clutch--update-mode-line)
                              #'ignore))
                     (clutch-disconnect))))
                (should
                 (string-match-p
                  banner
                  (with-current-buffer buf
                    (substring-no-properties header-line-format)))))
            (kill-buffer buf)))))))

(ert-deftest clutch-test-toggle-auto-commit-contract ()
  "Auto-commit toggle should switch clean states and reject unsafe states."
  (dolist (case '((manual-to-auto t t nil t nil)
                  (auto-to-manual t nil nil nil nil)
                  (dirty-manual t t t error t)
                  (unsupported nil nil nil error nil)))
    (pcase-let ((`(,label ,supported ,manual ,dirty ,expected ,dirty-after)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
              (clutch-connection 'fake-conn)
              captured-auto-commit
              mode-line-updated)
          (when dirty
            (puthash clutch-connection t clutch--tx-dirty-cache))
          (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                    ((symbol-function 'clutch-db-manual-commit-supported-p)
                     (lambda (_conn) supported))
                    ((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_conn) manual))
                    ((symbol-function 'clutch-db-set-auto-commit)
                     (lambda (_conn value)
                       (setq captured-auto-commit value)))
                    ((symbol-function 'clutch--update-mode-line)
                     (lambda () (setq mode-line-updated t))))
            (if (eq expected 'error)
                (should-error (clutch-toggle-auto-commit) :type 'user-error)
              (clutch-toggle-auto-commit)
              (should (eq captured-auto-commit expected))
              (should mode-line-updated))
            (when (eq expected 'error)
              (should-not captured-auto-commit))
            (should (eq (not (null (clutch--tx-dirty-p clutch-connection)))
                        dirty-after))))))))

(ert-deftest clutch-test-sqlite-omits-manual-commit-ui ()
  "SQLite should not advertise Clutch manual-commit controls."
  (require 'clutch-db-sqlite)
  (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/bookmarks.db")))
    (should-not (clutch-db-manual-commit-supported-p conn))
    (should-not (clutch--manual-commit-supported-p conn))
    (should-not (clutch--tx-header-line-segment conn))))

;;;; Connection — display key and icons

(ert-deftest clutch-test-icon-dispatches-public-nerd-icons-functions ()
  "Icon helper should dispatch through public nerd-icons render functions."
  (cl-letf (((symbol-function 'require) (lambda (&rest _) t))
            ((symbol-function 'nerd-icons-octicon)
             (lambda (name)
               (unless (string= name "missing")
                 (concat "oct:" name))))
            ((symbol-function 'nerd-icons-devicon)
             (lambda (name) (concat "dev:" name))))
    (should (equal (clutch--icon '(octicon . "nf-oct-sort_desc") "fallback")
                   "oct:nf-oct-sort_desc"))
    (should (equal (clutch--icon '(devicon . "nf-dev-mysql") "fallback")
                   "dev:nf-dev-mysql"))
    (should (equal (clutch--icon '(octicon . "missing") "fallback")
                   "fallback"))))

(ert-deftest clutch-test-header-line-schema-segment-contract ()
  "Connection header line should show effective schema or database context."
  (dolist (case '((:label "mysql redundant schema"
                   :display-name "MySQL"
                   :display-key "user@host"
                   :user "user"
                   :database "sales"
                   :schema "sales"
                   :expected-key "user@host"
                   :expected-schema "sales")
                  (:label "clickhouse database fallback"
                   :clickhouse t
                   :display-name "ClickHouse"
                   :display-key "default@127.0.0.1"
                   :database "demo"
                   :expected-key "default@127.0.0.1"
                   :expected-schema "demo")
                  (:label "oracle non-default schema"
                   :display-name "Oracle"
                   :display-key "scott@dbhost"
                   :user "SCOTT"
                   :database "ORCL"
                   :schema "SALES"
                   :expected-key "scott@dbhost"
                   :expected-schema "SALES")))
    (ert-info ((plist-get case :label))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--connection-clickhouse-p)
                   (lambda (_conn) (plist-get case :clickhouse)))
                  ((symbol-function 'clutch--icon) (lambda (&rest _) "[schema]"))
                  ((symbol-function 'clutch--db-backend-icon-for-key)
                   (lambda (_key) nil))
                  ((symbol-function 'clutch-db-display-name)
                   (lambda (_conn) (plist-get case :display-name)))
                  ((symbol-function 'clutch--connection-display-key)
                   (lambda (_conn) (plist-get case :display-key)))
                  ((symbol-function 'clutch-db-user)
                   (lambda (_conn) (plist-get case :user)))
                  ((symbol-function 'clutch-db-database)
                   (lambda (_conn) (plist-get case :database)))
                  ((symbol-function 'clutch-db-current-schema)
                   (lambda (_conn) (plist-get case :schema)))
                  ((symbol-function 'clutch--schema-status-header-line-segment)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch--tx-header-line-segment)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch--header-line-indent) (lambda () "")))
          (let ((line (clutch--build-connection-header-line)))
            (should (string-match-p (regexp-quote
                                     (plist-get case :expected-key))
                                    line))
            (should (string-match-p
                     (format "\\[schema\\] %s"
                             (regexp-quote
                              (plist-get case :expected-schema)))
                     line))
            (should-not (string-match-p "Schema:" line))))))))

(ert-deftest clutch-test-mongodb-completion-uses-shell-and-collection-candidates ()
  "MongoDB completion should offer shell methods and cached collections."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "users" nil schema)
    (puthash "orders" nil schema)
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema)))
      (with-temp-buffer
        (clutch-mongodb-mode)
        (should (derived-mode-p 'prog-mode))
        (should-not (derived-mode-p 'js-mode))
        (should (eq indent-line-function #'clutch-mongodb-indent-line))
        (should (equal comment-start "// "))
        (insert "db.")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "" (nth 2 capf))))
          (should (equal (car capf) (point)))
          (should (equal (cadr capf) (point)))
          (should (member "users" candidates))
          (should (member "orders" candidates))
          (should (member "runCommand()" candidates))
          (should-not (member "getSiblingDB()" candidates)))
        (erase-buffer)
        (insert "db.users.")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "" (nth 2 capf))))
          (should (member "find()" candidates))
          (should (member "aggregate()" candidates))
          (should (member "find({}).limit(20)" candidates))
          (should (member "aggregate([{ $match: {} }, { $limit: 20 }])"
                          candidates))
          (should (member "countDocuments()" candidates))
          (should-not (member "estimatedDocumentCount()" candidates)))
        (erase-buffer)
        (insert "db.users.fi")
        (cl-letf (((symbol-function 'completion-in-region)
                   (lambda (beg end collection &optional _predicate)
                     (delete-region beg end)
                     (insert (car (all-completions
                                   "fi" collection)))
                     t)))
          (clutch-mongodb-complete-at-point)
          (should (equal (buffer-string) "db.users.find()")))))))

(ert-deftest clutch-test-mongodb-explain-query-at-point-uses-current-helper ()
  "MongoDB explain command should explain the current helper at point."
  (let (captured displayed)
    (with-temp-buffer
      (clutch-mongodb-mode)
      (setq-local clutch-connection 'mongo-conn)
      (insert "db.users.find({active: true}).limit(5)")
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 #'ignore)
                ((symbol-function 'clutch-db-explain-query)
                 (lambda (conn query)
                   (setq captured (list conn query))
                   "{\"summary\":{\"winningStage\":\"IXSCAN\"}}"))
                ((symbol-function 'clutch--show-json-text-buffer)
                 (lambda (buffer-name text)
                   (setq displayed (list buffer-name text)))))
        (clutch-mongodb-explain-query-at-point)))
    (should (equal captured
                   '(mongo-conn "db.users.find({active: true}).limit(5)")))
    (should (equal displayed
                   '("*clutch mongodb: explain*"
                     "{\"summary\":{\"winningStage\":\"IXSCAN\"}}")))))

(ert-deftest clutch-test-mongodb-completion-uses-cursor-chain-candidates ()
  "MongoDB completion should offer cursor helpers after chainable queries."
  (with-temp-buffer
    (clutch-mongodb-mode)
    (insert "db.users.find({}).")
    (let* ((capf (clutch-mongodb-completion-at-point))
           (candidates (all-completions "" (nth 2 capf))))
      (should (member "limit()" candidates))
      (should (member "sort()" candidates))
      (should (member "explain()" candidates))
      (should-not (member "batchSize()" candidates))
      (should-not (member "find()" candidates)))
    (erase-buffer)
    (insert "db.users.find({}).sort({createdAt: -1}).")
    (let* ((capf (clutch-mongodb-completion-at-point))
           (candidates (all-completions "" (nth 2 capf))))
      (should (member "limit()" candidates))
      (should (member "skip()" candidates)))
    (erase-buffer)
    (insert "db.users.aggregate([]).")
    (let* ((capf (clutch-mongodb-completion-at-point))
           (candidates (all-completions "" (nth 2 capf))))
      (should (member "allowDiskUse()" candidates))
      (should (member "maxTimeMS()" candidates))
      (should (member "explain()" candidates))
      (should-not (member "limit()" candidates)))
    (erase-buffer)
    (insert "db.users.find({}).li")
    (cl-letf (((symbol-function 'completion-in-region)
               (lambda (beg end collection &optional _predicate)
                 (delete-region beg end)
                 (insert (car (all-completions
                               "li" collection)))
                 t)))
      (clutch-mongodb-complete-at-point)
      (should (equal (buffer-string) "db.users.find({}).limit()")))))

(ert-deftest clutch-test-mongodb-completion-offers-mql-operators-and-fields ()
  "MongoDB completion should offer MQL operators and sampled field names."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "orders" '("status" "userId" "items.productId") schema)
    (puthash "products" '("sku" "category") schema)
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema)))
      (with-temp-buffer
        (clutch-mongodb-mode)
        (insert "db.orders.aggregate([{$")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "$" (nth 2 capf))))
          (should (member "$match" candidates))
          (should (member "$lookup" candidates))
          (should (member "$sum" candidates))
          (should (member "$dateTrunc" candidates))
          (should-not (member "$vectorSearch" candidates)))
        (erase-buffer)
        (insert "db.orders.find({st")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "st" (nth 2 capf))))
          (should (member "status" candidates))
          (should-not (member "find()" candidates)))
        (erase-buffer)
        (insert "db.orders.aggregate([{$group:{_id:\"$u")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "$u" (nth 2 capf))))
          (should (member "$userId" candidates)))
        (erase-buffer)
        (insert "db.orders.aggregate([{$lookup:{from:\"pro")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "pro" (nth 2 capf))))
          (should (member "products" candidates)))
        (erase-buffer)
        (insert "db.orders.aggregate([{$merge:{into:\"pro")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "pro" (nth 2 capf))))
          (should (member "products" candidates)))))))

(ert-deftest clutch-test-mongodb-completion-inserts-key-colon ()
  "MongoDB field and operator completion should add a key separator."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "orders" '("status" "userId") schema)
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema)))
      (with-temp-buffer
        (clutch-mongodb-mode)
        (insert "db.orders.find({st")
        (pcase-let ((`(,beg ,end ,collection . ,plist)
                     (clutch-mongodb-completion-at-point)))
          (delete-region beg end)
          (insert "status")
          (funcall (plist-get plist :exit-function) "status" 'finished)
          (should (equal (buffer-string) "db.orders.find({status: ")))
        (erase-buffer)
        (insert "db.orders.aggregate([{$m")
        (pcase-let ((`(,beg ,end ,collection . ,plist)
                     (clutch-mongodb-completion-at-point)))
          (delete-region beg end)
          (insert "$match")
          (funcall (plist-get plist :exit-function) "$match" 'finished)
          (should (equal (buffer-string)
                         "db.orders.aggregate([{$match: ")))))))

(ert-deftest clutch-test-mongodb-mode-keymap-keeps-document-actions-only ()
  "MongoDB query buffers should expose document actions, not SQL transaction keys."
  (with-temp-buffer
    (clutch-mongodb-mode)
    (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c C-c"))
                #'clutch-execute-dwim))
    (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c C-j"))
                #'clutch-jump))
    (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c C-d"))
                #'clutch-describe-dwim))
    (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c C-o"))
                #'clutch-act-dwim))
    (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c C-l"))
                #'clutch-switch-schema))
    (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c C-p"))
                #'clutch-mongodb-explain-query-at-point))
    (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c ?"))
                #'clutch-mongodb-dispatch))
    (dolist (key '("C-c C-m" "C-c C-u" "C-c C-a"))
      (should-not (lookup-key clutch-mongodb-mode-map (kbd key))))))

(ert-deftest clutch-test-mongodb-mode-indents-mql-pipeline ()
  "MongoDB mode should indent MQL pipelines without deriving from JS mode."
  (with-temp-buffer
    (clutch-mongodb-mode)
    (insert "db.demo_orders.aggregate([\n{$match:{status:\"paid\"}},\n{$group:{_id:\"$userId\"}}\n])")
    (indent-region (point-min) (point-max))
    (should (equal (buffer-string)
                   "db.demo_orders.aggregate([\n  {$match:{status:\"paid\"}},\n  {$group:{_id:\"$userId\"}}\n])"))))

(ert-deftest clutch-test-mongodb-mode-fontifies-mql-structure ()
  "MongoDB mode should distinguish MQL operators, keys, methods, and strings."
  (with-temp-buffer
    (clutch-mongodb-mode)
    (insert "db.orders.aggregate([{$match:{status:\"paid\"}}])")
    (font-lock-ensure)
    (goto-char (point-min))
    (search-forward "orders")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-namespace-face))
    (search-forward "aggregate")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-method-face))
    (search-forward "$match")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-operator-face))
    (search-forward "status")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-key-face))
    (search-forward "paid")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'font-lock-string-face))))

(ert-deftest clutch-test-object-browse-query-uses-backend-specific-query ()
  "Object browsing should use a backend-specific query before SQL formatting."
  (cl-letf (((symbol-function 'clutch-db-object-browse-query)
             (lambda (conn entry)
               (should (eq conn 'document-conn))
               (should (equal entry '(:name "users")))
               "doc.browse(\"users\");"))
            ((symbol-function 'clutch--object-sql-name)
             (lambda (&rest _args)
               (ert-fail "SQL formatter should not run when backend handles browsing"))))
    (should (equal (clutch--object-browse-query 'document-conn '(:name "users"))
                   "doc.browse(\"users\");"))))

(ert-deftest clutch-test-object-browse-query-errors-for-document-backend-without-query ()
  "Native document backends should not silently fall back to SQL browsing."
  (cl-letf (((symbol-function 'clutch-db-backend-key)
             (lambda (_conn) 'mongodb))
            ((symbol-function 'clutch-backend-data-model)
             (lambda (_backend) 'document))
            ((symbol-function 'clutch-db-object-browse-query)
             (lambda (&rest _args) nil))
            ((symbol-function 'clutch--object-sql-name)
             (lambda (&rest _args)
               (ert-fail "SQL formatter should not run for document browsing"))))
    (should-error
     (clutch--object-browse-query 'document-conn '(:name "users"))
     :type 'user-error)))

(ert-deftest clutch-test-object-browse-query-uses-sql-for-sql-interface-mongodb ()
  "MongoDB SQL Interface object browsing should insert SELECT SQL."
  (require 'clutch-db-jdbc)
  (let ((conn (make-clutch-jdbc-conn :params '(:driver mongodb))))
    (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
               (lambda (_conn) 'mongodb))
              ((symbol-function 'clutch-db-object-browse-query)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--object-sql-name)
               (lambda (_conn entry)
                 (plist-get entry :name))))
      (should (equal (clutch--object-browse-query
                      conn '(:name "users") '(:surface sql-interface))
                     "SELECT * FROM users;")))))

(ert-deftest clutch-test-connection-display-key-derives-generic-jdbc-endpoint ()
  "Generic JDBC connections should show endpoint details derived from :url."
  (require 'clutch-db-jdbc)
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver jdbc
                         :url "jdbc:kingbase8://127.0.0.1:54321/test"
                         :display-name "KingbaseES"
                         :user "system"))))
    (should (equal (clutch--connection-display-key conn)
                   "system@127.0.0.1:54321"))))

;;;; Connection — reconnect and disconnect

(ert-deftest clutch-test-disconnect-blocks-dirty-manual-commit-connection ()
  "Disconnect should warn before dropping uncommitted manual-commit work."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        disconnected)
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'yes-or-no-p) (lambda (_prompt) nil))
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq disconnected t))))
      (should-error (clutch-disconnect) :type 'user-error)
      (should-not disconnected)
      (should clutch-connection)
      (should (clutch--tx-dirty-p clutch-connection)))))

(ert-deftest clutch-test-do-disconnect-stops-ssh-tunnel ()
  "Disconnect should stop any SSH tunnel associated with the connection."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        disconnected
        stopped)
    (puthash 'fake-conn '(:kind ssh :process fake-proc) clutch--connection-transport-cache)
    (cl-letf (((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
              ((symbol-function 'clutch--invalidate-derived-buffers) #'ignore)
              ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
              ((symbol-function 'clutch--forget-problem-record) #'ignore)
              ((symbol-function 'clutch--clear-connection-metadata-caches) #'ignore)
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq disconnected t)))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'delete-process)
               (lambda (proc) (setq stopped proc))))
      (clutch--do-disconnect 'fake-conn)
      (should disconnected)
      (should (eq stopped 'fake-proc))
      (should-not (gethash 'fake-conn clutch--connection-transport-cache)))))

(ert-deftest clutch-test-do-disconnect-clears-schema-metadata-state ()
  "Disconnect should clear schema refresh state for the connection object."
  (clutch-test--with-isolated-metadata-caches
    (let ((conn (list 'fake-conn))
          (other-conn (list 'fake-conn))
          (clutch--schema-cache (make-hash-table :test 'eq))
          (clutch--schema-status-cache (make-hash-table :test 'eq))
          (clutch--schema-refresh-tickets (make-hash-table :test 'eq))
          (clutch--schema-cache-updated-hook nil))
      (puthash conn '(:state refreshing) clutch--schema-status-cache)
      (puthash conn 7 clutch--schema-refresh-tickets)
      (puthash conn (make-hash-table :test 'equal) clutch--schema-cache)
      (puthash other-conn '(:state ready) clutch--schema-status-cache)
      (should (equal conn other-conn))
      (should-not (eq conn other-conn))
      (cl-letf (((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                ((symbol-function 'clutch--invalidate-derived-buffers) #'ignore)
                ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                ((symbol-function 'clutch--forget-problem-record) #'ignore)
                ((symbol-function 'clutch-db-disconnect) #'ignore))
        (clutch--do-disconnect conn)
        (should-not (gethash conn clutch--schema-status-cache))
        (should-not (gethash conn clutch--schema-refresh-tickets))
        (should-not (gethash conn clutch--schema-cache))
        (should (equal (gethash other-conn clutch--schema-status-cache)
                       '(:state ready)))))))

(ert-deftest clutch-test-kill-console-disconnects-and-invalidates ()
  "Killing a console buffer disconnects and invalidates derived buffers."
  (let ((disconnected nil)
        (console (generate-new-buffer " *clutch-console*"))
        (result (generate-new-buffer " *clutch-result*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--clear-connection-metadata-caches)
                   #'ignore)
                  ((symbol-function 'clutch--save-console) #'ignore))
          (with-current-buffer result
            (setq-local clutch-connection 'fake-conn))
          (with-current-buffer console
            (clutch-mode)
            (setq clutch-connection 'fake-conn))
          (kill-buffer console)
          (should disconnected)
          ;; Derived buffer's connection should be invalidated.
          (should-not (buffer-local-value 'clutch-connection result)))
      (when (buffer-live-p console) (kill-buffer console))
      (when (buffer-live-p result) (kill-buffer result)))))

(ert-deftest clutch-test-kill-dead-console-clears-schema-metadata-state ()
  "Killing a console with a dead connection should clear schema state."
  (clutch-test--with-isolated-metadata-caches
    (let ((conn (list 'dead-conn))
          (console (generate-new-buffer " *clutch-dead-console*"))
          (clutch--schema-status-cache (make-hash-table :test 'eq))
          (clutch--schema-refresh-tickets (make-hash-table :test 'eq))
          (clutch--schema-cache-updated-hook nil)
          disconnected)
      (puthash conn '(:state refreshing) clutch--schema-status-cache)
      (puthash conn 7 clutch--schema-refresh-tickets)
      (unwind-protect
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                    (lambda (_conn) nil))
                   ((symbol-function 'clutch--mark-dml-results-connection-closed)
                    #'ignore)
                   ((symbol-function 'clutch--invalidate-derived-buffers)
                    #'ignore)
                   ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                   ((symbol-function 'clutch--forget-problem-record) #'ignore)
                   ((symbol-function 'clutch-db-disconnect)
                    (lambda (_conn) (setq disconnected t))))
          (with-current-buffer console
            (clutch-mode)
            (setq-local clutch-connection conn))
          (kill-buffer console)
          (should-not disconnected)
          (should-not (gethash conn clutch--schema-status-cache))
          (should-not (gethash conn clutch--schema-refresh-tickets)))
        (when (buffer-live-p console) (kill-buffer console))))))

(ert-deftest clutch-test-refresh-schema-status-ui-matches-connection-identity ()
  "Schema status refresh should update only buffers attached to the same object."
  (let ((conn (list 'same-fields))
        (equal-but-distinct-conn (list 'same-fields))
        (target (generate-new-buffer " *clutch-schema-status-target*"))
        (other (generate-new-buffer " *clutch-schema-status-other*"))
        refreshed)
    (unwind-protect
        (progn
          (with-current-buffer target
            (setq-local clutch-connection conn
                        clutch--query-buffer-local-p t))
          (with-current-buffer other
            (setq-local clutch-connection equal-but-distinct-conn
                        clutch--query-buffer-local-p t))
          (should (equal conn equal-but-distinct-conn))
          (should-not (eq conn equal-but-distinct-conn))
          (cl-letf (((symbol-function 'clutch--connection-key)
                     (lambda (_conn) "same-label"))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda () (push (current-buffer) refreshed)))
                    ((symbol-function 'clutch--update-mode-line) #'ignore))
            (clutch--refresh-schema-status-ui conn))
          (should (equal refreshed (list target))))
      (when (buffer-live-p target) (kill-buffer target))
      (when (buffer-live-p other) (kill-buffer other)))))

(ert-deftest clutch-test-kill-non-owner-buffers-does-not-disconnect ()
  "Killing indirect SQL or derived result buffers should not disconnect."
  (dolist (case '((indirect " *clutch-indirect*")
                  (result " *clutch-result-kill*")))
    (pcase-let ((`(,kind ,name) case))
      (ert-info ((format "kind: %s" kind))
        (let ((disconnected nil)
              (buf (generate-new-buffer name)))
          (unwind-protect
              (cl-letf (((symbol-function 'clutch--connection-alive-p)
                         (lambda (_c) t))
                        ((symbol-function 'clutch-db-disconnect)
                         (lambda (_c) (setq disconnected t)))
                        ((symbol-function 'clutch--save-console) #'ignore)
                        ((symbol-function 'clutch--result-buffer-cleanup)
                         #'ignore))
                (with-current-buffer buf
                  (pcase kind
                    ('indirect
                     (clutch-mode)
                     (clutch--indirect-mode 1))
                    ('result
                     (clutch-result-mode)))
                  (setq clutch-connection 'fake-conn))
                (kill-buffer buf))
            (when (buffer-live-p buf) (kill-buffer buf)))
          (should-not disconnected))))))

(ert-deftest clutch-test-kill-owner-buffers-disconnect ()
  "Killing plain SQL or REPL buffers that own a connection should disconnect."
  (dolist (case '((plain-sql " *clutch-plain-sql*" clutch-mode)
                  (repl " *clutch-repl-kill*" clutch-repl-mode)))
    (pcase-let ((`(,kind ,name ,mode-fn) case))
      (ert-info ((format "kind: %s" kind))
        (let ((disconnected nil)
              (buf (generate-new-buffer name)))
          (unwind-protect
              (cl-letf (((symbol-function 'clutch--connection-alive-p)
                         (lambda (_c) t))
                        ((symbol-function 'clutch-db-disconnect)
                         (lambda (_c) (setq disconnected t)))
                        ((symbol-function
                          'clutch--confirm-disconnect-transaction-loss)
                         #'ignore)
                        ((symbol-function
                          'clutch--mark-dml-results-connection-closed)
                         #'ignore)
                        ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                        ((symbol-function 'clutch--clear-connection-metadata-caches)
                         #'ignore)
                        ((symbol-function 'clutch--save-console) #'ignore))
                (with-current-buffer buf
                  (funcall mode-fn)
                  (setq clutch-connection 'fake-conn))
                (kill-buffer buf))
            (when (buffer-live-p buf) (kill-buffer buf)))
          (should disconnected))))))

(ert-deftest clutch-test-reconnect-invalidates-derived-buffers ()
  "Reconnecting in a console should invalidate derived buffers holding the old connection."
  (let ((old-conn (list 'old))
        (new-conn (list 'new))
        (result (generate-new-buffer " *clutch-result-reconnect*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p)
                   (lambda (c) (memq c (list old-conn new-conn))))
                  ((symbol-function 'clutch-db-disconnect) #'ignore)
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--read-connection-params)
                   (lambda () '(:backend mysql :host "localhost")))
                  ((symbol-function 'clutch--effective-sql-product) (lambda (_p) 'mysql))
                  ((symbol-function 'clutch--build-conn) (lambda (_p) new-conn))
                  ((symbol-function 'clutch--bind-connection-context) #'ignore)
                  ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                  ((symbol-function 'clutch--update-mode-line) #'ignore)
                  ((symbol-function 'clutch--connection-key) (lambda (_c) "test")))
          (with-current-buffer result
            (setq-local clutch-connection old-conn))
          (let ((clutch-connection old-conn))
            (clutch-connect))
          ;; After reconnect, derived buffer should be invalidated.
          (should-not (buffer-local-value 'clutch-connection result)))
      (when (buffer-live-p result) (kill-buffer result)))))

(ert-deftest clutch-test-reconnect-preserves-pending ()
  "Reconnect preserves staged changes in result buffers."
  (let ((result-buf (generate-new-buffer "*clutch-test-result*"))
        (clutch-buf (generate-new-buffer "*clutch-test*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch--pending-deletes (list (vector 1)))
            (setq-local clutch--pending-edits nil)
            (setq-local clutch--pending-inserts nil))
          (with-current-buffer clutch-buf
            (cl-letf (((symbol-function 'clutch--build-conn)
                       (lambda (_) 'fake-conn))
                      ((symbol-function 'clutch-db-live-p)
                       (lambda (_) t))
                      ((symbol-function 'clutch--connection-key)
                       (lambda (_) "fake"))
                      ((symbol-function 'clutch--update-mode-line) #'ignore))
              (setq-local clutch--connection-params '(:backend mysql))
              (clutch--try-reconnect)))
          (with-current-buffer result-buf
            (should (equal clutch--pending-deletes (list (vector 1))))))
      (kill-buffer result-buf)
      (kill-buffer clutch-buf))))

(ert-deftest clutch-test-try-reconnect-releases-old-ssh-transport ()
  "Reconnect should stop the old SSH tunnel after the new connection is ready."
  (let ((released nil))
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn
                  clutch--connection-params '(:backend pg
                                              :host "db.internal"
                                              :port 5432
                                              :ssh-host "bastion-prod")
                  clutch--conn-sql-product 'pg)
      (cl-letf (((symbol-function 'clutch--build-conn)
                 (lambda (_params) 'new-conn))
                ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                ((symbol-function 'clutch--release-connection-transport)
                 (lambda (conn) (setq released conn)))
                ((symbol-function 'clutch--rebind-connection-buffers) #'ignore)
                ((symbol-function 'clutch--finalize-rebound-connection)
                 (lambda (_conn) 'done))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "alice@db.internal:5432/appdb"))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn) (eq conn 'new-conn)))
                ((symbol-function 'message) #'ignore))
        (should (clutch--try-reconnect))
        (should (eq released 'old-conn))))))

(ert-deftest clutch-test-replace-connection-rejects-dead-candidate-once ()
  "Replacement should not retry or bind a candidate lost during teardown."
  (let ((new-live t) released rebound (builds 0))
    (cl-letf (((symbol-function 'clutch--build-conn)
               (lambda (_params) (cl-incf builds) 'new-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (conn) (if (eq conn 'new-conn) new-live t)))
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq new-live nil)))
              ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
              ((symbol-function 'clutch--release-connection-transport)
               (lambda (conn) (push conn released)))
              ((symbol-function 'clutch--rebind-connection-buffers)
               (lambda (&rest _args) (setq rebound t))))
      (should-error
       (clutch--replace-connection 'old-conn '(:backend pg) 'pg)
       :type 'clutch-db-error)
      (should (= builds 1))
      (should-not rebound)
      (should (equal released '(new-conn old-conn))))))

(ert-deftest clutch-test-result-buffer-reconnects-using-inherited-context ()
  "Result buffers should inherit reconnect params from their source buffer."
  (let (result-buf built)
    (unwind-protect
        (with-temp-buffer
          (let ((source-buf (current-buffer))
                (result (make-clutch-db-result
                         :connection 'old-conn
                         :columns nil
                         :rows nil)))
            (setq-local clutch-connection 'old-conn
                        clutch--connection-params '(:backend mysql :database "db")
                        clutch--conn-sql-product 'mysql)
            (cl-letf (((symbol-function 'clutch-db-live-p)
                       (lambda (_conn) t))
                      ((symbol-function 'clutch--connection-key)
                       (lambda (conn) (symbol-name conn)))
                      ((symbol-function 'clutch-result--display-dml) #'ignore)
                      ((symbol-function 'clutch-result--show-buffer)
                       (lambda (buf) (setq result-buf buf) buf)))
              (clutch-result--display result "UPDATE demo SET x = 1" 0.1))
            (with-current-buffer result-buf
              (should (equal clutch--connection-params
                             '(:backend mysql :database "db")))
              (should (eq clutch--conn-sql-product 'mysql))
              (cl-letf (((symbol-function 'clutch-db-live-p)
                         (lambda (conn) (eq conn 'new-conn)))
                        ((symbol-function 'clutch--build-conn)
                         (lambda (params)
                           (setq built params)
                           'new-conn))
                        ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                        ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                        ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-result-status-line) #'ignore)
                        ((symbol-function 'clutch--connection-key)
                         (lambda (conn) (symbol-name conn)))
                        ((symbol-function 'message) #'ignore))
                (clutch--ensure-connection)
                (should (eq clutch-connection 'new-conn))
                (should (equal built '(:backend mysql :database "db")))))
            (with-current-buffer source-buf
              (should (eq clutch-connection 'new-conn)))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf)))))

(ert-deftest clutch-test-object-buffer-reconnects-using-inherited-context ()
  "Object definition buffers should inherit reconnect params from their source buffer."
  (let (object-buf built)
    (unwind-protect
        (with-temp-buffer
          (let ((source-buf (current-buffer)))
            (setq-local clutch-connection 'old-conn
                        clutch--connection-params '(:backend mysql :database "db")
                        clutch--conn-sql-product 'mysql)
            (cl-letf (((symbol-function 'clutch-db-object-definition)
                       (lambda (_conn _entry) "CREATE TABLE demo (id INT)"))
                      ((symbol-function 'sql-mode) #'ignore)
                      ((symbol-function 'sql-set-product) #'ignore)
                      ((symbol-function 'font-lock-ensure) #'ignore)
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args)
                         (setq object-buf buf)
                         buf)))
              (clutch-object-show-ddl-or-source '(:name "demo" :type "TABLE")))
            (with-current-buffer object-buf
              (should (equal clutch--connection-params
                             '(:backend mysql :database "db")))
              (should (eq clutch--conn-sql-product 'mysql))
              (cl-letf (((symbol-function 'clutch-db-live-p)
                         (lambda (conn) (eq conn 'new-conn)))
                        ((symbol-function 'clutch--build-conn)
                         (lambda (params)
                           (setq built params)
                           'new-conn))
                        ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                        ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                        ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                        ((symbol-function 'clutch--connection-key)
                         (lambda (conn) (symbol-name conn)))
                        ((symbol-function 'message) #'ignore))
                (clutch--ensure-connection)
                (should (eq clutch-connection 'new-conn))
                (should (equal built '(:backend mysql :database "db")))))
            (with-current-buffer source-buf
              (should (eq clutch-connection 'new-conn)))))
      (when (buffer-live-p object-buf)
        (kill-buffer object-buf)))))

(ert-deftest clutch-test-connect-failure-preserves-old-live-connection ()
  "Interactive connect should not drop the old live session on failure."
  (let ((clutch-connection 'old-conn)
        disconnected)
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 #'ignore)
                ((symbol-function 'clutch--read-connection-params)
                 (lambda () '(:backend mysql :database "newdb")))
                ((symbol-function 'clutch--build-conn)
                 (lambda (_params)
                   (signal 'clutch-db-error '("connect failed"))))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch-connect) :type 'clutch-db-error)
        (should (eq clutch-connection 'old-conn))
        (should-not disconnected)))))

(ert-deftest clutch-test-connect-rejects-connection-that-dies-during-disconnect ()
  "Reconnect should not retry or activate a new connection that became dead."
  (let ((built nil)
        (activated nil))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch-connection 'old-conn)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn)
                   (pcase conn
                     ('old-conn t)
                     ('new-conn-1 nil)
                     (_ t))))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 #'ignore)
                ((symbol-function 'clutch--connect-params-for-current-buffer)
                 (lambda () '(:backend jdbc :database "newdb")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'jdbc))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (push params built)
                   'new-conn-1))
                ((symbol-function 'clutch--do-disconnect)
                 #'ignore)
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (conn params product)
                   (setq activated (list conn params product))))
                ((symbol-function 'message) #'ignore))
        (should-error (clutch-connect) :type 'clutch-db-error)
        (should (equal built '((:backend jdbc :database "newdb"))))
        (should-not activated)))))

(ert-deftest clutch-test-connect-clears-stale-schema-refresh-state-before-prime ()
  "Reconnect should not inherit a stale `refreshing' schema status."
  (clutch-test--with-isolated-metadata-caches
    (let ((old-conn (list 'old-conn))
          (new-conn (list 'new-conn))
          (other-conn (list 'other-conn))
          (clutch--schema-status-cache (make-hash-table :test 'eq))
          (clutch--schema-refresh-tickets (make-hash-table :test 'eq))
          (clutch--schema-cache-updated-hook nil)
          status-before-prime)
      (puthash old-conn '(:state refreshing) clutch--schema-status-cache)
      (puthash new-conn '(:state refreshing) clutch--schema-status-cache)
      (puthash new-conn 7 clutch--schema-refresh-tickets)
      (puthash other-conn '(:state ready) clutch--schema-status-cache)
      (with-temp-buffer
        (setq-local clutch-connection old-conn
                    clutch--console-name "dev")
        (cl-letf (((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (eq conn new-conn)))
                  ((symbol-function 'clutch--connect-params-for-current-buffer)
                   (lambda () '(:backend mysql :database "app")))
                  ((symbol-function 'clutch--materialize-connection-params)
                   #'identity)
                  ((symbol-function 'clutch--effective-sql-product)
                   (lambda (_params) 'mysql))
                  ((symbol-function 'clutch--build-conn)
                   (lambda (_params) new-conn))
                  ((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "test-conn"))
                  ((symbol-function 'clutch--prime-schema-cache)
                   (lambda (conn)
                     (setq status-before-prime
                           (gethash conn clutch--schema-status-cache))))
                  ((symbol-function 'clutch--update-mode-line) #'ignore)
                  ((symbol-function 'message) #'ignore))
          (clutch-connect)
          (should (eq clutch-connection new-conn))
          (should-not status-before-prime)
          (should-not (gethash old-conn clutch--schema-status-cache))
          (should-not (gethash new-conn clutch--schema-refresh-tickets))
          (should (equal (gethash other-conn clutch--schema-status-cache)
                         '(:state ready))))))))

(defmacro clutch-test--with-connect-build-stubs (spec &rest body)
  "Run BODY with connection construction recorded.
SPEC is (BUILT PRODUCT CONN &optional ACTIVATED).  BUILT records the params
passed to `clutch--build-conn'; ACTIVATED, when non-nil, records the final
`clutch--activate-current-buffer-connection' call."
  (declare (indent 1))
  (pcase-let ((`(,built ,product ,conn . ,rest) spec))
    (let ((activated (car rest)))
      `(let ((clutch-test--conn ,conn))
         (cl-letf (((symbol-function 'clutch--build-conn)
                    (lambda (conn-params)
                      (setq ,built conn-params)
                      clutch-test--conn))
                   ((symbol-function 'clutch--effective-sql-product)
                    (lambda (_params) ,product))
                   ((symbol-function 'clutch--connection-alive-p)
                    (lambda (conn)
                      (eq conn clutch-test--conn)))
                   ((symbol-function 'clutch--clear-reconnect-metadata-caches)
                    #'ignore)
                   ((symbol-function 'clutch--activate-current-buffer-connection)
                    (lambda (conn conn-params product)
                      ,@(when activated
                          `((setq ,activated
                                  (list conn conn-params product))))
                      (setq-local clutch-connection conn
                                  clutch--connection-params conn-params
                                  clutch--conn-sql-product product)))
                   ((symbol-function 'clutch--connection-key)
                    (lambda (_conn) "test-conn"))
                   ((symbol-function 'clutch--update-console-buffer-name)
                    #'ignore)
                   ((symbol-function 'message) #'ignore))
           ,@body)))))

(ert-deftest clutch-test-connect-in-query-console-reuses-saved-connection ()
  "Query console reconnect should reuse that console's saved connection."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend mysql :database "app_a"))
             ("beta" . (:backend mysql :database "app_b"))))
          built
          read-called)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "should-not-be-used"))))
        (clutch-test--with-connect-build-stubs (built 'mysql 'new-conn)
          (clutch-connect)
          (should-not read-called)
          (should (equal built
                         '(:backend mysql :database "app_a"
                           :pass-entry "alpha"))))))))

(ert-deftest clutch-test-connect-in-query-console-preserves-tramp-origin ()
  "Query console reconnect should keep its stored TRAMP origin."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend pg
                         :host "db"
                         :port 5432
                         :database "appdb"))))
          (clutch-tramp-context-policy 'ask)
          built)
      (setq-local clutch--console-name "alpha"
                  clutch--connection-params
                  '(:backend pg
                    :host "db"
                    :port 5432
                    :database "appdb"
                    :pass-entry "alpha"
                    :tramp-default-directory "/ssh:devbox:/workspace/"))
      (cl-letf (((symbol-function 'y-or-n-p)
                 (lambda (_prompt)
                   (ert-fail "stored TRAMP origin should not prompt again"))))
        (clutch-test--with-connect-build-stubs (built 'postgres 'new-conn)
          (clutch-connect)
          (should (equal built
                         '(:backend pg
                           :host "db"
                           :port 5432
                           :database "appdb"
                           :pass-entry "alpha"
                           :tramp-default-directory
                           "/ssh:devbox:/workspace/"))))))))

(ert-deftest clutch-test-connect-in-query-console-skips-stale-tramp-origin-for-unforwardable-profile ()
  "Reconnect should not carry old TRAMP origin onto non-TCP profiles."
  (dolist (case '((:saved (:backend sqlite :database "/tmp/app.db")
                   :expected (:backend sqlite :database "/tmp/app.db"
                              :pass-entry "alpha"))
                  (:saved (:backend oracle
                           :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL")
                   :expected (:backend oracle
                              :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
                              :pass-entry "alpha"
                              :password "secret"))))
    (let* ((saved (plist-get case :saved))
           (expected (plist-get case :expected)))
      (let ((clutch-connection-alist `(("alpha" . ,saved)))
            (clutch-tramp-context-policy 'ask)
            built)
        (with-temp-buffer
          (setq-local clutch--console-name "alpha"
                      clutch--connection-params
                      '(:backend pg
                        :host "db"
                        :port 5432
                        :database "appdb"
                        :pass-entry "alpha"
                        :tramp-default-directory "/ssh:devbox:/workspace/"))
          (cl-letf (((symbol-function 'y-or-n-p)
                     (lambda (_prompt)
                       (ert-fail "unforwardable profile should not prompt for TRAMP")))
                    ((symbol-function 'clutch--resolve-password)
                     (lambda (params)
                       (when (clutch-backend-jdbc-transport-p
                              (plist-get params :backend) params)
                         "secret"))))
            (clutch-test--with-connect-build-stubs (built 'generic 'new-conn)
              (clutch-connect)
              (should (equal built expected)))))))))

(ert-deftest clutch-test-connect-stores-resolved-password-in-connection-context ()
  "Connect should retain resolved credentials for reconnects."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend mysql :database "app_a"))))
          (expected '(:backend mysql :database "app_a"
                      :pass-entry "alpha" :password "secret"))
          built
          activated)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--resolve-password)
                 (lambda (_params) "secret")))
        (clutch-test--with-connect-build-stubs
            (built 'mysql 'new-conn activated)
          (clutch-connect)
          (should (equal built expected))
          (should (equal (cadr activated) expected)))))))

(ert-deftest clutch-test-connect-resolves-password-once ()
  "Interactive connect should not query the password source twice."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend mysql :database "app_a"))))
          (resolve-count 0)
          captured-params)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn) (eq conn 'new-conn)))
                ((symbol-function 'clutch--resolve-password)
                 (lambda (_params)
                   (cl-incf resolve-count)
                   "secret"))
                ((symbol-function 'clutch-db-connect)
                 (lambda (_backend params)
                   (setq captured-params params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (= resolve-count 1))
        (should (equal (plist-get captured-params :password) "secret"))))))

(ert-deftest clutch-test-connect-in-query-console-errors-when-saved-connection-missing ()
  "Query console reconnect should error when its saved connection no longer exists."
  (with-temp-buffer
    (let ((clutch-connection-alist '(("beta" . (:backend mysql :database "app_b"))))
          read-called)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "fallback"))))
        (should-error (clutch-connect) :type 'user-error)
        (should-not read-called)))))

(ert-deftest clutch-test-query-console-does-not-create-buffer-on-connect-failure ()
  "Query console should not create a visible buffer before connect succeeds."
  (let* ((name "alpha")
         (buffer-name (clutch--console-buffer-base-name name))
         (clutch-connection-alist
          '(("alpha" . (:backend mysql :database "app_a")))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--effective-sql-product)
                   (lambda (_params) 'mysql))
                  ((symbol-function 'clutch--build-conn)
                   (lambda (_params)
                     (user-error "Connection refused"))))
          (should-error (clutch-query-console name) :type 'user-error)
          (should-not (get-buffer buffer-name)))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-opens-ad-hoc-sqlite-file ()
  "Query console should open a SQL workspace for an ad hoc SQLite file."
  (let* ((db-file "/tmp/clutch-direct-console.db")
         (name (format "SQLite: %s" (abbreviate-file-name db-file)))
         (buffer-name (clutch--console-buffer-base-name name))
         (params (list :backend 'sqlite :database db-file))
         built)
    (unwind-protect
        (cl-letf (((symbol-function 'read-file-name)
                   (lambda (&rest _args) db-file)))
          (clutch-test--with-connect-build-stubs (built 'sqlite 'sqlite-conn)
            (clutch-query-sqlite-file db-file)
            (should (equal built params))
            (should (eq clutch-connection 'sqlite-conn))
            (should (equal clutch--connection-params params))
            (should (eq clutch--conn-sql-product 'sqlite))
            (should (equal clutch--console-name name))
            (should (equal clutch--console-ad-hoc-params params))
            (should (eq (current-buffer) (get-buffer buffer-name)))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-mongodb-surfaces-use-registered-modes ()
  "MongoDB consoles should select editing mode from their backend surface."
  (dolist (case
           (list
            (list :label "native MQL"
                  :name "mongodb-local"
                  :params '(:backend mongodb
                            :host "127.0.0.1"
                            :port 27017
                            :database "app")
                  :major-mode 'clutch-mongodb-mode
                  :sql-mode nil)
            (list :label "SQL interface"
                  :name "sql-interface-mongodb-local"
                  :params '(:backend mongodb
                            :surface sql-interface
                            :host "cluster0.a.query.mongodb.net"
                            :database "analytics")
                  :major-mode 'clutch-mode
                  :sql-mode t)))
    (ert-info ((format "MongoDB query console surface: %s"
                       (plist-get case :label)))
      (let* ((name (plist-get case :name))
             (buffer-name (clutch--console-buffer-base-name name))
             (params (plist-get case :params))
             built)
        (unwind-protect
            (clutch-test--with-connect-build-stubs (built nil 'mongodb-conn)
              (clutch-query-console (list :name name :params params))
              (should (equal built params))
              (should (eq major-mode (plist-get case :major-mode)))
              (should-not (derived-mode-p 'js-mode))
              (if (plist-get case :sql-mode)
                  (progn
                    (should (derived-mode-p 'sql-mode))
                    (should (memq #'clutch-completion-at-point
                                  completion-at-point-functions))
                    (should-not (memq #'clutch-mongodb-completion-at-point
                                      completion-at-point-functions)))
                (should (derived-mode-p 'prog-mode))
                (should-not (derived-mode-p 'sql-mode))
                (should (eq indent-line-function #'clutch-mongodb-indent-line))
                (should (memq #'clutch-mongodb-completion-at-point
                              completion-at-point-functions))
                (should-not (memq #'clutch-completion-at-point
                                  completion-at-point-functions))))
          (when-let* ((buf (get-buffer buffer-name)))
            (kill-buffer buf)))))))

(ert-deftest clutch-test-query-console-redis-uses-redis-mode ()
  "Redis query consoles should use Redis command editing, not SQL mode."
  (let* ((name "redis-local")
         (buffer-name (clutch--console-buffer-base-name name))
         (params '(:backend redis
                   :host "127.0.0.1"
                   :port 6379
                   :database 0))
         built)
    (unwind-protect
        (clutch-test--with-connect-build-stubs (built nil 'redis-conn)
          (clutch-query-console (list :name name :params params))
          (should (equal built params))
          (should (eq major-mode 'clutch-redis-mode))
          (should (derived-mode-p 'prog-mode))
          (should-not (derived-mode-p 'sql-mode))
          (should (eq (lookup-key clutch-redis-mode-map (kbd "C-c C-c"))
                      #'clutch-redis-execute-command-at-point))
          (should (eq (lookup-key clutch-redis-mode-map (kbd "C-c C-j"))
                      #'clutch-jump))
          (should (eq (lookup-key clutch-redis-mode-map (kbd "C-c C-d"))
                      #'clutch-describe-dwim))
          (should (eq (lookup-key clutch-redis-mode-map (kbd "C-c C-o"))
                      #'clutch-act-dwim))
          (should (memq #'clutch-redis-completion-at-point
                        completion-at-point-functions))
          (erase-buffer)
          (insert "HG")
          (let* ((capf (clutch-redis-completion-at-point))
                 (candidates (clutch-test--completion-candidates capf)))
            (should (member "HGETALL" candidates))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-redis-command-at-point-keeps-semicolon-as-input ()
  "Redis command execution should be line-oriented, not semicolon-delimited."
  (with-temp-buffer
    (clutch-redis-mode)
    (insert "GET user:1;  ")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--execute-and-mark)
                 (lambda (command beg end &optional _conn)
                   (setq captured
                         (list command
                               (buffer-substring-no-properties beg end))))))
        (clutch-redis-execute-command-at-point))
      (should (equal captured '("GET user:1;" "GET user:1;"))))))

(ert-deftest clutch-test-query-console-no-match-builds-ad-hoc-connection ()
  "No-match query-console choices should build temporary backend params."
  (let* ((db-file "/tmp/clutch-new-sqlite-console.db")
         (sqlite-params (list :backend 'sqlite :database db-file))
         (network-params '(:backend pg
                           :host "db.example.com"
                           :port 5544
                           :user "alice"
                           :ssh-host "bastion-prod"
                           :password "secret"
                           :database "app_db")))
    (dolist (case
             (list
              (list :label "sqlite file"
                    :console-choice ""
                    :backend-choice "sqlite"
                    :params sqlite-params
                    :name (format "SQLite: %s" (abbreviate-file-name db-file))
                    :product 'sqlite
                    :conn 'sqlite-conn)
              (list :label "network params"
                    :console-choice "new-pg"
                    :backend-choice "pg"
                    :params network-params
                    :name (clutch--ad-hoc-console-name network-params)
                    :product 'pg
                    :conn 'pg-conn
                    :default-port 5432)))
      (pcase-let* ((label (plist-get case :label))
                   (console-choice (plist-get case :console-choice))
                   (backend-choice (plist-get case :backend-choice))
                   (params (plist-get case :params))
                   (buffer-name
                    (clutch--console-buffer-base-name
                     (plist-get case :name)))
                   (product (plist-get case :product))
                   (conn (plist-get case :conn))
                   (default-port (plist-get case :default-port)))
        (ert-info ((format "case: %s" label))
          (let ((clutch-connection-alist
                 '(("alpha" . (:backend mysql :database "app_a"))))
                console-candidates
                built
                port-default)
            (unwind-protect
                (cl-letf (((symbol-function 'completing-read)
                           (lambda (prompt collection &rest _args)
                             (pcase prompt
                               ("Console: "
                                (setq console-candidates collection)
                                console-choice)
                               ("Backend: " backend-choice)
                               (_ (error "Unexpected prompt: %s" prompt)))))
                          ((symbol-function 'read-file-name)
                           (lambda (&rest _args) db-file))
                          ((symbol-function 'read-string)
                           (lambda (prompt &optional _initial _history
                                           default-value _inherit)
                             (pcase prompt
                               ("Host (127.0.0.1): " "db.example.com")
                               ("User: " "alice")
                               ("SSH host from ~/.ssh/config (optional): "
                                "bastion-prod")
                               ("Database (optional): " "app_db")
                               (_ (or default-value "")))))
                          ((symbol-function 'read-number)
                           (lambda (_prompt default)
                             (setq port-default default)
                             5544))
                          ((symbol-function 'clutch--resolve-password)
                           (lambda (_params) nil))
                          ((symbol-function 'read-passwd)
                           (lambda (&rest _args) "secret")))
                  (clutch-test--with-connect-build-stubs (built product conn)
                    (call-interactively #'clutch-query-console)
                    (should (member "alpha" console-candidates))
                    (should-not (member "New connection..." console-candidates))
                    (should-not (member "SQLite file..." console-candidates))
                    (when default-port
                      (should (= port-default default-port)))
                    (should (equal built params))
                    (should (equal clutch--console-ad-hoc-params params))
                    (should (eq (current-buffer) (get-buffer buffer-name)))))
              (when-let* ((buf (get-buffer buffer-name)))
                (kill-buffer buf)))))))))

(ert-deftest clutch-test-query-console-tramp-origin-contract ()
  "Query console connection origin should come from the command source buffer."
  (dolist (case
           '((:label "ssh auto"
              :policy auto
              :source "/ssh:devbox:/workspace/"
              :connection (:backend pg
                           :host "db"
                           :port 5432
                           :user "alice"
                           :database "appdb")
              :expected (:backend pg
                         :host "db"
                         :port 5432
                         :user "alice"
                         :database "appdb"
                         :pass-entry "alpha"
                         :tramp-default-directory "/ssh:devbox:/workspace/"))
             (:label "container ask"
              :policy ask
              :source "/docker:vscode@f500f94f96e3:/workspace/"
              :prompt-match "/docker:vscode@f500f94f96e3:"
              :connection (:backend pg
                           :host "127.0.0.1"
                           :port 5432
                           :user "alice"
                           :database "appdb")
              :expected (:backend pg
                         :host "127.0.0.1"
                         :port 5432
                         :user "alice"
                         :database "appdb"
                         :pass-entry "alpha"
                         :tramp-default-directory
                         "/docker:vscode@f500f94f96e3:/workspace/"))))
    (ert-info ((plist-get case :label))
      (let* ((name "alpha")
             (buffer-name (clutch--console-buffer-base-name name))
             (clutch-connection-alist
              `((,name . ,(plist-get case :connection))))
             (clutch-tramp-context-policy (plist-get case :policy))
             (default-directory (plist-get case :source))
             built
             asked)
        (unwind-protect
            (cl-letf (((symbol-function 'y-or-n-p)
                       (lambda (prompt)
                         (setq asked prompt)
                         (if-let* ((match (plist-get case :prompt-match)))
                             (progn
                               (should (string-match-p match prompt))
                               t)
                           (ert-fail "Unexpected TRAMP origin prompt")))))
              (clutch-test--with-connect-build-stubs
                  (built 'postgres 'pg-conn)
                (clutch-query-console name)
                (should (equal built (plist-get case :expected)))
                (should (equal clutch--connection-params built))
                (should (eq (current-buffer) (get-buffer buffer-name)))))
          (if (plist-get case :prompt-match)
              (should asked)
            (should-not asked))
          (when-let* ((buf (get-buffer buffer-name)))
            (kill-buffer buf)))))))

(ert-deftest clutch-test-connect-in-ad-hoc-sqlite-console-reuses-file-params ()
  "Ad hoc SQLite consoles should reconnect using their file params."
  (with-temp-buffer
    (let ((params '(:backend sqlite :database "/tmp/ad-hoc.db"))
          built
          read-called)
      (clutch-mode)
      (setq-local clutch--console-name "SQLite: /tmp/ad-hoc.db"
                  clutch--console-ad-hoc-params params)
      (cl-letf (((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "should-not-be-used"))))
        (clutch-test--with-connect-build-stubs (built 'sqlite 'new-conn)
          (clutch-connect)
          (should-not read-called)
          (should (equal built params)))))))

(ert-deftest clutch-test-query-console-switches-to-existing-connected-buffer ()
  "Query console should reuse an existing connected console buffer."
  (let* ((name "alpha")
         (existing (get-buffer-create " *clutch-query-console-existing*"))
         (clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
         built)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name name)
          (setq-local clutch-connection 'live-conn)
          (cl-letf (((symbol-function 'clutch--find-console-buffer)
                     (lambda (&rest _args) existing))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (eq conn 'live-conn)))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda () nil))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params)
                       (setq built t)
                       'unexpected-conn)))
            (clutch-query-console name)
            (should-not built)
            (should (eq (current-buffer) existing))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-query-console-rename-reuses-open-buffer-by-storage-identity ()
  "Renaming a saved connection should keep using the open console buffer."
  (let* ((old-name "alpha")
         (new-name "beta")
         (params '(:backend mysql
                   :host "db.internal"
                   :port 3306
                   :user "app"
                   :database "prod"))
         (storage-name (clutch--console-persistence-name old-name params))
         (existing (get-buffer-create " *clutch-query-console-renamed*"))
         (clutch-connection-alist `((,new-name . ,params)))
         built
         renamed-to)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name old-name)
          (setq-local clutch--console-storage-name storage-name)
          (setq-local clutch-connection 'live-conn)
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (eq conn 'live-conn)))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params)
                       (setq built t)
                       'unexpected-conn))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda ()
                       (setq renamed-to clutch--console-name))))
            (clutch-query-console new-name)
            (should-not built)
            (should (eq (current-buffer) existing))
            (should (equal clutch--console-name new-name))
            (should (equal clutch--console-storage-name storage-name))
            (should (equal renamed-to new-name))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-find-console-buffer-identity-contract ()
  "Console lookup should resolve storage identity and legacy params."
  (let (buffers)
    (cl-labels
        ((make-console
          (suffix &rest props)
          (let ((buf (get-buffer-create
                      (format " *clutch-query-console-%s*" suffix))))
            (push buf buffers)
            (with-current-buffer buf
              (clutch-mode)
              (when (plist-member props :name)
                (setq-local clutch--console-name (plist-get props :name)))
              (when (plist-member props :storage)
                (setq-local clutch--console-storage-name
                            (plist-get props :storage)))
              (when (plist-member props :params)
                (setq-local clutch--connection-params
                            (plist-get props :params))))
            buf)))
      (unwind-protect
          (let* ((params '(:backend mysql
                           :host "db.internal"
                           :port 3306
                           :user "app"
                           :database "prod"))
                 (new-params '(:backend mysql
                               :host "db-b.internal"
                               :port 3306
                               :user "app"
                               :database "prod"))
                 (stable "console-stable")
                 (wrong-alias
                  (make-console "wrong-alias"
                                :name "beta" :storage "console-other"))
                 (right-storage
                  (make-console "right-storage"
                                :name "alpha" :storage stable))
                 (different-storage
                  (make-console "different-storage"
                                :name "alpha" :storage "console-old"))
                 (legacy-match
                  (make-console "legacy-buffer"
                                :name "alpha" :params params))
                 (legacy-mismatch
                  (make-console "legacy-mismatch"
                                :name "alpha" :params params)))
            (ignore wrong-alias different-storage legacy-match legacy-mismatch)
            (should (eq (clutch--find-console-buffer "beta" stable)
                        right-storage))
            (should-not (clutch--find-console-buffer "alpha" "console-new"))
            (should (eq (clutch--find-console-buffer
                         "beta"
                         (clutch--console-persistence-name "beta" params))
                        legacy-match))
            (should-not (clutch--find-console-buffer
                         "alpha"
                         (clutch--console-persistence-name "alpha"
                                                          new-params))))
        (dolist (buf buffers)
          (when (buffer-live-p buf)
            (kill-buffer buf)))))))

(ert-deftest clutch-test-open-query-console-keeps-same-name-identities-separate ()
  "Opening a same-name console should not overwrite a different identity."
  (let* ((name "same-name-identity")
         (base-name (clutch--console-buffer-base-name name))
         (old-params '(:backend mysql :host "old.example" :database "app"))
         (new-params '(:backend mysql :host "new.example" :database "app"))
         (old-storage (clutch--console-persistence-name name old-params))
         (old-buffer (get-buffer-create base-name))
         (clutch-console-directory (make-temp-file "clutch-console-" t))
         opened)
    (unwind-protect
        (progn
          (with-current-buffer old-buffer
            (clutch-mode)
            (setq-local clutch--console-name name
                        clutch--console-storage-name old-storage
                        clutch--connection-params old-params
                        clutch-connection 'old-conn))
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (eq conn 'old-conn)))
                    ((symbol-function 'clutch--effective-sql-product)
                     (lambda (_params) 'mysql))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params) 'new-conn))
                    ((symbol-function 'clutch--activate-current-buffer-connection)
                     (lambda (conn params _product)
                       (setq-local clutch-connection conn
                                   clutch--connection-params params)))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     #'ignore))
            (clutch--open-query-console name new-params new-params)
            (setq opened (current-buffer)))
          (should-not (eq opened old-buffer))
          (with-current-buffer old-buffer
            (should (eq clutch-connection 'old-conn))
            (should (equal clutch--connection-params old-params))))
      (dolist (buffer (delete-dups (list opened old-buffer)))
        (when (buffer-live-p buffer)
          (kill-buffer buffer)))
      (delete-directory clutch-console-directory t))))

(ert-deftest clutch-test-query-console-reconnects-dead-existing-buffer ()
  "Query console should reconnect an existing dead console before switching."
  (let* ((name "alpha")
         (existing (get-buffer-create " *clutch-query-console-dead*"))
         (clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
         built
         activated)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name name)
          (setq-local clutch-connection 'dead-conn)
          (cl-letf (((symbol-function 'clutch--find-console-buffer)
                     (lambda (&rest _args) existing)))
            (clutch-test--with-connect-build-stubs
                (built 'mysql 'new-conn activated)
              (clutch-query-console name)
              (should (equal built
                             '(:backend mysql :database "app_a"
                               :pass-entry "alpha")))
              (should (equal activated
                             '(new-conn
                               (:backend mysql :database "app_a"
                                :pass-entry "alpha")
                               mysql)))
              (should (eq (current-buffer) existing)))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-query-console-persistence-file-precedence ()
  "Query console should read identity files before legacy alias files."
  (dolist (case '(("legacy fallback" nil "SELECT legacy;")
                  ("identity wins" "SELECT stable;" "SELECT stable;")))
    (pcase-let ((`(,label ,identity-content ,expected) case))
      (ert-info ((format "case: %s" label))
        (let* ((name "alpha")
               (dir (make-temp-file "clutch-console-" t))
               (buffer-name (clutch--console-buffer-base-name name))
               (params '(:backend mysql
                         :host "db.internal"
                         :port 3306
                         :user "app"
                         :database "prod"))
               (storage-name (clutch--console-persistence-name name params))
               (clutch-console-directory dir)
               (clutch-connection-alist `((,name . ,params)))
               built)
          (unwind-protect
              (progn
                (with-temp-file (expand-file-name "alpha.sql" dir)
                  (insert "SELECT legacy;"))
                (when identity-content
                  (with-temp-file (clutch--console-file storage-name)
                    (insert identity-content)))
                (clutch-test--with-connect-build-stubs
                    (built 'mysql 'conn)
                  (clutch-query-console name)
                  (should (equal built (append params '(:pass-entry "alpha"))))
                  (should (equal (buffer-string) expected))
                  (should-not (equal clutch--console-storage-name name))))
            (when-let* ((buf (get-buffer buffer-name)))
              (kill-buffer buf))
            (delete-directory dir t)))))))

(ert-deftest clutch-test-connect-outside-console-still-uses-generic-read-flow ()
  "Non-console buffers should keep the generic interactive connect flow."
  (with-temp-buffer
    (let ((clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
          built)
      (cl-letf (((symbol-function 'clutch--read-connection-params)
                 (lambda () '(:backend mysql :database "manual_db"))))
        (clutch-test--with-connect-build-stubs (built 'mysql 'new-conn)
          (clutch-connect)
          (should (equal built '(:backend mysql :database "manual_db"))))))))

(provide 'clutch-test-connection)

;;; clutch-test-connection.el ends here
