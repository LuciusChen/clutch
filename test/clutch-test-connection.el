;;; clutch-test-connection.el --- Connection workflow ERT tests -*- lexical-binding: t; -*-

;;; Commentary:

;; Connection setup, lifecycle, transport, query-console, and transaction tests.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-document)
  (require 'clutch-mongodb)
  (require 'clutch-redis))

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

(ert-deftest clutch-test-backend-key-returns-nil-for-opaque-connections ()
  "Backend key detection should tolerate opaque connections."
  (should-not (clutch-db-backend-key 'fake-conn)))

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
  "Test connection key generation."
  :tags '(:smoke)
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
             (no-database mysql "root" "127.0.0.1" 3306 nil nil
              "root@127.0.0.1:3306" "root@127.0.0.1")
             (empty-database mysql "root" "127.0.0.1" 3306 "" nil
              "root@127.0.0.1:3306" "root@127.0.0.1")
             (sqlite-file sqlite nil nil nil "/tmp/bookmarks.db" nil
              "sqlite:/tmp/bookmarks.db" "bookmarks.db")
             (sqlite-memory sqlite nil nil nil ":memory:" nil nil
              ":memory:")))
    (pcase-let ((`(,label ,backend ,user ,host ,port ,database
                          ,remote ,expected-key ,expected-display)
                 case))
      (ert-info ((symbol-name label))
        (let ((clutch--connection-transport-cache
               (make-hash-table :test 'eq))
              (conn 'fake-conn))
          (when remote
            (puthash conn remote clutch--connection-transport-cache))
          (cl-letf (((symbol-function 'clutch-db-backend-key)
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
             (:label "raw url is not forwardable, even with host and port"
              :policy auto
              :source "/ssh:devbox:/workspace/"
              :params (:backend oracle :url "jdbc:oracle:thin:@//db:1521/ORCL"
                       :host "db" :port 1521)
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

(ert-deftest clutch-test-prepare-sqlite-path-before-connection ()
  "SQLite preparation resolves local file identity without changing inputs."
  (dolist (file '("app.db" "./app.db" ":memory:" "" "/tmp/app.db"))
    (let* ((params (list :backend 'sqlite :database file))
           (before (copy-tree params))
           (prepared (clutch-prepare-connection-params params "/tmp/project/")))
      (should (equal params before))
      (should (equal (plist-get prepared :database)
                     (if (member file '(":memory:" "")) file
                       (expand-file-name file "/tmp/project/"))))))
  (should (equal (clutch-prepare-connection-params
                  '(:backend pg :database "app") "/tmp/project/")
                 '(:backend pg :database "app"))))

(ert-deftest clutch-test-build-conn-backend-facing-params ()
  "The backend should receive its own connection keys and nothing else.
Clutch-level keys such as `:pass-entry' and `:profile-entry' are stripped,
a password that a credential source resolves is materialized as
`:password', and no timeout defaults are synthesized: the timeout globals
are bound to non-default values so a backend that received them would
fail the comparison rather than coincide."
  (let ((clutch-connect-timeout-seconds 11)
        (clutch-read-idle-timeout-seconds 42))
    (dolist (case
             '((:label "mysql without timeouts"
                :params (:backend mysql :host "127.0.0.1" :port 3306 :user "u")
                :backend mysql
                :expected (:host "127.0.0.1" :port 3306 :user "u"))
               (:label "pg without timeouts"
                :params (:backend pg :host "127.0.0.1" :port 5432 :user "u")
                :backend pg
                :expected (:host "127.0.0.1" :port 5432 :user "u"))
               (:label "oracle without timeouts"
                :params (:backend oracle :host "db" :port 1521 :user "u")
                :backend oracle
                :expected (:host "db" :port 1521 :user "u"))
               (:label "sqlite without timeouts"
                :params (:backend sqlite :database ":memory:")
                :backend sqlite
                :expected (:database ":memory:"))
               (:label "passwordless mongodb"
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
                :authinfo ("machine mongo.example.com login root port 27017 password secret")
                :params (:backend mongodb
                         :host "mongo.example.com"
                         :port 27017
                         :database "app"
                         :auth-database "admin"
                         :user "root"
                         :pass-entry "mongodb-root")
                :backend mongodb
                :expected (:host "mongo.example.com"
                           :port 27017
                           :database "app"
                           :auth-database "admin"
                           :user "root"
                           :password "secret"))
               (:label "profile entry"
                :authinfo ("machine clutch/app backend mysql db-host db.example.com port 3306 login u database app password secret")
                :params (:profile-entry "clutch/app")
                :backend mysql
                :expected (:host "db.example.com"
                           :port 3306
                           :user "u"
                           :database "app"
                           :password "secret"))))
      (ert-info ((plist-get case :label))
        (let ((conn (make-clutch-test-conn))
              captured-backend captured-params)
          (clutch-test--with-authinfo (plist-get case :authinfo)
            (cl-letf (((symbol-function 'clutch-db-connect)
                       (lambda (backend params)
                         (setq captured-backend backend
                               captured-params params)
                         conn)))
              (should (eq (clutch--build-conn (plist-get case :params)) conn))
              (should (eq captured-backend (plist-get case :backend)))
              (should (equal captured-params (plist-get case :expected))))))))))

(ert-deftest clutch-test-build-conn-rewrites-endpoints-through-forwards ()
  "Forwarded host/port connections should target the local forwarded port.
SSH-backed and TRAMP-backed connections hand the backend 127.0.0.1 and
the forward's local port without the transport key, and remember the
remote endpoint and the transport for display and cleanup."
  (dolist (case
           (list
            (list :label "SSH, SQL backend"
                  :starter 'clutch--start-ssh-tunnel
                  :transport '(:kind ssh :process fake-proc :local-port 40123
                               :ssh-host "bastion-prod")
                  :params '(:backend pg
                            :host "db.internal"
                            :port 5432
                            :user "alice"
                            :database "appdb"
                            :ssh-host "bastion-prod"))
            (list :label "SSH, MongoDB backend"
                  :starter 'clutch--start-ssh-tunnel
                  :transport '(:kind ssh :process fake-proc :local-port 47017
                               :ssh-host "bastion-prod")
                  :params '(:backend mongodb
                            :host "mongo.internal"
                            :port 27017
                            :database "app"
                            :ssh-host "bastion-prod"))
            (list :label "TRAMP, SQL backend"
                  :starter 'clutch--start-tramp-tcp-forward
                  :transport '(:kind tramp :process fake-listener :local-port 40124
                               :tramp-default-directory "/ssh:devbox:/workspace/")
                  :params '(:backend pg
                            :host "db"
                            :port 5432
                            :user "alice"
                            :database "appdb"
                            :tramp-default-directory "/ssh:devbox:/workspace/"))
            (list :label "TRAMP, MongoDB backend"
                  :starter 'clutch--start-tramp-tcp-forward
                  :transport '(:kind tramp :process fake-listener :local-port 47018
                               :tramp-default-directory "/ssh:devbox:/workspace/")
                  :params '(:backend mongodb
                            :host "mongo.internal"
                            :port 27017
                            :database "app"
                            :tramp-default-directory "/ssh:devbox:/workspace/"))))
    (ert-info ((plist-get case :label))
      (let* ((clutch--connection-transport-cache (make-hash-table :test 'eq))
             (input (plist-get case :params))
             (transport (plist-get case :transport))
             (transport-key (if (eq (plist-get transport :kind) 'ssh)
                                :ssh-host
                              :tramp-default-directory))
             (conn (make-clutch-test-conn))
             captured)
        (clutch-test--without-credentials
          (cl-letf (((symbol-function (plist-get case :starter))
                     (lambda (params)
                       (should (eq (plist-get params :backend)
                                   (plist-get input :backend)))
                       (should (equal (plist-get params :host)
                                      (plist-get input :host)))
                       (should (= (plist-get params :port)
                                  (plist-get input :port)))
                       (should (equal (plist-get params transport-key)
                                      (plist-get input transport-key)))
                       transport))
                    ((symbol-function 'clutch-db-connect)
                     (lambda (backend params)
                       (should (eq backend (plist-get input :backend)))
                       (setq captured params)
                       conn)))
            (should (eq (clutch-open-connection input) conn))
            (should (equal (plist-get captured :host) "127.0.0.1"))
            (should (= (plist-get captured :port)
                       (plist-get transport :local-port)))
            (should-not (plist-member captured transport-key))
            (let ((cached (gethash conn clutch--connection-transport-cache)))
              (should (equal (plist-get cached :host) (plist-get input :host)))
              (should (equal (plist-get cached transport-key)
                             (plist-get input transport-key)))
              (should (eq (plist-get cached :kind)
                          (plist-get transport :kind))))))))))

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

(ert-deftest clutch-test-canonicalize-rejects-removed-ssh-tunnel ()
  "Removed SSH tunnel modes should require separate connection entries."
  (dolist (mode '(always direct-first))
    (let ((err (should-error
                (clutch--canonicalize-connection-params
                 (list :backend 'pg
                       :host "db"
                       :port 5432
                       :ssh-host "bastion-prod"
                       :ssh-tunnel mode))
                :type 'user-error)))
      (should (equal
               (cadr err)
               (concat
                "Connection parameter :ssh-tunnel was removed; "
                "define separate direct and :ssh-host connections"))))))

(ert-deftest clutch-test-build-conn-stops-ssh-tunnel-when-connect-quits ()
  "Every failure during DB connect should stop the SSH tunnel it opened.
The tunnel is already forwarding while `clutch-db-connect' waits.  A
`clutch-db-error' is translated to `user-error' for display; every other
condition crosses the function raw, so cleanup cannot live in that handler."
  (skip-unless (executable-find "sleep"))
  (dolist (case '((quit nil quit)
                  (wrong-type-argument (stringp nil) wrong-type-argument)
                  (clutch-db-error ("db connect failed") user-error)))
    (pcase-let ((`(,signal-condition ,signal-data ,expected-caught) case))
      (ert-info ((format "signals %s, surfaces as %s"
                         signal-condition expected-caught))
        (let ((tunnel (make-process :name "clutch-test-tunnel"
                                    :command '("sleep" "30")
                                    :noquery t))
              caught)
          (unwind-protect
              (clutch-test--without-credentials
                (cl-letf (((symbol-function 'clutch--start-ssh-tunnel)
                           (lambda (_params)
                             (list :process tunnel :local-port 40123
                                   :ssh-host "bastion-prod")))
                          ((symbol-function 'clutch-db-connect)
                           (lambda (_backend _params)
                             (signal signal-condition signal-data))))
                  (condition-case err
                      (clutch--build-conn
                       '(:backend pg
                         :host "db.internal"
                         :port 5432
                         :user "alice"
                         :database "appdb"
                         :ssh-host "bastion-prod"))
                    (t (setq caught (car err))))
                  (should (eq caught expected-caught))
                  (should-not (process-live-p tunnel))))
            (when (process-live-p tunnel)
              (delete-process tunnel))))))))

(ert-deftest clutch-test-start-ssh-tunnel-cleans-up-when-wait-quits ()
  "A quit during tunnel readiness must delete the just-started process.
The readiness wait polls with `accept-process-output', which is quittable,
and the ssh -N process has no owner yet at that point."
  (skip-unless (executable-find "sleep"))
  (let ((real-make-process (symbol-function 'make-process))
        tunnel caught)
    (unwind-protect
        (cl-letf (((symbol-function 'executable-find)
                   (lambda (_name) "/usr/bin/ssh"))
                  ((symbol-function 'clutch--allocate-local-port)
                   (lambda () 40124))
                  ((symbol-function 'make-process)
                   (lambda (&rest _args)
                     (setq tunnel (funcall real-make-process
                                           :name "clutch-test-tunnel"
                                           :command '("sleep" "30")
                                           :noquery t))))
                  ((symbol-function 'clutch--wait-for-ssh-tunnel)
                   (lambda (&rest _args) (signal 'quit nil))))
          (condition-case nil
              (clutch--start-ssh-tunnel '(:backend pg
                                          :host "db.internal"
                                          :port 5432
                                          :ssh-host "bastion-prod"))
            (quit (setq caught t)))
          (should caught)
          (should-not (process-live-p tunnel)))
      (when (and tunnel (process-live-p tunnel))
        (delete-process tunnel)))))

(ert-deftest clutch-test-start-network-forward-rejects-opaque-url-profiles ()
  "SSH tunnels and TRAMP forwarding should reject URL-only profiles."
  (dolist (case
           (list
            (list :label "ssh tunnel: raw JDBC URL"
                  :starter #'clutch--start-ssh-tunnel
                  :params '(:backend oracle
                            :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
                            :user "scott"
                            :ssh-host "bastion-prod"))
            (list :label "ssh tunnel: MongoDB URL"
                  :starter #'clutch--start-ssh-tunnel
                  :params '(:backend mongodb
                            :url "mongodb://mongo.internal:27017/app"
                            :ssh-host "bastion-prod")
                  :message (concat "Structured forwarding via SSH tunnels "
                                   "requires :host/:port params, not :url"))
            (list :label "tramp forward: raw JDBC URL"
                  :starter #'clutch--start-tramp-tcp-forward
                  :params '(:backend oracle
                            :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
                            :user "scott"
                            :tramp-default-directory "/ssh:devbox:/workspace/"))
            (list :label "tramp forward: MongoDB URL"
                  :starter #'clutch--start-tramp-tcp-forward
                  :params '(:backend mongodb
                            :url "mongodb://mongo.internal:27017/app"
                            :tramp-default-directory "/ssh:devbox:/workspace/")
                  :message (concat "Structured forwarding via TRAMP forwarding "
                                   "requires :host/:port params, not :url"))))
    (ert-info ((format "opaque url rejection: %s" (plist-get case :label)))
      (let (connect-called process-called)
        (cl-letf (((symbol-function 'executable-find)
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
                      (funcall (plist-get case :starter) (plist-get case :params))
                      :type 'user-error)))
            (when-let* ((message (plist-get case :message)))
              (should (equal (cadr err) message))))
          (should-not connect-called)
          (should-not process-called))))))

(ert-deftest clutch-test-start-ssh-tunnel-starts-batch-tunnel-directly ()
  "SSH tunnel startup should rely on the real batch tunnel command."
  (let (make-process-args)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'make-process)
               (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'clutch--wait-for-ssh-tunnel)
               (lambda (&rest _args) t)))
      (let ((tunnel (condition-case err
                        (clutch--start-ssh-tunnel
                         '(:backend pg
                           :host "db.internal"
                           :port 5432
                           :ssh-host "bastion-prod"))
                      (clutch-db-error
                       (if (string-match-p "Operation not permitted"
                                           (error-message-string err))
                           (ert-skip (error-message-string err))
                         (signal (car err) (cdr err)))))))
        (should (integerp (plist-get tunnel :local-port)))
        (should (eq (plist-get tunnel :process) 'fake-proc))
        (should (equal (plist-get make-process-args :command)
                       (list "ssh"
                             "-N"
                             "-o" "BatchMode=yes"
                             "-o" "ExitOnForwardFailure=yes"
                             "-L" (format "127.0.0.1:%d:db.internal:5432"
                                          (plist-get tunnel :local-port))
                             "bastion-prod")))))))

(ert-deftest clutch-test-start-tramp-forward-starts-ssh-forward ()
  "OpenSSH TRAMP forward startup should run ssh -L for ssh-like directories.
SSH-like hops become -J, and an rpc directory adds tramp-rpc's
ControlMaster options before the target."
  (dolist (case
           (list
            (list :label "direct ssh directory"
                  :tramp-default-directory "/ssh:devbox:/workspace/"
                  :host "db" :port 5432
                  :forward "127.0.0.1:%d:db:5432"
                  :tail '("devbox"))
            (list :label "ssh-like hops mapped to ProxyJump"
                  :tramp-default-directory "/rpc:jump|rpc:devbox:/workspace/"
                  :host "db" :port 5432
                  :forward "127.0.0.1:%d:db:5432"
                  :tail '("-J" "jump" "devbox"))
            (list :label "tramp-rpc ControlMaster reuse"
                  :tramp-default-directory "/rpc:devbox:/workspace/"
                  :host "127.0.0.1" :port 55433
                  :controlmaster-options '("-o" "ControlMaster=auto"
                                           "-o" "ControlPath=/tmp/tramp-rpc.sock")
                  :forward "127.0.0.1:%d:127.0.0.1:55433"
                  :tail '("-o" "ControlMaster=auto"
                          "-o" "ControlPath=/tmp/tramp-rpc.sock"
                          "devbox"))))
    (ert-info ((plist-get case :label))
      (let (make-process-args waited)
        (cl-letf (((symbol-function 'executable-find)
                   (lambda (program)
                     (when (equal program "ssh") "/usr/bin/ssh")))
                  ((symbol-function 'make-process)
                   (lambda (&rest args)
                     (setq make-process-args args)
                     'fake-proc))
                  ((symbol-function 'clutch--wait-for-ssh-tunnel)
                   (lambda (proc port params buffer timeout)
                     (setq waited (list proc port params buffer timeout))))
                  ;; tramp-rpc is not installed; answer for an rpc vector as it would.
                  ((symbol-function 'tramp-rpc-controlmaster-options)
                   (lambda (vec)
                     (should (equal (tramp-file-name-method vec) "rpc"))
                     (plist-get case :controlmaster-options))))
          (let* ((transport (condition-case err
                                (clutch--start-tramp-tcp-forward
                                 (list :backend 'pg
                                       :host (plist-get case :host)
                                       :port (plist-get case :port)
                                       :tramp-default-directory
                                       (plist-get case :tramp-default-directory)))
                              (clutch-db-error
                               (if (string-match-p "Operation not permitted"
                                                   (error-message-string err))
                                   (ert-skip (error-message-string err))
                                 (signal (car err) (cdr err))))))
                 (local-port (plist-get transport :local-port)))
            (should (integerp local-port))
            (should (equal (plist-get make-process-args :command)
                           (append (list "ssh"
                                         "-N"
                                         "-o" "BatchMode=yes"
                                         "-o" "ExitOnForwardFailure=yes"
                                         "-L" (format (plist-get case :forward)
                                                      local-port))
                                   (plist-get case :tail))))
            (should (eq (plist-get make-process-args :noquery) t))
            (should (eq (plist-get transport :kind) 'tramp))
            (should (eq (plist-get transport :process) 'fake-proc))
            (should (equal (plist-get transport :tramp-default-directory)
                           (plist-get case :tramp-default-directory)))
            (should (equal (nth 0 waited) 'fake-proc))
            (should (= (nth 1 waited) local-port))
            (should (equal (plist-get (nth 2 waited) :ssh-host) "devbox"))))))))

(ert-deftest clutch-test-start-tramp-forward-starts-container-relay ()
  "Container TRAMP forward startup should create a local relay listener.
A local container path runs the container runtime directly; a container
path behind SSH hops runs it remotely through the hop, with the exec
command quoted for the remote shell."
  (dolist (case
           (list
            (list :label "docker, local"
                  :executable "docker"
                  :tramp-default-directory "/docker:vscode@app:/workspace/"
                  :command (list "docker" "exec" "-i" "-u" "vscode" "app"
                                 "sh" "-lc" clutch--container-relay-script
                                 "clutch-container-relay" "db" "5432"))
            (list :label "podman behind an ssh hop"
                  :executable "ssh"
                  :tramp-default-directory "/ssh:devbox|podman:app:/workspace/"
                  :command (append
                            (list "ssh" "-T" "-o" "BatchMode=yes" "devbox")
                            (mapcar
                             #'shell-quote-argument
                             (list "podman" "exec" "-i" "app" "sh" "-lc"
                                   clutch--container-relay-script
                                   "clutch-container-relay" "db" "5432"))))))
    (ert-info ((plist-get case :label))
      (let (transport)
        (cl-letf (((symbol-function 'executable-find)
                   (lambda (program)
                     (when (equal program (plist-get case :executable))
                       (concat "/usr/bin/" program)))))
          (setq transport
                (condition-case err
                    (clutch--start-tramp-tcp-forward
                     (list :backend 'pg
                           :host "db"
                           :port 5432
                           :tramp-default-directory
                           (plist-get case :tramp-default-directory)))
                  (file-error
                   (let ((message (error-message-string err)))
                     (if (string-match-p
                          "\\`Cannot bind server socket: Operation not permitted\\'"
                          message)
                         (ert-skip
                          (format "Cannot bind localhost server socket: %s"
                                  message))
                       (signal (car err) (cdr err))))))))
        (unwind-protect
            (let ((listener (plist-get transport :process)))
              (should (equal (process-name listener)
                             "clutch-tramp-container-db:5432"))
              (should (eq (process-contact listener :server) t))
              (should (equal (process-contact listener :host) "127.0.0.1"))
              (should (equal (process-coding-system listener)
                             '(no-conversion . no-conversion)))
              (should-not (process-query-on-exit-flag listener))
              (should (equal (process-get listener :clutch-container-command)
                             (plist-get case :command)))
              (should (eq (plist-get transport :kind) 'tramp))
              (should (= (plist-get transport :local-port)
                         (process-contact listener :service)))
              (should (equal (plist-get transport :tramp-default-directory)
                             (plist-get case :tramp-default-directory))))
          (clutch--stop-connection-transport transport)
          (should-not (process-live-p (plist-get transport :process))))))))

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
  (skip-unless (executable-find "cat"))
  (let ((real-make-comint (symbol-function 'make-comint-in-buffer))
        started message-text)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'make-comint-in-buffer)
               (lambda (name buffer program startfile &rest switches)
                 (setq started (list name buffer program startfile switches))
                 ;; A real comint session, running cat instead of ssh.
                 (funcall real-make-comint name buffer "cat" nil)))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq message-text (apply #'format fmt args)))))
      (clutch-test--with-pop-to-buffer-capture popped
        (clutch-prepare-ssh-host "bastion-prod")
        (let ((proc (get-buffer-process popped)))
          (unwind-protect
              (progn
                (should (equal (car started) "clutch-ssh-prepare-bastion-prod"))
                (should (equal (buffer-name popped)
                               "*clutch-ssh-prepare bastion-prod*"))
                (should (equal (cddr started)
                               '("ssh"
                                 nil
                                 ("bastion-prod" "exit"))))
                (should (process-live-p proc))
                (should-not (process-query-on-exit-flag proc))
                (should (equal (process-get proc :clutch-ssh-host) "bastion-prod"))
                (should (eq (process-sentinel proc) #'clutch--ssh-prepare-sentinel))
                (should (string-match-p "Complete any SSH prompts" message-text)))
            (delete-process proc)))))))

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
    (clutch-test--without-credentials
      (cl-letf (((symbol-function 'clutch-db-connect)
                 (lambda (&rest _args)
                   (setq connect-called t)
                   'fake-conn)))
        (should-error
         (clutch--build-conn
          '(:backend oracle :host "db" :port 1521 :user "u" :sid "orcl"
            :pass-entry "prod-oracle"))
         :type 'user-error)
        (should-not connect-called)))))

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
  (clutch-test--with-authinfo
      '("machine 127.0.0.1 login postgres port 5432 password postgres")
    (cl-letf (((symbol-function 'auth-source-pass-entries)
               (lambda ()
                 (signal 'file-missing
                         '("Opening directory" "No such file or directory"
                           "/home/user/.password-store"))))
              ((symbol-function 'auth-source-pass-parse-entry)
               (lambda (_entry)
                 (ert-fail "No pass entry should be parsed when listing fails"))))
      (should (equal (clutch--resolve-password
                      '(:host "127.0.0.1"
                        :user "postgres"
                        :port 5432
                        :pass-entry "local-pg"))
                     "postgres")))))

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
                 ("connect-timeout" . "8")))))
    (let ((params (clutch--canonicalize-connection-params
                   '(:profile-entry "mysql/prod"))))
      (should (eq (plist-get params :backend) 'mysql))
      (should (equal (plist-get params :host) "db.example.com"))
      (should (= (plist-get params :port) 3306))
      (should (equal (plist-get params :user) "app_user"))
      (should (equal (plist-get params :database) "app_db"))
      (should (equal (plist-get params :ssh-host) "arch"))
      (should (= (plist-get params :connect-timeout) 8))
      (should (equal (plist-get params :password) "secret"))
      (should-not (plist-member params :profile-entry)))))

(ert-deftest clutch-test-profile-entry-authinfo-contract ()
  "Authinfo profiles should fill connection fields before backend setup.
The `machine' is the profile id and `db-host' the database host.  Saved
connection fields override profile defaults, and an explicit `:pass-entry'
hides the profile password.  The explicit backend guides backend-specific
value parsing.  Profile field errors surface as they are, not wrapped as
lookup failures."
  (dolist (case
           '((:label "profile fields fill the connection"
              :authinfo ("machine mysql/reporting backend mysql db-host db.example.com port 3306 login report_user database reporting ssh-host arch password secret")
              :params (:profile-entry "mysql/reporting")
              :expected (:backend mysql :host "db.example.com" :port 3306
                         :user "report_user" :database "reporting"
                         :ssh-host "arch" :password "secret"))
             (:label "explicit fields win"
              :authinfo ("machine mysql/prod backend pg db-host db.example.com port 3306 login profile_user database profile_db ssh-host arch password profile-secret")
              :params (:backend mysql
                       :profile-entry "mysql/prod"
                       :database "readonly"
                       :pass-entry "override-pass")
              :expected (:backend mysql :host "db.example.com" :port 3306
                         :user "profile_user" :database "readonly"
                         :pass-entry "override-pass" :ssh-host "arch")
              :absent (:password))
             (:label "explicit backend guides value parsing"
              :authinfo ("machine redis/prod db-host 127.0.0.1 port 6379 database 0")
              :params (:backend redis :profile-entry "redis/prod")
              :expected (:backend redis :host "127.0.0.1" :port 6379 :database 0))
             (:label "field errors surface unwrapped"
              :authinfo ("machine mysql/bad backend mysql db-host db.example.com port bad password x")
              :params (:profile-entry "mysql/bad")
              :error "Connection profile field :port must be a non-negative integer, got \"bad\"")))
    (ert-info ((plist-get case :label))
      (clutch-test--with-authinfo (plist-get case :authinfo)
        (if-let* ((message (plist-get case :error)))
            (should (equal (cadr (should-error
                                  (clutch--canonicalize-connection-params
                                   (plist-get case :params))
                                  :type 'user-error))
                           message))
          (let ((params (clutch--canonicalize-connection-params
                         (plist-get case :params))))
            (cl-loop for (key value) on (plist-get case :expected) by #'cddr
                     do (should (equal (plist-get params key) value)))
            (dolist (key (cons :profile-entry (plist-get case :absent)))
              (should-not (plist-member params key)))))))))

(ert-deftest clutch-test-resolve-password-errors-when-auth-source-secret-fails ()
  "auth-source secret retrieval failures should surface directly."
  (let ((err (cl-letf (((symbol-function 'auth-source-search)
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

(ert-deftest clutch-test-build-conn-failure-guidance ()
  "Connect failures should surface as `user-error' with the right guidance.
The backend message appears once, without nested connection-failed
wrappers.  The debug workflow is offered as enabling `clutch-debug-mode'
while capture is off and as inspecting the debug buffer while it is on.
A missing JDBC driver or agent points to its own fix command instead."
  (dolist (case
           `((:label "backend message, debug capture off"
              :message "Connection failed (oracle): Connection attempt timed out"
              :params (:backend oracle :host "db" :port 1521 :user "u")
              :mentions ("Connection failed [(]oracle[)]: Connection attempt timed out"
                         "clutch-debug-mode"
                         ,(regexp-quote clutch-debug-buffer-name)))
             (:label "connection refused, debug capture off"
              :message "Connection refused (host=db.example.com, port=3306)"
              :params (:backend mysql :host "db.example.com" :port 3306)
              :mentions ("check host and port"
                         "clutch-debug-mode"
                         ,(regexp-quote clutch-debug-buffer-name)))
             (:label "connection refused, debug capture on"
              :debug-mode t
              :message "Connection refused (host=db.example.com, port=3306)"
              :params (:backend mysql :host "db.example.com" :port 3306)
              :mentions ("check host and port"
                         ,(regexp-quote clutch-debug-buffer-name))
              :omits ("clutch-debug-mode"))
             (:label "missing JDBC driver"
              :message "SQLException [SQLState=08001]: No suitable driver found for jdbc:oracle:thin:@//db:1521/ORCL"
              :params (:backend oracle :driver jdbc :host "db" :port 1521)
              :mentions ("clutch-jdbc-install-driver RET oracle")
              :omits ("clutch-debug-mode"
                      ,(regexp-quote clutch-debug-buffer-name)))
             (:label "missing JDBC agent"
              :message "JDBC agent jar not found: /tmp/clutch-jdbc-agent.jar\nRun M-x clutch-jdbc-ensure-agent"
              :params (:backend oracle :driver jdbc :host "db" :port 1521)
              :mentions ("Run M-x clutch-jdbc-ensure-agent")
              :omits ("clutch-debug-mode"
                      ,(regexp-quote clutch-debug-buffer-name)))))
    (ert-info ((plist-get case :label))
      (let* ((clutch-debug-mode (plist-get case :debug-mode))
             (err (clutch-test--without-credentials
                    (cl-letf (((symbol-function 'clutch-db-connect)
                               (lambda (_backend _params)
                                 (signal 'clutch-db-error
                                         (list (plist-get case :message))))))
                      (should-error
                       (clutch--build-conn (plist-get case :params))
                       :type 'user-error)))))
        (dolist (pattern (plist-get case :mentions))
          (should (string-match-p pattern (cadr err))))
        (dolist (pattern (plist-get case :omits))
          (should-not (string-match-p pattern (cadr err))))))))

(ert-deftest clutch-test-build-conn-wraps-ssh-setup-errors-before-user-display ()
  "SSH setup errors should be normalized before they reach the minibuffer."
  (let ((err (clutch-test--without-credentials
               (cl-letf (((symbol-function 'clutch--prepare-connect-params)
                          (lambda (_params)
                            (signal 'clutch-db-error
                                    '("SSH tunnel to arch failed: SSH authentication to arch was rejected")))))
                 (should-error
                  (clutch--build-conn '(:backend mysql :host "db" :port 3306 :ssh-host "arch"))
                  :type 'user-error)))))
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
      (clutch-test--without-credentials
        (cl-letf (((symbol-function 'clutch-db-connect)
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
              (should (eq (plist-get context :backend) 'oracle)))))))))

(ert-deftest clutch-test-build-conn-rejects-removed-read-timeout ()
  "Test that removed timeout keys fail fast with a clear error."
  (clutch-test--without-credentials
    (should-error
     (clutch--build-conn '(:backend mysql :host "127.0.0.1" :port 3306
                           :user "u" :read-timeout 5))
     :type 'user-error)))

(ert-deftest clutch-test-effective-sql-product-contract ()
  "Connection SQL products should come from explicit or backend metadata."
  (dolist (case '(((:host "127.0.0.1" :port 3306 :user "u" :database "db")
                   nil)
                  ((:backend pg) postgres)
                  ((:backend oracle) oracle)
                  ((:backend sqlserver) ms)
                  ((:backend db2) db2)
                  ((:backend redshift) postgres)
                  ((:backend clickhouse :sql-product mysql) mysql)))
    (pcase-let ((`(,params ,expected) case))
      (should (eq (clutch--effective-sql-product params) expected)))))

(ert-deftest clutch-test-bind-connection-context-syncs-local-sql-product ()
  "Connection rebinding should rebuild local SQL dialect state."
  (let ((default-product (default-value 'sql-product)))
    (with-temp-buffer
      (clutch-mode)
      (font-lock-mode 1)
      (insert "SELECT NVL(name, 0) FROM DUAL;")
      (font-lock-ensure)
      (goto-char (point-min))
      (search-forward "NVL")
      (let ((nvl-pos (match-beginning 0)))
        (should-not (get-text-property nvl-pos 'face))
        (clutch--bind-connection-context nil '(:backend oracle) 'oracle)
        (font-lock-ensure)
        (should (local-variable-p 'sql-product))
        (should (eq sql-product 'oracle))
        (should (equal mode-name "clutch"))
        (should (get-text-property nvl-pos 'face))
        (clutch--bind-connection-context nil '(:backend jdbc))
        (font-lock-ensure)
        (should-not clutch--conn-sql-product)
        (should (eq sql-product default-product))
        (should (equal mode-name "clutch"))
        (should-not (get-text-property nvl-pos 'face))))
    (should (eq (default-value 'sql-product) default-product))))

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

(ert-deftest clutch-test-readers-do-not-require-clutch-entrypoint ()
  "Saved-connection readers should consume assembled configuration directly."
  (dolist (case `((clutch--read-connection-params
                   (:backend mysql :database "app_a" :pass-entry "alpha"))
                  (clutch--read-query-console-target
                   "alpha")))
    (pcase-let ((`(,reader ,expected) case))
      (ert-info ((symbol-name reader))
        (let ((clutch-connection-alist
               '(("alpha" . (:backend mysql :database "app_a"))))
              (orig-require (symbol-function 'require)))
          (cl-letf (((symbol-function 'require)
                     (lambda (feature &optional filename noerror)
                       (if (eq feature 'clutch)
                           (error "Reader attempted to load the composition root")
                         (funcall orig-require feature filename noerror)))))
            (clutch-test--with-minibuffer-answers '("alpha")
              (should (equal (funcall reader) expected)))))))))

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
        (clutch-test--without-credentials
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
                    ((symbol-function 'read-passwd)
                     (lambda (&rest _args) (plist-get case :password))))
            (should (equal (clutch--read-connection-params)
                           (plist-get case :expected)))
            (should (string-match-p "Backend" backend-prompt))
            (should (= port-default (plist-get case :default-port)))))))))

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

(ert-deftest clutch-test-connection-header-follows-live-backend-state ()
  "Header evaluation should observe an asynchronously closed connection."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-sqlite-connect '(:database ":memory:"))))
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (clutch--bind-connection-context
           conn '(:backend sqlite :database ":memory:") 'sqlite)
          (clutch--update-mode-line)
          (should (equal mode-name "clutch"))
          (should (eq (caar header-line-format) :eval))
          (should-not
           (string-match-p
            "DISCONNECTED"
            (substring-no-properties
             (eval (cadar header-line-format)))))
          (clutch-db-disconnect conn)
          (should
           (string-match-p
            "DISCONNECTED"
            (substring-no-properties
             (eval (cadar header-line-format))))))
      (when (clutch-db-live-p conn)
        (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-update-mode-line-shows-elapsed-when-executing ()
  "Busy buffers should show the running query's elapsed time in `mode-name'."
  (with-temp-buffer
    (clutch-mode)
    (let ((clutch--execution-start-time (- (float-time) 1.21)))
      (clutch--update-mode-line)
      (should (string-match-p "\\`clutch 1s [0-9]00ms\\'" mode-name)))))

(ert-deftest clutch-test-format-elapsed ()
  "Durations under a second show milliseconds; longer ones add whole seconds."
  (pcase-dolist (`(,seconds ,text) '((0.042 "42ms")
                                     (0.9996 "1s 000ms")
                                     (61.5 "61s 500ms")))
    (should (equal (clutch--format-elapsed seconds) text))))

(ert-deftest clutch-test-update-mode-line-preserves-result-header ()
  "Execution UI updates should keep a result table's header and mode name.
Re-running a query from a result buffer renamed its mode to \"clutch\"."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local header-line-format " result header")
    (clutch--update-mode-line)
    (should (equal header-line-format " result header"))
    (should (equal mode-name "clutch-result"))))

(ert-deftest clutch-test-result-footer-timing-contract ()
  "The footer timing slot shows the running time while executing, else the last."
  (dolist (executing '(t nil))
    (ert-info ((format "executing: %s" executing))
      (with-temp-buffer
        (clutch-result-mode)
        (let ((clutch--footer-base-string "Σ 1 of ? rows")
              (clutch--query-elapsed 0.042)
              (clutch--execution-start-time
               (and executing (- (float-time) 1.21))))
          (clutch--refresh-footer-display)
          (let ((footer (substring-no-properties
                         (clutch--footer-mode-line-display))))
            (should (eq (not (null (string-match-p "1s [0-9]00ms" footer)))
                        executing))
            (should (eq (not (null (string-match-p "42ms" footer)))
                        (not executing)))))))))

(ert-deftest clutch-test-transaction-refresh-updates-result-footer ()
  "Transaction transitions should rebuild attached and current result footers."
  (let ((conn (make-clutch-test-conn :commit-mode 'auto))
        (params '(:backend pg))
        (clutch--tx-state-cache (make-hash-table :test 'eq))
        (owner (generate-new-buffer " *clutch-tx-owner*"))
        (result (generate-new-buffer " *clutch-tx-result*")))
    (unwind-protect
        (clutch-test--with-isolated-metadata-caches
          (with-current-buffer owner
            (clutch-mode)
            (clutch--bind-connection-context conn params 'postgres))
          (with-current-buffer result
            (clutch-result-mode)
            (clutch--bind-connection-context conn params 'postgres)
            (setq-local clutch--result-rows '((1))
                        clutch--filtered-rows nil
                        clutch--page-current 0
                        clutch--page-total-rows 1
                        clutch-result-max-rows 100)
            (clutch--refresh-footer-line)
            (should (string-match-p
                     "Tx: Auto"
                     (substring-no-properties clutch--footer-base-string))))
          ;; A transaction begun with BEGIN in Auto mode holds uncommitted work.
          (clutch--set-tx-dirty conn)
          (with-current-buffer result
            (should (string-match-p
                     "Tx: Auto\\*"
                     (substring-no-properties clutch--footer-base-string))))
          (clutch--clear-tx-state conn)
          (with-current-buffer result
            (should-not (string-match-p
                         "Tx: Auto\\*"
                         (substring-no-properties clutch--footer-base-string))))
          (with-current-buffer owner
            (clutch-toggle-auto-commit))
          (with-current-buffer result
            (should (string-match-p
                     "Tx: Manual"
                     (substring-no-properties clutch--footer-base-string)))
            (should-not (string-match-p "Tx: Manual\\*"
                                        clutch--footer-base-string))
            (clutch-toggle-auto-commit)
            (should (string-match-p
                     "Tx: Auto"
                     (substring-no-properties clutch--footer-base-string))))
          (with-current-buffer owner
            (clutch-toggle-auto-commit))
          (clutch--set-tx-dirty conn)
          (with-current-buffer result
            (should (string-match-p
                     "Tx: Manual\\*"
                     (substring-no-properties clutch--footer-base-string))))
          (clutch--set-tx-uncertain conn)
          (setf (clutch-test-conn-commit-mode conn) nil)
          (clutch--refresh-transaction-ui conn)
          (with-current-buffer result
            (should (string-match-p
                     "Tx: Uncertain"
                     (substring-no-properties clutch--footer-base-string)))
            (clutch--clear-tx-state conn)
            (clutch-disconnect)
            (let ((footer (substring-no-properties
                           (clutch--footer-mode-line-display))))
              (should (string-match-p "DISCONNECTED" footer))
              (should-not (string-match-p "Tx:" footer)))))
      (when (buffer-live-p owner)
        (kill-buffer owner))
      (when (buffer-live-p result)
        (kill-buffer result)))))

(ert-deftest clutch-test-execution-refresh-tick-stops-when-no-busy-buffers ()
  "The execution refresh timer should stop itself when no buffers are busy."
  (with-temp-buffer
    (let ((clutch--execution-refresh-timer 'fake-timer)
          cancelled)
      (cl-letf (((symbol-function 'buffer-list)
                 (lambda (&optional _frame) (list (current-buffer))))
                ((symbol-function 'cancel-timer)
                 (lambda (_timer) (setq cancelled t))))
        (clutch--execution-refresh-tick)
        (should cancelled)
        (should-not clutch--execution-refresh-timer)))))

(ert-deftest clutch-test-run-db-query-updates-manual-commit-dirty-state ()
  "Query execution should update dirty state from SQL and backend DDL semantics.
Staged statements do not publish it before their batch succeeds."
  (dolist (case '((dml "UPDATE demo SET x = 1" nil nil dirty)
                  (dml-in-with-clause
                   "WITH i AS (INSERT INTO demo VALUES (1) RETURNING id) SELECT * FROM i"
                   nil nil dirty)
                  (select-into-table "SELECT * INTO demo_copy FROM demo" nil nil dirty)
                  (select-into-variable "SELECT x INTO @x FROM demo" nil nil nil)
                  (data-change-table
                   "SELECT * FROM FINAL TABLE (INSERT INTO demo VALUES (1))"
                   nil nil dirty)
                  (transactional-ddl "CREATE TABLE demo (id int)" dirty nil dirty)
                  (autocommit-ddl "CREATE TABLE demo (id int)" clear t nil)
                  (commit "COMMIT" nil t nil)
                  (unknown-ddl "ALTER TABLE demo ADD x NUMBER" nil t dirty)
                  (deferred-dml "UPDATE demo SET x = 1" nil nil nil t)))
    (pcase-let ((`(,label ,sql ,schema-effect ,initial-dirty ,expected ,defer)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
              (conn (make-clutch-test-conn :commit-mode 'manual)))
          (when initial-dirty
            (puthash conn 'dirty clutch--tx-state-cache))
          (cl-letf (((symbol-function 'clutch-db-query)
                     (lambda (_conn _sql) '(:ok t)))
                    ((symbol-function 'clutch-db-schema-transaction-effect)
                     (lambda (_conn _sql) schema-effect)))
            (clutch--run-db-query conn sql nil defer)
            (should (eq (clutch--tx-state conn) expected))))))))

(ert-deftest clutch-test-transaction-state-follows-the-server-report ()
  "Uncommitted work should be known until the server reports no transaction.
On MySQL a CREATE TEMPORARY TABLE, which commits nothing, cleared the work
of an INSERT before it, and in Auto mode a typed BEGIN and INSERT were not
recorded at all, so a disconnect lost the work without asking.  SQL that
writes inside a transaction the server reports open marks it; only a
report of none open clears it."
  (pcase-dolist (`(,label ,manual ,ddl-effect ,steps ,state)
                 '(("Manual, DDL that commits nothing" t clear
                    (("INSERT INTO t VALUES (1)" t)
                     ("CREATE TEMPORARY TABLE x (id int)" t))
                    dirty)
                   ("Manual, DDL that commits" t clear
                    (("INSERT INTO t VALUES (1)" t)
                     ("CREATE TABLE x (id int)" nil))
                    nil)
                   ("Manual, DDL inside the transaction" t dirty
                    (("CREATE TABLE x (id int)" t))
                    dirty)
                   ("Manual, a read in the open transaction" t clear
                    (("SELECT 1" t))
                    nil)
                   ("Manual, a commit that chains the next transaction" t clear
                    (("INSERT INTO t VALUES (1)" t) ("COMMIT AND CHAIN" t))
                    nil)
                   ("Auto, a typed BEGIN and a write" nil clear
                    (("BEGIN" t) ("INSERT INTO t VALUES (1)" t))
                    dirty)
                   ("Auto, a typed BEGIN alone" nil clear
                    (("BEGIN" t))
                    nil)
                   ("Auto, a rollback to a savepoint" nil clear
                    (("START TRANSACTION" t) ("INSERT INTO t VALUES (1)" t)
                     ("SAVEPOINT s" t) ("ROLLBACK TO SAVEPOINT s" t))
                    dirty)
                   ("Auto, a typed COMMIT" nil clear
                    (("BEGIN" t) ("INSERT INTO t VALUES (1)" t) ("COMMIT" nil))
                    nil)
                   ("Auto, a batch that commits then writes in a new transaction" nil clear
                    (("COMMIT; BEGIN; INSERT INTO t VALUES (1);" t))
                    dirty)
                   ("Auto, a write that commits itself" nil clear
                    (("INSERT INTO t VALUES (1)" nil))
                    nil)))
    (ert-info (label)
      (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
            (clutch--running-queries (make-hash-table :test 'eq))
            (conn (make-clutch-test-conn :commit-mode (if manual 'manual 'auto)))
            open
            (remaining steps))
        (cl-letf (((symbol-function 'clutch-db-transaction-open-p)
                   (lambda (_conn) open))
                  ((symbol-function 'clutch-db-schema-transaction-effect)
                   (lambda (_conn _sql) ddl-effect))
                  ((symbol-function 'clutch-db-query)
                   (lambda (_conn sql)
                     (let ((step (pop remaining)))
                       (should (equal sql (car step)))
                       (setq open (cadr step)))
                     (make-clutch-db-result :affected-rows 1))))
          (pcase-dolist (`(,sql ,_) steps)
            (clutch--run-db-query conn sql))
          (should (eq (clutch--tx-state conn) state)))))))

(ert-deftest clutch-test-uncertain-transaction-requires-rollback ()
  "An unknown batch should block work while allowing session recovery."
  (with-temp-buffer
    (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
          (clutch-connection (make-clutch-test-conn :commit-mode 'auto))
          committed
          marked
          notice
          rolled-back)
      (puthash clutch-connection 'uncertain clutch--tx-state-cache)
      (cl-letf (((symbol-function 'clutch-db-commit)
                 (lambda (_conn) (setq committed t)))
                ((symbol-function 'clutch-db-rollback)
                 (lambda (_conn) (setq rolled-back t)))
                ((symbol-function 'clutch--mark-dml-results-rolled-back)
                 (lambda (_conn) (setq marked t)))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (setq notice (apply #'format format-string args)))))
        ;; The shared backend fails the test on any query.
        (should-error
         (clutch--run-db-query clutch-connection "SELECT 1")
         :type 'user-error)
        (should-error (clutch-commit) :type 'user-error)
        (should-error (clutch-toggle-auto-commit) :type 'user-error)
        (should-not committed)
        (should-not (clutch-db-manual-commit-p clutch-connection))
        (clutch-rollback)
        (should rolled-back)
        (should-not marked)
        (should (string-match-p "verify" notice))
        (should-not (clutch--tx-state clutch-connection))))))

(ert-deftest clutch-test-disconnect-confirmation-describes-transaction-state ()
  "Closing prompts should not present an uncertain commit as known-lost work.
Work of a transaction begun with BEGIN in Auto mode is asked about too."
  (dolist (case '((dirty "Uncommitted changes")
                  (uncertain "outcome is unknown")
                  (nil nil)))
    (pcase-let ((`(,state ,expected-pattern) case))
      (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
            prompt)
        (puthash 'fake-conn state clutch--tx-state-cache)
        (cl-letf (((symbol-function 'yes-or-no-p)
                   (lambda (text)
                     (setq prompt text)
                     t)))
          (clutch--confirm-session-close
           'fake-conn "Disconnect? ")
          (if expected-pattern
              (progn
                (should (string-match-p expected-pattern prompt))
                (should (string-suffix-p "Disconnect? " prompt)))
            (should-not prompt)))))))

(ert-deftest clutch-test-transaction-commands-report-the-backend-outcome ()
  "Commit and rollback should end the transaction as the backend reports.
In Auto mode they refuse.  A failed COMMIT response must not leave the
transaction merely dirty, and a failed backend transaction should be
reported as rolled back, not committed."
  (pcase-dolist (`(,command ,mode ,answer ,error-pattern ,state ,banner)
                 '((clutch-commit manual nil nil nil "committed")
                   (clutch-rollback manual nil nil nil "rolled back")
                   (clutch-commit manual error "outcome is uncertain" uncertain nil)
                   (clutch-commit manual rolled-back
                                  "\\`Transaction had already failed" nil "rolled back")
                   (clutch-commit auto nil
                                  "\\`Connection is in autocommit mode\\'" nil nil)
                   (clutch-rollback auto nil
                                    "\\`Connection is in autocommit mode\\'" nil nil)))
    (ert-info ((format "%s in %s mode, backend answer %s" command mode answer))
      (let* ((clutch--tx-state-cache (make-hash-table :test 'eq))
             (conn (make-clutch-test-conn :commit-mode mode))
             (dml (clutch-test--make-dml-result-buf conn))
             called)
        (when (eq mode 'manual)
          (puthash conn 'dirty clutch--tx-state-cache))
        (unwind-protect
            (with-temp-buffer
              (setq-local clutch-connection conn)
              (cl-letf (((symbol-function (if (eq command 'clutch-commit)
                                              'clutch-db-commit
                                            'clutch-db-rollback))
                         (lambda (_conn)
                           (setq called t)
                           (if (eq answer 'error)
                               (signal 'clutch-db-error '("commit response lost"))
                             answer))))
                (if error-pattern
                    (should (string-match-p
                             error-pattern
                             (error-message-string
                              (should-error (funcall command)
                                            :type 'user-error))))
                  (funcall command)))
              (should (eq called (eq mode 'manual)))
              (should (eq (clutch--tx-state conn) state))
              (let ((header (buffer-local-value 'header-line-format dml)))
                (if banner
                    (should (string-match-p banner header))
                  (should-not header))))
          (kill-buffer dml))))))

(ert-deftest clutch-test-session-teardown-step-matrix ()
  "Each teardown kind should run exactly the steps it documents.
The three session-end paths differ only in these steps, so a new step added
to one caller instead of the shared transition shows up here."
  (dolist (case '((disconnect clutch--do-disconnect
                              (mark-closed invalidate clear-tx clear-metadata
                               debug-event forget-problem disconnect release))
                  (dead clutch--cleanup-dead-connection
                        (mark-closed invalidate clear-tx clear-metadata
                         forget-problem release))
                  ;; Preserve keeps the dirty-transaction flag: it is the
                  ;; evidence that the server rolled back an open
                  ;; transaction, read later by clutch--lost-transaction-p.
                  (preserve clutch--preserve-dead-connection-for-reconnect
                            (mark-closed clear-metadata release
                             refresh-preserved))))
    (pcase-let ((`(,kind ,command ,expected) case))
      (ert-info ((format "kind: %s" kind))
        (let ((clutch-debug-mode t)
              steps)
          (cl-letf (((symbol-function 'clutch--mark-dml-results-connection-closed)
                     (lambda (_conn) (push 'mark-closed steps)))
                    ((symbol-function 'clutch--invalidate-derived-buffers)
                     (lambda (_conn) (push 'invalidate steps)))
                    ((symbol-function 'clutch--clear-tx-state)
                     (lambda (_conn) (push 'clear-tx steps)))
                    ((symbol-function 'clutch--clear-connection-metadata-caches)
                     (lambda (_conn) (push 'clear-metadata steps)))
                    ((symbol-function 'clutch--record-disconnect-debug-event)
                     (lambda (_conn) (push 'debug-event steps)))
                    ((symbol-function 'clutch--forget-problem-record)
                     (lambda (_buf _conn) (push 'forget-problem steps)))
                    ((symbol-function 'clutch-db-disconnect)
                     (lambda (_conn) (push 'disconnect steps)))
                    ((symbol-function 'clutch--release-connection-transport)
                     (lambda (_conn) (push 'release steps)))
                    ((symbol-function
                      'clutch--refresh-preserved-connection-buffers)
                     (lambda (_conn) (push 'refresh-preserved steps))))
            (funcall command 'fake-conn)
            (should (equal (nreverse steps) expected))))))))

(ert-deftest clutch-test-preserve-teardown-keeps-lost-transaction-evidence ()
  "Retiring a dirty connection must keep the dirty flag for later refusal.
The flag is the only record that the server rolled back an open
transaction; clearing it on preserve let `clutch-commit' reconnect and
commit an empty transaction instead of reporting the loss."
  (clutch-test--with-isolated-metadata-caches
    (with-temp-buffer
      (let* ((clutch--tx-state-cache (make-hash-table :test 'eq))
             (conn (make-clutch-test-conn :live nil))
             (dml (clutch-test--make-dml-result-buf conn)))
        (unwind-protect
            (progn
              (setq-local clutch-connection conn)
              (clutch--set-tx-dirty conn)
              (clutch--preserve-dead-connection-for-reconnect conn)
              ;; The evidence survives retirement.
              (should (clutch--tx-dirty-p conn))
              (should (clutch--lost-transaction-p conn))
              (should (string-match-p
                       "closed" (buffer-local-value 'header-line-format dml)))
              ;; A transaction command refuses to run on a replacement
              ;; session, consuming the evidence and marking results
              ;; rolled back.
              (should-error (clutch--ensure-transaction-connection)
                            :type 'user-error)
              (should (string-match-p
                       "rolled back"
                       (buffer-local-value 'header-line-format dml)))
              (should-not (clutch--tx-dirty-p conn)))
          (kill-buffer dml))))))

(ert-deftest clutch-test-session-teardown-releases-transport-when-close-fails ()
  "A failing backend disconnect must still release the transport."
  (clutch-test--with-isolated-metadata-caches
    (let ((conn (make-clutch-test-conn))
          (clutch--connection-transport-cache (make-hash-table :test 'eq)))
      (puthash conn '(:host "db" :port 1) clutch--connection-transport-cache)
      (cl-letf (((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (signal 'clutch-db-error '("close failed")))))
        (should-error (clutch--do-disconnect conn) :type 'clutch-db-error)
        (should-not (gethash conn clutch--connection-transport-cache))))))

(ert-deftest clutch-test-transaction-commands-refuse-a-dead-session ()
  "Transaction commands must not run on a replacement of a dead session.
A dead manual-commit session was already rolled back by the server, so a
replacement connection would run the statement against an empty
transaction.  A dead uncertain session must not be replaced and reported
as rolled back either."
  (pcase-dolist (`(,state ,error-pattern ,banner ,state-after)
                 '((dirty "uncommitted changes were lost" "rolled back" nil)
                   (uncertain "verify" nil uncertain)))
    (dolist (command '(clutch-commit clutch-rollback clutch-toggle-auto-commit))
      (ert-info ((format "%s with %s state" command state))
        (let* ((clutch--tx-state-cache (make-hash-table :test 'eq))
               (conn (make-clutch-test-conn :live nil :commit-mode 'manual))
               (dml (clutch-test--make-dml-result-buf conn))
               rpc-called)
          (puthash conn state clutch--tx-state-cache)
          (unwind-protect
              (with-temp-buffer
                (setq-local clutch-connection conn)
                (cl-letf (((symbol-function 'clutch-db-commit)
                           (lambda (_conn) (setq rpc-called t)))
                          ((symbol-function 'clutch-db-rollback)
                           (lambda (_conn) (setq rpc-called t)))
                          ((symbol-function 'clutch-db-set-auto-commit)
                           (lambda (&rest _) (setq rpc-called t))))
                  ;; A reconnect attempt would fail with "Not connected".
                  (should (string-match-p
                           error-pattern
                           (error-message-string
                            (should-error (funcall command)
                                          :type 'user-error)))))
                (should-not rpc-called)
                (let ((header (buffer-local-value 'header-line-format dml)))
                  (if banner
                      (should (string-match-p banner header))
                    (should-not header)))
                ;; A loss is reported once; the session is then clean so the
                ;; next command reconnects normally.
                (should (eq (clutch--tx-state conn) state-after)))
            (kill-buffer dml)))))))

(ert-deftest clutch-test-reconnect-reports-prior-transaction-state-accurately ()
  "Reconnect should distinguish lost work from an unknown submission outcome.
Backends without a SQL product, such as Redis, reconnect too."
  (pcase-dolist (`(,state ,params ,product ,banner ,message-pattern)
                 '((dirty (:backend pg :host "db.internal") pg
                          "rolled back" "uncommitted")
                   (uncertain (:backend pg :host "db.internal") pg
                              nil "outcome is unknown")
                   (nil (:backend redis :host "cache.internal") nil
                        nil "Reconnected to")))
    (ert-info ((format "state: %s" state))
      (clutch-test--with-isolated-metadata-caches
        (let* ((clutch--tx-state-cache (make-hash-table :test 'eq))
               (old (make-clutch-test-conn :live nil))
               (new (make-clutch-test-conn))
               (dml (clutch-test--make-dml-result-buf old))
               messages)
          (unwind-protect
              (with-temp-buffer
                (setq-local clutch-connection old
                            clutch--connection-params params
                            clutch--conn-sql-product product)
                (when state
                  (puthash old state clutch--tx-state-cache))
                (cl-letf (((symbol-function 'clutch--build-conn)
                           (lambda (_params) new))
                          ;; The shared backend has no schema to prime and
                          ;; no endpoint to name.
                          ((symbol-function 'clutch--finalize-rebound-connection)
                           #'ignore)
                          ((symbol-function 'clutch--connection-key)
                           (lambda (_conn) "test-key"))
                          ((symbol-function 'message)
                           (lambda (format-string &rest args)
                             (push (apply #'format format-string args)
                                   messages))))
                  (should (eq (clutch--try-reconnect) new)))
                (should (eq clutch-connection new))
                (should (eq clutch--conn-sql-product product))
                (should (eq (buffer-local-value 'clutch-connection dml) new))
                (let ((header (buffer-local-value 'header-line-format dml)))
                  (if banner
                      (should (string-match-p banner header))
                    (should-not header)))
                (should-not (clutch--tx-state old))
                (should (seq-find (lambda (text)
                                    (string-match-p message-pattern text))
                                  messages)))
            (kill-buffer dml)))))))

(ert-deftest clutch-test-try-reconnect-refuses-a-namespace-it-cannot-reach ()
  "The automatic reconnect should refuse when it cannot return to the namespace.
It connected to where the parameters lead and ran the next statement
there, as for a DuckDB console moved into an attached database."
  (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
        built)
    (with-temp-buffer
      (setq-local clutch-connection 'dead-conn
                  clutch--connection-params '(:backend jdbc :url "jdbc:duckdb:/tmp/h.duckdb"))
      (cl-letf (((symbol-function 'clutch-db-unreachable-namespace)
                 (lambda (_conn) "att.main"))
                ((symbol-function 'clutch--build-conn)
                 (lambda (_params) (setq built t) 'new-conn)))
        (should (string-match-p
                 "att\\.main"
                 (error-message-string
                  (should-error (clutch--try-reconnect) :type 'user-error))))
        (should-not built)
        (should (eq clutch-connection 'dead-conn))))))

(ert-deftest clutch-test-query-console-refused-reconnect-stays-in-the-console ()
  "Reopening a dead console whose reconnect is refused should show the console.
The refusal says to press C-c C-e, which only the console binds to
connecting again, but it came before the console was shown."
  (let* ((existing (get-buffer-create " *clutch-query-console-refused*"))
         (clutch-connection-alist '(("alpha" . (:backend jdbc :url "jdbc:duckdb:/tmp/a.duckdb")))))
    (unwind-protect
        (save-window-excursion
          (with-current-buffer existing
            (clutch-mode)
            (setq-local clutch--console-name "alpha")
            (setq-local clutch-connection (make-clutch-test-conn :live nil)))
          (with-temp-buffer
            (switch-to-buffer (current-buffer))
            (cl-letf (((symbol-function 'clutch--try-reconnect)
                       (lambda () (user-error "The session was in att.main"))))
              (should-error (clutch-query-console "alpha") :type 'user-error)
              (should (eq (window-buffer (selected-window)) existing)))))
      (kill-buffer existing))))

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
        (clutch-test--with-isolated-metadata-caches
          (let* ((clutch--tx-state-cache (make-hash-table :test 'eq))
                 (conn (make-clutch-test-conn :commit-mode 'manual))
                 (buf (clutch-test--make-dml-result-buf conn)))
            (when (eq action 'rollback)
              (puthash conn 'dirty clutch--tx-state-cache))
            (unwind-protect
                (progn
                  (with-temp-buffer
                    (setq-local clutch-connection conn)
                    (pcase action
                      ('rollback (clutch-rollback))
                      ('commit (clutch-commit))
                      ('disconnect (clutch-disconnect))))
                  (with-current-buffer buf
                    (should
                     (string-match-p
                      banner (substring-no-properties header-line-format)))
                    (clutch--refresh-result-status-line)
                    (should
                     (string-match-p
                      banner (substring-no-properties header-line-format)))))
              (kill-buffer buf))))))))

(ert-deftest clutch-test-toggle-auto-commit-contract ()
  "Auto-commit toggle should switch clean states and reject unsafe states."
  (dolist (case '((manual-to-auto manual nil auto nil)
                  (auto-to-manual auto nil manual nil)
                  (dirty-manual manual t manual t)
                  (unsupported nil nil nil nil)))
    (pcase-let ((`(,label ,mode ,dirty ,mode-after ,dirty-after) case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
              (clutch-connection (make-clutch-test-conn :commit-mode mode)))
          (when dirty
            (puthash clutch-connection 'dirty clutch--tx-state-cache))
          (if (eq mode-after mode)
              (should-error (clutch-toggle-auto-commit) :type 'user-error)
            (clutch-toggle-auto-commit))
          (should (eq (clutch-test-conn-commit-mode clutch-connection)
                      mode-after))
          (should (eq (not (null (clutch--tx-dirty-p clutch-connection)))
                      dirty-after)))))))

(ert-deftest clutch-test-transaction-shortcuts-follow-attached-result-context ()
  "Result and Record views should dispatch transaction keys for supported SQL."
  (let ((overriding-local-map nil)
        (overriding-terminal-local-map nil))
    (dolist (mode '(clutch-result-mode clutch-record-mode))
      (with-temp-buffer
        (funcall mode)
        (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
              (conn (make-clutch-test-conn :commit-mode 'manual)))
          (clutch--bind-connection-context conn '(:backend oracle) 'oracle)
          (should clutch--transaction-shortcuts-mode)
          (dolist (binding '(("C-c C-m" . clutch-commit)
                             ("C-c C-u" . clutch-rollback)
                             ("C-c C-a" . clutch-toggle-auto-commit)))
            (should (eq (key-binding (kbd (car binding)))
                        (cdr binding))))
          (puthash conn 'dirty clutch--tx-state-cache)
          (call-interactively (key-binding (kbd "C-c C-m")))
          (should-not (clutch--tx-dirty-p conn))
          (when (eq mode 'clutch-result-mode)
            (should (eq (key-binding (kbd "C-c C-c"))
                        #'clutch-result-submit))))))
    (dolist (case '((clutch-result-mode nil)
                    (special-mode manual)))
      (pcase-let ((`(,mode ,commit-mode) case))
        (with-temp-buffer
          (funcall mode)
          (clutch--bind-connection-context
           (make-clutch-test-conn :commit-mode commit-mode)
           '(:backend oracle) 'oracle)
          (should-not clutch--transaction-shortcuts-mode)
          (should-not (key-binding (kbd "C-c C-m"))))))))

(ert-deftest clutch-test-transaction-shortcuts-leave-with-the-connection ()
  "Result and Record views should drop transaction shortcuts with their connection.
The owner invalidating them and disconnecting from them both end it."
  (let ((overriding-local-map nil)
        (overriding-terminal-local-map nil))
    (pcase-dolist (`(,mode ,trigger) '((clutch-result-mode invalidate)
                                       (clutch-record-mode invalidate)
                                       (clutch-result-mode disconnect)))
      (ert-info ((format "%s: %s" mode trigger))
        (clutch-test--with-isolated-metadata-caches
          (let ((conn (make-clutch-test-conn :commit-mode 'manual)))
            (with-temp-buffer
              (funcall mode)
              (clutch--bind-connection-context conn '(:backend oracle) 'oracle)
              (should clutch--transaction-shortcuts-mode)
              (should (eq (key-binding (kbd "C-c C-m")) #'clutch-commit))
              (if (eq trigger 'disconnect)
                  (clutch-disconnect)
                (with-temp-buffer
                  (clutch--invalidate-derived-buffers conn)))
              (should-not clutch-connection)
              (should-not clutch--transaction-shortcuts-mode)
              (should-not (key-binding (kbd "C-c C-m"))))))))))

(ert-deftest clutch-test-sqlite-omits-manual-commit-ui ()
  "SQLite should not advertise Clutch manual-commit controls."
  (require 'clutch-db-sqlite)
  (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/bookmarks.db")))
    (should-not (clutch-db-manual-commit-supported-p conn))))

;;;; Connection — display key and icons

(ert-deftest clutch-test-icon-caches-missing-nerd-icons ()
  "Missing nerd-icons should be checked once and keep the fallback text."
  (let ((clutch--nerd-icons-availability 'unchecked)
        (calls 0))
    (cl-letf (((symbol-function 'require)
               (lambda (&rest _)
                 (cl-incf calls)
                 nil)))
      (dotimes (_ 5)
        (should (equal (clutch--icon
                        '(mdicon . "nf-md-database_check_outline") "⬢")
                       "⬢")))
      (should (= calls 1)))))

(ert-deftest clutch-test-icon-dispatches-public-nerd-icons-functions ()
  "Icon helper should dispatch through public nerd-icons render functions."
  (let ((clutch--nerd-icons-availability 'unchecked)
        (calls 0))
    (cl-letf (((symbol-function 'require)
               (lambda (&rest _)
                 (cl-incf calls)
                 t))
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
                     "fallback"))
      (should (= calls 1)))))

(ert-deftest clutch-test-mongodb-completion-contexts ()
  "MongoDB completion should offer what the text before point calls for.
After `db.' it offers cached collections and shell methods; after a
collection, its helper methods and templates; after a chainable query,
the cursor helpers that may still follow; MQL operators after `$';
sampled field names as document keys and `$' field paths; and
collections inside `from:' and `into:' strings.  Completing inserts the
candidate, and a finished key completion adds its colon."
  (clutch-test--with-isolated-metadata-caches
    (let ((conn (make-clutch-test-conn))
          (schema (make-hash-table :test 'equal)))
      (puthash "users" nil schema)
      (puthash "orders" '("status" "userId" "items.productId") schema)
      (puthash "products" '("sku" "category") schema)
      (puthash conn schema clutch--schema-cache)
      (with-temp-buffer
        (clutch-mongodb-mode)
        (setq-local clutch-connection conn)
        (pcase-dolist (`(,text ,prefix ,offered ,withheld)
                       '(("db." ""
                          ("users" "orders" "runCommand()") ("getSiblingDB()"))
                         ("db.users." ""
                          ("find()" "aggregate()" "countDocuments()"
                           "find({}).limit(20)"
                           "aggregate([{ $match: {} }, { $limit: 20 }])")
                          ("estimatedDocumentCount()"))
                         ("db.users.find({})." ""
                          ("limit()" "sort()" "explain()") ("batchSize()" "find()"))
                         ("db.users.find({}).sort({createdAt: -1})." ""
                          ("limit()" "skip()") nil)
                         ("db.users.aggregate([])." ""
                          ("allowDiskUse()" "maxTimeMS()" "explain()") ("limit()"))
                         ("db.orders.aggregate([{$" "$"
                          ("$match" "$lookup" "$sum" "$dateTrunc") ("$vectorSearch"))
                         ("db.orders.find({st" "st" ("status") ("find()"))
                         ("db.orders.aggregate([{$group:{_id:\"$u" "$u"
                          ("$userId") nil)
                         ("db.orders.aggregate([{$lookup:{from:\"pro" "pro"
                          ("products") nil)
                         ("db.orders.aggregate([{$merge:{into:\"pro" "pro"
                          ("products") nil)))
          (ert-info ((format "candidates after %S" text))
            (erase-buffer)
            (insert text)
            (let* ((capf (clutch-mongodb-completion-at-point))
                   (candidates (all-completions prefix (nth 2 capf))))
              (should (= (cadr capf) (point)))
              (should (= (- (cadr capf) (car capf)) (length prefix)))
              (dolist (candidate offered)
                (should (member candidate candidates)))
              (dolist (candidate withheld)
                (should-not (member candidate candidates))))))
        (pcase-dolist (`(,text ,prefix ,completed)
                       '(("db.users.fi" "fi" "db.users.find()")
                         ("db.users.find({}).li" "li" "db.users.find({}).limit()")))
          (ert-info ((format "completing after %S" text))
            (erase-buffer)
            (insert text)
            (cl-letf (((symbol-function 'completion-in-region)
                       (lambda (beg end collection &optional _predicate)
                         (delete-region beg end)
                         (insert (car (all-completions prefix collection)))
                         t)))
              (clutch-mongodb-complete-at-point)
              (should (equal (buffer-string) completed)))))
        (pcase-dolist (`(,text ,key ,completed)
                       '(("db.orders.find({st" "status" "db.orders.find({status: ")
                         ("db.orders.aggregate([{$m" "$match"
                          "db.orders.aggregate([{$match: ")))
          (ert-info ((format "finishing key %S" key))
            (erase-buffer)
            (insert text)
            (pcase-let ((`(,beg ,end ,_collection . ,plist)
                         (clutch-mongodb-completion-at-point)))
              (delete-region beg end)
              (insert key)
              (funcall (plist-get plist :exit-function) key 'finished)
              (should (equal (buffer-string) completed)))))))))

(ert-deftest clutch-test-mongodb-completion-defers-uncached-field-sampling ()
  "Installed MongoDB CAPF should defer uncached field metadata sampling."
  (clutch-test--with-isolated-metadata-caches
    (let* ((schema (make-hash-table :test 'equal))
           (conn (make-clutch-mongodb-conn :database "test"))
           (sync-samples 0)
           (scheduled nil))
      (puthash "orders" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-live-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-list-columns)
                 (lambda (&rest _args)
                   (cl-incf sync-samples)
                   '("status")))
                ((symbol-function 'run-with-idle-timer)
                 (lambda (delay repeat function &rest args)
                   (push (list delay repeat function args) scheduled)
                   'fake-timer))
                ((symbol-function 'completion-in-region)
                 (lambda (&rest _args) t)))
        (with-temp-buffer
          (clutch-mongodb-mode)
          (setq-local clutch-connection conn)
          (should (memq #'clutch-mongodb-completion-at-point
                        completion-at-point-functions))
          (insert "db.orders.find({st")
          (completion-at-point)
          (should (= sync-samples 0))
          (should (= (length scheduled) 1))
          (pcase-let ((`((,delay ,repeat ,_function ,_args)) scheduled))
            (should (numberp delay))
            (should (<= 0 delay 0.1))
            (should-not repeat)))))))

(ert-deftest clutch-test-mongodb-completion-keeps-cached-fields-and-operators ()
  "Installed MongoDB CAPF should preserve cached fields and operators."
  (clutch-test--with-isolated-metadata-caches
    (let* ((schema (make-hash-table :test 'equal))
           (conn (make-clutch-mongodb-conn :database "test"))
           (captured nil))
      (puthash "orders" '("status" "userId") schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-list-columns)
                 (lambda (&rest _args)
                   (ert-fail "cached completion sampled documents synchronously")))
                ((symbol-function 'clutch-db-list-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "cached completion queued field metadata")))
                ((symbol-function 'completion-in-region)
                 (lambda (beg end collection &optional predicate)
                   (setq captured
                         (all-completions
                          (buffer-substring-no-properties beg end)
                          collection predicate))
                   t)))
        (with-temp-buffer
          (clutch-mongodb-mode)
          (setq-local clutch-connection conn)
          (insert "db.orders.find({st")
          (completion-at-point)
          (should (equal captured '("status")))
          (erase-buffer)
          (insert "db.orders.aggregate([{$")
          (completion-at-point)
          (should (member "$match" captured)))))))

(ert-deftest clutch-test-mongodb-explain-query-at-point-uses-current-helper ()
  "MongoDB explain command should explain the current helper at point."
  (let ((conn (make-clutch-test-conn))
        captured)
    (with-temp-buffer
      (clutch-mongodb-mode)
      (setq-local clutch-connection conn)
      (insert "db.users.find({active: true}).limit(5)")
      (cl-letf (((symbol-function 'clutch-db-explain-query)
                 (lambda (conn query)
                   (setq captured (list conn query))
                   "{\"summary\":{\"winningStage\":\"IXSCAN\"}}")))
        (clutch-test--with-pop-to-buffer-capture shown
          (clutch-mongodb-explain-query-at-point)
          (should (equal captured
                         (list conn "db.users.find({active: true}).limit(5)")))
          (should (equal (buffer-name shown) "*clutch mongodb: explain*"))
          (should (string-match-p "IXSCAN"
                                  (with-current-buffer shown
                                    (buffer-string)))))))))

(ert-deftest clutch-test-mongodb-mode-setup ()
  "MongoDB query buffers should be set up for MQL without SQL transaction keys.
The mode is a `prog-mode', not a `js-mode', with its own indentation and
`//' comments, and does not expose the SQL commit, rollback and
auto-commit keys."
  (with-temp-buffer
    (clutch-mongodb-mode)
    (should (derived-mode-p 'prog-mode))
    (should-not (derived-mode-p 'js-mode))
    (should (eq indent-line-function #'clutch-mongodb-indent-line))
    (should (equal comment-start "// "))
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
  (let ((conn (make-clutch-mongodb-conn :database "app")))
    (should (equal (clutch--object-browse-query conn '(:name "users"))
                   "db.getCollection(\"users\").find({}).limit(20);"))))

(ert-deftest clutch-test-object-browse-query-uses-sql-for-sql-interface-mongodb ()
  "MongoDB SQL Interface object browsing should insert SELECT SQL."
  (require 'clutch-db-jdbc)
  (let ((conn (make-clutch-jdbc-conn :params '(:driver mongodb))))
    (should (equal (clutch--object-browse-query conn '(:name "users"))
                   "SELECT * FROM \"users\";"))))

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

(ert-deftest clutch-test-disconnect-asks-before-dropping-uncommitted-work ()
  "Disconnect should ask before dropping uncommitted work in either commit mode.
Only Manual mode asked, so the work of a typed BEGIN and INSERT in Auto
mode was lost without a question.  `clutch-commit' and `clutch-rollback'
stay Manual mode commands and say how to end such a transaction."
  (dolist (mode '(manual auto))
    (ert-info ((format "commit mode: %s" mode))
      (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
            (conn (make-clutch-test-conn :commit-mode mode))
            prompts)
        (with-temp-buffer
          (setq-local clutch-connection conn)
          (puthash conn 'dirty clutch--tx-state-cache)
          (cl-letf (((symbol-function 'yes-or-no-p)
                     (lambda (prompt) (push prompt prompts) nil)))
            (should-error (clutch-disconnect) :type 'user-error)
            (should (string-match-p "Uncommitted changes will be lost" (car prompts)))
            (should (clutch-db-live-p conn))
            (should (eq clutch-connection conn))
            (should (clutch--tx-dirty-p conn))
            (when (eq mode 'auto)
              (dolist (command '(clutch-commit clutch-rollback))
                (should (string-match-p
                         "COMMIT or ROLLBACK"
                         (error-message-string
                          (should-error (funcall command) :type 'user-error)))))
              (should (clutch--tx-dirty-p conn)))))))))

(ert-deftest clutch-test-toggle-to-manual-adopts-a-transaction-begun-with-sql ()
  "Switching to Manual mode should keep the work of a transaction begun in Auto.
Refusing it left a staged batch, which Auto mode refuses inside such a
transaction, nowhere to go.  Leaving Manual mode with work, or either
way with an uncertain outcome, is still refused."
  (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
        (conn (make-clutch-test-conn :commit-mode 'auto)))
    (with-temp-buffer
      (setq-local clutch-connection conn)
      (puthash conn 'dirty clutch--tx-state-cache)
      (clutch-toggle-auto-commit)
      (should (eq (clutch-test-conn-commit-mode conn) 'manual))
      (should (clutch--tx-dirty-p conn))
      (should-error (clutch-toggle-auto-commit) :type 'user-error)
      (should (eq (clutch-test-conn-commit-mode conn) 'manual))
      (setf (clutch-test-conn-commit-mode conn) 'auto)
      (puthash conn 'uncertain clutch--tx-state-cache)
      (should-error (clutch-toggle-auto-commit) :type 'user-error)
      (should (eq (clutch-test-conn-commit-mode conn) 'auto)))))

(ert-deftest clutch-test-connect-refuses-while-a-query-runs ()
  "Connecting elsewhere should wait for the statement running on the buffer.
The server finished a statement whose connection was closed under it."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
        (clutch--running-queries (make-hash-table :test 'eq))
        (clutch-connection-alist nil))
    (unwind-protect
        (with-temp-buffer
          (setq-local clutch-connection conn)
          (puthash conn (list :buffer (current-buffer)) clutch--running-queries)
          ;; A prompt would run out of answers and fail the test.
          (clutch-test--with-minibuffer-answers nil
            (should-error (clutch-connect) :type 'user-error))
          (should (clutch-db-live-p conn))
          (should (eq clutch-connection conn)))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-switch-schema-refuses-while-a-query-runs ()
  "Switching schema should wait for the statement running on the connection.
It should not ask the connection for its schemas first: JDBC waited behind
the statement, and a switch that reconnects closed the connection under it."
  (let ((clutch-connection 'fake-conn)
        (clutch--running-queries (make-hash-table :test 'eq))
        asked)
    (puthash clutch-connection (list :buffer (current-buffer))
             clutch--running-queries)
    (cl-letf (((symbol-function 'clutch-db-list-schemas)
               (lambda (_conn) (setq asked t) '("a" "b")))
              ((symbol-function 'clutch-db-current-schema)
               (lambda (_conn) (setq asked t) "a")))
      (should-error (clutch-switch-schema) :type 'user-error)
      (should-not asked))))

(ert-deftest clutch-test-disconnect-leaves-a-running-statement-unknown ()
  "A statement whose connection is closed under it should not report failure.
The server may still finish it, so its outcome is unknown; an error that
arrived before the disconnect, waiting to be shown, keeps its message."
  (require 'clutch-db-sqlite)
  (let ((clutch--running-queries (make-hash-table :test 'eq))
        reply errors)
    (cl-letf (((symbol-function 'clutch-db-query-async)
               (lambda (_conn _sql callback) (setq reply callback) t))
              ((symbol-function 'clutch--show-statement-status) #'ignore))
      (cl-flet ((run (reply-first err)
                  (let ((conn (clutch-db-sqlite-connect '(:database ":memory:"))))
                    (clutch--run-db-query-async
                     conn "UPDATE t SET n = 1" nil
                     (lambda (_result error &optional _cancelled)
                       (push error errors)))
                    (if reply-first
                        (progn (funcall reply nil err)
                               (clutch--do-disconnect conn))
                      (clutch--do-disconnect conn)
                      (funcall reply nil err))
                    (ert-run-idle-timers))))
        (run t '(clutch-db-error "Deadlock found"))
        (run nil '(clutch-db-error "Connection closed"))
        (should (equal (nreverse errors)
                       '((clutch-db-error "Deadlock found")
                         (clutch-db-error
                          "Disconnected while the statement ran; its outcome is unknown"))))))))

(ert-deftest clutch-test-disconnect-refreshes-derived-result-footer ()
  "Disconnect should refresh result chrome without replacing its table header."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
        (params '(:backend sqlite :database ":memory:"))
        (result (generate-new-buffer " *clutch-disconnect-result*")))
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (clutch--bind-connection-context conn params 'sqlite)
          (with-current-buffer result
            (clutch-result-mode)
            (clutch--bind-connection-context conn params 'sqlite)
            (setq-local clutch--result-rows '((1))
                        clutch--filtered-rows nil
                        clutch--page-current 0
                        clutch--page-total-rows 1
                        clutch-result-max-rows 100
                        header-line-format "TABLE")
            (clutch--refresh-footer-line)
            (should-not
             (string-match-p
              "DISCONNECTED"
              (substring-no-properties
               (clutch--footer-mode-line-display)))))
          (with-current-buffer result
            (clutch-disconnect))
          (should-not (clutch-db-live-p conn))
          (with-current-buffer result
            (should-not clutch-connection)
            (should (equal header-line-format "TABLE"))
            (should
             (string-match-p
              "DISCONNECTED"
              (substring-no-properties
               (clutch--footer-mode-line-display))))))
      (when (clutch-db-live-p conn)
        (clutch-db-disconnect conn))
      (when (buffer-live-p result)
        (kill-buffer result)))))

(ert-deftest clutch-test-do-disconnect-stops-ssh-tunnel ()
  "Disconnect should stop any SSH tunnel associated with the connection."
  (skip-unless (executable-find "sleep"))
  (require 'clutch-db-sqlite)
  (let ((clutch--connection-transport-cache (make-hash-table :test 'eq))
        (conn (clutch-db-sqlite-connect '(:database ":memory:")))
        (proc (start-process "clutch-test-tunnel" nil "sleep" "60")))
    (unwind-protect
        (progn
          (puthash conn (list :kind 'ssh :process proc)
                   clutch--connection-transport-cache)
          (clutch--do-disconnect conn)
          (should-not (clutch-db-live-p conn))
          (should-not (process-live-p proc))
          (should-not (gethash conn clutch--connection-transport-cache)))
      (when (process-live-p proc) (delete-process proc))
      (when (clutch-db-live-p conn) (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-teardown-clears-the-connections-schema-metadata-state ()
  "Ending a session should clear schema refresh state for its connection object.
That holds for disconnecting a live connection and for killing a console
whose connection is already dead, which is not closed again; an equal but
distinct connection keeps its state."
  (pcase-dolist (`(,label ,live)
                 '(("disconnecting a live connection" t)
                   ("killing a console whose connection is dead" nil)))
    (ert-info (label)
      (clutch-test--with-isolated-metadata-caches
        (let ((conn (make-clutch-test-conn :live live))
              (other-conn (make-clutch-test-conn :live live))
              (console (generate-new-buffer " *clutch-dead-console*")))
          (should (equal conn other-conn))
          (should-not (eq conn other-conn))
          (puthash conn '(:state refreshing) clutch--schema-status-cache)
          (puthash conn 7 clutch--schema-refresh-tickets)
          (puthash conn (make-hash-table :test 'equal) clutch--schema-cache)
          (puthash other-conn '(:state ready) clutch--schema-status-cache)
          (unwind-protect
              (progn
                (if live
                    (clutch--do-disconnect conn)
                  (with-current-buffer console
                    (clutch-mode)
                    (setq-local clutch-connection conn))
                  (cl-letf (((symbol-function 'clutch-db-disconnect)
                             (lambda (_conn) (ert-fail "closed a dead connection"))))
                    (kill-buffer console)))
                (should-not (gethash conn clutch--schema-status-cache))
                (should-not (gethash conn clutch--schema-refresh-tickets))
                (should-not (gethash conn clutch--schema-cache))
                (should (equal (gethash other-conn clutch--schema-status-cache)
                               '(:state ready))))
            (when (buffer-live-p console) (kill-buffer console))))))))

(ert-deftest clutch-test-refresh-schema-status-ui-matches-connection-identity ()
  "Schema status refresh should update only buffers attached to the same object."
  (let ((conn (list 'same-fields))
        (equal-but-distinct-conn (list 'same-fields))
        (target (generate-new-buffer " *clutch-schema-status-target*"))
        (other (generate-new-buffer " *clutch-schema-status-other*"))
        (describe (generate-new-buffer " *clutch-schema-status-describe*"))
        (clutch--schema-status-cache (make-hash-table :test 'eq))
        reverted)
    (unwind-protect
        (progn
          (with-current-buffer target
            (setq-local clutch-connection conn
                        clutch--query-buffer-local-p t
                        clutch--console-name "target"))
          (with-current-buffer other
            (setq-local clutch-connection equal-but-distinct-conn
                        clutch--query-buffer-local-p t
                        clutch--console-name "other"))
          (with-current-buffer describe
            (setq-local clutch-connection conn
                        revert-buffer-function
                        (lambda (ignore-auto noconfirm)
                          (setq reverted (list ignore-auto noconfirm)))))
          (puthash conn '(:state ready :tables 7) clutch--schema-status-cache)
          (should (equal conn equal-but-distinct-conn))
          (should-not (eq conn equal-but-distinct-conn))
          (cl-letf (((symbol-function 'clutch--refresh-connection-render-state)
                     #'ignore)
                    ((symbol-function 'clutch--update-mode-line) #'ignore))
            (clutch--refresh-schema-status-ui conn))
          (should (equal (buffer-name target) "*clutch: target* [schema 7t]"))
          (should (equal (buffer-name other) " *clutch-schema-status-other*"))
          (should (equal reverted '(t t))))
      (when (buffer-live-p target) (kill-buffer target))
      (when (buffer-live-p other) (kill-buffer other))
      (when (buffer-live-p describe) (kill-buffer describe)))))

(ert-deftest clutch-test-refresh-schema-status-ui-skips-inherited-default-revert ()
  "Schema status refresh should not revert an unrelated attached buffer."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
        (attached (generate-new-buffer " *clutch-schema-status-attached*"))
        (original-default (default-value 'revert-buffer-function))
        default-reverted)
    (unwind-protect
        (progn
          (set-default 'revert-buffer-function
                       (lambda (&rest _args)
                         (setq default-reverted t)))
          (with-current-buffer attached
            (kill-local-variable 'revert-buffer-function)
            (setq-local clutch-connection conn)
            (should-not (local-variable-p 'revert-buffer-function)))
          (clutch--refresh-schema-status-ui conn)
          (should-not default-reverted))
      (set-default 'revert-buffer-function original-default)
      (when (buffer-live-p attached)
        (kill-buffer attached))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-kill-buffer-ends-the-session-only-from-its-owner ()
  "Killing a buffer should end its session only when the buffer owns it.
Killing a plain SQL buffer or a REPL disconnects and invalidates derived
buffers; killing an indirect SQL edit or a derived result buffer does not
disconnect."
  (require 'clutch-db-sqlite)
  (pcase-dolist (`(,label ,kind ,owner)
                 '(("plain SQL buffer" clutch-mode t)
                   ("REPL" clutch-repl-mode t)
                   ("indirect SQL edit" indirect nil)
                   ("derived result buffer" clutch-result-mode nil)))
    (ert-info (label)
      (let ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
            (buf (generate-new-buffer " *clutch-killed*"))
            (result (generate-new-buffer " *clutch-derived-result*")))
        (unwind-protect
            (progn
              (with-current-buffer result
                (setq-local clutch-connection conn))
              (with-current-buffer buf
                (pcase kind
                  ('indirect
                   (clutch-mode)
                   (clutch--indirect-mode 1))
                  (_ (funcall kind)))
                (setq clutch-connection conn))
              (kill-buffer buf)
              (if owner
                  (progn
                    (should-not (clutch-db-live-p conn))
                    ;; Buffers derived from the dead session lose their binding.
                    (should-not (buffer-local-value 'clutch-connection result)))
                (should (clutch-db-live-p conn))
                (should (eq (buffer-local-value 'clutch-connection result) conn))))
          (when (buffer-live-p buf) (kill-buffer buf))
          (when (buffer-live-p result) (kill-buffer result))
          (when (clutch-db-live-p conn) (clutch-db-disconnect conn)))))))

(ert-deftest clutch-test-reconnect-invalidates-derived-buffers ()
  "Explicit reconnect should invalidate old attachments before priming metadata."
  (require 'clutch-db-sqlite)
  (clutch-test--with-isolated-metadata-caches
    (let* ((db-file (make-temp-file "clutch-reconnect-" nil ".db"))
           (old-conn (clutch-db-sqlite-connect '(:database ":memory:")))
           (other-conn (clutch-db-sqlite-connect '(:database ":memory:")))
           (result (generate-new-buffer " *clutch-result-reconnect*"))
           (clutch-connection-alist nil)
           (prime (symbol-function 'clutch--prime-schema-cache))
           new prime-called status-before-prime)
      (puthash old-conn '(:state refreshing) clutch--schema-status-cache)
      (puthash old-conn 7 clutch--schema-refresh-tickets)
      (puthash other-conn '(:state ready) clutch--schema-status-cache)
      (unwind-protect
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection old-conn)
            (with-current-buffer result
              (setq-local clutch-connection old-conn))
            (cl-letf (((symbol-function 'clutch--prime-schema-cache)
                       (lambda (conn)
                         (setq prime-called t
                               status-before-prime
                               (gethash old-conn clutch--schema-status-cache))
                         (funcall prime conn))))
              (clutch-test--with-minibuffer-answers (list "sqlite" db-file)
                (clutch-connect)))
            (setq new clutch-connection)
            (should (clutch-db-live-p new))
            (should-not (eq new old-conn))
            (should-not (clutch-db-live-p old-conn))
            (should-not (buffer-local-value 'clutch-connection result))
            (should prime-called)
            (should-not status-before-prime)
            (should-not (gethash old-conn clutch--schema-status-cache))
            (should-not (gethash old-conn clutch--schema-refresh-tickets))
            (should (equal (gethash other-conn clutch--schema-status-cache)
                           '(:state ready))))
        (when (buffer-live-p result)
          (kill-buffer result))
        (dolist (conn (list old-conn other-conn new))
          (when (and conn (clutch-db-live-p conn)) (clutch-db-disconnect conn)))
        (delete-file db-file)))))

(ert-deftest clutch-test-try-reconnect-releases-old-ssh-transport ()
  "Reconnect should stop the old SSH tunnel after the new connection is ready."
  (skip-unless (executable-find "sleep"))
  (require 'clutch-db-sqlite)
  (let* ((db-file (make-temp-file "clutch-tunnel-" nil ".db"))
         (params (list :backend 'sqlite :database db-file))
         (old-conn (clutch-db-sqlite-connect params))
         (proc (start-process "clutch-test-tunnel" nil "sleep" "60"))
         (clutch--connection-transport-cache (make-hash-table :test 'eq))
         (clutch--problem-records-by-conn (make-hash-table :test 'eq))
         new)
    (unwind-protect
        (with-temp-buffer
          (setq-local clutch-connection old-conn
                      clutch--connection-params params
                      clutch--conn-sql-product 'sqlite)
          (puthash old-conn (list :kind 'ssh :process proc)
                   clutch--connection-transport-cache)
          (puthash old-conn '(:summary "old") clutch--problem-records-by-conn)
          (clutch-db-disconnect old-conn)
          (should (clutch--try-reconnect))
          (setq new clutch-connection)
          (should (clutch-db-live-p new))
          (should-not (process-live-p proc))
          (should-not (gethash old-conn clutch--connection-transport-cache))
          (should-not (gethash old-conn clutch--problem-records-by-conn)))
      (when (process-live-p proc) (delete-process proc))
      (when (and new (clutch-db-live-p new)) (clutch-db-disconnect new))
      (delete-file db-file))))

(ert-deftest clutch-test-replace-connection-discards-a-candidate-it-cannot-hand-over ()
  "Replacement should discard a candidate it cannot hand over, without retrying.
A candidate lost during the old teardown is neither retried nor bound.  A
failing old-connection teardown must not orphan the new connection.  The
new connection's transport is released however the handover fails,
including when the new connection died meanwhile and when disconnecting
it is quit."
  (pcase-dolist (`(,label ,old-teardown ,new-teardown ,error ,new-closed)
                 '(("the candidate dies during the old teardown"
                    kill-new nil clutch-db-error nil)
                   ("the old teardown signals"
                    signal nil clutch-db-error t)
                   ("the old teardown signals and the candidate died meanwhile"
                    kill-all-and-signal nil clutch-db-error nil)
                   ("quit while disconnecting the orphan"
                    signal quit quit t)))
    (ert-info (label)
      (let ((alive '(old-conn new-conn))
            (builds 0)
            released rebound disconnected)
        (cl-letf (((symbol-function 'clutch--build-conn)
                   (lambda (_params) (cl-incf builds) 'new-conn))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (memq conn alive)))
                  ((symbol-function 'clutch--clear-tx-state) #'ignore)
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (conn)
                     (if (eq conn 'old-conn)
                         (pcase old-teardown
                           ('kill-new (setq alive '(old-conn)))
                           ('signal
                            (signal 'clutch-db-error '("old teardown failed")))
                           ('kill-all-and-signal
                            (setq alive nil)
                            (signal 'clutch-db-error '("old teardown failed"))))
                       (setq disconnected conn)
                       (when (eq new-teardown 'quit)
                         (signal 'quit nil)))))
                  ((symbol-function 'clutch--release-connection-transport)
                   (lambda (conn) (push conn released)))
                  ((symbol-function 'clutch--rebind-connection-buffers)
                   (lambda (&rest _args) (setq rebound t))))
          (should (eq (car (condition-case err
                               (progn (clutch--replace-connection
                                       'old-conn '(:backend pg) 'pg)
                                      nil)
                             ((error quit) err)))
                      error))
          (should (= builds 1))
          (should-not rebound)
          (should (eq disconnected (and new-closed 'new-conn)))
          (should (memq 'new-conn released))
          (should (memq 'old-conn released)))))))

(ert-deftest clutch-test-derived-buffers-reconnect-using-inherited-context ()
  "Derived buffers reconnect their logical session without losing local state."
  (require 'clutch-db-sqlite)
  (dolist (kind '(result object))
    (ert-info ((format "derived buffer: %s" kind))
      (let* ((db-file (make-temp-file "clutch-derived-" nil ".db"))
             (params (list :backend 'sqlite :database db-file))
             (old-conn (clutch-db-sqlite-connect params))
             (pending-deletes (list (vector 'delete)))
             (pending-edits (list (cons '(0 . 0) 'edit)))
             (pending-inserts (list (list (cons "name" "insert"))))
             new)
        (clutch-db-query old-conn "CREATE TABLE demo (id INT)")
        (unwind-protect
            (with-temp-buffer
              (let ((source-buf (current-buffer)))
                (clutch-mode)
                (clutch--bind-connection-context old-conn params 'sqlite)
                (clutch-test--with-pop-to-buffer-capture derived-buf
                  (pcase kind
                    ('result
                     (clutch-result--display
                      (make-clutch-db-result
                       :connection old-conn :columns nil :rows nil)
                      "UPDATE demo SET x = 1" 0.1))
                    ('object
                     (clutch-object-show-ddl-or-source
                      '(:name "demo" :type "TABLE"))))
                  (clutch-db-disconnect old-conn)
                  (with-current-buffer derived-buf
                    (should (eq clutch-connection old-conn))
                    (should (equal clutch--connection-params params))
                    (should (eq clutch--conn-sql-product 'sqlite))
                    (when (eq kind 'result)
                      (setq-local clutch--pending-deletes pending-deletes
                                  clutch--pending-edits pending-edits
                                  clutch--pending-inserts pending-inserts))
                    (clutch--ensure-connection)
                    (setq new clutch-connection)
                    (should (clutch-db-live-p new))
                    (should-not (eq new old-conn))
                    (should (equal (clutch-db-database new) db-file))
                    (when (eq kind 'result)
                      (should (eq clutch--pending-deletes pending-deletes))
                      (should (eq clutch--pending-edits pending-edits))
                      (should (eq clutch--pending-inserts pending-inserts))))
                  (with-current-buffer source-buf
                    (should (eq clutch-connection new))))))
          (dolist (conn (list old-conn new))
            (when (and conn (clutch-db-live-p conn)) (clutch-db-disconnect conn)))
          (delete-file db-file))))))

(ert-deftest clutch-test-connect-failure-preserves-old-live-connection ()
  "Interactive connect should not drop the old live session on failure."
  (let ((clutch-connection 'old-conn)
        disconnected)
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--confirm-session-close)
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

(ert-deftest clutch-test-connect-discards-a-candidate-it-cannot-activate ()
  "`clutch-connect' should discard a candidate it cannot activate, without retrying.
A new connection that became dead during the old teardown is neither
retried nor activated, and a failing old-connection teardown must not
orphan the new connection."
  (pcase-dolist (`(,label ,new-alive ,old-teardown-signals)
                 '(("the candidate died during the old teardown" nil nil)
                   ("the old teardown signals" t t)))
    (ert-info (label)
      (let (built released disconnected activated)
        (with-temp-buffer
          (setq-local clutch-connection 'old-conn)
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (if (eq conn 'new-conn) new-alive t)))
                    ((symbol-function 'clutch--connect-params-for-current-buffer)
                     (lambda () '(:backend jdbc :database "newdb")))
                    ((symbol-function 'clutch--effective-sql-product)
                     (lambda (_params) 'jdbc))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (params) (push params built) 'new-conn))
                    ((symbol-function 'clutch--do-disconnect)
                     (lambda (_conn)
                       (when old-teardown-signals
                         (signal 'clutch-db-error '("old teardown failed")))))
                    ((symbol-function 'clutch-db-disconnect)
                     (lambda (conn) (setq disconnected conn)))
                    ((symbol-function 'clutch--release-connection-transport)
                     (lambda (conn) (push conn released)))
                    ((symbol-function 'clutch--activate-current-buffer-connection)
                     (lambda (conn params product)
                       (setq activated (list conn params product)))))
            (should-error (clutch-connect) :type 'clutch-db-error)
            (should (equal built '((:backend jdbc :database "newdb"))))
            (should-not activated)
            (should (eq disconnected (and new-alive 'new-conn)))
            (should (memq 'new-conn released))))))))

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
                   ((symbol-function 'clutch--clear-connection-metadata-caches)
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

(ert-deftest clutch-test-connect-takes-the-params-of-the-buffer-it-runs-in ()
  "`C-c C-e' should connect with the params of the buffer it runs in.
A saved query console reconnects with that console's saved connection and
an ad hoc SQLite console with its file params, without reopening the
picker.  A console whose saved connection no longer exists errors instead
of silently connecting elsewhere.  A non-console buffer keeps the generic
interactive connect flow, and every buffer records its session target."
  (let* ((dir (make-temp-file "clutch-console-" t))
         (alpha-file (expand-file-name "alpha.db" dir))
         (ad-hoc-file (expand-file-name "ad-hoc.db" dir))
         (clutch-connection-alist
          `(("alpha" . (:backend sqlite :database ,alpha-file)))))
    (unwind-protect
        (pcase-dolist (`(,label ,console-name ,ad-hoc ,answers ,expected)
                       `(("saved console" "alpha" nil nil ,alpha-file)
                         ("ad hoc SQLite console" "SQLite: ad hoc" t nil ,ad-hoc-file)
                         ("console whose saved connection vanished" "gone" nil nil error)
                         ("plain buffer" nil nil ("alpha") ,alpha-file)))
          (ert-info (label)
            (let (conn)
              (unwind-protect
                  (with-temp-buffer
                    (clutch-mode)
                    (when console-name
                      (setq-local clutch--console-name console-name))
                    (when ad-hoc
                      (setq-local clutch--console-ad-hoc-params
                                  (list :backend 'sqlite :database ad-hoc-file)))
                    ;; A prompt beyond ANSWERS runs out of them and fails the test.
                    (clutch-test--without-credentials
                      (clutch-test--with-minibuffer-answers answers
                        (if (eq expected 'error)
                            (should-error (clutch-connect) :type 'user-error)
                          (clutch-connect))))
                    (setq conn clutch-connection)
                    (if (eq expected 'error)
                        (should-not conn)
                      (should (clutch-db-live-p conn))
                      (should (equal (clutch-db-database conn) expected))
                      (should (equal clutch--session-target
                                     (clutch--connection-target
                                      (clutch-prepare-connection-params
                                       (or clutch--console-ad-hoc-params
                                           (clutch--saved-connection-params
                                            "alpha"))))))))
                (when (and conn (clutch-db-live-p conn))
                  (clutch-db-disconnect conn))))))
      (delete-directory dir t))))

(ert-deftest clutch-test-connect-in-query-console-keeps-tramp-origin-only-for-forwardable-profiles ()
  "Query console reconnect should keep its stored TRAMP origin only where it forwards.
A saved host and port profile keeps the origin its session was opened from.
A SQLite or `:url' profile cannot be forwarded, so an old origin is not
carried onto it."
  (dolist (case '((:saved (:backend pg :host "db" :port 5432 :database "appdb")
                   :expected (:backend pg :host "db" :port 5432 :database "appdb"
                              :pass-entry "alpha"
                              :tramp-default-directory "/ssh:devbox:/workspace/"))
                  (:saved (:backend sqlite :database "/tmp/app.db")
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
      (ert-info ((format "saved: %s" (plist-get saved :backend)))
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
                         (ert-fail "a stored TRAMP origin should not prompt again")))
                      ((symbol-function 'clutch--resolve-password)
                       (lambda (params)
                         (when (clutch-backend-jdbc-transport-p
                                (plist-get params :backend) params)
                           "secret"))))
              (clutch-test--with-connect-build-stubs (built 'generic 'new-conn)
                (clutch-connect)
                (should (equal built expected))))))))))

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

(ert-deftest clutch-test-query-console-does-not-create-buffer-on-connect-failure ()
  "Query console should not create a visible buffer before connect succeeds."
  (let* ((dir (make-temp-file "clutch-console-" t))
         (clutch-connection-alist
          `(("alpha" . (:backend sqlite
                        :database ,(expand-file-name "missing/app.db" dir)))))
         (buffers-before (buffer-list)))
    (unwind-protect
        ;; The failed connect records its problem in the current buffer.
        (with-temp-buffer
          (clutch-test--without-credentials
            (should-error (clutch-query-console "alpha") :type 'user-error))
          (should-not (cl-set-difference (buffer-list)
                                         (cons (current-buffer) buffers-before))))
      (dolist (buffer (cl-set-difference (buffer-list) buffers-before))
        (when (buffer-live-p buffer)
          (kill-buffer buffer)))
      (delete-directory dir t))))

(ert-deftest clutch-test-query-console-opens-ad-hoc-sqlite-file ()
  "Query console should open a SQL workspace for an ad hoc SQLite file."
  (let* ((dir (make-temp-file "clutch-console-" t))
         (clutch-console-directory (expand-file-name "consoles" dir))
         (db-file (expand-file-name "direct.db" dir))
         (params (list :backend 'sqlite :database db-file))
         conn opened)
    (unwind-protect
        (progn
          (clutch-query-sqlite-file db-file)
          (setq opened (current-buffer)
                conn clutch-connection)
          (should (clutch-db-live-p conn))
          (should (equal (clutch-db-database conn) db-file))
          (should (equal clutch--connection-params params))
          (should (eq clutch--conn-sql-product 'sqlite))
          (should (equal clutch--console-name
                         (format "SQLite: %s" (abbreviate-file-name db-file))))
          (should (equal clutch--console-ad-hoc-params params)))
      (when (buffer-live-p opened)
        (kill-buffer opened))
      (when (and conn (clutch-db-live-p conn)) (clutch-db-disconnect conn))
      (delete-directory dir t))))

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
             (params (plist-get case :params))
             built
             opened)
        (unwind-protect
            (clutch-test--with-connect-build-stubs (built nil 'mongodb-conn)
              (clutch-query-console (list :name name :params params))
              (setq opened (current-buffer))
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
          (when (buffer-live-p opened)
            (kill-buffer opened)))))))

(ert-deftest clutch-test-query-console-redis-uses-redis-mode ()
  "Redis query consoles should use Redis command editing, not SQL mode."
  (let* ((name "redis-local")
         (params '(:backend redis
                   :host "127.0.0.1"
                   :port 6379
                   :database 0))
         built
         opened)
    (unwind-protect
        (clutch-test--with-connect-build-stubs (built nil 'redis-conn)
          (clutch-query-console (list :name name :params params))
          (setq opened (current-buffer))
          (should (equal built params))
          (should (eq major-mode 'clutch-redis-mode))
          (should (derived-mode-p 'prog-mode))
          (should-not (derived-mode-p 'sql-mode))
          (should (eq (lookup-key clutch-redis-mode-map (kbd "C-c C-c"))
                      #'clutch-redis-execute-command-at-point))
          (should (memq #'clutch-redis-completion-at-point
                        completion-at-point-functions))
          (erase-buffer)
          (insert "HG")
          (let* ((capf (clutch-redis-completion-at-point))
                 (candidates (clutch-test--completion-candidates capf)))
            (should (member "HGETALL" candidates))))
      (when (buffer-live-p opened)
        (kill-buffer opened)))))

(ert-deftest clutch-test-redis-command-at-point-keeps-semicolon-as-input ()
  "Redis command execution should be line-oriented, not semicolon-delimited.
The line is picked at point, so it flashes as SQL picked at point does."
  (with-temp-buffer
    (clutch-redis-mode)
    (insert "GET user:1;  ")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--execute-and-mark)
                 (lambda (command beg end &optional at-point)
                   (setq captured
                         (list command
                               (buffer-substring-no-properties beg end)
                               at-point)))))
        (clutch-redis-execute-command-at-point))
      (should (equal captured '("GET user:1;" "GET user:1;" t))))))

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
                   (product (plist-get case :product))
                   (conn (plist-get case :conn))
                   (default-port (plist-get case :default-port)))
        (ert-info ((format "case: %s" label))
          (let ((clutch-connection-alist
                 '(("alpha" . (:backend mysql :database "app_a"))))
                console-candidates
                built
                port-default
                opened)
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
                    (setq opened (current-buffer))
                    (should (member "alpha" console-candidates))
                    (should-not (member "New connection..." console-candidates))
                    (should-not (member "SQLite file..." console-candidates))
                    (when default-port
                      (should (= port-default default-port)))
                    (should (equal built params))
                    (should (equal clutch--console-ad-hoc-params params))))
              (when (buffer-live-p opened)
                (kill-buffer opened)))))))))

(ert-deftest clutch-test-query-console-picker-switches-to-open-ad-hoc-console ()
  "The public picker should merge saved consoles and reuse an open ad hoc one."
  (require 'clutch-db-sqlite)
  (let* ((dir (make-temp-file "clutch-console-" t))
         (clutch-console-directory (expand-file-name "consoles" dir))
         (params (list :backend 'sqlite :database (expand-file-name "ad-hoc.db" dir)))
         (saved-params (list :backend 'sqlite :database (expand-file-name "new.db" dir)))
         (old-params (list :backend 'sqlite :database (expand-file-name "old.db" dir)))
         (name (clutch--ad-hoc-console-name params))
         (ad-hoc (generate-new-buffer " *clutch-open-ad-hoc*"))
         (matching (generate-new-buffer " *clutch-open-saved*"))
         (collision (generate-new-buffer " *clutch-open-collision*"))
         (ad-hoc-conn (clutch-db-sqlite-connect params))
         (matching-conn (clutch-db-sqlite-connect saved-params))
         (collision-conn (clutch-db-sqlite-connect old-params))
         (clutch-connection-alist `(("alpha" . ,saved-params)))
         ;; The pick renames the ad hoc console; its candidate is the name before.
         (ad-hoc-name (buffer-name ad-hoc))
         (matching-name (buffer-name matching))
         (collision-name (buffer-name collision))
         candidates)
    (unwind-protect
        (progn
          (with-current-buffer ad-hoc
            (clutch-mode)
            (setq-local clutch--console-name name
                        clutch--console-storage-name
                        (clutch--console-persistence-name name params)
                        clutch--console-ad-hoc-params params
                        clutch--connection-params params
                        clutch-connection ad-hoc-conn))
          (dolist (spec `((,matching ,saved-params ,matching-conn)
                          (,collision ,old-params ,collision-conn)))
            (pcase-let ((`(,buffer ,console-params ,conn) spec))
              (with-current-buffer buffer
                (clutch-mode)
                (setq-local clutch--console-name "alpha"
                            clutch--console-storage-name
                            (clutch--console-persistence-name
                             "alpha" console-params)
                            clutch--connection-params console-params
                            clutch-connection conn))))
          (cl-letf (((symbol-function 'completing-read)
                     (lambda (prompt collection &rest _args)
                       (should (equal prompt "Console: "))
                       (setq candidates collection)
                       ad-hoc-name)))
            (call-interactively #'clutch-query-console))
          (should (member ad-hoc-name candidates))
          (should (= (cl-count "alpha" candidates :test #'equal) 1))
          (should-not (member matching-name candidates))
          (should (member collision-name candidates))
          (should (eq (current-buffer) ad-hoc))
          (should (eq clutch-connection ad-hoc-conn)))
      (dolist (buffer (list ad-hoc matching collision))
        (when (buffer-live-p buffer)
          (kill-buffer buffer)))
      (dolist (conn (list ad-hoc-conn matching-conn collision-conn))
        (when (clutch-db-live-p conn) (clutch-db-disconnect conn)))
      (delete-directory dir t))))

(ert-deftest clutch-test-query-console-picker-returns-to-a-moved-ad-hoc-console ()
  "Picking an ad hoc console that followed a namespace switch should return there.
The picker named it by the parameters that followed the switch, so it
opened a second console on a second connection, and returning to it
replaced the parameters `C-c C-e' connects with by those."
  (require 'clutch-db-sqlite)
  (let* ((dir (make-temp-file "clutch-console-" t))
         (clutch-console-directory (expand-file-name "consoles" dir))
         (opened-with (list :backend 'sqlite
                            :database (expand-file-name "app.db" dir)))
         (moved (list :backend 'sqlite
                      :database (expand-file-name "other.db" dir)))
         (name (clutch--ad-hoc-console-name opened-with))
         (console (generate-new-buffer " *clutch-moved-ad-hoc*"))
         (console-name (buffer-name console))
         (conn (clutch-db-sqlite-connect opened-with))
         (clutch-connection-alist nil))
    (unwind-protect
        (progn
          (with-current-buffer console
            (clutch-mode)
            (setq-local clutch--console-name name
                        clutch--console-storage-name
                        (clutch--console-persistence-name name opened-with)
                        clutch--console-ad-hoc-params opened-with
                        clutch--connection-params moved
                        clutch-connection conn))
          (cl-letf (((symbol-function 'completing-read)
                     (lambda (&rest _args) console-name)))
            (call-interactively #'clutch-query-console))
          (should (eq (current-buffer) console))
          (should (eq clutch-connection conn))
          (should (equal clutch--console-ad-hoc-params opened-with))
          (should (equal clutch--connection-params moved)))
      (when (buffer-live-p console)
        (kill-buffer console))
      (when (clutch-db-live-p conn) (clutch-db-disconnect conn))
      (delete-directory dir t))))

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
             (clutch-connection-alist
              `((,name . ,(plist-get case :connection))))
             (clutch-tramp-context-policy (plist-get case :policy))
             (default-directory (plist-get case :source))
             built
             asked
             opened)
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
                (setq opened (current-buffer))
                (should (equal built (plist-get case :expected)))
                (should (equal clutch--connection-params built))))
          (if (plist-get case :prompt-match)
              (should asked)
            (should-not asked))
          (when (buffer-live-p opened)
            (kill-buffer opened)))))))

(ert-deftest clutch-test-connect-in-a-console-moves-its-session ()
  "`C-c C-e' in a console should move its whole session to a new connection.
It bound only the console: over a lost session its results reconnected
to sessions of their own, and over a live one they were left with no
connection.  The new connection started in the mode the entry starts in,
and uncommitted work lost with the old one went unreported."
  (pcase-dolist (`(,label ,old-live ,tx-state ,expected-message)
                 '(("live" t nil "Connected to test-conn")
                   ("lost with uncommitted work" nil dirty
                    "Connected to test-conn; uncommitted changes were lost")))
    (ert-info (label)
      (let ((result (generate-new-buffer " *clutch-connect-result*"))
            (clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
            (clutch--tx-state-cache (make-hash-table :test 'eq))
            (old-conn (make-clutch-test-conn :commit-mode 'manual :live old-live))
            (new-conn (make-clutch-test-conn :commit-mode 'auto))
            built messages)
        (unwind-protect
            (with-temp-buffer
              (clutch-mode)
              (setq-local clutch--console-name "alpha"
                          clutch--session-target
                          (clutch--connection-target
                           (clutch-prepare-connection-params
                            (clutch--saved-connection-params "alpha")))
                          clutch-connection old-conn)
              (with-current-buffer result
                (clutch-result-mode)
                (setq-local clutch-connection old-conn
                            clutch--dml-result t))
              (when tx-state
                (puthash old-conn tx-state clutch--tx-state-cache))
              ;; The test backend has no schema or connection key to render.
              (cl-letf (((symbol-function 'clutch--build-conn)
                         (lambda (params) (setq built params) new-conn))
                        ((symbol-function 'clutch--connection-key)
                         (lambda (_conn) "test-conn"))
                        ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                        ((symbol-function 'message)
                         (lambda (format-string &rest args)
                           (push (apply #'format format-string args) messages))))
                (clutch-test--without-credentials
                  (clutch-connect)))
              (should (equal built '(:backend mysql :database "app_a"
                                     :pass-entry "alpha")))
              (should (eq clutch-connection new-conn))
              (should (eq (buffer-local-value 'clutch-connection result) new-conn))
              (should (eq (clutch-test-conn-commit-mode new-conn) 'manual))
              (should-not (clutch-db-live-p old-conn))
              (let ((banner (buffer-local-value 'header-line-format result)))
                (should (eq (and tx-state t)
                            (and (stringp banner)
                                 (string-match-p "rolled back" banner)
                                 t))))
              (should-not (clutch--tx-state old-conn))
              (should (member expected-message messages)))
          (kill-buffer result))))))

(ert-deftest clutch-test-connect-in-a-console-whose-entry-moved-connects-it-alone ()
  "`C-c C-e' should not move a console's session to another target.
The console's saved entry led elsewhere since its session connected, and
moving the session took its results there, so an edit staged on one
server was written to another.  Once connected there, the console's next
`C-c C-e' moves its session again."
  (pcase-dolist (`(,label ,before ,after)
                 '(("host"
                    (:backend mysql :host "db1" :database "app")
                    (:backend mysql :host "db2" :database "app"))
                   ("database property"
                    (:backend mongodb :surface sql-interface :host "db1"
                     :user "u" :password "pw" :props (("database" . "app")))
                    (:backend mongodb :surface sql-interface :host "db1"
                     :user "u" :password "pw" :props (("database" . "other"))))))
    (ert-info (label)
      (let ((result (generate-new-buffer " *clutch-connect-result*"))
            (clutch-connection-alist (list (cons "alpha" before)))
            built replaced)
        (unwind-protect
            (with-temp-buffer
              (clutch-mode)
              (setq-local clutch--console-name "alpha"
                          clutch--session-target
                          (clutch--connection-target
                           (clutch-prepare-connection-params
                            (clutch--saved-connection-params "alpha")))
                          clutch-connection 'old-conn)
              (with-current-buffer result
                (setq-local clutch-connection 'old-conn))
              (setq clutch-connection-alist (list (cons "alpha" after)))
              (clutch-test--with-connect-build-stubs (built 'mysql 'new-conn)
                (cl-letf (((symbol-function 'clutch--connection-alive-p)
                           (lambda (conn) (memq conn '(old-conn new-conn))))
                          ((symbol-function 'clutch--confirm-session-close) #'ignore)
                          ((symbol-function 'clutch-db-disconnect) #'ignore)
                          ((symbol-function 'clutch--release-connection-transport)
                           #'ignore)
                          ((symbol-function 'clutch--refresh-connection-render-state)
                           #'ignore)
                          ((symbol-function 'clutch--replace-connection)
                           (lambda (&rest _) (setq replaced t))))
                  (clutch-connect)
                  (should-not replaced)
                  (should (equal (plist-get built :host) (plist-get after :host)))
                  (should (equal (plist-get built :props) (plist-get after :props)))
                  (should (eq clutch-connection 'new-conn))
                  (should-not (buffer-local-value 'clutch-connection result))
                  (clutch-connect)
                  (should replaced))))
          (kill-buffer result))))))

(ert-deftest clutch-test-connect-in-a-console-moves-its-session-when-its-entry-still-leads-there ()
  "`C-c C-e' should move a console's session when its entry still leads there.
A new password or password entry in the saved entry leads to the same
server, so the session and its results move as they do otherwise.  A
SQLite file named with `~' was compared as written when the console
connected and resolved by `C-c C-e', so the session never moved."
  (pcase-dolist (`(,label ,opened-with ,connects-with)
                 '(("credentials rotated"
                    (:backend sqlite :database "~/app.db")
                    (:backend sqlite :database "~/app.db"
                     :password "rotated" :pass-entry "vault/alpha"))
                   ("file named with ~"
                    (:backend sqlite :database "~/clutch-test-lite.db")
                    (:backend sqlite :database "~/clutch-test-lite.db"))))
    (ert-info (label)
      (let* ((home (make-temp-file "clutch-home-" t))
             (process-environment (cons (format "HOME=%s" home) process-environment))
             (abbreviated-home-dir nil)
             (clutch-console-directory (expand-file-name "consoles" home))
             (clutch-connection-alist (list (cons "alpha" opened-with)))
             (result (generate-new-buffer " *clutch-connect-result*"))
             old new opened)
        (unwind-protect
            (clutch-test--without-credentials
              (clutch-query-console "alpha")
              (setq opened (current-buffer)
                    old clutch-connection)
              (with-current-buffer result
                (setq-local clutch-connection old))
              (setq clutch-connection-alist (list (cons "alpha" connects-with)))
              (clutch-connect)
              (setq new clutch-connection)
              (should-not (eq new old))
              (should-not (clutch-db-live-p old))
              (should (clutch-db-live-p new))
              (should (eq (buffer-local-value 'clutch-connection result) new)))
          (when (buffer-live-p opened)
            (kill-buffer opened))
          (kill-buffer result)
          (dolist (conn (list old new))
            (when (and conn (clutch-db-live-p conn)) (clutch-db-disconnect conn)))
          (delete-directory home t))))))

(ert-deftest clutch-test-query-console-reuses-an-open-connected-console ()
  "Query console should reuse an open console buffer whose connection is live.
It is found by its alias and legacy params, and by its storage identity
once the saved connection was renamed, which then renames the console."
  (require 'clutch-db-sqlite)
  (pcase-dolist (`(,label ,alias ,by-storage)
                 '(("found by its alias and legacy params" "alpha" nil)
                   ("found by its storage identity after a rename" "beta" t)))
    (ert-info (label)
      (clutch-test--with-isolated-metadata-caches
        (let* ((dir (make-temp-file "clutch-console-" t))
               (clutch-console-directory (expand-file-name "consoles" dir))
               (params (list :backend 'sqlite
                             :database (expand-file-name "prod.db" dir)))
               (storage-name (clutch--console-persistence-name "alpha" params))
               (conn (clutch-db-sqlite-connect params))
               (existing (generate-new-buffer " *clutch-query-console-existing*"))
               (clutch-connection-alist `((,alias . ,params))))
          (unwind-protect
              (progn
                (with-current-buffer existing
                  (clutch-mode)
                  (setq-local clutch--console-name "alpha"
                              clutch-connection conn)
                  (if by-storage
                      (setq-local clutch--console-storage-name storage-name)
                    (setq-local clutch--connection-params params)))
                (clutch-query-console alias)
                (should (eq (current-buffer) existing))
                (should (eq clutch-connection conn))
                (should (equal clutch--console-name alias))
                (should (equal clutch--console-storage-name storage-name))
                (should (equal (buffer-name existing) (format "*clutch: %s*" alias)))
                (should (local-variable-p 'sql-product))
                (should (eq sql-product 'sqlite))
                (should (equal mode-name "clutch")))
            (when (buffer-live-p existing)
              (kill-buffer existing))
            (when (clutch-db-live-p conn) (clutch-db-disconnect conn))
            (delete-directory dir t)))))))

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
  (require 'clutch-db-sqlite)
  (let* ((dir (make-temp-file "clutch-console-" t))
         (clutch-console-directory (expand-file-name "consoles" dir))
         (name "same-name-identity")
         (old-params (list :backend 'sqlite
                           :database (expand-file-name "old.db" dir)))
         (new-params (list :backend 'sqlite
                           :database (expand-file-name "new.db" dir)))
         (old-conn (clutch-db-sqlite-connect old-params))
         (old-buffer (generate-new-buffer " *clutch-old-console*"))
         new opened)
    (unwind-protect
        (progn
          (with-current-buffer old-buffer
            (clutch-mode)
            (setq-local clutch--console-name name
                        clutch--console-storage-name
                        (clutch--console-persistence-name name old-params)
                        clutch--connection-params old-params
                        clutch-connection old-conn))
          ;; The console is renamed once connected; the name it was created
          ;; with is the one global font lock saw during mode initialization.
          (cl-letf (((symbol-function 'clutch--update-console-buffer-name)
                     #'ignore))
            (clutch--open-query-console name new-params new-params))
          (setq opened (current-buffer)
                new clutch-connection)
          (should-not (eq opened old-buffer))
          ;; Global font lock skips buffers hidden during mode initialization.
          (should-not (string-prefix-p " " (buffer-name opened)))
          (should (clutch-db-live-p new))
          (should (equal (clutch-db-database new) (plist-get new-params :database)))
          (with-current-buffer old-buffer
            (should (eq clutch-connection old-conn))
            (should (equal clutch--connection-params old-params))))
      (dolist (buffer (delete-dups (list opened old-buffer)))
        (when (buffer-live-p buffer)
          (kill-buffer buffer)))
      (dolist (conn (list old-conn new))
        (when (and conn (clutch-db-live-p conn)) (clutch-db-disconnect conn)))
      (delete-directory dir t))))

(ert-deftest clutch-test-query-console-reopens-a-dead-console-in-place ()
  "Reopening a dead console should reconnect it as the automatic reconnect does.
It built a connection from the saved entry and bound only the console, so
its results stayed on the dead connection and reconnected to sessions of
their own, its uncommitted work was not reported lost, and the database it
had moved to gave way to the saved one.  Once reconnected, it was also put
in the mode of the saved entry, which a changed `:surface' makes another,
and the mode change dropped the new connection from the console."
  (require 'clutch-db-sqlite)
  (dolist (mode '(clutch-mode clutch-mongodb-mode))
    (ert-info ((format "console mode: %s" mode))
      (let* ((dir (make-temp-file "clutch-console-" t))
             (clutch-console-directory (expand-file-name "consoles" dir))
             (session-file (expand-file-name "used_since.db" dir))
             (session (list :backend 'sqlite :database session-file :pass-entry "alpha"))
             (clutch-connection-alist
              `(("alpha" . (:backend sqlite
                            :database ,(expand-file-name "app_a.db" dir)))))
             (clutch--tx-state-cache (make-hash-table :test 'eq))
             (existing (generate-new-buffer " *clutch-query-console-dead*"))
             (attached (generate-new-buffer " *clutch-query-console-dead-result*"))
             (dead (clutch-db-sqlite-connect session))
             new messages)
        (unwind-protect
            (progn
              (clutch-db-disconnect dead)
              (puthash dead 'dirty clutch--tx-state-cache)
              (with-current-buffer attached
                (setq-local clutch-connection dead))
              (with-current-buffer existing
                (funcall mode)
                (setq-local clutch--console-name "alpha"
                            clutch--console-storage-name
                            (clutch--console-persistence-name
                             "alpha" (clutch--saved-connection-params "alpha"))
                            clutch-connection dead
                            clutch--connection-params session
                            clutch--conn-sql-product 'postgres))
              (cl-letf (((symbol-function 'message)
                         (lambda (format-string &rest args)
                           (push (apply #'format format-string args) messages))))
                (clutch-test--without-credentials
                  (clutch-query-console "alpha")))
              (setq new clutch-connection)
              (should (eq (current-buffer) existing))
              (should (eq major-mode mode))
              (should (clutch-db-live-p new))
              (should (equal (clutch-db-database new) session-file))
              (dolist (buffer (list existing attached))
                (should (eq (buffer-local-value 'clutch-connection buffer) new))
                (should (equal (buffer-local-value 'clutch--connection-params buffer)
                               session)))
              (should (eq (buffer-local-value 'clutch--conn-sql-product existing)
                          'postgres))
              (should-not (clutch--tx-state dead))
              (should (member (format "Reconnected to sqlite:%s; uncommitted changes were lost"
                                      session-file)
                              messages)))
          (dolist (buffer (list existing attached))
            (when (buffer-live-p buffer)
              (kill-buffer buffer)))
          (when (and new (clutch-db-live-p new)) (clutch-db-disconnect new))
          (delete-directory dir t))))))

(ert-deftest clutch-test-query-console-persistence-file-precedence ()
  "Query console should read identity files before legacy alias files."
  (dolist (case '(("legacy fallback" nil "SELECT legacy;")
                  ("identity wins" "SELECT stable;" "SELECT stable;")))
    (pcase-let ((`(,label ,identity-content ,expected) case))
      (ert-info ((format "case: %s" label))
        (let* ((dir (make-temp-file "clutch-console-" t))
               (clutch-console-directory (expand-file-name "consoles" dir))
               (params (list :backend 'sqlite
                             :database (expand-file-name "prod.db" dir)))
               (storage-name (clutch--console-persistence-name "alpha" params))
               (clutch-connection-alist `(("alpha" . ,params)))
               conn opened)
          (unwind-protect
              (progn
                (make-directory clutch-console-directory t)
                (with-temp-file (clutch--console-file "alpha")
                  (insert "SELECT legacy;"))
                (when identity-content
                  (with-temp-file (clutch--console-file storage-name)
                    (insert identity-content)))
                (clutch-test--without-credentials
                  (clutch-query-console "alpha"))
                (setq opened (current-buffer)
                      conn clutch-connection)
                (should (clutch-db-live-p conn))
                (should (equal (buffer-string) expected))
                (should-not (equal clutch--console-storage-name "alpha")))
            (when (buffer-live-p opened)
              (kill-buffer opened))
            (when (and conn (clutch-db-live-p conn)) (clutch-db-disconnect conn))
            (delete-directory dir t)))))))

(ert-deftest clutch-test-connect-outside-a-console-follows-the-picked-target ()
  "`C-c C-e' outside a console should move a session picked again.
Picking the connection the session was on started a new session in the
mode that connection starts in and left the buffer's results without a
connection.  Picking another connects the buffer alone."
  (let* ((dir (make-temp-file "clutch-console-" t))
         (clutch-connection-alist
          `(("alpha" . (:backend sqlite :database ,(expand-file-name "db1.db" dir)))
            ("beta" . (:backend sqlite :database ,(expand-file-name "db2.db" dir)))))
         (result (generate-new-buffer " *clutch-connect-result*"))
         conns)
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (clutch-test--without-credentials
            (clutch-test--with-minibuffer-answers '("alpha")
              (clutch-connect))
            (push clutch-connection conns)
            (with-current-buffer result
              (setq-local clutch-connection (car conns)))
            ;; Picking the session's own connection moves the session.
            (clutch-test--with-minibuffer-answers '("alpha")
              (clutch-connect))
            (push clutch-connection conns)
            (should-not (eq (nth 0 conns) (nth 1 conns)))
            (should-not (clutch-db-live-p (nth 1 conns)))
            (should (eq (buffer-local-value 'clutch-connection result) (nth 0 conns)))
            ;; Picking another connects the buffer alone.
            (clutch-test--with-minibuffer-answers '("beta")
              (clutch-connect))
            (push clutch-connection conns)
            (should-not (eq (nth 0 conns) (nth 1 conns)))
            (should-not (clutch-db-live-p (nth 1 conns)))
            (should-not (buffer-local-value 'clutch-connection result))
            (should (equal (clutch-db-database (nth 0 conns))
                           (expand-file-name "db2.db" dir)))
            (should (equal clutch--session-target
                           (clutch--connection-target
                            (clutch-prepare-connection-params
                             (clutch--saved-connection-params "beta")))))))
      (kill-buffer result)
      (dolist (conn conns)
        (when (clutch-db-live-p conn) (clutch-db-disconnect conn)))
      (delete-directory dir t))))

(ert-deftest clutch-test-connect-in-an-indirect-edit-ends-only-its-own-session ()
  "`C-c C-e' in an indirect edit should leave its console's session and end its own.
Connecting the edit elsewhere asked to close and then disconnected the
console's session, which the edit only borrows.  Connecting the edit
elsewhere a second time left the session it had connected open and asked
nothing about its uncommitted work, as if that session were still its
console's."
  (require 'clutch-db-sqlite)
  (let* ((dir (make-temp-file "clutch-console-" t))
         (clutch-connection-alist
          `(("alpha" . (:backend sqlite :database ,(expand-file-name "db1.db" dir)))
            ("beta" . (:backend sqlite :database ,(expand-file-name "db2.db" dir)))
            ("gamma" . (:backend sqlite :database ,(expand-file-name "db3.db" dir)))))
         (clutch--tx-state-cache (make-hash-table :test 'eq))
         (console (generate-new-buffer " *clutch-test-console*"))
         (console-conn (clutch-db-sqlite-connect
                        (clutch--saved-connection-params "alpha")))
         (target (clutch--connection-target
                  (clutch-prepare-connection-params
                   (clutch--saved-connection-params "alpha"))))
         beta-conn gamma-conn prompts)
    (unwind-protect
        (progn
          (with-current-buffer console
            (setq-local clutch-connection console-conn
                        clutch--session-target target))
          ;; Closing the console's session would ask about this work.
          (puthash console-conn 'dirty clutch--tx-state-cache)
          (with-temp-buffer
            (clutch-mode)
            (clutch--indirect-mode 1)
            (setq-local clutch-connection console-conn
                        clutch--session-target target)
            (clutch-test--without-credentials
              (cl-letf (((symbol-function 'yes-or-no-p)
                         (lambda (_prompt)
                           (ert-fail "the console session must not be closed"))))
                (clutch-test--with-minibuffer-answers '("beta")
                  (clutch-connect)))
              (setq beta-conn clutch-connection)
              (should (equal (clutch-db-database beta-conn)
                             (expand-file-name "db2.db" dir)))
              (should (clutch-db-live-p console-conn))
              (should (eq (buffer-local-value 'clutch-connection console)
                          console-conn))
              (should (clutch--tx-dirty-p console-conn))
              ;; The session the edit connected is its own.
              (puthash beta-conn 'dirty clutch--tx-state-cache)
              (cl-letf (((symbol-function 'yes-or-no-p)
                         (lambda (prompt) (push prompt prompts) t)))
                (clutch-test--with-minibuffer-answers '("gamma")
                  (clutch-connect)))
              (setq gamma-conn clutch-connection)
              (should (equal (clutch-db-database gamma-conn)
                             (expand-file-name "db3.db" dir)))
              (should-not (clutch-db-live-p beta-conn))
              (should (= (length prompts) 1))
              (should (string-match-p "Uncommitted changes will be lost" (car prompts)))
              (should (clutch-db-live-p console-conn)))))
      (kill-buffer console)
      (dolist (conn (list console-conn beta-conn gamma-conn))
        (when (and conn (clutch-db-live-p conn)) (clutch-db-disconnect conn)))
      (delete-directory dir t))))

(ert-deftest clutch-test-connect-over-dead-connection-releases-only-its-transport ()
  "Connecting over a dead connection should release its transport only.
Other buffers keep the dead connection so their next command can reconnect
them in place."
  (skip-unless (executable-find "sleep"))
  (require 'clutch-db-sqlite)
  (let* ((db-file (make-temp-file "clutch-manual-" nil ".db"))
         (old-conn (clutch-db-sqlite-connect (list :backend 'sqlite :database db-file)))
         (proc (start-process "clutch-test-tunnel" nil "sleep" "60"))
         (other (generate-new-buffer " *clutch-other*"))
         (clutch-connection-alist nil)
         (clutch--connection-transport-cache (make-hash-table :test 'eq))
         new)
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (setq-local clutch-connection old-conn)
          (with-current-buffer other
            (setq-local clutch-connection old-conn))
          (puthash old-conn (list :kind 'ssh :process proc)
                   clutch--connection-transport-cache)
          (clutch-db-disconnect old-conn)
          (clutch-test--with-minibuffer-answers (list "sqlite" db-file)
            (clutch-connect))
          (setq new clutch-connection)
          (should-not (process-live-p proc))
          (should-not (gethash old-conn clutch--connection-transport-cache))
          (should (clutch-db-live-p new))
          (should-not (eq new old-conn))
          (should (eq (buffer-local-value 'clutch-connection other) old-conn)))
      (kill-buffer other)
      (when (process-live-p proc) (delete-process proc))
      (when (and new (clutch-db-live-p new)) (clutch-db-disconnect new))
      (delete-file db-file))))

(ert-deftest clutch-test-connect-keeps-bound-connection-when-schema-priming-quits ()
  "A quit while priming the schema must not disconnect the bound connection."
  (let* ((db-file (make-temp-file "clutch-quit-" nil ".db"))
         (params (list :backend 'sqlite :database db-file))
         (clutch--connection-transport-cache (make-hash-table :test 'eq))
         new)
    (unwind-protect
        (with-temp-buffer
          (setq-local clutch--console-name "SQLite: quit"
                      clutch--console-ad-hoc-params params)
          (cl-letf (((symbol-function 'clutch--prime-schema-cache)
                     (lambda (_conn) (signal 'quit nil))))
            (condition-case nil (clutch-connect) (quit nil)))
          (setq new clutch-connection)
          (should (clutch-db-live-p new))
          (should (gethash new clutch--connection-transport-cache))
          (should (equal clutch--session-target (clutch--connection-target params))))
      (when (and new (clutch-db-live-p new)) (clutch-db-disconnect new))
      (delete-file db-file))))

;;;; Schema and database switching

(ert-deftest clutch-test-switch-schema-updates-session-and-buffer-context ()
  "Schema switching should update backend state and attached buffer params."
  (dolist (case '((oracle
                   (:driver oracle :schema "SALES")
                   ("SALES" "ANALYTICS") "SALES" "ANALYTICS"
                   (:driver oracle :schema "ANALYTICS"))
                  (mysql
                   (:backend mysql :database "sales")
                   ("sales" "analytics") "sales" "analytics"
                   (:backend mysql :database "analytics"))
                  ;; Two schemas whose names differ only in case are two
                  ;; schemas; switching between them was refused as a
                  ;; switch to the current one.
                  (oracle-case-twin
                   (:driver oracle :schema "\"alt_lower\"")
                   ("alt_lower" "ALT_LOWER") "alt_lower" "ALT_LOWER"
                   (:driver oracle :schema "ALT_LOWER"))))
    (pcase-let ((`(,label ,params ,schemas ,current ,selected ,expected) case))
      (ert-info ((format "backend: %s" label))
        (clutch-test--with-isolated-metadata-caches
          (let ((conn (list 'fake-conn label))
                (effective current)
                switched refresh-quiet message-text)
            (puthash conn (make-hash-table :test 'equal) clutch--schema-cache)
            (with-temp-buffer
              (setq-local clutch-connection conn
                          clutch--connection-params (copy-sequence params))
              (cl-letf (((symbol-function 'clutch-db-list-schemas)
                         (lambda (_conn) schemas))
                        ((symbol-function 'clutch-db-current-schema)
                         (lambda (_conn) effective))
                        ((symbol-function 'clutch-db-set-current-schema)
                         (lambda (_conn schema)
                           (setq switched schema
                                 effective schema)
                           schema))
                        ((symbol-function 'clutch-db-update-namespace-params)
                         (lambda (actual-conn _actual-params)
                           (should (eq actual-conn conn))
                           (copy-sequence expected)))
                        ((symbol-function 'clutch--refresh-current-schema)
                         (lambda (&optional quiet)
                           (setq refresh-quiet quiet)
                           t))
                        ((symbol-function 'message)
                         (lambda (fmt &rest args)
                           (setq message-text (apply #'format fmt args)))))
                (clutch-test--with-minibuffer-answers (list selected)
                  (clutch-switch-schema))
                (should (equal switched selected))
                (should (equal clutch--connection-params expected))
                (should-not (gethash conn clutch--schema-cache))
                (should refresh-quiet)
                (should (equal message-text
                               (format "Current schema/database: %s"
                                       selected)))))))))))

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
        (cl-letf (((symbol-function 'clutch-db-list-schemas)
                   (lambda (_conn) '("SALES" "ANALYTICS")))
                  ((symbol-function 'clutch-db-current-schema)
                   (lambda (_conn) "SALES"))
                  ((symbol-function 'clutch-db-set-current-schema)
                   (lambda (_conn _schema)
                     (signal 'clutch-db-error
                             (list "ORA-12592: TNS:bad packet" details)))))
          (let ((error (should-error
                        (clutch-test--with-minibuffer-answers '("ANALYTICS")
                          (clutch-switch-schema))
                        :type 'user-error)))
            (should (string-match-p "ORA-12592" (cadr error)))
            (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                    (cadr error))))
          (should (equal (plist-get clutch--buffer-error-details :summary)
                         "ORA-12592: TNS:bad packet"))
          (should (eq (plist-get clutch--buffer-error-details :backend)
                      'oracle))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "set-current-schema" text))
            (should (string-match-p "Operation: schema-switch" text))
            (should (string-match-p "Phase: error" text))
            (should (string-match-p
                     "ALTER SESSION SET CURRENT_SCHEMA" text))))))))

(ert-deftest clutch-test-switch-schema-clickhouse-replaces-selected-database ()
  "ClickHouse switching should delegate one replacement with selected params."
  (let ((old-conn (make-clutch-test-conn))
        replacement)
    (with-temp-buffer
      (setq-local clutch-connection old-conn
                  clutch--connection-params '(:backend clickhouse
                                              :host "127.0.0.1"
                                              :port 8123
                                              :database "default"
                                              :user "default"))
      (cl-letf (((symbol-function 'clutch-db-list-schemas)
                 (lambda (_conn) '("default" "demo")))
                ((symbol-function 'clutch-db-current-schema)
                 (lambda (_conn) "default"))
                ((symbol-function 'clutch-db-namespace-reconnect-params)
                 (lambda (_conn params namespace)
                   (plist-put (copy-sequence params) :database namespace)))
                ((symbol-function 'clutch--replace-connection)
                 (lambda (conn params product)
                   (setq replacement (list conn params product)))))
        (clutch-test--with-minibuffer-answers '("demo")
          (clutch-switch-schema))
        (should
         (equal replacement
                (list old-conn
                      '(:backend clickhouse
                        :host "127.0.0.1"
                        :port 8123
                        :database "demo"
                        :user "default")
                      nil)))))))

(ert-deftest clutch-test-columns-cache-empty-synchronous-success ()
  "Synchronous completion fallback should retain empty success until refresh."
  (clutch-test--with-isolated-metadata-caches
    (let ((conn 'test-conn)
          (requests 0))
      (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_) t))
                ((symbol-function 'clutch-db-list-columns)
                 (lambda (_conn _table) (cl-incf requests) nil)))
        (clutch--install-schema-cache conn '("empty"))
        (dotimes (_ 3)
          (should-not (clutch--ensure-columns
                       conn (gethash conn clutch--schema-cache) "empty")))
        (should (= requests 1))
        (clutch--install-schema-cache conn '("empty"))
        (clutch--ensure-columns conn (gethash conn clutch--schema-cache) "empty")
        (should (= requests 2))))))

(ert-deftest clutch-test-mongodb-completion-caches-empty-column-success ()
  "Installed CAPF should cache empty success until metadata is refreshed."
  (dolist (columns '(nil ("name")))
    (clutch-test--with-isolated-metadata-caches
      (let* ((conn (make-clutch-mongodb-conn :database "test"))
             (schema (make-hash-table :test 'equal))
             (requests 0)
             callback)
        (puthash "users" nil schema)
        (puthash conn schema clutch--schema-cache)
        (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_) t))
                  ((symbol-function 'clutch-db-busy-p) (lambda (_) nil))
                  ((symbol-function 'clutch-db-list-columns-async)
                   (lambda (_conn _table success &optional _errback)
                     (cl-incf requests)
                     (setq callback success)
                     t))
                  ((symbol-function 'completion-in-region)
                   (lambda (&rest _) t)))
          (with-temp-buffer
            (clutch-mongodb-mode)
            (setq-local clutch-connection conn)
            (insert "db.users.find({na")
            (completion-at-point)
            (completion-at-point)
            (should (= requests 1))
            (funcall callback columns)
            (completion-at-point)
            (should (= requests 1))
            (clutch--install-schema-cache conn '("users"))
            (completion-at-point)
            (should (= requests 2))))))))

(ert-deftest clutch-test-mongodb-completion-ignores-collection-text-in-strings ()
  "Field completion should resolve code, not collection-like data."
  (dolist (expression '("db.users.find({note: \"ordinary\", na"
                        "db.users.find({note: \"db.orders.find(\", na"
                        "db.users.find({note: 1 /* db.orders.find( */, na"
                        "db.users.find({note: 1 // db.orders.find(\n, na"
                        "db.getCollection(\"users\").find({note: \"db.orders.find(\", na"))
    (clutch-test--with-isolated-metadata-caches
      (let ((conn (make-clutch-test-conn))
            (schema (make-hash-table :test 'equal)))
        (puthash "users" '("name" "note") schema)
        (puthash "orders" '("order_id") schema)
        (puthash conn schema clutch--schema-cache)
        (with-temp-buffer
          (clutch-mongodb-mode)
          (setq-local clutch-connection conn)
          (insert expression)
          (should (equal (clutch-test--completion-candidates
                          (clutch-mongodb-completion-at-point))
                         '("name"))))))))

(provide 'clutch-test-connection)

;;; clutch-test-connection.el ends here
