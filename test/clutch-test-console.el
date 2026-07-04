;;; clutch-test-console.el --- Console and REPL ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Query console persistence, yank cleanup, and REPL tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common))

;;;; Console test helpers

(defmacro clutch-test-console--with-temp-directory (var &rest body)
  "Run BODY with VAR bound to a temporary console directory."
  (declare (indent 1) (debug (symbolp body)))
  `(let ((,var (make-temp-file "clutch-console-" t)))
     (unwind-protect
         (progn ,@body)
       (delete-directory ,var t))))

;;;; Console — yank cleanup and save

(ert-deftest clutch-test-console-yank-cleanup-strips-trailing-whitespace ()
  "Yank into a query console should clean trailing whitespace in pasted region."
  (let ((clutch-console-yank-cleanup t))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "test")
      (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
      (insert "existing SQL;\n")
      (let ((start (point)))
        (insert "SELECT *   \nFROM t   \n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-substring start (point-max))
                       "SELECT *\nFROM t\n"))))))

(ert-deftest clutch-test-console-yank-cleanup-does-not-touch-existing-text ()
  "Yank cleanup should not modify text outside the pasted region."
  (let ((clutch-console-yank-cleanup t))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "test")
      (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
      (insert "SELECT 1;   \n")
      (let ((pre-text (buffer-string))
            (start (point)))
        (insert "SELECT 2;\n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-substring 1 (1+ (length pre-text)))
                       pre-text))))))

(ert-deftest clutch-test-console-yank-cleanup-respects-defcustom ()
  "Yank cleanup should be a no-op when `clutch-console-yank-cleanup' is nil."
  (let ((clutch-console-yank-cleanup nil))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "test")
      (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
      (let ((start (point)))
        (insert "SELECT *   \n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-string) "SELECT *   \n"))))))

(ert-deftest clutch-test-console-yank-cleanup-skips-non-console-buffers ()
  "Yank cleanup should not run in non-console clutch buffers."
  (let ((clutch-console-yank-cleanup t))
    (with-temp-buffer
      (clutch-mode)
      ;; clutch--console-name is nil — not a console
      (let ((start (point)))
        (insert "SELECT *   \n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-string) "SELECT *   \n"))))))

(ert-deftest clutch-test-save-console-reports-write-error ()
  "Console persistence failures should surface a minibuffer warning."
  (let (reported)
    (with-temp-buffer
      (setq-local clutch--console-name "demo")
      (cl-letf (((symbol-function 'make-directory) #'ignore)
                ((symbol-function 'write-region)
                 (lambda (&rest _args)
                   (error "Disk full")))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq reported (apply #'format fmt args)))))
        (clutch--save-console)
        (should (string-match-p
                 "Failed to save console demo: disk full"
                 reported))))))

(ert-deftest clutch-test-console-persistence-name-identity-rules ()
  "Console persistence names should be stable, distinct, and credential-safe."
  (let ((params '(:backend mysql
                  :host "db.internal"
                  :port 3306
                  :user "app"
                  :database "prod")))
    (should (equal (clutch--console-persistence-name "prod" nil)
                   "prod"))
    (should (equal (clutch--console-persistence-name "old-name" params)
                   (clutch--console-persistence-name "new-name" params)))
    (let ((name (clutch--console-persistence-name "old-name" params)))
      (should (string-prefix-p "console-" name))
      (should-not (string-match-p "db\\.internal\\|app\\|prod" name))))
  (dolist (case '((endpoint
                   "prod"
                   (:backend mysql :host "db-a" :port 3306 :database "app")
                   "prod"
                   (:backend mysql :host "db-b" :port 3306 :database "app")
                   different)
                  (oracle-sid
                   "oracle-a"
                   (:backend oracle :host "db" :port 1521 :sid "SID1")
                   "oracle-b"
                   (:backend oracle :host "db" :port 1521 :sid "SID2")
                   different)
                  (url-password
                   "prod"
                   (:backend mysql
                    :url "jdbc:mysql://db/prod?user=app&Password=secret;pwd=other")
                   "prod"
                   (:backend mysql
                    :url "jdbc:mysql://db/prod?user=app&Password=changed;pwd=changed")
                   same)
                  (non-identity-params
                   "a"
                   (:backend mysql :host "db" :port 3306 :user "app"
                    :password "one" :pass-entry "old" :display-name "Old"
                    :connect-timeout 1 :read-idle-timeout 2)
                   "b"
                   (:backend mysql :host "db" :port 3306 :user "app"
                    :password "two" :pass-entry "new" :display-name "New"
                    :connect-timeout 9 :read-idle-timeout 8)
                   same)))
    (pcase-let ((`(,label ,name-a ,params-a ,name-b ,params-b ,relation)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((actual-a (clutch--console-persistence-name name-a params-a))
              (actual-b (clutch--console-persistence-name name-b params-b)))
          (pcase relation
            ('same (should (equal actual-a actual-b)))
            ('different (should-not (equal actual-a actual-b))))
          (should-not
           (string-match-p
            "secret\\|other\\|changed\\|Password\\|pwd\\|db/prod"
            actual-a)))))))

(ert-deftest clutch-test-save-console-writes-buffer-to-file ()
  "Saving a named console should write its current contents to disk."
  (clutch-test-console--with-temp-directory dir
    (with-temp-buffer
      (let ((clutch-console-directory dir))
        (setq-local clutch--console-name "test-db")
        (insert "SELECT 1;\nSELECT 2;")
        (clutch--save-console)
        (let ((path (expand-file-name "test-db.sql" dir)))
          (should (file-exists-p path))
          (with-temp-buffer
            (insert-file-contents path)
            (should (equal (buffer-string) "SELECT 1;\nSELECT 2;"))))))))

(ert-deftest clutch-test-save-console-uses-storage-name-when-present ()
  "Saving a console should use its stable storage name when configured."
  (clutch-test-console--with-temp-directory dir
    (with-temp-buffer
      (let ((clutch-console-directory dir))
        (setq-local clutch--console-name "prod-renamed")
        (setq-local clutch--console-storage-name "prod-stable")
        (insert "SELECT stable;")
        (clutch--save-console)
        (should (file-exists-p (expand-file-name "prod-stable.sql" dir)))
        (should-not (file-exists-p (expand-file-name "prod-renamed.sql" dir)))))))

(ert-deftest clutch-test-save-console-skips-unnamed-buffer ()
  "Saving should be a no-op when the console buffer has no name."
  (clutch-test-console--with-temp-directory parent
    (let ((dir (expand-file-name "missing" parent)))
      (with-temp-buffer
        (let ((clutch-console-directory dir))
          (insert "SELECT 1;")
          (clutch--save-console)
          (should-not (file-exists-p dir)))))))

(ert-deftest clutch-test-save-console-creates-directory-if-missing ()
  "Saving a named console should create its directory when needed."
  (clutch-test-console--with-temp-directory parent
    (let ((dir (expand-file-name "nested" parent)))
      (with-temp-buffer
        (let ((clutch-console-directory dir))
          (setq-local clutch--console-name "test")
          (insert "SELECT 42;")
          (clutch--save-console)
          (should (file-directory-p dir))
          (should (file-exists-p (expand-file-name "test.sql" dir))))))))

;;;; REPL

(defun clutch-test-console--face-at-match (regexp text)
  "Return the face at the first match for REGEXP in TEXT."
  (should (string-match regexp text))
  (get-text-property (match-beginning 0) 'face text))

(defun clutch-test-console--buffer-face-at-match (regexp)
  "Return the display face at the first buffer match for REGEXP."
  (save-excursion
    (goto-char (point-min))
    (should (search-forward-regexp regexp nil t))
    (or (get-text-property (match-beginning 0) 'face)
        (get-text-property (match-beginning 0) 'font-lock-face))))

(ert-deftest clutch-test-repl-output-keeps-faces-after-font-lock ()
  "REPL output should remain styled after comint font-lock runs."
  (with-temp-buffer
    (let ((proc (start-process "clutch-test-repl" (current-buffer) "cat")))
      (unwind-protect
          (progn
            (set-process-query-on-exit-flag proc nil)
            (clutch-repl-mode)
            (clutch-repl--output
             (concat (clutch-repl--prompt)
                     (clutch--message-count 1)
                     " row shown in "
                     (clutch--message-ident "result buffer")))
            (font-lock-ensure)
            (should (eq (clutch-test-console--buffer-face-at-match "db>")
                        'minibuffer-prompt))
            (should (eq (clutch-test-console--buffer-face-at-match "1")
                        'font-lock-constant-face))
            (should (eq (clutch-test-console--buffer-face-at-match
                         "result buffer")
                        'clutch-field-name-face)))
        (delete-process proc)))))

(ert-deftest clutch-test-repl-output-recreates-missing-process ()
  "REPL output should recover when the dummy comint process is missing."
  (with-temp-buffer
    (unwind-protect
        (progn
          (clutch-repl-mode)
          (clutch-repl--output (clutch-repl--prompt))
          (should (process-live-p (get-buffer-process (current-buffer))))
          (should (equal (substring-no-properties (buffer-string)) "db> ")))
      (when-let* ((proc (get-buffer-process (current-buffer))))
        (delete-process proc)))))

(ert-deftest clutch-test-repl-send-input-recreates-missing-process ()
  "RET in a REPL buffer should not fail when comint has no process."
  (with-temp-buffer
    (unwind-protect
        (let (sent)
          (clutch-repl-mode)
          (insert (clutch-repl--prompt) "SELECT 1;")
          (goto-char (point-max))
          (cl-letf (((symbol-function 'clutch-repl--execute-and-print)
                     (lambda (sql) (setq sent sql))))
            (clutch-repl-send-input))
          (should (process-live-p (get-buffer-process (current-buffer))))
          (should (equal sent "SELECT 1;")))
      (when-let* ((proc (get-buffer-process (current-buffer))))
        (delete-process proc)))))

(ert-deftest clutch-test-repl-input-sender-accumulates-until-semicolon ()
  "REPL input sender should accumulate partial SQL and show continuation prompt."
  (with-temp-buffer
    (let ((clutch-repl--pending-input "")
          output)
      (cl-letf (((symbol-function 'clutch-repl--output)
                 (lambda (text) (push text output)))
                ((symbol-function 'clutch-repl--execute-and-print)
                 (lambda (_sql) (error "Should not execute"))))
        (clutch-repl--input-sender nil "SELECT 1")
        (should (equal clutch-repl--pending-input "SELECT 1"))
        (should (equal (substring-no-properties (car output)) "    -> "))
        (should (eq (clutch-test-console--face-at-match "    -> " (car output))
                    'minibuffer-prompt))))))

(ert-deftest clutch-test-repl-input-sender-executes-on-semicolon ()
  "REPL input sender should execute when statement ends with semicolon."
  (with-temp-buffer
    (let ((clutch-repl--pending-input "SELECT")
          sent)
      (cl-letf (((symbol-function 'clutch-repl--execute-and-print)
                 (lambda (sql) (setq sent sql)))
                ((symbol-function 'clutch-repl--output)
                 (lambda (_text) (error "Should not output continuation"))))
        (clutch-repl--input-sender nil " 1;")
        (should (equal sent "SELECT\n 1;"))
        (should (equal clutch-repl--pending-input ""))))))

(ert-deftest clutch-test-repl-execute-and-print-not-connected ()
  "REPL should print not-connected message when no live connection."
  (with-temp-buffer
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (user-error "Not connected.  Use C-c C-e to connect")))
                ((symbol-function 'clutch-repl--output)
                 (lambda (text) (setq captured text))))
        (clutch-repl--execute-and-print "SELECT 1")
        (should (string-match-p "Not connected" captured))
        (should (string-match-p "db> $" captured))
        (should (eq (clutch-test-console--face-at-match "ERROR" captured)
                    'error))
        (should (eq (clutch-test-console--face-at-match "db>" captured)
                    'minibuffer-prompt))))))

(ert-deftest clutch-test-repl-execute-and-print-ensures-connection ()
  "REPL should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured
          captured-conn
          output)
      (setq-local clutch-connection 'stale-conn)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :affected-rows 3)))
                ((symbol-function 'clutch-repl--output)
                 (lambda (text) (setq output text))))
        (clutch-repl--execute-and-print "UPDATE t SET x = 1;")
        (should ensured)
        (should (eq captured-conn 'new-conn))
        (should (string-match-p "Affected rows" output))
        (should (eq (clutch-test-console--face-at-match "Affected rows" output)
                    'font-lock-keyword-face))
        (should (eq (clutch-test-console--face-at-match "3" output)
                    'font-lock-constant-face))
        (should (eq (clutch-test-console--face-at-match "db>" output)
                    'minibuffer-prompt))))))

(ert-deftest clutch-test-repl-execute-and-print-select-result ()
  "REPL should show SELECT rows in a result buffer and print a summary."
  (with-temp-buffer
    (let ((clutch-connection 'fake-conn)
          (repl-buffer (current-buffer))
          (result-buffer-name " *clutch-test-result*")
          displayed
          output)
      (unwind-protect
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch-db-query)
                     (lambda (_conn _sql)
                       (make-clutch-db-result
                        :columns '((:name "id"))
                        :rows '((1)))))
                    ((symbol-function 'clutch--render-static-table)
                     (lambda (&rest _)
                       (ert-fail "REPL SELECT should use result buffers")))
                    ((symbol-function 'clutch-result--display)
                     (lambda (result sql elapsed)
                       (setq displayed (list result sql elapsed))
                       (set-buffer (get-buffer-create result-buffer-name))
                       'result-buffer))
                    ((symbol-function 'clutch-repl--output)
                     (lambda (text)
                       (should (eq (current-buffer) repl-buffer))
                       (setq output text))))
            (clutch-repl--execute-and-print "SELECT 1;")
            (should displayed)
            (should (equal (nth 1 displayed) "SELECT 1;"))
            (should (string-match-p "1 row" output))
            (should (string-match-p "result buffer" output))
            (should (string-match-p "db> $" output))
            (should (eq (clutch-test-console--face-at-match "1" output)
                        'font-lock-constant-face))
            (should (eq (clutch-test-console--face-at-match "result buffer" output)
                        'clutch-field-name-face))
            (should (eq (clutch-test-console--face-at-match "db>" output)
                        'minibuffer-prompt)))
        (when-let* ((buf (get-buffer result-buffer-name)))
          (kill-buffer buf))))))


(provide 'clutch-test-console)

;;; clutch-test-console.el ends here
